import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .helpers import parse_atom, parse_goal, split_top_level, strip_comment, strip_quotes
from .models import Program, ProvenanceQuestion, Rule
from .postgres_backend import PostgresBackend
from .sql_rule_builder import SQLRuleBuilder


class InputParser:
    """Parse user input into Program."""

    _ENV_KEYS = ("WHY_NOT_DB_CONNECTION", "DATABASE_URL", "POSTGRES_CONNECTION_STRING")
    _DEFAULT_MAX_WORKERS = min(8, max(1, (os.cpu_count() or 4)))

    @staticmethod
    def _load_dotenv_if_present(path: str = ".env") -> None:
        env_path = Path(path)
        if not env_path.exists() or not env_path.is_file():
            return

        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = strip_quotes(value.strip())
            if key and key not in os.environ:
                os.environ[key] = value

    @classmethod
    def _extract_connection_string_from_env(cls) -> Optional[str]:
        cls._load_dotenv_if_present()
        for key in cls._ENV_KEYS:
            value = os.getenv(key)
            if value and value.strip():
                return strip_quotes(value.strip())
        return None

    @classmethod
    def _max_workers(cls) -> int:
        raw = os.getenv("WHY_NOT_MAX_WORKERS", "").strip()
        if not raw:
            return cls._DEFAULT_MAX_WORKERS
        try:
            return max(1, int(raw))
        except ValueError:
            return cls._DEFAULT_MAX_WORKERS

    @staticmethod
    def _extract_connection_string(line: str) -> Optional[str]:
        m = re.match(r"(?is)^(CONNECTION|CONN|DB)\s*:?\s+(.+)$", line.strip())
        if not m:
            return None
        return strip_quotes(m.group(2).strip())

    @staticmethod
    def _extract_tables_from_sql(sql_text: str) -> Set[str]:
        tables: Set[str] = set()
        for m in re.finditer(r"(?is)\bFROM\s+([A-Za-z_][A-Za-z0-9_\.]*)", sql_text):
            tables.add(m.group(1))
        for m in re.finditer(r"(?is)\bJOIN\s+([A-Za-z_][A-Za-z0-9_\.]*)", sql_text):
            tables.add(m.group(1))
        return tables

    @staticmethod
    def _load_table_rows(connection_string: str, table: str) -> Tuple[str, List[str], List[Tuple[str, ...]]]:
        backend = PostgresBackend(connection_string)
        iterator = backend.iter_table_rows(table)
        rows: List[Tuple[str, ...]] = []
        cols: List[str] = []
        try:
            while True:
                rows.append(next(iterator))
        except StopIteration as done:
            cols = done.value or []
        return table, cols, rows

    @staticmethod
    def _run_sql(connection_string: str, sql_text: str) -> Dict[str, Any]:
        return PostgresBackend(connection_string).execute_sql(sql_text)

    def parse(self, query_text: str) -> Program:
        raw_lines = [strip_comment(l) for l in query_text.splitlines()]
        lines = [l for l in raw_lines if l]

        schemas: Dict[str, List[str]] = {}
        edb: Dict[str, List[Tuple[str, ...]]] = {}
        rules: List[Rule] = []
        sql_blocks: List[str] = []
        sql_results: List[Dict[str, Any]] = []
        question: Optional[ProvenanceQuestion] = None
        connection_string: Optional[str] = self._extract_connection_string_from_env()

        i = 0
        auto_rule_idx = 1

        while i < len(lines):
            line = lines[i].strip()
            upper = line.upper().rstrip("?")

            conn = self._extract_connection_string(line)
            if conn is not None:
                connection_string = conn
                i += 1
                continue

            if upper.startswith("WHYNOT ") or upper.startswith("WHY "):
                mode = "WHYNOT" if upper.startswith("WHYNOT ") else "WHY"
                atom_text = line.split(None, 1)[1].rstrip("?").strip()
                question = ProvenanceQuestion(mode=mode, target=parse_atom(atom_text))
                i += 1
                continue

            if upper.startswith("SCHEMA "):
                schema_atom = parse_atom(line.split(None, 1)[1])
                schemas[schema_atom.predicate] = schema_atom.terms
                i += 1
                continue

            if upper.startswith("SQL:") or upper.startswith("SELECT "):
                sql_text = line[4:].strip() if upper.startswith("SQL:") else line
                i += 1
                while i < len(lines) and not sql_text.strip().endswith(";"):
                    sql_text += " " + lines[i].strip()
                    i += 1
                sql_blocks.append(sql_text)
                continue

            if ":-" in line:
                left, right = line.split(":-", 1)
                left = left.strip()
                right = right.strip().rstrip(".")
                if ":" in left:
                    rid, head_text = left.split(":", 1)
                    rule_id = rid.strip()
                    head = parse_atom(head_text.strip())
                else:
                    rule_id = f"r{auto_rule_idx}"
                    auto_rule_idx += 1
                    head = parse_atom(left)
                goals = [parse_goal(x) for x in split_top_level(right, ",")]
                rules.append(Rule(rule_id=rule_id, head=head, body=goals))
                i += 1
                continue

            atom = parse_atom(line.rstrip("."))
            edb.setdefault(atom.predicate, []).append(tuple(atom.terms))
            i += 1

        if question is None:
            raise ValueError("Missing provenance question. Include WHY ... or WHYNOT ...")

        if connection_string:
            idb_heads = {r.head.predicate for r in rules}
            needed_tables: Set[str] = set()

            for sql_text in sql_blocks:
                needed_tables.update(self._extract_tables_from_sql(sql_text))

            for rule in rules:
                for goal in rule.body:
                    if goal.kind in ("pos", "neg") and goal.atom.predicate not in idb_heads:
                        needed_tables.add(goal.atom.predicate)

            tables_to_fetch = [table for table in sorted(needed_tables) if table not in edb]

            workers = self._max_workers()
            if tables_to_fetch:
                if workers == 1 or len(tables_to_fetch) == 1:
                    for table in tables_to_fetch:
                        tname, cols, rows = self._load_table_rows(connection_string, table)
                        edb.setdefault(tname, []).extend(rows)
                        if tname not in schemas:
                            schemas[tname] = cols if cols else []
                else:
                    with ThreadPoolExecutor(max_workers=min(workers, len(tables_to_fetch))) as executor:
                        futures = {
                            executor.submit(self._load_table_rows, connection_string, table): table
                            for table in tables_to_fetch
                        }
                        for future in as_completed(futures):
                            tname, cols, rows = future.result()
                            edb.setdefault(tname, []).extend(rows)
                            if tname not in schemas:
                                schemas[tname] = cols if cols else []

            if sql_blocks:
                if workers == 1 or len(sql_blocks) == 1:
                    for sql_text in sql_blocks:
                        sql_results.append(self._run_sql(connection_string, sql_text))
                else:
                    ordered_results: List[Optional[Dict[str, Any]]] = [None] * len(sql_blocks)
                    with ThreadPoolExecutor(max_workers=min(workers, len(sql_blocks))) as executor:
                        futures = {
                            executor.submit(self._run_sql, connection_string, sql_text): idx
                            for idx, sql_text in enumerate(sql_blocks)
                        }
                        for future in as_completed(futures):
                            idx = futures[future]
                            ordered_results[idx] = future.result()
                    sql_results.extend([r for r in ordered_results if r is not None])

        for pred, facts in edb.items():
            if pred not in schemas and facts:
                arity = len(facts[0])
                schemas[pred] = [f"c{i + 1}" for i in range(arity)]

        if sql_blocks:
            sql_builder = SQLRuleBuilder(schemas)
            for idx, sql_text in enumerate(sql_blocks, start=1):
                ast = sql_builder.parse_sql(sql_text)
                head_pred = question.target.predicate if len(sql_blocks) == 1 else f"QSQL{idx}"
                rules.append(sql_builder.to_rule(ast, rule_id=f"sql_r{idx}", head_predicate=head_pred))

        return Program(
            edb=edb,
            schemas=schemas,
            rules=rules,
            question=question,
            connection_string=connection_string,
            sql_queries=sql_blocks,
            sql_results=sql_results,
        )
