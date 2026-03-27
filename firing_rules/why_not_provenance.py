"""
WHY-NOT provenance engine using firing rules.

This module now supports two query authoring styles:
1) Datalog-style rules
2) SQL SELECT-FROM-WHERE (translated into one Datalog rule)

Design goal (extensibility):
- Parsing/normalization is separated from provenance evaluation.
- SQL-to-rule translation is isolated in SQLRuleBuilder.
- Provenance execution is encapsulated in ProvenanceEngine.
"""

from __future__ import annotations

import itertools
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


# =========================
# Core data model
# =========================


@dataclass
class Atom:
    """Predicate atom (e.g., Train(X,Y), Q(s,n))."""

    predicate: str
    terms: List[str]


@dataclass
class Goal:
    """Rule-body goal.

    kind values:
        - pos: positive atom
        - neg: negated atom
        - cmp: comparison
    """

    kind: str
    atom: Optional[Atom] = None
    op: Optional[str] = None
    left: Optional[str] = None
    right: Optional[str] = None
    raw: str = ""


@dataclass
class Rule:
    """Non-recursive Datalog rule."""

    rule_id: str
    head: Atom
    body: List[Goal]


@dataclass
class ProvenanceQuestion:
    """Provenance question from input.

    mode:
        WHY or WHYNOT
    target:
        Target tuple atom, e.g., Q(s,n)
    """

    mode: str
    target: Atom


@dataclass
class Program:
    """Normalized program representation used by provenance engine.

    edb:
        Base facts grouped by predicate.
    schemas:
        Column names for each predicate. Needed for SQL translation.
    rules:
        Datalog rules (either authored directly or generated from SQL).
    question:
        WHY/WHYNOT question.
    """

    edb: Dict[str, List[Tuple[str, ...]]]
    schemas: Dict[str, List[str]]
    rules: List[Rule]
    question: ProvenanceQuestion


# =========================
# Helpers
# =========================


COMPARISON_OPS = [">=", "<=", "!=", "=", ">", "<"]


def _strip_comment(line: str) -> str:
    """Remove inline comments of the form '# ...'."""

    return line.split("#", 1)[0].strip()


def _strip_quotes(value: str) -> str:
    """Remove matching single/double quotes from a token if present."""

    value = value.strip()
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    return value


def _is_variable(term: str) -> bool:
    """Variable convention: token starts with uppercase letter."""

    return len(term) > 0 and term[0].isupper()


def _split_top_level(s: str, delimiter: str) -> List[str]:
    """Split by delimiter while respecting parentheses depth.

    Inputs:
        s: source string.
        delimiter: one-character delimiter like ','.
    Output:
        list of top-level chunks.
    Use case:
        splitting argument lists, SQL select list, SQL WHERE conjunctions.
    """

    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == delimiter and depth == 0:
            piece = "".join(buf).strip()
            if piece:
                parts.append(piece)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _split_top_level_and(s: str) -> List[str]:
    """Split SQL boolean conjunctions at top-level AND only."""

    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    i = 0
    upper = s.upper()
    while i < len(s):
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1

        if depth == 0 and upper[i : i + 3] == "AND":
            left_ok = i == 0 or upper[i - 1].isspace()
            right_ok = i + 3 >= len(s) or upper[i + 3].isspace()
            if left_ok and right_ok:
                piece = "".join(buf).strip()
                if piece:
                    parts.append(piece)
                buf = []
                i += 3
                continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_atom(text: str) -> Atom:
    """Parse atom text into Atom.

    Input:
        e.g., "Train(X,Z)", "Q(s,n)"
    Output:
        Atom(predicate, terms)
    """

    s = text.strip().rstrip(".")
    m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)", s)
    if not m:
        raise ValueError(f"Invalid atom syntax: {text}")
    pred = m.group(1)
    terms = [_strip_quotes(t.strip()) for t in _split_top_level(m.group(2).strip(), ",")]
    return Atom(pred, terms)


def _parse_goal(goal_text: str) -> Goal:
    """Parse one Datalog-style goal from rule body.

    Supported:
        - not Atom(...)
        - comparisons (X = Y, X > 3)
        - positive Atom(...)
    """

    raw = goal_text.strip()
    if raw.lower().startswith("not "):
        return Goal(kind="neg", atom=_parse_atom(raw[4:].strip()), raw=raw)

    for op in COMPARISON_OPS:
        idx = raw.find(op)
        if idx > 0:
            left = raw[:idx].strip()
            right = raw[idx + len(op) :].strip()
            if left and right and "(" not in left and ")" not in left:
                return Goal(kind="cmp", op=op, left=_strip_quotes(left), right=_strip_quotes(right), raw=raw)

    return Goal(kind="pos", atom=_parse_atom(raw), raw=raw)


# =========================
# SQL parsing + translation
# =========================


@dataclass
class SQLTableRef:
    """One SQL table reference in FROM/JOIN section."""

    table: str
    alias: str


@dataclass
class SQLQuery:
    """Simplified SQL query AST for this engine."""

    select_items: List[str]
    tables: List[SQLTableRef]
    conditions: List[str]


class SQLRuleBuilder:
    """Translate simplified SQL into a single Datalog rule.

    Supported SQL subset:
        SELECT <items>
        FROM <table alias> [JOIN <table alias> ON <cond>]...
        [WHERE <cond AND cond ...>]

    Conditions supported:
        - comparisons (alias.col = alias2.col, alias.col = 'const', etc.)
        - NOT EXISTS (SELECT 1 FROM table alias WHERE ...)

    Notes:
        - Requires schemas for predicates to map alias.col to atom positions.
        - Unconstrained columns in NOT EXISTS subquery become fresh variables.
    """

    def __init__(self, schemas: Dict[str, List[str]]):
        self.schemas = schemas

    def parse_sql(self, sql_text: str) -> SQLQuery:
        """Parse SQL string into SQLQuery AST.

        Input:
            sql_text: full SELECT ... statement.
        Output:
            SQLQuery(select_items, tables, conditions)
        """

        sql = " ".join(sql_text.strip().rstrip(";").split())
        m = re.match(r"(?is)^SELECT\s+(.*?)\s+FROM\s+(.*)$", sql)
        if not m:
            raise ValueError("Invalid SQL: missing SELECT ... FROM ...")
        select_part = m.group(1).strip()
        from_where = m.group(2).strip()

        where_part = ""
        m_where = re.search(r"(?is)\bWHERE\b", from_where)
        if m_where:
            from_part = from_where[: m_where.start()].strip()
            where_part = from_where[m_where.end() :].strip()
        else:
            from_part = from_where

        tables, join_conditions = self._parse_from_part(from_part)
        where_conditions = _split_top_level_and(where_part) if where_part else []
        all_conditions = join_conditions + where_conditions

        select_items = [x.strip() for x in _split_top_level(select_part, ",")]
        return SQLQuery(select_items=select_items, tables=tables, conditions=all_conditions)

    def _parse_from_part(self, from_part: str) -> Tuple[List[SQLTableRef], List[str]]:
        """Parse FROM clause and collect JOIN ON conditions.

        Output:
            (tables, join_conditions)
        """

        tables: List[SQLTableRef] = []
        join_conditions: List[str] = []
        upper = from_part.upper()

        # Case A: comma-separated table refs (no JOIN keyword)
        if " JOIN " not in f" {upper} ":
            for piece in _split_top_level(from_part, ","):
                tables.append(self._parse_table_ref(piece))
            return tables, join_conditions

        # Case B: explicit JOIN syntax
        join_iter = list(re.finditer(r"(?is)\bJOIN\b", from_part))
        first_join_idx = join_iter[0].start()
        base_table = from_part[:first_join_idx].strip()
        tables.append(self._parse_table_ref(base_table))

        pattern = re.compile(r"(?is)\bJOIN\s+(.+?)\s+ON\s+(.+?)(?=\bJOIN\b|$)")
        for m in pattern.finditer(from_part[first_join_idx:]):
            table_ref_text = m.group(1).strip()
            on_text = m.group(2).strip()
            tables.append(self._parse_table_ref(table_ref_text))
            join_conditions.extend(_split_top_level_and(on_text))

        return tables, join_conditions

    @staticmethod
    def _parse_table_ref(text: str) -> SQLTableRef:
        """Parse one table reference: 'Train t1' or 'Train AS t1'."""

        toks = text.strip().split()
        if len(toks) == 1:
            table = toks[0]
            alias = toks[0]
        elif len(toks) == 2:
            table, alias = toks[0], toks[1]
        elif len(toks) == 3 and toks[1].upper() == "AS":
            table, alias = toks[0], toks[2]
        else:
            raise ValueError(f"Unsupported table reference: {text}")
        return SQLTableRef(table=table, alias=alias)

    def to_rule(self, sql: SQLQuery, rule_id: str, head_predicate: str) -> Rule:
        """Convert SQLQuery AST into one Datalog rule.

        Inputs:
            sql: parsed SQL query AST.
            rule_id: generated rule name.
            head_predicate: head predicate name (usually target predicate from WHYNOT).
        Output:
            Rule object.
        """

        # Build table goals and alias.col -> variable mapping.
        col_var: Dict[str, str] = {}
        goals: List[Goal] = []

        for table_ref in sql.tables:
            if table_ref.table not in self.schemas:
                raise ValueError(f"Missing schema for table '{table_ref.table}'. Add SCHEMA line.")

            cols = self.schemas[table_ref.table]
            terms: List[str] = []
            for c in cols:
                v = self._mk_var(table_ref.alias, c)
                col_var[f"{table_ref.alias}.{c}"] = v
                terms.append(v)
            goals.append(Goal(kind="pos", atom=Atom(table_ref.table, terms), raw=f"{table_ref.table}({', '.join(terms)})"))

        # Translate WHERE / JOIN conditions.
        for cond in sql.conditions:
            translated = self._condition_to_goal(cond, col_var)
            goals.append(translated)

        # Translate SELECT list to head terms.
        head_terms = [self._resolve_sql_expr(item, col_var) for item in sql.select_items]
        head = Atom(head_predicate, head_terms)

        return Rule(rule_id=rule_id, head=head, body=goals)

    def _condition_to_goal(self, cond: str, col_var: Dict[str, str]) -> Goal:
        """Translate one SQL condition into a Goal.

        Supported cases:
            - NOT EXISTS subquery
            - simple comparison
        """

        c = cond.strip()
        if c.upper().startswith("NOT EXISTS"):
            atom = self._parse_not_exists_to_atom(c, col_var)
            return Goal(kind="neg", atom=atom, raw=f"not {atom.predicate}({', '.join(atom.terms)})")

        left, op, right = self._parse_comparison(c)
        return Goal(
            kind="cmp",
            op=op,
            left=self._resolve_sql_expr(left, col_var),
            right=self._resolve_sql_expr(right, col_var),
            raw=c,
        )

    def _parse_not_exists_to_atom(self, cond: str, outer_col_var: Dict[str, str]) -> Atom:
        """Parse NOT EXISTS subquery into a negated atom.

        Supported shape:
            NOT EXISTS (
                SELECT ...
                FROM Table alias
                WHERE alias.col = outer_alias.col AND alias.col2 = 'const' ...
            )

        Output atom terms are ordered by table schema.
        Unconstrained subquery columns receive fresh variables.
        """

        m = re.match(
            r"(?is)^NOT\s+EXISTS\s*\(\s*SELECT\s+.+?\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+(.+)\)$",
            cond.strip(),
        )
        if not m:
            raise ValueError(f"Unsupported NOT EXISTS format: {cond}")

        table = m.group(1)
        alias = m.group(2)
        where_part = m.group(3).strip()

        if table not in self.schemas:
            raise ValueError(f"Missing schema for table '{table}' used in NOT EXISTS.")

        # Start with fresh variables for each subquery column.
        col_to_term: Dict[str, str] = {}
        for col in self.schemas[table]:
            col_to_term[col] = self._mk_var(alias, col)

        # Apply equality constraints from subquery WHERE.
        for sub_cond in _split_top_level_and(where_part):
            l_raw, op, r_raw = self._parse_comparison(sub_cond)
            if op != "=":
                raise ValueError("Only '=' comparisons are supported inside NOT EXISTS subquery WHERE.")

            l_alias, l_col = self._parse_colref(l_raw)
            r_alias, r_col = self._parse_colref(r_raw)

            # alias.col = something
            if l_alias == alias and l_col:
                col_to_term[l_col] = self._resolve_sql_expr(r_raw, outer_col_var)
                continue
            if r_alias == alias and r_col:
                col_to_term[r_col] = self._resolve_sql_expr(l_raw, outer_col_var)
                continue

            raise ValueError("NOT EXISTS WHERE must constrain subquery alias columns via '=' conditions.")

        terms = [col_to_term[col] for col in self.schemas[table]]
        return Atom(table, terms)

    @staticmethod
    def _parse_comparison(expr: str) -> Tuple[str, str, str]:
        """Parse one comparison expression into (left, op, right)."""

        s = expr.strip()
        for op in COMPARISON_OPS:
            idx = s.find(op)
            if idx > 0:
                left = s[:idx].strip()
                right = s[idx + len(op) :].strip()
                if left and right:
                    return left, op, right
        raise ValueError(f"Unsupported comparison expression: {expr}")

    @staticmethod
    def _parse_colref(token: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse alias.column reference token.

        Returns:
            (alias, column) if token is alias.column, else (None, None).
        """

        t = token.strip()
        m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", t)
        if not m:
            return None, None
        return m.group(1), m.group(2)

    def _resolve_sql_expr(self, expr: str, col_var: Dict[str, str]) -> str:
        """Resolve SQL operand into Datalog term (variable or constant).

        Input:
            expr: alias.col, literal, or simple unqualified token.
            col_var: mapping alias.col -> variable.
        Output:
            Variable token if expression refers to a column; otherwise constant token.
        """

        e = expr.strip()

        # Remove optional "AS name" from SELECT item.
        m_as = re.match(r"(?is)^(.+?)\s+AS\s+[A-Za-z_][A-Za-z0-9_]*$", e)
        if m_as:
            e = m_as.group(1).strip()

        alias, col = self._parse_colref(e)
        if alias and col:
            key = f"{alias}.{col}"
            if key not in col_var:
                raise ValueError(f"Unknown column reference: {key}")
            return col_var[key]

        # Numeric literals / quoted literals / bare constants
        return _strip_quotes(e)

    @staticmethod
    def _mk_var(alias: str, column: str) -> str:
        """Generate stable variable name for a SQL alias.column."""

        return f"{alias}_{column}".upper()


# =========================
# Input parser (Datalog + SQL)
# =========================


class InputParser:
    """Parse user input into Program.

    Supports mixed input containing:
      - SCHEMA lines: SCHEMA Train(from,to).
      - EDB facts: Train(n,c).
      - Datalog rules
      - SQL query block (SELECT ... ;)
      - provenance question WHY/WHYNOT

    Extensibility:
      - New frontends can be added by normalizing into Program.
    """

    def parse(self, query_text: str) -> Program:
        """Main parse function.

        Input:
            query_text: full user input.
        Output:
            Program object used by ProvenanceEngine.
        """

        raw_lines = [_strip_comment(l) for l in query_text.splitlines()]
        lines = [l for l in raw_lines if l]

        schemas: Dict[str, List[str]] = {}
        edb: Dict[str, List[Tuple[str, ...]]] = {}
        rules: List[Rule] = []
        sql_blocks: List[str] = []
        question: Optional[ProvenanceQuestion] = None

        i = 0
        auto_rule_idx = 1

        while i < len(lines):
            line = lines[i].strip()
            upper = line.upper().rstrip("?")

            # Provenance question
            if upper.startswith("WHYNOT ") or upper.startswith("WHY "):
                mode = "WHYNOT" if upper.startswith("WHYNOT ") else "WHY"
                atom_text = line.split(None, 1)[1].rstrip("?").strip()
                question = ProvenanceQuestion(mode=mode, target=_parse_atom(atom_text))
                i += 1
                continue

            # SCHEMA declaration
            if upper.startswith("SCHEMA "):
                schema_atom = _parse_atom(line.split(None, 1)[1])
                schemas[schema_atom.predicate] = schema_atom.terms
                i += 1
                continue

            # SQL query block start: "SQL: ..." or "SELECT ..."
            if upper.startswith("SQL:") or upper.startswith("SELECT "):
                sql_text = line[4:].strip() if upper.startswith("SQL:") else line
                i += 1
                while i < len(lines) and not sql_text.strip().endswith(";"):
                    sql_text += " " + lines[i].strip()
                    i += 1
                sql_blocks.append(sql_text)
                continue

            # Datalog rule
            if ":-" in line:
                left, right = line.split(":-", 1)
                left = left.strip()
                right = right.strip().rstrip(".")
                if ":" in left:
                    rid, head_text = left.split(":", 1)
                    rule_id = rid.strip()
                    head = _parse_atom(head_text.strip())
                else:
                    rule_id = f"r{auto_rule_idx}"
                    auto_rule_idx += 1
                    head = _parse_atom(left)
                goals = [_parse_goal(x) for x in _split_top_level(right, ",")]
                rules.append(Rule(rule_id=rule_id, head=head, body=goals))
                i += 1
                continue

            # EDB fact
            atom = _parse_atom(line.rstrip("."))
            edb.setdefault(atom.predicate, []).append(tuple(atom.terms))
            i += 1

        if question is None:
            raise ValueError("Missing provenance question. Include WHY ... or WHYNOT ...")

        # Infer schemas from EDB facts where explicit schema is missing.
        for pred, facts in edb.items():
            if pred not in schemas and facts:
                arity = len(facts[0])
                schemas[pred] = [f"c{i+1}" for i in range(arity)]

        # Translate SQL blocks into rules.
        if sql_blocks:
            sql_builder = SQLRuleBuilder(schemas)
            for idx, sql_text in enumerate(sql_blocks, start=1):
                ast = sql_builder.parse_sql(sql_text)
                head_pred = question.target.predicate if len(sql_blocks) == 1 else f"QSQL{idx}"
                rule = sql_builder.to_rule(ast, rule_id=f"sql_r{idx}", head_predicate=head_pred)
                rules.append(rule)

        return Program(edb=edb, schemas=schemas, rules=rules, question=question)


# =========================
# Provenance engine
# =========================


def _resolve_term(term: str, binding: Dict[str, str]) -> str:
    """Resolve one term under a variable binding."""

    return binding[term] if _is_variable(term) else term


def _eval_comparison(left: str, op: str, right: str) -> bool:
    """Evaluate comparison on grounded operands.

    Numeric compare is used when both operands parse as numbers;
    otherwise lexical compare.
    """

    def to_num(s: str) -> Optional[float]:
        try:
            return float(s)
        except ValueError:
            return None

    l_num = to_num(left)
    r_num = to_num(right)
    l_val, r_val = (l_num, r_num) if l_num is not None and r_num is not None else (left, right)

    if op == "=":
        return l_val == r_val
    if op == "!=":
        return l_val != r_val
    if op == ">":
        return l_val > r_val
    if op == "<":
        return l_val < r_val
    if op == ">=":
        return l_val >= r_val
    if op == "<=":
        return l_val <= r_val
    raise ValueError(f"Unsupported operator: {op}")


class ProvenanceEngine:
    """WHY-NOT provenance evaluator using firing-rule records."""

    def __init__(self) -> None:
        self.parser = InputParser()

    def explain_why_not(self, query_text: str) -> Dict[str, Any]:
        """Execute full WHY-NOT workflow.

        Input:
            query_text: complete input string (facts/rules/sql/question).
        Output:
            JSON-serializable dict with failed derivations and explanation graph.
        """

        program = self.parser.parse(query_text)
        if program.question.mode != "WHYNOT":
            raise ValueError("This engine currently returns WHY-NOT only. Use WHYNOT ...")

        target = program.question.target

        # Current relation store (EDB + materialized IDB).
        relations: Dict[str, set] = {pred: set(rows) for pred, rows in program.edb.items()}

        # Step A: materialize IDB predicates (non-recursive rules, order-sensitive).
        for rule in program.rules:
            domains = self._compute_domains_for_full_eval(rule, relations)
            bindings = self._enumerate_bindings(domains)
            records = self._build_firing_records(rule, bindings, relations)
            head_tuples = {_ground_head(rule.head, rec["binding"]) for rec in records if rec["status"]}
            relations.setdefault(rule.head.predicate, set()).update(head_tuples)

        # Step B: provenance-focused evaluation only for rules defining target predicate.
        target_rules = [r for r in program.rules if r.head.predicate == target.predicate]
        if not target_rules:
            raise ValueError(f"No rule defines target predicate {target.predicate}")

        failed_relevant: List[Dict[str, Any]] = []
        for rule in target_rules:
            domains = self._compute_variable_domains(program, rule, target)
            bindings = self._enumerate_bindings(domains)
            records = self._build_firing_records(rule, bindings, relations)
            relevant = [r for r in records if _ground_head(rule.head, r["binding"]) == tuple(target.terms)]
            failed_relevant.extend([r for r in relevant if not r["status"]])

        graph = self._build_explanation_graph(target, failed_relevant)

        return {
            "mode": "WHYNOT",
            "target": f"{target.predicate}({', '.join(target.terms)})",
            "failed_derivation_count": len(failed_relevant),
            "failed_derivations": failed_relevant,
            "explanation_graph": graph,
            "message": (
                "Tuple is missing because every relevant derivation failed."
                if failed_relevant
                else "No failed derivations found for target head under current domains."
            ),
        }

    # ---------- domain / enumeration ----------

    def _compute_domains_for_full_eval(self, rule: Rule, relations: Dict[str, set]) -> Dict[str, List[str]]:
        """Compute active-domain variable domains for full rule materialization.

        Input:
            rule: rule to evaluate.
            relations: current EDB/IDB tuples.
        Output:
            dict var -> sorted active domain constants.
        Use case:
            Full evaluation before provenance filtering.
        """

        active = set()
        for rows in relations.values():
            for tup in rows:
                active.update(tup)

        vars_in_rule = set()
        for t in rule.head.terms:
            if _is_variable(t):
                vars_in_rule.add(t)
        for g in rule.body:
            if g.kind in ("pos", "neg"):
                vars_in_rule.update([x for x in g.atom.terms if _is_variable(x)])
            else:
                if _is_variable(g.left):
                    vars_in_rule.add(g.left)
                if _is_variable(g.right):
                    vars_in_rule.add(g.right)

        return {v: sorted(active) for v in vars_in_rule}

    def _compute_variable_domains(self, program: Program, rule: Rule, target: Atom) -> Dict[str, List[str]]:
        """Compute target-aware domains for WHY-NOT candidate derivations.

        Input:
            program: normalized program (for EDB active domain).
            rule: one target-defining rule.
            target: target tuple from WHYNOT.
        Output:
            dict var -> candidate constants.
        Use case:
            Implements finite candidate enumeration constrained by target head.
        """

        active = set()
        for rows in program.edb.values():
            for tup in rows:
                active.update(tup)

        domains: Dict[str, List[str]] = {}

        # Constrain head variables to target constants where applicable.
        for h, t in zip(rule.head.terms, target.terms):
            if _is_variable(h):
                domains[h] = [t] if not _is_variable(t) else sorted(active)

        # Remaining vars use full active domain.
        vars_in_rule = set()
        for t in rule.head.terms:
            if _is_variable(t):
                vars_in_rule.add(t)
        for g in rule.body:
            if g.kind in ("pos", "neg"):
                vars_in_rule.update([x for x in g.atom.terms if _is_variable(x)])
            else:
                if _is_variable(g.left):
                    vars_in_rule.add(g.left)
                if _is_variable(g.right):
                    vars_in_rule.add(g.right)

        for v in vars_in_rule:
            domains.setdefault(v, sorted(active))

        return domains

    @staticmethod
    def _enumerate_bindings(domains: Dict[str, List[str]]) -> List[Dict[str, str]]:
        """Enumerate variable bindings via Cartesian product.

        Input:
            domains: var -> finite set of constants.
        Output:
            list of variable assignments.
        """

        if not domains:
            return [dict()]
        var_order = sorted(domains.keys())
        return [{v: vals[i] for i, v in enumerate(var_order)} for vals in itertools.product(*(domains[v] for v in var_order))]

    # ---------- goal/rule evaluation ----------

    def _evaluate_goal(self, goal: Goal, binding: Dict[str, str], relations: Dict[str, set]) -> Tuple[bool, str]:
        """Evaluate one goal for one binding.

        Input:
            goal: body goal.
            binding: variable assignment.
            relations: tuple store.
        Output:
            (ok, grounded_goal_label)
        """

        if goal.kind in ("pos", "neg"):
            grounded = tuple(_resolve_term(t, binding) for t in goal.atom.terms)
            exists = grounded in relations.get(goal.atom.predicate, set())
            if goal.kind == "pos":
                return exists, f"{goal.atom.predicate}({', '.join(grounded)})"
            return (not exists), f"not {goal.atom.predicate}({', '.join(grounded)})"

        left = _resolve_term(goal.left, binding)
        right = _resolve_term(goal.right, binding)
        return _eval_comparison(left, goal.op, right), f"{left} {goal.op} {right}"

    def _build_firing_records(
        self, rule: Rule, bindings: Sequence[Dict[str, str]], relations: Dict[str, set]
    ) -> List[Dict[str, Any]]:
        """Construct firing records for all candidate derivations.

        Output record format:
            {
              rule_id,
              binding,
              goal_results: [{goal_index, goal, ok}, ...],
              status
            }
        """

        rows: List[Dict[str, Any]] = []
        for b in bindings:
            goal_results = []
            for i, g in enumerate(rule.body, start=1):
                ok, grounded_goal = self._evaluate_goal(g, b, relations)
                goal_results.append({"goal_index": i, "goal": grounded_goal, "ok": ok})
            rows.append(
                {
                    "rule_id": rule.rule_id,
                    "binding": dict(b),
                    "goal_results": goal_results,
                    "status": all(x["ok"] for x in goal_results),
                }
            )
        return rows

    # ---------- explanation graph ----------

    @staticmethod
    def _build_explanation_graph(target: Atom, failed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build WHY-NOT explanation graph.

        Nodes:
            tuple node (missing target), failed rule nodes, failed goal nodes.
        Edges:
            tuple -> rule, rule -> failed goal.
        """

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []

        tuple_id = f"tuple:{target.predicate}({', '.join(target.terms)})"
        nodes.append({"id": tuple_id, "type": "tuple", "label": f"{target.predicate}({', '.join(target.terms)})", "missing": True})

        for rec in failed_records:
            b = rec["binding"]
            btxt = ", ".join(f"{k}={v}" for k, v in sorted(b.items()))
            rid = f"rule:{rec['rule_id']}:{btxt}"
            nodes.append({"id": rid, "type": "rule", "label": f"{rec['rule_id']}({btxt})", "status": "failed"})
            edges.append({"from": tuple_id, "to": rid})

            for g in rec["goal_results"]:
                if g["ok"]:
                    continue
                gid = f"goal:{rec['rule_id']}:{btxt}:g{g['goal_index']}"
                nodes.append({"id": gid, "type": "goal", "label": g["goal"], "ok": False})
                edges.append({"from": rid, "to": gid})

        return {"nodes": nodes, "edges": edges}


def _ground_head(head: Atom, binding: Dict[str, str]) -> Tuple[str, ...]:
    """Ground rule head with a concrete binding."""

    return tuple(_resolve_term(t, binding) for t in head.terms)


# =========================
# Public API + CLI
# =========================


def explain_why_not(query_text: str) -> Dict[str, Any]:
    """Public API function.

    Input:
        query_text: full program/query string.
    Output:
        WHY-NOT provenance result dictionary.
    Use case:
        Importable entrypoint for other Python modules.
    """

    return ProvenanceEngine().explain_why_not(query_text)


def main() -> None:
    """CLI entrypoint.

    Input:
        - argv[1] if provided (single string query)
        - otherwise stdin
    Output:
        pretty-printed JSON result
    """

    if len(sys.argv) > 1:
        query_text = sys.argv[1]
    else:
        query_text = sys.stdin.read()

    result = explain_why_not(query_text)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
