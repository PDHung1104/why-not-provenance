import re
from typing import Dict, List, Optional, Tuple

from .constants import COMPARISON_OPS
from .helpers import split_top_level, split_top_level_and, strip_quotes
from .models import Atom, Goal, Rule, SQLQuery, SQLTableRef


class SQLRuleBuilder:
    """Translate simplified SQL into a single Datalog rule."""

    def __init__(self, schemas: Dict[str, List[str]]):
        self.schemas = schemas

    def parse_sql(self, sql_text: str) -> SQLQuery:
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
        where_conditions = split_top_level_and(where_part) if where_part else []
        all_conditions = join_conditions + where_conditions

        select_items = [x.strip() for x in split_top_level(select_part, ",")]
        return SQLQuery(select_items=select_items, tables=tables, conditions=all_conditions)

    def _parse_from_part(self, from_part: str) -> Tuple[List[SQLTableRef], List[str]]:
        tables: List[SQLTableRef] = []
        join_conditions: List[str] = []
        upper = from_part.upper()

        if " JOIN " not in f" {upper} ":
            for piece in split_top_level(from_part, ","):
                tables.append(self._parse_table_ref(piece))
            return tables, join_conditions

        join_iter = list(re.finditer(r"(?is)\bJOIN\b", from_part))
        first_join_idx = join_iter[0].start()
        base_table = from_part[:first_join_idx].strip()
        tables.append(self._parse_table_ref(base_table))

        pattern = re.compile(r"(?is)\bJOIN\s+(.+?)\s+ON\s+(.+?)(?=\bJOIN\b|$)")
        for m in pattern.finditer(from_part[first_join_idx:]):
            table_ref_text = m.group(1).strip()
            on_text = m.group(2).strip()
            tables.append(self._parse_table_ref(table_ref_text))
            join_conditions.extend(split_top_level_and(on_text))

        return tables, join_conditions

    @staticmethod
    def _parse_table_ref(text: str) -> SQLTableRef:
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

        for cond in sql.conditions:
            goals.append(self._condition_to_goal(cond, col_var))

        head_terms = [self._resolve_sql_expr(item, col_var) for item in sql.select_items]
        return Rule(rule_id=rule_id, head=Atom(head_predicate, head_terms), body=goals)

    def _condition_to_goal(self, cond: str, col_var: Dict[str, str]) -> Goal:
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

        col_to_term: Dict[str, str] = {col: self._mk_var(alias, col) for col in self.schemas[table]}

        for sub_cond in split_top_level_and(where_part):
            l_raw, op, r_raw = self._parse_comparison(sub_cond)
            if op != "=":
                raise ValueError("Only '=' comparisons are supported inside NOT EXISTS subquery WHERE.")

            l_alias, l_col = self._parse_colref(l_raw)
            r_alias, r_col = self._parse_colref(r_raw)

            if l_alias == alias and l_col:
                col_to_term[l_col] = self._resolve_sql_expr(r_raw, outer_col_var)
                continue
            if r_alias == alias and r_col:
                col_to_term[r_col] = self._resolve_sql_expr(l_raw, outer_col_var)
                continue

            raise ValueError("NOT EXISTS WHERE must constrain subquery alias columns via '=' conditions.")

        return Atom(table, [col_to_term[col] for col in self.schemas[table]])

    @staticmethod
    def _parse_comparison(expr: str) -> Tuple[str, str, str]:
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
        t = token.strip()
        m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", t)
        if not m:
            return None, None
        return m.group(1), m.group(2)

    def _resolve_sql_expr(self, expr: str, col_var: Dict[str, str]) -> str:
        e = expr.strip()
        m_as = re.match(r"(?is)^(.+?)\s+AS\s+[A-Za-z_][A-Za-z0-9_]*$", e)
        if m_as:
            e = m_as.group(1).strip()

        alias, col = self._parse_colref(e)
        if alias and col:
            key = f"{alias}.{col}"
            if key not in col_var:
                raise ValueError(f"Unknown column reference: {key}")
            return col_var[key]

        return strip_quotes(e)

    @staticmethod
    def _mk_var(alias: str, column: str) -> str:
        return f"{alias}_{column}".upper()
