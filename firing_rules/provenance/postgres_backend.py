from typing import Any, Dict, Generator, List, Sequence, Tuple


class PostgresBackend:
    """Minimal PostgreSQL adapter for loading EDB tables and executing SQL queries."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._driver = None

    def _connect(self):
        try:
            import psycopg  # type: ignore

            self._driver = "psycopg"
            return psycopg.connect(self.connection_string)
        except Exception:
            try:
                import psycopg2  # type: ignore

                self._driver = "psycopg2"
                return psycopg2.connect(self.connection_string)
            except Exception as e:
                raise RuntimeError(
                    "PostgreSQL driver not available. Install 'psycopg[binary]' or 'psycopg2-binary'."
                ) from e

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        parts = [p.strip() for p in identifier.split(".") if p.strip()]
        if not parts:
            raise ValueError(f"Invalid identifier: {identifier}")
        quoted_parts = []
        for p in parts:
            safe = p.replace('"', '""')
            quoted_parts.append(f'"{safe}"')
        return ".".join(quoted_parts)

    @staticmethod
    def _row_to_str_tuple(row: Sequence[Any]) -> Tuple[str, ...]:
        return tuple("NULL" if v is None else str(v) for v in row)

    def iter_table_rows(self, table: str, batch_size: int = 1000) -> Generator[Tuple[str, ...], None, List[str]]:
        """Yield table rows in batches; returns column names via StopIteration.value."""
        sql = f"SELECT * FROM {self._quote_ident(table)}"
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description] if cur.description else []
                while True:
                    batch = cur.fetchmany(batch_size)
                    if not batch:
                        break
                    for row in batch:
                        yield self._row_to_str_tuple(row)
                return cols
        finally:
            conn.close()

    def fetch_table(self, table: str) -> Tuple[List[str], List[Tuple[str, ...]]]:
        """Compatibility helper: returns full table payload."""
        iterator = self.iter_table_rows(table)
        rows: List[Tuple[str, ...]] = []
        cols: List[str] = []
        try:
            while True:
                rows.append(next(iterator))
        except StopIteration as done:
            cols = done.value or []
        return cols, rows

    def execute_sql(self, sql_text: str, batch_size: int = 1000) -> Dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text)
                cols = [d[0] for d in cur.description] if cur.description else []
                rows: List[List[str]] = []
                if cur.description:
                    while True:
                        batch = cur.fetchmany(batch_size)
                        if not batch:
                            break
                        for row in batch:
                            rows.append(list(self._row_to_str_tuple(row)))
            return {
                "columns": cols,
                "rows": rows,
                "row_count": len(rows),
            }
        finally:
            conn.close()
