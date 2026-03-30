from dataclasses import dataclass


@dataclass
class SQLTableRef:
    """One SQL table reference in FROM/JOIN section."""

    table: str
    alias: str
