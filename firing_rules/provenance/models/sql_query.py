from dataclasses import dataclass
from typing import List

from .sql_table_ref import SQLTableRef


@dataclass
class SQLQuery:
    """Simplified SQL query AST for this engine."""

    select_items: List[str]
    tables: List[SQLTableRef]
    conditions: List[str]
