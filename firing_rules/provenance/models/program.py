from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .provenance_question import ProvenanceQuestion
from .rule import Rule


@dataclass
class Program:
    """Normalized program representation used by provenance engine."""

    edb: Dict[str, List[Tuple[str, ...]]]
    schemas: Dict[str, List[str]]
    rules: List[Rule]
    question: ProvenanceQuestion
    connection_string: Optional[str] = None
    sql_queries: List[str] = field(default_factory=list)
    sql_results: List[Dict[str, Any]] = field(default_factory=list)
