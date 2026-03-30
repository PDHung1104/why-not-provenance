from dataclasses import dataclass
from typing import Optional

from .atom import Atom


@dataclass
class Goal:
    """Rule-body goal."""

    kind: str
    atom: Optional[Atom] = None
    op: Optional[str] = None
    left: Optional[str] = None
    right: Optional[str] = None
    raw: str = ""
