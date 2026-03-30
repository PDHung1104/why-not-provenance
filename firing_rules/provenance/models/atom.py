from dataclasses import dataclass
from typing import List


@dataclass
class Atom:
    """Predicate atom (e.g., Train(X,Y), Q(s,n))."""

    predicate: str
    terms: List[str]
