from dataclasses import dataclass
from typing import List

from .atom import Atom
from .goal import Goal


@dataclass
class Rule:
    """Non-recursive Datalog rule."""

    rule_id: str
    head: Atom
    body: List[Goal]
