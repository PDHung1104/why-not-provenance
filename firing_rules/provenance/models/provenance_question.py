from dataclasses import dataclass

from .atom import Atom


@dataclass
class ProvenanceQuestion:
    """Provenance question from input."""

    mode: str
    target: Atom
