from typing import Any, Dict

from .engine import ProvenanceEngine


def explain_why_not(query_text: str) -> Dict[str, Any]:
    return ProvenanceEngine().explain_why_not(query_text)
