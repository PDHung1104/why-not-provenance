"""Input and normalization helpers for provenance JSON payloads."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


# Loading component: reads provenance JSON from upload, pasted text, or local file path.
def load_payload(uploaded_file: Any, local_path: str, pasted_json: str) -> Dict[str, Any]:
    raw_text = ""

    if uploaded_file is not None:
        raw_text = uploaded_file.getvalue().decode("utf-8")
    elif pasted_json.strip():
        raw_text = pasted_json
    elif local_path.strip():
        raw_text = Path(local_path).read_text(encoding="utf-8")

    if not raw_text.strip():
        raise ValueError("No JSON input was provided.")

    payload = json.loads(raw_text)
    if not isinstance(payload, dict):
        raise ValueError("Expected top-level JSON object.")

    return payload


# Extraction component: safely pulls graph arrays from the output payload contract.
def extract_graph(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    graph = payload.get("explanation_graph", {})
    if not isinstance(graph, dict):
        return [], []

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    return nodes if isinstance(nodes, list) else [], edges if isinstance(edges, list) else []
