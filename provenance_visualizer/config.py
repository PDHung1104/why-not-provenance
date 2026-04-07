"""Configuration component for the provenance visualizer app."""

from __future__ import annotations

from typing import Dict, Tuple

# Page-level settings component: keeps UI metadata in one place.
PAGE_TITLE = "WHY-NOT Provenance Visualizer"
PAGE_LAYOUT = "wide"

# Input defaults component: local path used when users choose file-path mode.
DEFAULT_LOCAL_PATH = "firing_rules/test_result.json"

# Graph styling component: maps provenance node type -> (shape, fill color).
NODE_STYLE: Dict[str, Tuple[str, str]] = {
    "tuple": ("box", "#ffd6d6"),
    "rule": ("ellipse", "#ffe9b3"),
    "goal": ("diamond", "#d6eaff"),
}
