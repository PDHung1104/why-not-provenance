"""Helpers for formatting failed derivation records in table form."""

from __future__ import annotations

from typing import Any, Dict, List


# Summarization component: builds compact rows for rule, binding, and failing goals.
def summarize_failed_derivations(failed_derivations: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    for record in failed_derivations:
        binding = record.get("binding", {})
        if isinstance(binding, dict):
            binding_text = ", ".join(f"{k}={v}" for k, v in sorted(binding.items()))
        else:
            binding_text = ""

        goal_results = record.get("goal_results", [])
        failed_goals: List[str] = []
        if isinstance(goal_results, list):
            for goal in goal_results:
                if isinstance(goal, dict) and not goal.get("ok", False):
                    failed_goals.append(str(goal.get("goal", "")))

        rows.append(
            {
                "rule_id": str(record.get("rule_id", "")),
                "binding": binding_text,
                "failed_goals": " | ".join(failed_goals),
            }
        )

    return rows
