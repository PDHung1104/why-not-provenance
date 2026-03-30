"""CLI + compatibility wrapper for WHY-NOT provenance engine."""

from __future__ import annotations

import json
import sys
from typing import Any, Dict

from provenance.api import explain_why_not


def main() -> None:
    if len(sys.argv) > 1:
        query_text = sys.argv[1]
    else:
        query_text = sys.stdin.read()

    result: Dict[str, Any] = explain_why_not(query_text)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
