from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Resolve repository paths once so test helpers can locate inputs/expectations reliably
# regardless of where pytest is invoked from.
ROOT_DIR = Path(__file__).resolve().parents[2]
ENGINE_DIR = ROOT_DIR / "firing_rules"
SHORT_DIR = ROOT_DIR / "tests" / "short"
EXPECTED_DIR = ROOT_DIR / "tests" / "short_expected"

# Import engine modules from the local project checkout instead of requiring an
# installed package.
if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))

from provenance.api import explain_why_not  # noqa: E402


def _canonicalize(value: Any) -> Any:
    # Recursively normalize dict/list ordering so equality checks are stable even
    # when engine internals emit semantically equivalent data in different orders.
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in sorted(value.items())}

    if isinstance(value, list):
        canonical_items = [_canonicalize(item) for item in value]
        return sorted(canonical_items, key=lambda item: json.dumps(item, sort_keys=True))

    return value


def _run_short_case(case_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Shared harness for all short cases: locate files, force in-memory deterministic
    # execution, run the engine, and compare against the expected JSON snapshot.
    case_file = SHORT_DIR / case_name
    expected_path = EXPECTED_DIR / f"{case_file.stem}.json"

    assert case_file.exists(), f"Missing short test input: {case_file}"
    assert expected_path.exists(), f"Missing expected output: {expected_path}"

    # Clear DB-related env vars so these tests always use local in-memory facts.
    monkeypatch.setenv("WHY_NOT_DB_CONNECTION", "")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("POSTGRES_CONNECTION_STRING", "")
    # Pin workers to one thread to avoid nondeterministic ordering across runs.
    monkeypatch.setenv("WHY_NOT_MAX_WORKERS", "1")

    query_text = case_file.read_text(encoding="utf-8")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    actual = explain_why_not(query_text)

    assert _canonicalize(actual) == _canonicalize(expected)


def test_01_missing_join(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("01_missing_join.txt", monkeypatch)


def test_02_negation_blocks_derivation(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("02_negation_blocks_derivation.txt", monkeypatch)


def test_03_comparison_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("03_comparison_failure.txt", monkeypatch)


def test_04_sql_equality_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("04_sql_equality_miss.txt", monkeypatch)


def test_05_sql_not_exists_short(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("05_sql_not_exists_short.txt", monkeypatch)


def test_06_sql_selection_short(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_short_case("06_sql_selection_short.txt", monkeypatch)

