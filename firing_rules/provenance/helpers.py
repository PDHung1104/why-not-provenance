import re
from typing import Dict, List, Optional

from .constants import COMPARISON_OPS
from .models import Atom, Goal


def strip_comment(line: str) -> str:
    return line.split("#", 1)[0].strip()


def strip_quotes(value: str) -> str:
    value = value.strip()
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    return value


def is_variable(term: str) -> bool:
    # Variables are represented in ALL_CAPS style (e.g., X, Y, W_W_CITY).
    # This avoids misclassifying string literals like "Singapore" as variables.
    return re.fullmatch(r"[A-Z_][A-Z0-9_]*", term) is not None


def split_top_level(s: str, delimiter: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == delimiter and depth == 0:
            piece = "".join(buf).strip()
            if piece:
                parts.append(piece)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def split_top_level_and(s: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    i = 0
    upper = s.upper()
    while i < len(s):
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1

        if depth == 0 and upper[i : i + 3] == "AND":
            left_ok = i == 0 or upper[i - 1].isspace()
            right_ok = i + 3 >= len(s) or upper[i + 3].isspace()
            if left_ok and right_ok:
                piece = "".join(buf).strip()
                if piece:
                    parts.append(piece)
                buf = []
                i += 3
                continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_atom(text: str) -> Atom:
    s = text.strip().rstrip(".")
    m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)", s)
    if not m:
        raise ValueError(f"Invalid atom syntax: {text}")
    pred = m.group(1)
    terms = [strip_quotes(t.strip()) for t in split_top_level(m.group(2).strip(), ",")]
    return Atom(pred, terms)


def parse_goal(goal_text: str) -> Goal:
    raw = goal_text.strip()
    if raw.lower().startswith("not "):
        return Goal(kind="neg", atom=parse_atom(raw[4:].strip()), raw=raw)

    for op in COMPARISON_OPS:
        idx = raw.find(op)
        if idx > 0:
            left = raw[:idx].strip()
            right = raw[idx + len(op) :].strip()
            if left and right and "(" not in left and ")" not in left:
                return Goal(kind="cmp", op=op, left=strip_quotes(left), right=strip_quotes(right), raw=raw)

    return Goal(kind="pos", atom=parse_atom(raw), raw=raw)


def resolve_term(term: str, binding: Dict[str, str]) -> str:
    return binding[term] if is_variable(term) else term


def eval_comparison(left: str, op: str, right: str) -> bool:
    def to_num(s: str) -> Optional[float]:
        try:
            return float(s)
        except ValueError:
            return None

    l_num = to_num(left)
    r_num = to_num(right)
    l_val, r_val = (l_num, r_num) if l_num is not None and r_num is not None else (left, right)

    if op == "=":
        return l_val == r_val
    if op == "!=":
        return l_val != r_val
    if op == ">":
        return l_val > r_val
    if op == "<":
        return l_val < r_val
    if op == ">=":
        return l_val >= r_val
    if op == "<=":
        return l_val <= r_val
    raise ValueError(f"Unsupported operator: {op}")
