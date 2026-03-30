# WHY-NOT Provenance Engine

This project implements a **firing-rules WHY-NOT provenance engine** with:
- Datalog-style facts/rules
- SQL-to-Datalog translation
- Optional PostgreSQL backend loading + SQL execution

The codebase has been refactored into smaller modules for easier maintenance.

---

## Project structure

```text
.
├── why_not_provenance.py          # CLI wrapper
└── provenance/
    ├── __init__.py
    ├── api.py                     # public API: explain_why_not
    ├── constants.py               # shared constants
    ├── helpers.py                 # parsing/eval helper functions
    ├── sql_rule_builder.py        # SQL AST + SQL -> Datalog Rule
    ├── postgres_backend.py        # PostgreSQL adapter
    ├── input_parser.py            # unified parser (facts/rules/sql/question)
    ├── engine.py                  # WHY-NOT provenance evaluator
    └── models/
        ├── __init__.py
        ├── atom.py                # Atom dataclass
        ├── goal.py                # Goal dataclass
        ├── rule.py                # Rule dataclass
        ├── provenance_question.py # ProvenanceQuestion dataclass
        ├── program.py             # Program dataclass
        ├── sql_table_ref.py       # SQLTableRef dataclass
        └── sql_query.py           # SQLQuery dataclass
```

> Dataclasses are split as **1 dataclass per file** under `provenance/models/`.

---

## Install

### Python dependency for PostgreSQL (optional)

```bash
pip install psycopg[binary]
```

Fallback driver supported: `psycopg2-binary`.

If you use PostgreSQL-backed mode, copy `.env.example` to `.env` and fill in your DB URL.

---

## Run

### From stdin

```bash
python3 why_not_provenance.py <<'EOF'
Train(n,c).
Train(c,s).
Train(n,w).
Train(w,s).
r1: Q(X,Y) :- Train(X,Z), Train(Z,Y), not Train(X,Y).
WHYNOT Q(s,n)
EOF
```

### From Python

```python
from provenance.api import explain_why_not

query = """
Train(n,c).
Train(c,s).
r1: Q(X,Y) :- Train(X,Z), Train(Z,Y), not Train(X,Y).
WHYNOT Q(s,n)
"""

result = explain_why_not(query)
print(result["failed_derivation_count"])
```

---

## Input format

You can mix these blocks in one input string:

1. **SCHEMA**
```text
SCHEMA Train(src,dst).
```

2. **EDB facts**
```text
Train(n,c).
```

3. **Datalog rules**
```text
r1: Q(X,Y) :- Train(X,Z), Train(Z,Y), not Train(X,Y).
```

4. **SQL query block** (`SELECT ... ;` or `SQL: SELECT ... ;`)

5. **Provenance question**
```text
WHYNOT Q(s,n)
```

6. **PostgreSQL connection string via environment variable (optional)**

Create a `.env` file in the project root:

```text
WHY_NOT_DB_CONNECTION=postgresql://user:pass@localhost:5432/mydb
```

Supported env keys (first non-empty one is used):
`WHY_NOT_DB_CONNECTION`, `DATABASE_URL`, `POSTGRES_CONNECTION_STRING`.

> Backward compatibility: inline directives like `CONNECTION: ...` are still accepted, but `.env`/env vars are now the preferred approach.

---

## PostgreSQL-backed mode

When a connection string is available via `.env`/environment variable:

1. Referenced base tables are loaded from PostgreSQL into EDB.
2. SQL blocks are executed directly against PostgreSQL.
3. Provenance runs on the normalized rules/data.
4. Raw SQL result payloads are returned in `query_results`.

### Example

```bash
# .env
# WHY_NOT_DB_CONNECTION=postgresql://user:pass@localhost:5432/mydb

python3 why_not_provenance.py <<'EOF'
SELECT t1.src, t2.dst
FROM Train t1 JOIN Train t2 ON t1.dst = t2.src
WHERE NOT EXISTS (
  SELECT 1 FROM Train t3
  WHERE t3.src = t1.src AND t3.dst = t2.dst
);

WHYNOT Q(s,n)
EOF
```

---

## Output fields

Top-level JSON includes:
- `mode`
- `target`
- `failed_derivation_count`
- `failed_derivations`
- `explanation_graph`
- `query_results` (non-empty when SQL blocks are executed via backend)
- `message`

---

## Notes / limitations

- Current explainer output supports **WHYNOT** only.
- SQL support is intentionally limited (no aggregates/OR/complex nesting).
- Rule evaluation is non-recursive and order-sensitive.
