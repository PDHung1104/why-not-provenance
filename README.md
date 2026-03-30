# WHY-NOT Provenance Playground

This repository contains a Python prototype for **WHY-NOT provenance** using firing-rule style explanations, plus SQL test planning and sample TPC-C export data.

## What this repo is for

It helps you answer:

> “Why is tuple `Q(...)` **missing** from my query result?”

Instead of only returning empty/non-empty results, the system returns:
- failed derivations
- per-goal success/failure
- an explanation graph (tuple -> rule -> failed goals)

---

## Repository layout

```text
.
├── firing_rules/          # Main implementation (engine + parser + CLI)
│   ├── why_not_provenance.py
│   ├── provenance/
│   ├── README.md
│   └── firing_rules.md
├── tests/                 # SQL-focused provenance test plan
│   └── README.md
└── TPC_C_export/          # Sample SQL schema/data files (TPC-C subset)
    ├── TPCCSchema.sql
    ├── TPCCItems.sql
    ├── TPCCWarehouses.sql
    ├── TPCCStocks.sql
    ├── TPCCClean.sql
    └── TPCCReadme.txt
```

---

## Quick start

1. Enter the project implementation directory:

```bash
cd firing_rules
```

2. Run on an input file:

```bash
python3 why_not_provenance.py < test.txt > test_result.json
```

3. (Optional) Enable PostgreSQL-backed mode with `.env`:

```env
WHY_NOT_DB_CONNECTION=postgresql://user:pass@host:5432/db
```

Supported env keys:
- `WHY_NOT_DB_CONNECTION`
- `DATABASE_URL`
- `POSTGRES_CONNECTION_STRING`

You can also control concurrency:

```bash
WHY_NOT_MAX_WORKERS=4 python3 why_not_provenance.py < test.txt > test_result.json
```

---

## What to read first

- `firing_rules/README.md` — implementation usage and input format
- `firing_rules/firing_rules.md` — firing-rules background/concepts
- `tests/README.md` — SQL-focused evaluation plan (correctness, completeness, informativeness)
- `TPC_C_export/TPCCReadme.txt` — context for bundled TPC-C data files

---

## Notes

- Current output mode is focused on **WHYNOT** explanation.
- SQL support is intentionally limited to a practical subset for provenance experiments.
- Completeness depends on active binding strategy (join-driven vs exhaustive style), so keep configuration fixed when benchmarking.
