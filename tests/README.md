# SQL Test Cases (TPC-C Data) for WHY-NOT Provenance

This test plan is aligned with `../TPC_C_export/TPCCReadme.txt` and the provided schema/data files:

- `items(i_id, i_im_id, i_name, i_price)`
- `warehouses(w_id, w_name, w_street, w_city, w_country)`
- `stocks(w_id, i_id, s_qty)`

Goal: evaluate
1. **Correctness**
2. **Completeness**
3. **Informativeness** of provenance output.

---

## 0) One-time DB setup

Load data into PostgreSQL:

```bash
psql "$WHY_NOT_DB_CONNECTION" -f ../TPC_C_export/TPCCSchema.sql
psql "$WHY_NOT_DB_CONNECTION" -f ../TPC_C_export/TPCCItems.sql
psql "$WHY_NOT_DB_CONNECTION" -f ../TPC_C_export/TPCCWarehouses.sql
psql "$WHY_NOT_DB_CONNECTION" -f ../TPC_C_export/TPCCStocks.sql
```

Set env in `firing_rules/.env`:

```env
WHY_NOT_DB_CONNECTION=postgresql://user:pass@host:5432/db
```

Run pattern:

```bash
cd ../firing_rules
python3 why_not_provenance.py < ../tests/<case>.txt > ../tests/<case>.json
```

---

## 1) Correctness cases

### SQL-01: Simple selection on warehouses

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city = 'Singapore';

WHYNOT Q(10,'York')
```

Why this is valid: `warehouses` has Singapore rows (e.g., w_id 301, 281, 22, 1004, 3), so `(10,'York')` is a plausible missing tuple.

Checks:
- Output is WHYNOT for `Q(10, York)`
- Failed goals are grounded (`warehouses(...)`, comparison goals)

---

### SQL-02: Selection that should succeed for an existing tuple (sanity)

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city = 'Singapore';

WHYNOT Q(301,'Sunbrook')
```

Checks:
- Should not produce a misleading missing-data narrative
- If failures are returned, verify they are not contradicting existing tuple membership logic

---

### SQL-03: Join stocks + items (existing combo)

```text
SELECT s.w_id, s.i_id
FROM stocks s
JOIN items i ON s.i_id = i.i_id
WHERE i.i_price > 90;

WHYNOT Q(301,1)
```

From provided data: item `1` has price `95.23` and stock `(301,1,338)` exists.

Checks:
- Provenance should reflect whether tuple is present under query semantics
- No incorrect failed-goal explanation for clearly satisfiable join

---

### SQL-04: Join + filter missing by predicate

```text
SELECT s.w_id, s.i_id
FROM stocks s
JOIN items i ON s.i_id = i.i_id
WHERE i.i_price > 99;

WHYNOT Q(301,1)
```

Item 1 price is `95.23`, so price predicate should fail.

Checks:
- Failure should point to grounded comparison (e.g., `95.23 > 99` false)

---

### SQL-05: NOT EXISTS anti-join on stocks

```text
SELECT w.w_id, i.i_id
FROM warehouses w JOIN items i ON i.i_id = 1
WHERE NOT EXISTS (
  SELECT 1 FROM stocks s
  WHERE s.w_id = w.w_id AND s.i_id = i.i_id
);

WHYNOT Q(301,1)
```

Stock `(301,1)` exists, so anti-join should exclude it.

Checks:
- Failed provenance should clearly indicate why NOT EXISTS condition fails
- Negated goal should appear grounded

---

## 2) Completeness cases

### SQL-06: Multi-join with multiple candidate failures

```text
SELECT w.w_id, i.i_id
FROM warehouses w
JOIN stocks s ON s.w_id = w.w_id
JOIN items i ON i.i_id = s.i_id
WHERE w.w_city = 'Singapore' AND i.i_price > 95;

WHYNOT Q(22,10)
```

Checks:
- Relevant failed derivations are all captured (under active binding strategy)
- Failures may come from city condition, join linkage, or price filter

---

### SQL-07: Multiple SQL blocks (deterministic handling)

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_country = 'Singapore';

SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_country = 'Malaysia';

WHYNOT Q(1,'Green')
```

Checks:
- Engine handles both SQL blocks without dropping relevant failures
- Results deterministic across repeated runs

---

## 3) Informativeness cases

### SQL-08: Goal-level readability

```text
SELECT s.w_id, s.i_id
FROM stocks s
JOIN items i ON s.i_id = i.i_id
WHERE i.i_price < 5;

WHYNOT Q(301,1)
```

Checks:
- `failed_derivations[*].goal_results` includes grounded, human-readable goals
- Explanation graph has tuple -> failed rule -> failed goals edges

---

### SQL-09: String literal handling

```text
SELECT w.w_id, w.w_city
FROM warehouses w
WHERE w.w_country = 'Singapore';

WHYNOT Q(1,'Patemon')
```

Checks:
- String constants parsed correctly
- Explanation clearly identifies failing condition (`w_country = Singapore`)

---

## 4) Concurrency and stability

Use one stable case (e.g., SQL-01) and run with different workers:

```bash
WHY_NOT_MAX_WORKERS=1 python3 why_not_provenance.py < ../tests/sql-01.txt > /tmp/out1.json
WHY_NOT_MAX_WORKERS=4 python3 why_not_provenance.py < ../tests/sql-01.txt > /tmp/out4.json
WHY_NOT_MAX_WORKERS=8 python3 why_not_provenance.py < ../tests/sql-01.txt > /tmp/out8.json
```

Checks:
- Same `failed_derivation_count`
- Same logical failed derivations
- No malformed bindings/goal results (thread-safety signal)

---

## 5) Suggested scoring rubric

For each case, score 0–2:

- **Correctness**: wrong (0), partial (1), correct (2)
- **Completeness**: missing relevant failures (0), partial (1), complete (2)
- **Informativeness**: vague (0), somewhat useful (1), clear/actionable (2)

Total score = sum across selected cases.

---

## Notes

- Keep comparison settings fixed when evaluating completeness:
  - same dataset
  - same worker count
  - same binding strategy (join-driven vs exhaustive)
- Current engine supports a practical SQL subset; avoid aggregates/OR/complex nesting in benchmark cases.
