# SQL-Focused Test Plan for WHY-NOT Provenance

This document defines **SQL test cases** to evaluate:

1. **Correctness** — does the system return the right WHY-NOT output?
2. **Completeness** — does it include all relevant failed derivations?
3. **Informativeness** — are failures explained clearly at goal level?

---

## How to run

From `firing_rules/`:

```bash
python3 why_not_provenance.py < ../tests/<case>.txt > ../tests/<case>.json
```

Quick inspect:

```bash
jq '.target, .failed_derivation_count, .message' ../tests/<case>.json
```

---

## Common expectations for all cases

- Output has `mode = "WHYNOT"`
- `target` matches the question tuple
- `failed_derivations[*].status` is always `false`
- `explanation_graph` contains:
  - missing tuple node
  - failed rule nodes
  - failed goal nodes

---

## Case SQL-01: Basic projection + selection

**Purpose:** baseline correctness on simple SQL.

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city = 'Singapore';

WHYNOT Q(10,'York')
```

**Checks**
- SQL parses successfully
- Failed goals include grounded predicates/comparisons tied to tuple `(10,'York')`

---

## Case SQL-02: Inner join missing match

**Purpose:** correctness of join failure explanation.

```text
SELECT t1.src, t2.dst
FROM Train t1
JOIN Train t2 ON t1.dst = t2.src;

WHYNOT Q(s,n)
```

**Checks**
- Failed derivations explain missing join path
- Goal-level failures indicate which join-side fact is missing

---

## Case SQL-03: Join + filter

**Purpose:** distinguish join failure vs filter failure.

```text
SELECT c.c_id, o.o_id
FROM customers c
JOIN orders o ON c.c_id = o.c_id
WHERE o.status = 'PAID';

WHYNOT Q(1,500)
```

**Checks**
- If join exists but status mismatches, provenance should show comparison failure
- If join does not exist, provenance should show failed positive atom goal

---

## Case SQL-04: NOT EXISTS anti-join

**Purpose:** validate negation handling in SQL translation.

```text
SELECT t1.src, t2.dst
FROM Train t1 JOIN Train t2 ON t1.dst = t2.src
WHERE NOT EXISTS (
  SELECT 1 FROM Train t3
  WHERE t3.src = t1.src AND t3.dst = t2.dst
);

WHYNOT Q(n,s)
```

**Checks**
- Failed goals should explicitly include grounded `not ...` goal outcome
- Explanation should tell whether tuple is excluded because anti-join condition failed

---

## Case SQL-05: Multiple SQL blocks

**Purpose:** completeness across multiple generated SQL rules.

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city = 'Singapore';

SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city = 'Tokyo';

WHYNOT Q(10,'York')
```

**Checks**
- Engine handles multiple SQL blocks deterministically
- Relevant failed derivations are not dropped

---

## Case SQL-06: Column alias handling

**Purpose:** correctness of alias/column resolution.

```text
SELECT w.w_id AS id, w.w_street AS street
FROM warehouses AS w
WHERE w.w_city = 'Singapore';

WHYNOT Q(10,'York')
```

**Checks**
- SQL aliasing is resolved correctly
- Same logical result as non-aliased query

---

## Case SQL-07: Numeric comparison predicates

**Purpose:** correctness of comparison operators.

```text
SELECT i.item_id, i.price
FROM items i
WHERE i.price > 100;

WHYNOT Q(7,80)
```

**Checks**
- Failed comparison goal is grounded (e.g., `80 > 100` false)

---

## Case SQL-08: String literal quoting

**Purpose:** robust handling of quoted constants.

```text
SELECT w.w_id, w.w_city
FROM warehouses w
WHERE w.w_city = 'New York';

WHYNOT Q(3,'New York')
```

**Checks**
- Literal parsing is correct
- No malformed atom/goal from spaces in string

---

## Case SQL-09: Backend connection via `.env`

**Purpose:** integration correctness with PostgreSQL mode.

`.env`:

```text
WHY_NOT_DB_CONNECTION=postgresql://user:pass@host:5432/db
```

Input:

```text
SELECT w.w_id, w.w_street
FROM warehouses w
WHERE w.w_city='Singapore';

WHYNOT Q(10,'York')
```

**Checks**
- Runs without inline `CONNECTION:` directive
- `query_results` is populated

---

## Case SQL-10: Concurrency consistency

**Purpose:** ensure thread-safe deterministic output.

Run same SQL case with:

```bash
WHY_NOT_MAX_WORKERS=1 python3 why_not_provenance.py < ../tests/sql-02.txt > out1.json
WHY_NOT_MAX_WORKERS=4 python3 why_not_provenance.py < ../tests/sql-02.txt > out4.json
WHY_NOT_MAX_WORKERS=8 python3 why_not_provenance.py < ../tests/sql-02.txt > out8.json
```

**Checks**
- Same `failed_derivation_count`
- Same logical failed derivations
- No race-related corruption in `binding` / `goal_results`

---

## Evaluating provenance quality

For each case, score 0–2:

- **Correctness**
  - 0: wrong/missing result
  - 1: partially right
  - 2: correct

- **Completeness**
  - 0: relevant failures missing
  - 1: partial coverage
  - 2: complete coverage under active binding strategy

- **Informativeness**
  - 0: vague
  - 1: somewhat useful
  - 2: clear, grounded, actionable

Total score = sum across chosen cases.

---

## Notes

- If join-driven binding is active, exhaustive Cartesian-style hypothetical failures may be reduced.
- When comparing completeness, keep binding strategy and worker count fixed.
