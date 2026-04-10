# Short correctness test cases

These are tiny, deterministic test cases for quick algorithm verification.
All previously Datalog-focused checks are rewritten as SQL equivalents for easier reading.

For pure in-memory behavior, disable DB env vars when running (otherwise SQL blocks execute against PostgreSQL if `.env` is present).

Run from repository root:

```bash
cd firing_rules
WHY_NOT_DB_CONNECTION='' DATABASE_URL='' POSTGRES_CONNECTION_STRING='' \
python3 why_not_provenance.py < ../tests/short/01_missing_join.txt
```

## Cases

1. `01_missing_join.txt`
   - SQL self-join + `NOT EXISTS` equivalent of 2-hop path-minus-direct-edge.
   - Verifies failed derivation capture with multiple candidate bindings.

2. `02_negation_blocks_derivation.txt`
   - SQL self-join + `NOT EXISTS` where anti-join condition blocks target tuple.
   - Verifies grounded negated-goal failures.

3. `03_comparison_failure.txt`
   - SQL numeric filter fails (`n.x > 2` with target `2`).
   - Verifies comparison-goal evaluation.

4. `04_sql_equality_miss.txt`
   - Minimal SQL equality filter miss (`b.v = 'a'` with target `c`).
   - Verifies simple grounded comparison failure.

5. `05_sql_not_exists_short.txt`
   - Minimal SQL + `NOT EXISTS` anti-join.
   - Verifies SQL lowering to negated atom goal.

6. `06_sql_selection_short.txt`
   - Minimal SQL selection with string predicate.
   - Verifies grounded comparison output for SQL filter.
