# WHY-NOT Provenance Engine (`why_not_provenance.py`)

This repository contains a single Python module, `why_not_provenance.py`, that implements a **firing-rules WHY-NOT provenance engine**.

It supports two input styles:
1. **Datalog-like facts + rules**
2. **SQL `SELECT-FROM-(JOIN)-WHERE`** (translated into one Datalog rule internally)

The engine explains why a target tuple is missing by enumerating candidate derivations and recording which goals fail.

---

## What the engine does

Given:
- base facts (EDB)
- optional rules (IDB)
- a provenance question (`WHY ...` / `WHYNOT ...`)

it returns JSON containing:
- failed derivations relevant to the target tuple
- per-goal success/failure for each derivation
- an explanation graph (tuple -> failed rule instances -> failed goals)

> Current execution output is **WHYNOT only**. `WHY` is parsed but not executed as a WHY explainer yet.

---

## Quick start

## 1) Run from stdin (Datalog style)

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

## 2) Run SQL style input

```bash
python3 why_not_provenance.py <<'EOF'
SCHEMA Train(src,dst).
Train(n,c).
Train(c,s).
Train(n,w).
Train(w,s).

SELECT t1.src, t2.dst
FROM Train t1 JOIN Train t2 ON t1.dst = t2.src
WHERE NOT EXISTS (
  SELECT 1 FROM Train t3
  WHERE t3.src = t1.src AND t3.dst = t2.dst
);

WHYNOT Q(s,n)
EOF
```

In SQL mode, the generated rule head predicate defaults to the target predicate in the provenance question (here `Q`).

---

## Input format

The parser accepts mixed sections in one text block.

### A) Comments
Use `#` inline or full-line comments.

```text
Train(n,c).  # base edge
```

### B) Facts (EDB)

```text
Predicate(a,b,...).
```

### C) Schemas (optional, required for SQL translation)

```text
SCHEMA Train(src,dst).
```

- If schema is omitted for a predicate that has EDB facts, fallback names are inferred (`c1`, `c2`, ...).
- For SQL alias/column resolution, explicit schemas are strongly recommended.

### D) Rules (Datalog)

```text
[rule_id:] Head(...) :- Goal1, Goal2, ... .
```

Goal types:
- positive atom: `Train(X,Z)`
- negated atom: `not Train(X,Y)`
- comparison: `X = Y`, `X != Y`, `X > 3`, `X <= 10`

### E) SQL query block

Starts with either:
- `SQL: SELECT ...;` (single/multi-line)
- `SELECT ...;` (single/multi-line)

Supported subset:
- `SELECT <items>`
- `FROM <table alias>` with comma joins or explicit `JOIN ... ON ...`
- `WHERE cond AND cond ...`
- `NOT EXISTS (SELECT ... FROM table alias WHERE ... )`

### F) Provenance question

```text
WHY Q(a,b)
WHYNOT Q(a,b)
```

`?` suffix is optional.

---

## Design overview

The module is intentionally split into layers so parsing/translation is decoupled from provenance evaluation.

## 1. Core data model
Dataclasses define normalized structures used everywhere:
- `Atom(predicate, terms)`
- `Goal(kind, ...)` where kind is `pos`, `neg`, or `cmp`
- `Rule(rule_id, head, body)`
- `ProvenanceQuestion(mode, target)`
- `Program(edb, schemas, rules, question)`

**Design intent:** keep frontend parsing concerns separate from evaluator logic by normalizing everything into `Program`.

## 2. Helpers
Utility functions handle lexical tasks:
- comment stripping (`_strip_comment`)
- quote normalization (`_strip_quotes`)
- variable detection (`_is_variable`: uppercase-leading tokens)
- robust top-level splitting (`_split_top_level`, `_split_top_level_and`)
- atom/goal parsing (`_parse_atom`, `_parse_goal`)

**Design intent:** centralize syntax handling to keep parser/evaluator simple.

## 3. SQL frontend (`SQLRuleBuilder`)
`SQLRuleBuilder` parses a supported SQL subset into `SQLQuery`, then translates it into one Datalog `Rule`.

Main responsibilities:
- parse `SELECT/FROM/WHERE`
- parse table refs and aliases (`Train t1`, `Train AS t1`)
- map `alias.column -> variable` using schema
- convert conditions to `Goal`
- convert `NOT EXISTS` to a **negated atom**

**Design intent:** SQL support is isolated; evaluator remains SQL-agnostic.

## 4. Unified input parser (`InputParser`)
Consumes full input text and produces a normalized `Program`.

It handles:
- schemas
- facts
- datalog rules
- SQL blocks
- provenance question

Then it:
- infers missing schemas from EDB arity
- translates SQL blocks into Datalog rules

**Design intent:** allow mixed authoring styles and future frontends, all targeting the same `Program` shape.

## 5. Provenance evaluator (`ProvenanceEngine`)
Pipeline in `explain_why_not`:

1. Parse input into `Program`
2. Materialize rule heads (non-recursive, order-sensitive)
3. Focus on rules with head predicate = target predicate
4. Compute target-aware variable domains
5. Enumerate bindings (Cartesian product)
6. Evaluate each goal under each binding
7. Keep failed derivations whose grounded head matches target
8. Build explanation graph

**Design intent:** explicit firing records for transparency/debuggability.

## 6. Explanation graph builder
Builds graph payload:
- tuple node (missing target)
- failed rule-instance nodes
- failed-goal nodes

Edges:
- tuple -> failed rule instance
- failed rule instance -> failed goals

---

## Key component functionality (function/class map)

- `InputParser.parse(...)`
  - Main normalization entrypoint.
- `SQLRuleBuilder.parse_sql(...)`
  - Produces SQL AST (`SQLQuery`).
- `SQLRuleBuilder.to_rule(...)`
  - SQL AST -> Datalog `Rule`.
- `ProvenanceEngine.explain_why_not(...)`
  - End-to-end execution and JSON response.
- `ProvenanceEngine._compute_domains_for_full_eval(...)`
  - Active-domain setup for full rule materialization.
- `ProvenanceEngine._compute_variable_domains(...)`
  - Target-constrained domains for WHY-NOT analysis.
- `ProvenanceEngine._enumerate_bindings(...)`
  - Cartesian product over variable domains.
- `ProvenanceEngine._evaluate_goal(...)`
  - Evaluate positive/negative/comparison goals.
- `ProvenanceEngine._build_firing_records(...)`
  - Build per-binding provenance records.
- `ProvenanceEngine._build_explanation_graph(...)`
  - Transform failures into graph nodes/edges.
- `explain_why_not(query_text)`
  - Public importable API function.
- `main()`
  - CLI wrapper (stdin or argv[1], JSON output).

---

## Output JSON structure

Top-level keys:
- `mode`: `"WHYNOT"`
- `target`: grounded target atom string
- `failed_derivation_count`
- `failed_derivations`: list of records:
  - `rule_id`
  - `binding`
  - `goal_results` (`goal_index`, `goal`, `ok`)
  - `status`
- `explanation_graph`:
  - `nodes`
  - `edges`
- `message`

---

## Usage as a library

```python
from why_not_provenance import explain_why_not

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

## Limitations and assumptions

- Non-recursive rule evaluation only.
- Materialization is rule-order sensitive.
- WHY queries are parsed but not yet produced as WHY explanations.
- SQL support is intentionally restricted (no aggregates, OR, nested join trees, etc.).
- `NOT EXISTS` subquery support expects equality constraints on subquery alias columns.

---

## Practical authoring tips

- Prefer explicit `SCHEMA` lines when using SQL.
- Keep variable names uppercase-initial; constants lowercase or quoted.
- End SQL blocks with `;` so parser can detect block end.
- Use comments generously—inline `# ...` is supported.
