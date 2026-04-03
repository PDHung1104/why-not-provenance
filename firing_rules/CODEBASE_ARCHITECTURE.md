# WHY-NOT Provenance Engine: Codebase Architecture

This document explains how the system is structured, how data flows through it, and what each component contributes.

---

## 1) System purpose

The project implements a **firing-rules WHY-NOT provenance engine** for a small Datalog + SQL subset.

Given an input program and a target tuple (e.g., `WHYNOT Q(301,'Sunbrook')`), the engine explains why the tuple is missing by returning:
- failed derivations,
- grounded failed goals,
- an explanation graph.

It can run in:
- **Pure in-memory mode** (facts/rules in input), or
- **PostgreSQL-backed mode** (load tables + execute SQL blocks).

---

## 2) High-level architecture

```text
CLI/API
  -> InputParser
      -> (optional) PostgresBackend
      -> SQLRuleBuilder
      -> Program (normalized IR)
  -> ProvenanceEngine
      -> materialize rule heads into relations
      -> evaluate target rules for missing tuple
      -> collect failed derivations
      -> build explanation graph
  -> JSON output
```

Core modules:
- `why_not_provenance.py` (CLI entrypoint)
- `provenance/api.py` (public Python API)
- `provenance/input_parser.py` (input normalization)
- `provenance/sql_rule_builder.py` (SQL -> Datalog rule)
- `provenance/engine.py` (provenance evaluation)
- `provenance/postgres_backend.py` (DB adapter)
- `provenance/helpers.py` + `provenance/models/*` (shared parsing/eval/model types)

---

## 3) End-to-end flow

### Step A: Entry layer
- **CLI** (`why_not_provenance.py`) reads stdin/argv and prints JSON.
- **API** (`provenance/api.py`) exposes `explain_why_not(query_text)`.

### Step B: Parse and normalize (`InputParser`)
`InputParser.parse()` consumes mixed input blocks:
- `SCHEMA ...`
- EDB facts (`R(a,b).`)
- Datalog rules (`r1: Q(X) :- ...`)
- SQL blocks (`SELECT ...;` or `SQL: SELECT ...;`)
- provenance question (`WHYNOT ...`)
- optional DB connection (`CONNECTION: ...` or env/.env)

Output is a `Program` dataclass with:
- `edb`: base facts by predicate,
- `schemas`: relation schemas,
- `rules`: explicit + SQL-translated rules,
- `question`: target atom + mode,
- `sql_queries` and `sql_results` (if DB mode).

### Step C: Optional PostgreSQL integration
If a connection string exists:
1. referenced tables are discovered,
2. base rows are fetched into EDB,
3. SQL blocks are executed (results stored in `query_results`).

Loading/execution can be parallelized via `WHY_NOT_MAX_WORKERS`.

### Step D: SQL translation (`SQLRuleBuilder`)
Each SQL block is converted into one Datalog-like rule:
- `FROM/JOIN` tables become positive relational goals,
- `ON` and `WHERE` comparisons become comparison goals,
- `NOT EXISTS (...)` becomes a negated atom goal (`kind='neg'`),
- `SELECT` expressions become rule head terms.

### Step E: Provenance evaluation (`ProvenanceEngine`)
`ProvenanceEngine.explain_why_not()`:
1. Builds initial relation store from EDB.
2. Performs forward head materialization for rules (collect successful heads).
3. Selects rules whose head predicate matches target predicate.
4. Enumerates bindings (join-driven on positive goals, then expands remaining vars).
5. Grounds and evaluates each goal:
   - positive atom membership,
   - negated atom absence,
   - comparison operators.
6. Collects relevant **failed derivations** for target tuple.
7. Builds explanation graph: tuple -> failed rule instance -> failed goals.

---

## 4) Component-by-component responsibilities

## `provenance/models/*` (domain model)
Small dataclasses used across the pipeline:
- `Atom`: predicate + terms
- `Goal`: one rule-body goal (`pos`, `neg`, `cmp`)
- `Rule`: head + body
- `ProvenanceQuestion`: mode + target
- `Program`: fully normalized input program
- `SQLQuery`, `SQLTableRef`: simplified SQL AST

**Contribution:** keeps parser, translator, and engine decoupled with explicit typed objects.

## `provenance/helpers.py`
Utility functions for:
- stripping comments/quotes,
- splitting top-level comma/AND expressions,
- parsing atoms/goals,
- variable detection,
- term resolution,
- comparison evaluation.

**Important detail:** variable detection now uses an ALL_CAPS pattern (`[A-Z_][A-Z0-9_]*`) to avoid treating string literals like `Singapore` as variables.

**Contribution:** central syntax + evaluation primitives reused by parser/engine.

## `provenance/input_parser.py`
Transforms free-form mixed input into `Program`.
Handles:
- env/.env connection-string discovery,
- SQL block collection,
- optional table preloading from PostgreSQL,
- SQL query execution capture,
- SQL-to-rule conversion.

**Contribution:** single normalization boundary; downstream engine works on uniform internal representation.

## `provenance/sql_rule_builder.py`
Implements constrained SQL parser + lowering to `Rule`.
Supports:
- `SELECT ... FROM ...`
- joins (`JOIN ... ON ...`)
- conjunctions (`AND`)
- comparisons (`=, !=, <, <=, >, >=`)
- limited `NOT EXISTS` patterns.

**Contribution:** lets provenance engine treat SQL and Datalog uniformly.

## `provenance/postgres_backend.py`
Minimal adapter over `psycopg`/`psycopg2`:
- safe identifier quoting,
- streamed table reads,
- SQL execution with batched fetches,
- normalization of row values to string tuples.

**Contribution:** bridges runtime data source with in-memory provenance engine.

## `provenance/engine.py`
Core WHY-NOT logic:
- domain computation,
- binding generation (join-driven + enumeration fallback),
- goal grounding and truth evaluation,
- failed derivation filtering for target tuple,
- explanation graph construction,
- optional multithreaded collection across target rules.

**Contribution:** produces the actual provenance explanation payload.

## `provenance/api.py` + `why_not_provenance.py`
Thin wrappers around the engine.

**Contribution:** clean programmatic API + CLI integration.

---

## 5) Execution semantics and design choices

- **Non-recursive, rule-based evaluation:** rules are evaluated with generated bindings; successful records materialize head tuples.
- **Join-driven binding strategy:** positive relational goals narrow bindings before full variable expansion.
- **String-based value model:** DB and facts are normalized to strings; numeric operators cast when possible.
- **WHY-NOT focus:** output emphasizes failed derivations and failed goals.
- **Graph output:** explanation is emitted both tabular (`failed_derivations`) and structural (`explanation_graph`).

---

## 6) Multiple SQL blocks (current behavior)

`InputParser` stores all SQL blocks and translates each to a separate rule (`sql_r1`, `sql_r2`, ...).
- If there is exactly one SQL block, its head predicate is the question predicate.
- With multiple SQL blocks, generated heads are `QSQL1`, `QSQL2`, ...

This means the provenance question must target the matching predicate for intended behavior; otherwise no target rules are found.

---

## 7) Extensibility points

If you want to extend this codebase, the clean seams are:
- **SQL support:** extend `SQLRuleBuilder` parsing/lowering.
- **Goal semantics:** add new goal kinds and evaluation logic in `engine.py`.
- **Storage backends:** add backend adapters similar to `PostgresBackend`.
- **Provenance ranking/filtering:** post-process `failed_derivations` before output.
- **Output formats:** add serializers (e.g., Graphviz, HTML) from explanation graph.

---

## 8) Known constraints

- SQL subset is intentionally limited (no aggregates/OR/complex nesting).
- WHY mode is parsed but engine currently returns WHY-NOT behavior only.
- Value typing is mostly string-first with numeric coercion for comparisons.
- Completeness/size can be sensitive to domain expansion for unbound vars.

---

## 9) Practical mental model

Think of the system as:
1. **Normalize everything to rules + facts**,
2. **Try all relevant bindings for the target tuple**,
3. **Record exactly which goals fail**,
4. **Return those failures as an explanation graph and JSON report**.

That is the core architecture and contribution of each component.
