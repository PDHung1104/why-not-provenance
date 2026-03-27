Firing Rules Algorithm

Purpose

Use firing rules to explain:

• WHY a tuple exists in a query result
• WHY-NOT a tuple is missing from a query result

A firing-rule system does not only compute the final query output. It also records:

• which derivations were considered
• which derivations succeeded
• which derivations failed
• which specific goals caused failure

This makes it suitable for provenance and explanation systems.

───

Core Idea

Given an original Datalog rule:

r1: Q(X, Y) :-
Train(X, Z),
Train(Z, Y),
not Train(X, Y).

a firing-rule system generates an instrumented version of this rule.

For each relevant variable binding, it records:

• variable values
• whether each goal succeeded
• whether the overall derivation succeeded

So instead of only knowing whether Q(X, Y) exists, we know how it succeeded or why it failed.

───

Terminology

EDB predicate

A base relation stored in the database.

Example:

• Train
• Listing
• Availability

IDB predicate

A derived relation defined by rules.

Example:

• Q
• AvailableSharedRoom

Head predicate

The predicate on the left-hand side of a rule.

Example:

Q(X, Y) :- ...

Here, Q is the head predicate.

Goal

A predicate or condition in the body of a rule.

Example:

Train(X, Z)
not Train(X, Y)
X > 3

Derivation

A specific binding of all variables in a rule.

Example:

X = n, Y = s, Z = c

This is one possible derivation of Q(n, s).

Firing rule

An instrumented rule that records per-goal success/failure for each derivation.

───

High-Level Algorithm

Step 1: Convert the query into Datalog rules

Start with a non-recursive query expressed in Datalog form.

Example:

r1: Q(X, Y) :-
Train(X, Z),
Train(Z, Y),
not Train(X, Y).

This is the original rule whose provenance will be explained.

───

Step 2: Capture the provenance question

The user asks one of:

• WHY Q(a, b)
• WHYNOT Q(a, b)

This step determines:

• target predicate
• target tuple or tuple pattern
• whether we are collecting successful or failed derivations

Example:

WHYNOT Q(s, n)

means:

• explain why Q(s, n) is absent

───

Step 3: Define domains for variables

For why-not provenance, the system must consider possible derivations that could have produced the missing answer.

To keep this finite and meaningful, assign a domain to each variable.

Example:

dom(X) = {n, s, c, w}
dom(Y) = {n, s, c, w}
dom(Z) = {n, s, c, w}

These domains are usually derived from:

• active domain of the database
• attribute domains
• user constraints

───

Step 4: Generate firing rules

For each original rule, generate an instrumented relation that records:

• variable bindings
• goal-level status
• overall derivation status

Example schema for a firing relation:

F_r1(X, Y, Z, g1_ok, g2_ok, g3_ok, status)

where:

• g1_ok = whether Train(X, Z) succeeds
• g2_ok = whether Train(Z, Y) succeeds
• g3_ok = whether not Train(X, Y) succeeds
• status = whether the whole derivation succeeds

───

Step 5: Enumerate candidate derivations

Enumerate all relevant variable bindings consistent with:

• the provenance question
• variable domains

Example for WHYNOT Q(s, n):

(s, n, c)
(s, n, w)
(s, n, s)
(s, n, n)

Each tuple above is a candidate derivation.

───

Step 6: Evaluate every goal for every derivation

For each candidate derivation, check each goal independently.

Example derivation:

X = s, Y = n, Z = w

Evaluate:

Train(s, w)
Train(w, n)
not Train(s, n)

Suppose results are:

Train(s, w) = false
Train(w, n) = false
not Train(s,n)= true

Then record:

g1_ok = false
g2_ok = false
g3_ok = true
status = false

───

Step 7: Compute overall success/failure

A derivation succeeds only if all goals succeed.

For positive and negated goals:

status = g1_ok AND g2_ok AND ... AND gn_ok

Interpretation:

• status = true -> successful derivation
• status = false -> failed derivation

───

Step 8: Prune to derivations relevant to the provenance question

Do not compute provenance for the entire database unless necessary.
Keep only derivations whose head matches the provenance question.

Example:

If the question is:

WHYNOT Q(s, n)

then keep only derivations whose head is Q(s, n).

This is a major optimization.

───

Step 9: Build the explanation graph

Construct a provenance graph using three node types:

Tuple nodes

Represent existing or missing tuples.

Examples:

• Q(s, n)
• Train(s, w)

Rule nodes

Represent derivations.

Examples:

• r1(s, n, w)
• r1(s, n, c)

Goal nodes

Represent goals within a derivation.

Examples:

• first goal of r1(s, n, w)
• second goal of r1(s, n, w)

Edge structure

For a WHY explanation:

• tuple node -> successful rule nodes
• successful rule node -> successful goal nodes

For a WHY-NOT explanation:

• missing tuple node -> failed rule nodes
• failed rule node -> failed goal nodes only

───

Step 10: Return explanation

WHY query

Return successful derivations explaining presence.

WHY-NOT query

Return failed derivations explaining absence.

The main idea is:

A tuple is missing because every possible relevant derivation failed.

───

Minimal Example

Database

Train

| from | to |
| ---- | --- |
| n    | c  |
| c    | s  |
| n    | w  |
| w    | s  |

Query

r1: Q(X, Y) :-
Train(X, Z),
Train(Z, Y),
not Train(X, Y).

───

Example 1: WHY Q(n, s)

Candidate derivations:

• (n, s, c)
• (n, s, w)

Both succeed because:

• Train(n, c) and Train(c, s) exist
• Train(n, w) and Train(w, s) exist
• Train(n, s) does not exist

So Q(n, s) exists.

───

Example 2: WHY-NOT Q(s, n)

Candidate derivations:

• (s, n, c)
• (s, n, w)
• (s, n, s)
• (s, n, n)

All fail because the needed train links are missing.

So Q(s, n) is absent because all derivations fail.

───

Example Firing Table

For WHYNOT Q(s, n):

| X | Y | Z | g1_ok | g2_ok | g3_ok | status |
| --- | --- | --- | ----- | ----- | ----- | ------ |
| s | n | c | false | false | true  | false  |
| s | n | w | false | false | true  | false  |
| s | n | s | false | false | true  | false  |
| s | n | n | false | false | true  | false  |

This table is the core data structure for explanation.

───

SQL Middleware Interpretation

In a SQL implementation, firing rules are typically compiled into SQL that produces rows like:

(X, Y, Z, g1_ok, g2_ok, g3_ok, status)

Typical mappings:

• positive goal -> join / existence check
• negated goal -> anti-join / NOT EXISTS
• comparison goal -> boolean expression
• full rule success -> conjunction of goal flags

───

Pseudocode

function explain_provenance(query_rules, provenance_question, database):
datalog_program = normalize_to_datalog(query_rules)

target_predicate = provenance_question.predicate
target_pattern = provenance_question.pattern
mode = provenance_question.mode # WHY or WHYNOT

domains = compute_variable_domains(datalog_program, database, target_pattern)

firing_relations = []

for rule in datalog_program:
candidate_bindings = enumerate_bindings(rule, domains, target_pattern)

for binding in candidate_bindings:
goal_results = []

for goal in rule.body:
result = evaluate_goal(goal, binding, database)
goal_results.append(result)

status = all(goal_results)

firing_relations.append({
"rule": rule.id,
"binding": binding,
"goal_results": goal_results,
"status": status
})

relevant = filter_by_target_head(firing_relations, target_predicate, target_pattern)

if mode == "WHY":
relevant = keep_successful_derivations(relevant)
else:
relevant = keep_failed_derivations(relevant)

explanation_graph = build_explanation_graph(relevant, database, mode)

return explanation_graph

───

Implementation Notes

For a coding agent

When implementing this, keep the logic separated into these modules:

1. parser

• query to Datalog normalization

2. domain_builder

• compute safe domains for variables

3. binding_enumerator

• generate candidate bindings

4. goal_evaluator

• evaluate positive, negated, and comparison goals

5. firing_relation_builder

• create per-rule status records

6. provenance_filter
• keep only derivations relevant to the provenance question

7. graph_builder

• convert firing records into tuple/rule/goal graph

This separation makes testing easier.

───

Recommended Data Model

Use a record like:

{
"rule_id": "r1",
"binding": {"X": "s", "Y": "n", "Z": "w"},
"goal_results": [
{"goal_index": 1, "goal": "Train(X,Z)", "ok": false},
{"goal_index": 2, "goal": "Train(Z,Y)", "ok": false},
{"goal_index": 3, "goal": "not Train(X,Y)", "ok": true}
],
"status": false
}

This structure is easy to:

• debug
• serialize
• turn into a graph
• summarize later

───

Key Insight

A normal query engine answers:

Is the tuple in the result?

A firing-rule provenance engine answers:

Which derivations were considered, and which exact goals made them succeed or fail?

That is the essence of the approach.