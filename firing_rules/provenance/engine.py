import itertools
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import AbstractSet, Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .helpers import eval_comparison, is_variable, resolve_term
from .input_parser import InputParser
from .models import Atom, Goal, Program, Rule


def ground_head(head: Atom, binding: Dict[str, str]) -> Tuple[str, ...]:
    return tuple(resolve_term(t, binding) for t in head.terms)


class ProvenanceEngine:
    """WHY-NOT provenance evaluator using firing-rule records."""

    _DEFAULT_MAX_WORKERS = min(8, max(1, (os.cpu_count() or 4)))

    def __init__(self) -> None:
        self.parser = InputParser()

    @classmethod
    def _max_workers(cls) -> int:
        raw = os.getenv("WHY_NOT_MAX_WORKERS", "").strip()
        if not raw:
            return cls._DEFAULT_MAX_WORKERS
        try:
            return max(1, int(raw))
        except ValueError:
            return cls._DEFAULT_MAX_WORKERS

    def explain_why_not(self, query_text: str) -> Dict[str, Any]:
        program = self.parser.parse(query_text)
        if program.question.mode != "WHYNOT":
            raise ValueError("This engine currently returns WHY-NOT only. Use WHYNOT ...")

        target = program.question.target
        relations: Dict[str, set] = {pred: set(rows) for pred, rows in program.edb.items()}

        for rule in program.rules:
            domains = self._compute_domains_for_full_eval(rule, relations)
            bindings = self._iter_bindings_join_driven(rule, domains, relations)
            head_tuples = {
                ground_head(rule.head, rec["binding"])
                for rec in self._iter_firing_records(rule, bindings, relations)
                if rec["status"]
            }
            relations.setdefault(rule.head.predicate, set()).update(head_tuples)

        target_rules = [r for r in program.rules if r.head.predicate == target.predicate]
        if not target_rules:
            raise ValueError(f"No rule defines target predicate {target.predicate}")

        failed_relevant: List[Dict[str, Any]] = []
        target_tuple = tuple(target.terms)
        workers = self._max_workers()
        relations_ro: Mapping[str, frozenset] = {pred: frozenset(rows) for pred, rows in relations.items()}

        if workers == 1 or len(target_rules) == 1:
            for rule in target_rules:
                failed_relevant.extend(self._collect_failed_for_rule(rule, program, target, target_tuple, relations_ro))
        else:
            ordered_results: List[Optional[List[Dict[str, Any]]]] = [None] * len(target_rules)
            with ThreadPoolExecutor(max_workers=min(workers, len(target_rules))) as executor:
                futures = {
                    executor.submit(self._collect_failed_for_rule, rule, program, target, target_tuple, relations_ro): idx
                    for idx, rule in enumerate(target_rules)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    ordered_results[idx] = future.result()
            for chunk in ordered_results:
                if chunk:
                    failed_relevant.extend(chunk)

        return {
            "mode": "WHYNOT",
            "target": f"{target.predicate}({', '.join(target.terms)})",
            "failed_derivation_count": len(failed_relevant),
            "failed_derivations": failed_relevant,
            "explanation_graph": self._build_explanation_graph(target, failed_relevant),
            "query_results": program.sql_results,
            "message": (
                "Tuple is missing because every relevant derivation failed."
                if failed_relevant
                else "No failed derivations found for target head under current domains."
            ),
        }

    def _compute_domains_for_full_eval(
        self, rule: Rule, relations: Mapping[str, AbstractSet[Tuple[str, ...]]]
    ) -> Dict[str, List[str]]:
        active = set()
        for rows in relations.values():
            for tup in rows:
                active.update(tup)

        vars_in_rule = set()
        for t in rule.head.terms:
            if is_variable(t):
                vars_in_rule.add(t)
        for g in rule.body:
            if g.kind in ("pos", "neg"):
                vars_in_rule.update([x for x in g.atom.terms if is_variable(x)])
            else:
                if is_variable(g.left):
                    vars_in_rule.add(g.left)
                if is_variable(g.right):
                    vars_in_rule.add(g.right)

        return {v: sorted(active) for v in vars_in_rule}

    def _compute_variable_domains(self, program: Program, rule: Rule, target: Atom) -> Dict[str, List[str]]:
        active = set()
        for rows in program.edb.values():
            for tup in rows:
                active.update(tup)

        domains: Dict[str, List[str]] = {}
        for h, t in zip(rule.head.terms, target.terms):
            if is_variable(h):
                domains[h] = [t] if not is_variable(t) else sorted(active)

        vars_in_rule = set()
        for t in rule.head.terms:
            if is_variable(t):
                vars_in_rule.add(t)
        for g in rule.body:
            if g.kind in ("pos", "neg"):
                vars_in_rule.update([x for x in g.atom.terms if is_variable(x)])
            else:
                if is_variable(g.left):
                    vars_in_rule.add(g.left)
                if is_variable(g.right):
                    vars_in_rule.add(g.right)

        for v in vars_in_rule:
            domains.setdefault(v, sorted(active))

        return domains

    @staticmethod
    def _variables_in_rule(rule: Rule) -> List[str]:
        vars_in_rule = set()
        for t in rule.head.terms:
            if is_variable(t):
                vars_in_rule.add(t)
        for g in rule.body:
            if g.kind in ("pos", "neg"):
                vars_in_rule.update([x for x in g.atom.terms if is_variable(x)])
            else:
                if is_variable(g.left):
                    vars_in_rule.add(g.left)
                if is_variable(g.right):
                    vars_in_rule.add(g.right)
        return sorted(vars_in_rule)

    def _initial_binding_from_head(
        self,
        rule: Rule,
        domains: Dict[str, List[str]],
        target_tuple: Optional[Tuple[str, ...]] = None,
    ) -> Optional[Dict[str, str]]:
        binding: Dict[str, str] = {}
        expected_terms: Optional[Sequence[str]] = target_tuple

        if expected_terms is None:
            expected_terms = [None] * len(rule.head.terms)

        for head_term, expected in zip(rule.head.terms, expected_terms):
            if is_variable(head_term):
                if expected is None:
                    continue
                if head_term in binding and binding[head_term] != expected:
                    return None
                if head_term in domains and expected not in domains[head_term]:
                    return None
                binding[head_term] = expected
            else:
                if expected is not None and head_term != expected:
                    return None

        return binding

    def _unify_atom_with_tuple(
        self,
        atom: Atom,
        row: Tuple[str, ...],
        binding: Dict[str, str],
        domains: Dict[str, List[str]],
    ) -> Optional[Dict[str, str]]:
        if len(atom.terms) != len(row):
            return None

        next_binding = dict(binding)
        for term, value in zip(atom.terms, row):
            if is_variable(term):
                if term in next_binding and next_binding[term] != value:
                    return None
                if term in domains and value not in domains[term]:
                    return None
                next_binding[term] = value
            else:
                if term != value:
                    return None

        return next_binding

    def _iter_joined_positive_bindings(
        self,
        positive_goals: Sequence[Goal],
        idx: int,
        current: Dict[str, str],
        domains: Dict[str, List[str]],
        relations: Mapping[str, AbstractSet[Tuple[str, ...]]],
    ) -> Iterator[Dict[str, str]]:
        if idx >= len(positive_goals):
            yield current
            return

        goal = positive_goals[idx]
        rows = relations.get(goal.atom.predicate, set())
        for row in rows:
            next_binding = self._unify_atom_with_tuple(goal.atom, row, current, domains)
            if next_binding is None:
                continue
            yield from self._iter_joined_positive_bindings(positive_goals, idx + 1, next_binding, domains, relations)

    def _expand_unbound_variables(
        self,
        base_binding: Dict[str, str],
        var_order: Sequence[str],
        domains: Dict[str, List[str]],
    ) -> Iterator[Dict[str, str]]:
        missing = [v for v in var_order if v not in base_binding]
        if not missing:
            yield dict(base_binding)
            return

        domain_subset = {v: domains.get(v, []) for v in missing}
        for addon in self._enumerate_bindings(domain_subset, missing):
            merged = dict(base_binding)
            merged.update(addon)
            yield merged

    def _iter_bindings_join_driven(
        self,
        rule: Rule,
        domains: Dict[str, List[str]],
        relations: Mapping[str, AbstractSet[Tuple[str, ...]]],
        target_tuple: Optional[Tuple[str, ...]] = None,
    ) -> Iterator[Dict[str, str]]:
        base = self._initial_binding_from_head(rule, domains, target_tuple)
        if base is None:
            return

        var_order = self._variables_in_rule(rule)
        positive_goals = [g for g in rule.body if g.kind == "pos"]

        if positive_goals:
            source = self._iter_joined_positive_bindings(positive_goals, 0, base, domains, relations)
        else:
            source = iter((base,))

        for partial in source:
            yield from self._expand_unbound_variables(partial, var_order, domains)

    @staticmethod
    def _enumerate_bindings(domains: Dict[str, List[str]], var_order: Sequence[str]) -> Iterator[Dict[str, str]]:
        if not var_order:
            yield dict()
            return
        for vals in itertools.product(*(domains[v] for v in var_order)):
            yield {v: vals[i] for i, v in enumerate(var_order)}

    def _evaluate_goal(
        self, goal: Goal, binding: Dict[str, str], relations: Mapping[str, AbstractSet[Tuple[str, ...]]]
    ) -> Tuple[bool, str]:
        if goal.kind in ("pos", "neg"):
            grounded = tuple(resolve_term(t, binding) for t in goal.atom.terms)
            exists = grounded in relations.get(goal.atom.predicate, set())
            if goal.kind == "pos":
                return exists, f"{goal.atom.predicate}({', '.join(grounded)})"
            return (not exists), f"not {goal.atom.predicate}({', '.join(grounded)})"

        left = resolve_term(goal.left, binding)
        right = resolve_term(goal.right, binding)
        return eval_comparison(left, goal.op, right), f"{left} {goal.op} {right}"

    def _iter_firing_records(
        self,
        rule: Rule,
        bindings: Iterable[Dict[str, str]],
        relations: Mapping[str, AbstractSet[Tuple[str, ...]]],
    ) -> Iterator[Dict[str, Any]]:
        for b in bindings:
            goal_results = []
            for i, g in enumerate(rule.body, start=1):
                ok, grounded_goal = self._evaluate_goal(g, b, relations)
                goal_results.append({"goal_index": i, "goal": grounded_goal, "ok": ok})
            yield {
                "rule_id": rule.rule_id,
                "binding": dict(b),
                "goal_results": goal_results,
                "status": all(x["ok"] for x in goal_results),
            }

    def _collect_failed_for_rule(
        self,
        rule: Rule,
        program: Program,
        target: Atom,
        target_tuple: Tuple[str, ...],
        relations: Mapping[str, frozenset],
    ) -> List[Dict[str, Any]]:
        failed: List[Dict[str, Any]] = []
        domains = self._compute_variable_domains(program, rule, target)
        bindings = self._iter_bindings_join_driven(rule, domains, relations, target_tuple)
        for rec in self._iter_firing_records(rule, bindings, relations):
            if ground_head(rule.head, rec["binding"]) != target_tuple:
                continue
            if not rec["status"]:
                failed.append(rec)
        return failed

    @staticmethod
    def _build_explanation_graph(target: Atom, failed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []

        tuple_id = f"tuple:{target.predicate}({', '.join(target.terms)})"
        nodes.append({"id": tuple_id, "type": "tuple", "label": f"{target.predicate}({', '.join(target.terms)})", "missing": True})

        for rec in failed_records:
            b = rec["binding"]
            btxt = ", ".join(f"{k}={v}" for k, v in sorted(b.items()))
            rid = f"rule:{rec['rule_id']}:{btxt}"
            nodes.append({"id": rid, "type": "rule", "label": f"{rec['rule_id']}({btxt})", "status": "failed"})
            edges.append({"from": tuple_id, "to": rid})

            for g in rec["goal_results"]:
                if g["ok"]:
                    continue
                gid = f"goal:{rec['rule_id']}:{btxt}:g{g['goal_index']}"
                nodes.append({"id": gid, "type": "goal", "label": g["goal"], "ok": False})
                edges.append({"from": rid, "to": gid})

        return {"nodes": nodes, "edges": edges}
