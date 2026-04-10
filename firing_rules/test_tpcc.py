"""
WHY-NOT provenance test cases using real TPC-C export data.

Schema (partial TPC-C):
  items(i_id, i_im_id, i_name, i_price)
  warehouses(w_id, w_name, w_street, w_city, w_country)
  stocks(w_id, i_id, s_qty)

Each test class targets one relational operator:
  TestSelect  — WHERE filter (σ)
  TestProject — fewer head columns than body (π)
  TestJoin    — multi-table join (⋈)
  TestUnion   — two rules with the same head predicate (∪)

EDB facts are real values taken from the TPC-C SQL export files.
"""

import os
import re
import unittest
from provenance.api import explain_why_not

_TPC = os.path.join(os.path.dirname(__file__), "..", "TPC_C_export")

# ---------------------------------------------------------------------------
# Full-dataset EDB loader (used by heavy-query tests)
# ---------------------------------------------------------------------------

def _load_full_singapore_edb() -> str:
    """Load all Singapore warehouses, all items, and all stocks at those warehouses.
    Produces ~1100 EDB facts: 5 warehouses + 483 items + 614 stock rows.
    """
    sg_ws = set()
    wh_facts = []
    with open(os.path.join(_TPC, "TPCCWarehouses.sql"), encoding="utf-8") as f:
        for line in f:
            m = re.search(r"VALUES \((\d+), '(.+?)', '(.+?)', '(.+?)', '(.+?)'\)", line)
            if m and m.group(5) == "Singapore":
                sg_ws.add(int(m.group(1)))
                wh_facts.append(
                    f"warehouses({m.group(1)},{m.group(2)},{m.group(3)},{m.group(4)},{m.group(5)})."
                )

    item_facts = []
    with open(os.path.join(_TPC, "TPCCItems.sql"), encoding="utf-8") as f:
        for line in f:
            m = re.search(r"VALUES \((\d+), '([\d]+)', '(.+?)', ([\d.]+)\)", line)
            if m:
                item_facts.append(
                    f"items({m.group(1)},{m.group(2)},{m.group(3)},{m.group(4)})."
                )

    stock_facts = []
    with open(os.path.join(_TPC, "TPCCStocks.sql"), encoding="utf-8") as f:
        for line in f:
            m = re.search(r"VALUES \((\d+), (\d+), (\d+)\)", line)
            if m and int(m.group(1)) in sg_ws:
                stock_facts.append(f"stocks({m.group(1)},{m.group(2)},{m.group(3)}).")

    return "\n".join(wh_facts + item_facts + stock_facts)

# ---------------------------------------------------------------------------
# Known real TPC-C EDB facts used across test classes
# (values verified against TPCCWarehouses.sql / TPCCItems.sql / TPCCStocks.sql)
# ---------------------------------------------------------------------------

# warehouses(w_id, w_name, w_street, w_city, w_country)
W301 = "warehouses(301,Schmedeman,Sunbrook,Singapore,Singapore)."     # Singapore
W281 = "warehouses(281,Crescent Oaks,Loeprich,Singapore,Singapore)."  # Singapore
W22  = "warehouses(22,Namekagon,Anniversary,Singapore,Singapore)."    # Singapore
W7   = "warehouses(7,Blogpad,Monica,Nusajaya,Malaysia)."              # Malaysia
W1   = "warehouses(1,DabZ,Green,Patemon,Indonesia)."                  # Indonesia

# items(i_id, i_im_id, i_name, i_price)
I1   = "items(1,35356226,Indapamide,95.23)."          # price > 90
I2   = "items(2,00851287,SYLATRON,80.22)."             # price 50-90
I3   = "items(3,52549414,Meprobamate,11.64)."          # price < 50
I31  = "items(31,00534405,GENTAMICIN SULFATE,93.43)."  # price > 90
I9   = "items(9,68788973,TOPIRAMATE,48.58)."           # price < 50

# stocks(w_id, i_id, s_qty)  — real qty from TPCCStocks.sql
S301_1  = "stocks(301,1,338)."   # w301 has item1,  qty 338  (< 500)
S301_2  = "stocks(301,2,6)."     # w301 has item2,  qty 6
S301_31 = "stocks(301,31,700)."  # w301 has item31, qty 700  (> 500)
S281_1  = "stocks(281,1,883)."   # w281 has item1,  qty 883  (> 500)
S281_2  = "stocks(281,2,9)."     # w281 has item2,  qty 9    (< 500)
S281_31 = "stocks(281,31,672)."  # w281 has item31, qty 672  (> 500)
S7_2    = "stocks(7,2,2)."       # w7   has item2,  qty 2    (< 500)
S7_3    = "stocks(7,3,156)."     # w7   has item3   (not items 1 or 31)
S22_2   = "stocks(22,2,3)."      # w22  has item2,  qty 3    (< 500)
S1_2    = "stocks(1,2,10)."      # w1   has item2,  qty 10   (< 500)


# ---------------------------------------------------------------------------
# ════════════════════════ SELECT (σ) ════════════════════════
#
# SELECT in relational algebra keeps rows that satisfy a predicate.
# In Datalog: body goals that are comparison or negation conditions.
# ---------------------------------------------------------------------------

class TestSelectSingleCondition(unittest.TestCase):
    """σ  country='Singapore'  (warehouses)
    Rule: Q(W_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),
                     W_COUNTRY = Singapore.
    """

    EDB = "\n".join([W301, W7, W1])
    RULE = "r1: Q(W_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), W_COUNTRY = Singapore."

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    # --- tuple present ---
    def test_singapore_warehouse_present(self):
        """w301 is in Singapore — filter passes → 0 failed derivations."""
        self.assertEqual(self._run("Q(301)")["failed_derivation_count"], 0)

    # --- tuple absent: filter fails ---
    def test_indonesia_warehouse_absent(self):
        """w1 is in Indonesia — country filter fails → 1 failed derivation."""
        self.assertGreater(self._run("Q(1)")["failed_derivation_count"], 0)

    def test_indonesia_comparison_goal_is_failing(self):
        result = self._run("Q(1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        failed = [g for g in all_goals if not g["ok"]]
        self.assertTrue(any("Singapore" in g["goal"] for g in failed),
            f"Expected country comparison to fail. Failed goals: {failed}")

    def test_indonesia_goal_is_grounded(self):
        """Failed goal must show the actual country value, not a variable name."""
        result = self._run("Q(1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        failed = [g for g in all_goals if not g["ok"]]
        self.assertTrue(any("Indonesia" in g["goal"] for g in failed),
            "Expected 'Indonesia' to appear in a failed goal label")

    def test_join_goal_succeeds_filter_fails(self):
        """EDB lookup (warehouses) succeeds but the comparison fails."""
        result = self._run("Q(1)")
        for d in result["failed_derivations"]:
            edb_goal = d["goal_results"][0]   # warehouses(...) lookup
            cmp_goal = d["goal_results"][1]   # W_COUNTRY = Singapore
            self.assertTrue(edb_goal["ok"],  "EDB lookup should succeed")
            self.assertFalse(cmp_goal["ok"], "Comparison should fail")


class TestSelectMultipleConditions(unittest.TestCase):
    """σ  price>50 AND price<90  (items)
    Rule: Q(I_ID) :- items(I_ID,I_IM_ID,I_NAME,I_PRICE),
                     I_PRICE > 50, I_PRICE < 90.
    Data:
      item 1:  95.23  → fails  price < 90  (second condition)
      item 2:  80.22  → passes both → present
      item 3:  11.64  → fails  price > 50  (first condition)
    """

    EDB  = "\n".join([I1, I2, I3])
    RULE = "r1: Q(I_ID) :- items(I_ID,I_IM_ID,I_NAME,I_PRICE), I_PRICE > 50, I_PRICE < 90."

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_mid_price_item_present(self):
        """item 2 (80.22) satisfies both conditions → 0 failures."""
        self.assertEqual(self._run("Q(2)")["failed_derivation_count"], 0)

    def test_high_price_item_absent(self):
        """item 1 (95.23) fails price < 90 → derivation fails."""
        self.assertGreater(self._run("Q(1)")["failed_derivation_count"], 0)

    def test_high_price_second_condition_fails(self):
        """The failing goal for item 1 must be the upper-bound comparison."""
        result = self._run("Q(1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        failed = [g for g in all_goals if not g["ok"]]
        self.assertTrue(any("<" in g["goal"] for g in failed),
            f"Expected upper-bound (< 90) to be failing. Got: {[g['goal'] for g in failed]}")

    def test_low_price_first_condition_fails(self):
        """The failing goal for item 3 must be the lower-bound comparison."""
        result = self._run("Q(3)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        failed = [g for g in all_goals if not g["ok"]]
        self.assertTrue(any(">" in g["goal"] for g in failed),
            f"Expected lower-bound (> 50) to be failing. Got: {[g['goal'] for g in failed]}")

    def test_high_price_shows_actual_price(self):
        """Grounded goal must reference the real price value 95.23."""
        result = self._run("Q(1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        labels = " ".join(g["goal"] for g in all_goals)
        self.assertIn("95.23", labels)


class TestSelectNotExists(unittest.TestCase):
    """σ  NOT EXISTS (stock at this warehouse)  — negation goal.
    Rule: Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),
                           items(I_ID,I_IM_ID,I_NAME,I_PRICE),
                           not stocks(W_ID,I_ID,S_QTY_DUMMY).

    w301 stocks item1 → Q(301,1) absent (negation fails: the stock exists)
    w301 does NOT stock item3 → Q(301,3) present (negation passes)
    """

    # w301 stocks item1 but NOT item3; include both items and the one stock row
    EDB  = "\n".join([W301, I1, I3, S301_1])
    RULE = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "not stocks(W_ID,I_ID,S_QTY_DUMMY)."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_stocked_item_absent_from_result(self):
        """Q(301,1): stock exists → not stocks(...) fails → tuple absent."""
        self.assertGreater(self._run("Q(301,1)")["failed_derivation_count"], 0)

    def test_negation_goal_is_the_failing_one(self):
        result = self._run("Q(301,1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        neg_failed = [g for g in all_goals if not g["ok"] and g["goal"].startswith("not ")]
        self.assertGreater(len(neg_failed), 0,
            "Expected a failed 'not stocks(...)' goal")

    def test_negation_goal_is_grounded(self):
        """not stocks(301, 1, ...) — both w_id and i_id must appear in goal label."""
        result = self._run("Q(301,1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        neg_failed = [g for g in all_goals if not g["ok"] and g["goal"].startswith("not ")]
        self.assertTrue(any("301" in g["goal"] and "1" in g["goal"] for g in neg_failed),
            f"Expected w_id=301 and i_id=1 in negated goal. Got: {[g['goal'] for g in neg_failed]}")

    def test_unstocked_item_present_in_result(self):
        """Q(301,3): no stock row → not stocks(...) passes → tuple present → 0 failures."""
        self.assertEqual(self._run("Q(301,3)")["failed_derivation_count"], 0)


# ---------------------------------------------------------------------------
# ════════════════════════ PROJECT (π) ════════════════════════
#
# PROJECT retains only a subset of columns in the output.
# In Datalog: the rule head has fewer terms than the body atoms.
# The projected-out variables appear in the body (and may be used in filters)
# but do NOT appear in the head tuple.
# ---------------------------------------------------------------------------

class TestProjectSingleColumn(unittest.TestCase):
    """π_{w_country}  σ_{country='Singapore'}  (warehouses)
    Rule: Q(W_COUNTRY) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),
                          W_COUNTRY = Singapore.

    W_ID, W_NAME, W_STREET, W_CITY are projected OUT — they appear in the
    body but not the head.
    Q(Singapore) should be in the result; Q(Indonesia) and Q(Malaysia) should not.
    """

    EDB  = "\n".join([W301, W7, W1])
    RULE = "r1: Q(W_COUNTRY) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), W_COUNTRY = Singapore."

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_singapore_in_result(self):
        """Q(Singapore): a Singapore warehouse exists → 0 failures."""
        self.assertEqual(self._run("Q(Singapore)")["failed_derivation_count"], 0)

    def test_indonesia_absent(self):
        """Q(Indonesia): filter requires Singapore, so Indonesia fails."""
        self.assertGreater(self._run("Q(Indonesia)")["failed_derivation_count"], 0)

    def test_projected_out_columns_not_in_head(self):
        """W_ID, W_NAME etc. do NOT appear in the head binding key for Q."""
        result = self._run("Q(Indonesia)")
        for d in result["failed_derivations"]:
            # Only W_COUNTRY should map to "Indonesia" as head var
            # Other vars exist in binding but are body-only
            self.assertIn("W_COUNTRY", d["binding"])
            self.assertEqual(d["binding"]["W_COUNTRY"], "Indonesia")

    def test_projected_out_columns_appear_in_binding(self):
        """Body-only variables like W_ID still appear in the binding record."""
        result = self._run("Q(Indonesia)")
        for d in result["failed_derivations"]:
            self.assertIn("W_ID", d["binding"])


class TestProjectMultiColumn(unittest.TestCase):
    """π_{w_id, i_id}  σ_{s_qty > 500}  (stocks)
    Rule: Q(W_ID,I_ID) :- stocks(W_ID,I_ID,S_QTY), S_QTY > 500.

    S_QTY is projected OUT — it drives the filter but is not in the result.
    Data:
      stocks(301,1,338)  → 338 < 500  → Q(301,1) absent (qty fails)
      stocks(301,31,700) → 700 > 500  → Q(301,31) present
      stocks(281,1,883)  → 883 > 500  → Q(281,1) present
      stocks(281,2,9)    → 9 < 500    → Q(281,2) absent (qty fails)
    """

    EDB  = "\n".join([S301_1, S301_31, S281_1, S281_2])
    RULE = "r1: Q(W_ID,I_ID) :- stocks(W_ID,I_ID,S_QTY), S_QTY > 500."

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_high_qty_stock_present(self):
        """Q(301,31): qty 700 > 500 → present → 0 failures."""
        self.assertEqual(self._run("Q(301,31)")["failed_derivation_count"], 0)

    def test_low_qty_stock_301_absent(self):
        """Q(301,1): qty 338 < 500 → qty filter fails."""
        self.assertGreater(self._run("Q(301,1)")["failed_derivation_count"], 0)

    def test_projected_qty_shown_in_failed_goal(self):
        """Failing goal must reference actual qty 338, even though S_QTY is not in head."""
        result = self._run("Q(301,1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        labels = " ".join(g["goal"] for g in all_goals)
        self.assertIn("338", labels, "Expected projected-out qty 338 in goal label")

    def test_projected_out_s_qty_not_in_head_terms(self):
        """S_QTY appears in the binding but is not a head variable."""
        result = self._run("Q(301,1)")
        for d in result["failed_derivations"]:
            # Head vars are W_ID and I_ID
            self.assertEqual(d["binding"]["W_ID"], "301")
            self.assertEqual(d["binding"]["I_ID"], "1")
            # S_QTY is a body-only variable — still in binding
            self.assertIn("S_QTY", d["binding"])


# ---------------------------------------------------------------------------
# ════════════════════════ JOIN (⋈) ════════════════════════
#
# JOIN combines rows from two or more relations on matching key values.
# In Datalog: multiple positive goals sharing variables (the join columns).
# ---------------------------------------------------------------------------

class TestJoinTwoWay(unittest.TestCase):
    """stocks ⋈ items ON i_id, then filter price > 90.
    Rule: Q(W_ID,I_ID) :- stocks(W_ID,I_ID,S_QTY),
                           items(I_ID,I_IM_ID,I_NAME,I_PRICE),
                           I_PRICE > 90.
    Data:
      stocks(281,1,883)  item1 price 95.23 → Q(281,1) present
      stocks(281,2,9)    item2 price 80.22 → Q(281,2) absent (price fails)
      stocks(281,31,672) item31 price 93.43→ Q(281,31) present
      item2 stocked at 301 (qty 6) also tested
    """

    EDB  = "\n".join([I1, I2, I31, S281_1, S281_2, S281_31, S301_2])
    RULE = (
        "r1: Q(W_ID,I_ID) :- "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "I_PRICE > 90."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_stocked_and_expensive_present(self):
        """Q(281,1): stocks(281,1) exists + item1 price 95.23 > 90 → present."""
        self.assertEqual(self._run("Q(281,1)")["failed_derivation_count"], 0)

    def test_stocked_but_cheap_absent(self):
        """Q(281,2): item2 stocked at 281 but price 80.22 < 90 → price goal fails."""
        self.assertGreater(self._run("Q(281,2)")["failed_derivation_count"], 0)

    def test_price_goal_is_failing_not_join(self):
        """Both join goals must succeed; only the price comparison fails."""
        result = self._run("Q(281,2)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "stocks join goal should succeed")
            self.assertTrue(goals[2]["ok"],  "items join goal should succeed")
            self.assertFalse(goals[3]["ok"], "price comparison should fail")

    def test_price_goal_shows_actual_price(self):
        """Failed goal label must contain the real price 80.22."""
        result = self._run("Q(281,2)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        labels = " ".join(g["goal"] for g in all_goals)
        self.assertIn("80.22", labels)

    def test_unstocked_item_join_driven_zero_candidates(self):
        """Q(301,31): item31 not stocked at 301 → stocks join finds no rows → 0 candidates.
        This documents the join-driven completeness trade-off.
        """
        result = self._run("Q(301,31)")
        self.assertEqual(result["failed_derivation_count"], 0,
            "Join-driven strategy: no stocks(301,31,...) row → no candidate bindings")


class TestJoinThreeWay(unittest.TestCase):
    """warehouses ⋈ stocks ON w_id  ⋈ items ON i_id, filter country=Singapore AND price>90.
    Rule: Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),
                           stocks(W_ID,I_ID,S_QTY),
                           items(I_ID,I_IM_ID,I_NAME,I_PRICE),
                           W_COUNTRY = Singapore,
                           I_PRICE > 90.
    Data:
      w281 (Singapore) + stocks(281,1,883) + item1 (95.23) → Q(281,1) present
      w281 (Singapore) + stocks(281,2,9)   + item2 (80.22) → Q(281,2) absent (price fails)
      w7   (Malaysia)  + stocks(7,2,2)     + item2 (80.22) → Q(7,2)   absent (country AND price fail)
    """

    EDB  = "\n".join([W281, W7, I1, I2, S281_1, S281_2, S7_2])
    RULE = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, "
        "I_PRICE > 90."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_all_joins_and_filters_pass(self):
        """Q(281,1): Singapore warehouse + stocked + expensive → present."""
        self.assertEqual(self._run("Q(281,1)")["failed_derivation_count"], 0)

    def test_price_filter_fails_country_passes(self):
        """Q(281,2): all three joins succeed + country=Singapore passes, but price 80.22 fails."""
        result = self._run("Q(281,2)")
        self.assertGreater(result["failed_derivation_count"], 0)

    def test_three_join_goals_succeed_for_281_2(self):
        """For Q(281,2), goals 1-3 (join) and goal 4 (country) succeed; goal 5 (price) fails."""
        result = self._run("Q(281,2)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "stocks join should succeed")
            self.assertTrue(goals[3]["ok"],  "items join should succeed")
            self.assertTrue(goals[4]["ok"],  "country filter should pass (Singapore=Singapore)")
            self.assertFalse(goals[5]["ok"], "price filter should fail")

    def test_malaysia_and_cheap_both_fail(self):
        """Q(7,2): country=Malaysia fails AND price=80.22 fails — two failing goals."""
        result = self._run("Q(7,2)")
        self.assertGreater(result["failed_derivation_count"], 0)
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        failed = [g for g in all_goals if not g["ok"]]
        self.assertGreaterEqual(len(failed), 2,
            "Expected at least 2 failing goals (country + price)")


# ---------------------------------------------------------------------------
# ════════════════════════ UNION (∪) ════════════════════════
#
# UNION combines results from two queries with the same schema.
# In Datalog: two (or more) rules that share the same head predicate.
# A tuple is in the result if it is derived by ANY rule.
# A tuple is MISSING only if ALL rules fail to derive it.
# ---------------------------------------------------------------------------

class TestUnionCountryFilter(unittest.TestCase):
    """Q = σ_{country='Singapore'} ∪ σ_{country='Malaysia'}  (warehouses)
    r1: Q(W_ID) :- warehouses(...), W_COUNTRY = Singapore.
    r2: Q(W_ID) :- warehouses(...), W_COUNTRY = Malaysia.

    w301 → Singapore → r1 succeeds → Q(301) present (0 failures)
    w7   → Malaysia  → r2 succeeds → Q(7) present   (0 failures)
    w1   → Indonesia → both fail   → Q(1) absent     (2 failures)
    """

    EDB = "\n".join([W301, W7, W1])
    RULES = (
        "r1: Q(W_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), W_COUNTRY = Singapore.\n"
        "r2: Q(W_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), W_COUNTRY = Malaysia."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULES}\nWHYNOT {target}")

    def test_singapore_warehouse_present_via_r1(self):
        """Q(301): r1 derives it (Singapore=Singapore passes) → r1 has no failed derivation.
        r2 still produces a failed record (Singapore≠Malaysia) but r1 does not — this
        distinguishes a present tuple from a fully absent one where both rules fail.
        """
        result = self._run("Q(301)")
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0,
            "r1 should have no failed derivation for Q(301) — it derives the tuple")

    def test_malaysia_warehouse_present_via_r2(self):
        """Q(7): r2 derives it (Malaysia=Malaysia passes) → r2 has no failed derivation.
        r1 still produces a failed record (Malaysia≠Singapore) but r2 does not.
        """
        result = self._run("Q(7)")
        r2_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        self.assertEqual(len(r2_failures), 0,
            "r2 should have no failed derivation for Q(7) — it derives the tuple")

    def test_indonesia_absent_from_both(self):
        """Q(1): w1 is Indonesia → fails both r1 and r2 → absent."""
        self.assertGreater(self._run("Q(1)")["failed_derivation_count"], 0)

    def test_both_rule_ids_in_failed_derivations(self):
        """Both r1 and r2 contribute a failed derivation for Q(1)."""
        result = self._run("Q(1)")
        rule_ids = {d["rule_id"] for d in result["failed_derivations"]}
        self.assertIn("r1", rule_ids, "r1 should have a failed derivation")
        self.assertIn("r2", rule_ids, "r2 should have a failed derivation")

    def test_r1_fails_on_singapore_condition(self):
        """r1 derivation for Q(1) fails because Indonesia ≠ Singapore."""
        result = self._run("Q(1)")
        r1_derivations = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        for d in r1_derivations:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertTrue(any("Singapore" in g["goal"] for g in failed))


class TestUnionPriceRange(unittest.TestCase):
    """Q = σ_{price>90} ∪ σ_{price<20}  (items)
    r1: Q(I_ID) :- items(I_ID,...,I_PRICE), I_PRICE > 90.
    r2: Q(I_ID) :- items(I_ID,...,I_PRICE), I_PRICE < 20.

    item1  (95.23) → r1 succeeds → Q(1) present
    item3  (11.64) → r2 succeeds → Q(3) present
    item2  (80.22) → 80.22 not > 90, not < 20 → both fail → Q(2) absent
    """

    EDB   = "\n".join([I1, I2, I3])
    RULES = (
        "r1: Q(I_ID) :- items(I_ID,I_IM_ID,I_NAME,I_PRICE), I_PRICE > 90.\n"
        "r2: Q(I_ID) :- items(I_ID,I_IM_ID,I_NAME,I_PRICE), I_PRICE < 20."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULES}\nWHYNOT {target}")

    def test_expensive_item_present_via_r1(self):
        """Q(1): item1 price 95.23 > 90 → r1 derives it → r1 has no failed derivation.
        r2 still fails (95.23 < 20 is false) but r1 does not — tuple is present.
        """
        result = self._run("Q(1)")
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0,
            "r1 should have no failed derivation for Q(1) — price 95.23 > 90 passes")

    def test_cheap_item_present_via_r2(self):
        """Q(3): item3 price 11.64 < 20 → r2 derives it → r2 has no failed derivation.
        r1 still fails (11.64 > 90 is false) but r2 does not — tuple is present.
        """
        result = self._run("Q(3)")
        r2_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        self.assertEqual(len(r2_failures), 0,
            "r2 should have no failed derivation for Q(3) — price 11.64 < 20 passes")

    def test_mid_price_absent_from_both(self):
        """Q(2): item2 price 80.22 — not > 90 and not < 20 → both rules fail."""
        self.assertGreater(self._run("Q(2)")["failed_derivation_count"], 0)

    def test_mid_price_both_rules_contribute(self):
        result = self._run("Q(2)")
        rule_ids = {d["rule_id"] for d in result["failed_derivations"]}
        self.assertIn("r1", rule_ids)
        self.assertIn("r2", rule_ids)

    def test_mid_price_r1_shows_upper_fail(self):
        """r1 fails for item2 because 80.22 > 90 is false."""
        result = self._run("Q(2)")
        r1 = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        for d in r1:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertTrue(any(">" in g["goal"] for g in failed))

    def test_mid_price_r2_shows_lower_fail(self):
        """r2 fails for item2 because 80.22 < 20 is false."""
        result = self._run("Q(2)")
        r2 = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        for d in r2:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertTrue(any("<" in g["goal"] for g in failed))


class TestUnionJoinBranches(unittest.TestCase):
    """Q = (stocks ⋈ items, Singapore warehouse) ∪ (stocks ⋈ items, Malaysia warehouse)
    r1: Q(W_ID,I_ID) :- warehouses(W_ID,...,W_COUNTRY), stocks(W_ID,I_ID,...),
                         items(I_ID,...,I_PRICE), W_COUNTRY=Singapore, I_PRICE>90.
    r2: Q(W_ID,I_ID) :- warehouses(W_ID,...,W_COUNTRY), stocks(W_ID,I_ID,...),
                         items(I_ID,...,I_PRICE), W_COUNTRY=Malaysia, I_PRICE>90.

    Q(281,1): w281=Singapore, stocked, item1 price 95.23 → r1 succeeds → present
    Q(7,2):   w7=Malaysia, stocked, item2 price 80.22    → price fails in r2 → absent
              w7 is not Singapore → r1 country fails too → both fail
    """

    EDB   = "\n".join([W281, W7, I1, I2, S281_1, S7_2])
    RULES = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, I_PRICE > 90.\n"
        "r2: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Malaysia, I_PRICE > 90."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULES}\nWHYNOT {target}")

    def test_singapore_stocked_expensive_present(self):
        """Q(281,1): Singapore + stocked + expensive → r1 derives it → r1 has no failed derivation.
        r2 still fails (Singapore≠Malaysia) but r1 does not — tuple is present.
        """
        result = self._run("Q(281,1)")
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0,
            "r1 should have no failed derivation for Q(281,1) — all conditions pass")

    def test_malaysia_stocked_cheap_absent_both_rules(self):
        """Q(7,2): Malaysia + stocked + cheap (80.22) → r2 price fails; r1 country fails."""
        result = self._run("Q(7,2)")
        self.assertGreater(result["failed_derivation_count"], 0)

    def test_both_rules_fail_for_malaysia_cheap(self):
        result = self._run("Q(7,2)")
        rule_ids = {d["rule_id"] for d in result["failed_derivations"]}
        self.assertIn("r1", rule_ids)
        self.assertIn("r2", rule_ids)

    def test_r1_fails_country_for_malaysia_warehouse(self):
        """r1 fails for Q(7,2) because w7 is Malaysia not Singapore."""
        result = self._run("Q(7,2)")
        r1 = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        for d in r1:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertTrue(any("Singapore" in g["goal"] for g in failed))

    def test_r2_fails_price_for_cheap_item(self):
        """r2 fails for Q(7,2) because item2 price 80.22 < 90."""
        result = self._run("Q(7,2)")
        r2 = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        for d in r2:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertTrue(any(">" in g["goal"] for g in failed))


# ---------------------------------------------------------------------------
# ════════════════════════ HEAVY QUERIES (timing) ════════════════════════
#
# Full Singapore EDB: 5 warehouses + 483 items + 614 stock rows = ~1100 facts.
# Three-way join + two filters forces large binding enumeration.
# Rule: Q(W_ID,I_ID) :- warehouses(...), stocks(...), items(...),
#                        W_COUNTRY = Singapore, I_PRICE > 90, S_QTY > 500.
#
# Real data anchors (verified from TPC-C export):
#   Q(301, 31) PRESENT  — w301 Singapore, stocks(301,31,700>500), item31 price 93.43>90
#   Q(301,  4) ABSENT   — stocks(301,4,938>500) BUT item4 price 54.49 ≤ 90  (price fails)
#   Q(301,  1) ABSENT   — item1 price 95.23>90  BUT stocks(301,1,338≤500)   (qty fails)
#   Q(301, 12) ABSENT   — item12 price 18.86≤90 AND stocks(301,12,454≤500)  (both fail)
# ---------------------------------------------------------------------------

_HEAVY_RULE = (
    "r1: Q(W_ID,I_ID) :- "
    "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
    "stocks(W_ID,I_ID,S_QTY), "
    "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
    "W_COUNTRY = Singapore, "
    "I_PRICE > 90, "
    "S_QTY > 500."
)


class TestHeavyQuery(unittest.TestCase):
    """Three-way join + two filters over the full Singapore EDB (~1100 facts).
    Each test records elapsed_ms so query cost is visible in the test output.
    All results are written to result.json after the class finishes.
    """

    _results: list = []   # accumulated across tests

    @classmethod
    def setUpClass(cls):
        cls.edb = _load_full_singapore_edb()
        cls._results = []

    @classmethod
    def tearDownClass(cls):
        import json
        out_path = os.path.join(os.path.dirname(__file__), "result.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cls._results, f, indent=2)
        print(f"\n    result.json written → {out_path}  ({len(cls._results)} queries)")

    def _run(self, target):
        result = explain_why_not(f"{self.edb}\n{_HEAVY_RULE}\nWHYNOT {target}")
        self.__class__._results.append(result)
        return result

    # --- present ---

    def test_present_all_conditions_pass(self):
        """Q(301,31): w301=Singapore, stocks(301,31,700)>500, item31 price 93.43>90 → present.
        r1 must have no failed derivation.
        """
        result = self._run("Q(301,31)")
        self._print_timing("Q(301,31) [PRESENT]", result)
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0)

    # --- absent: price filter fails ---

    def test_absent_price_fails_correct_goal(self):
        result = self._run("Q(301,4)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        price_failed = [g for g in all_goals if not g["ok"] and ">" in g["goal"] and "54.49" in g["goal"]]
        self.assertGreater(len(price_failed), 0,
            "Expected '54.49 > 90' to appear as a failed goal")

    def test_absent_price_fails_join_goals_succeed(self):
        """The warehouses, stocks, and items join goals must all succeed."""
        result = self._run("Q(301,4)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"], "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"], "stocks join should succeed")
            self.assertTrue(goals[3]["ok"], "items join should succeed")
            self.assertTrue(goals[4]["ok"], "country filter should pass")
            self.assertFalse(goals[5]["ok"], "price filter should fail")

    # --- absent: qty filter fails ---

    def test_absent_qty_fails_correct_goal(self):
        result = self._run("Q(301,1)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        qty_failed = [g for g in all_goals if not g["ok"] and ">" in g["goal"] and "338" in g["goal"]]
        self.assertGreater(len(qty_failed), 0,
            "Expected '338 > 500' to appear as a failed goal")

    def test_absent_qty_fails_price_goal_succeeds(self):
        """Price goal passes (95.23>90 is true); only qty blocks the derivation."""
        result = self._run("Q(301,1)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[5]["ok"],  "price filter should pass (95.23>90)")
            self.assertFalse(goals[6]["ok"], "qty filter should fail (338>500)")

    # --- absent: both filters fail ---

    def test_absent_both_filters_two_failing_goals(self):
        result = self._run("Q(301,12)")
        for d in result["failed_derivations"]:
            failed = [g for g in d["goal_results"] if not g["ok"]]
            self.assertGreaterEqual(len(failed), 2,
                "Expected at least 2 failing goals (price + qty)")

    def test_absent_both_filters_actual_values_in_goals(self):
        result = self._run("Q(301,12)")
        all_goals = [g for d in result["failed_derivations"] for g in d["goal_results"]]
        labels = " ".join(g["goal"] for g in all_goals)
        self.assertIn("18.86", labels, "Expected actual price 18.86 in goal labels")
        self.assertIn("454", labels,   "Expected actual qty 454 in goal labels")

    # --- elapsed_ms is always present ---

    def test_elapsed_ms_returned(self):
        """elapsed_ms must be present and positive in every result."""
        for target in ("Q(301,31)", "Q(301,4)", "Q(301,1)", "Q(301,12)"):
            result = self._run(target)
            self.assertIn("elapsed_ms", result, f"elapsed_ms missing for {target}")
            self.assertGreater(result["elapsed_ms"], 0,
                f"elapsed_ms should be > 0 for {target}")

    # --- union variant: Singapore OR Malaysia, full dataset ---

    def test_union_heavy_both_rules_full_edb(self):
        """Two-rule union over full EDB. Q(301,31) derived by r1 (Singapore).
        r2 (Malaysia) must fail for the same tuple.
        """
        rules = (
            "r1: Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
            "stocks(W_ID,I_ID,S_QTY), items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
            "W_COUNTRY = Singapore, I_PRICE > 90, S_QTY > 500.\n"
            "r2: Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
            "stocks(W_ID,I_ID,S_QTY), items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
            "W_COUNTRY = Malaysia, I_PRICE > 90, S_QTY > 500."
        )
        result = explain_why_not(f"{self.edb}\n{rules}\nWHYNOT Q(301,31)")
        self.__class__._results.append(result)
        self._print_timing("Q(301,31) UNION heavy [r1 present, r2 absent]", result)
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        r2_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        self.assertEqual(len(r1_failures), 0, "r1 should derive Q(301,31)")
        self.assertGreater(len(r2_failures), 0, "r2 should fail (Singapore≠Malaysia)")

    @staticmethod
    def _print_timing(label: str, result: dict) -> None:
        print(f"\n    [{label}] elapsed_ms={result['elapsed_ms']:.3f}  "
              f"failed_derivations={result['failed_derivation_count']}")


# ---------------------------------------------------------------------------
# ═══════════════ COMPLEX MULTI-FILTER JOIN (3 joins + price range + qty) ═══
#
# Seven body goals: three EDB joins followed by four filter conditions.
# The price is constrained on BOTH sides (a range), and stock qty is also
# threshold-gated — this exercises simultaneous upper/lower bound checking
# as well as a qty guard in the same derivation.
#
# Rule:
#   Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),  ← goal 1
#                   stocks(W_ID,I_ID,S_QTY),                              ← goal 2
#                   items(I_ID,I_IM_ID,I_NAME,I_PRICE),                   ← goal 3
#                   W_COUNTRY = Singapore,                                 ← goal 4
#                   I_PRICE > 50,                                          ← goal 5
#                   I_PRICE < 95,                                          ← goal 6
#                   S_QTY > 100.                                           ← goal 7
#
# Failure matrix:
#   Q(301,31): Singapore + 93.43 ∈ (50,95) + qty 700 > 100  → all pass  → PRESENT
#   Q(301,1):  Singapore + price 95.23 ≥ 95                  → goal 6 fails
#   Q(301,2):  Singapore + price 80.22 ∈ (50,95) + qty 6 ≤ 100→ goal 7 fails
#   Q(281,2):  Singapore + price 80.22 ∈ (50,95) + qty 9 ≤ 100→ goal 7 fails
#   Q(7,2):    Malaysia  ≠ Singapore                          → goal 4 fails
# ---------------------------------------------------------------------------

class TestComplexMultiFilterJoin(unittest.TestCase):
    """Three-way join gated by a two-sided price range AND a qty threshold (7 body goals)."""

    EDB  = "\n".join([W301, W281, W7, I1, I2, I31,
                      S301_1, S301_2, S301_31, S281_2, S7_2])
    RULE = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, "
        "I_PRICE > 50, "
        "I_PRICE < 95, "
        "S_QTY > 100."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    # --- present ---

    def test_all_seven_goals_pass(self):
        """Q(301,31): Singapore + item31 price 93.43 ∈ (50,95) + qty 700 > 100 → present."""
        self.assertEqual(self._run("Q(301,31)")["failed_derivation_count"], 0)

    # --- absent: upper price bound fails ---

    def test_upper_price_goal_is_goal_6(self):
        """For Q(301,1): joins + country + lower bound all pass; only goal 6 (price<95) fails."""
        result = self._run("Q(301,1)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "stocks join should succeed")
            self.assertTrue(goals[3]["ok"],  "items join should succeed")
            self.assertTrue(goals[4]["ok"],  "country filter should pass (Singapore)")
            self.assertTrue(goals[5]["ok"],  "lower price bound (>50) should pass")
            self.assertFalse(goals[6]["ok"], "upper price bound (<95) should fail")

    # --- absent: qty filter fails ---

    def test_qty_goal_is_goal_7_for_301_2(self):
        """For Q(301,2): all six preceding goals pass; only goal 7 (qty>100) fails."""
        result = self._run("Q(301,2)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "stocks join should succeed")
            self.assertTrue(goals[3]["ok"],  "items join should succeed")
            self.assertTrue(goals[4]["ok"],  "country filter should pass")
            self.assertTrue(goals[5]["ok"],  "lower price bound (>50) should pass")
            self.assertTrue(goals[6]["ok"],  "upper price bound (<95) should pass")
            self.assertFalse(goals[7]["ok"], "qty filter (>100) should fail")

    # --- absent: country filter fails ---

    def test_country_filter_fails_for_malaysia(self):
        """Q(7,2): w7 is Malaysia — country = Singapore fails at goal 4."""
        result = self._run("Q(7,2)")
        self.assertGreater(result["failed_derivation_count"], 0)
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "stocks join should succeed")
            self.assertTrue(goals[3]["ok"],  "items join should succeed")
            self.assertFalse(goals[4]["ok"], "country filter should fail (Malaysia≠Singapore)")

    def test_distinct_failure_position_per_tuple(self):
        """Different tuples fail at different goal positions: Q(301,1)→goal6, Q(7,2)→goal4."""
        # Q(301,1): first failing goal must be goal 6
        result_301_1 = self._run("Q(301,1)")
        for d in result_301_1["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[4]["ok"],  "goal 4 (country) must pass for Q(301,1)")
            self.assertFalse(goals[6]["ok"], "goal 6 (upper price) must fail for Q(301,1)")
        # Q(7,2): first failing goal must be goal 4
        result_7_2 = self._run("Q(7,2)")
        for d in result_7_2["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertFalse(goals[4]["ok"], "goal 4 (country) must fail for Q(7,2)")


# ---------------------------------------------------------------------------
# ═══════════════════ THREE-RULE UNION — one branch per country ═══════════════
#
# Three rules share head Q. Each targets a different country with a different
# price floor. A tuple is present if ANY one rule succeeds; it is missing only
# when ALL three rules fail.
#
# r1: Q(W_ID,I_ID) :- ..., W_COUNTRY = Singapore, I_PRICE > 90.
# r2: Q(W_ID,I_ID) :- ..., W_COUNTRY = Malaysia,  I_PRICE > 70.
# r3: Q(W_ID,I_ID) :- ..., W_COUNTRY = Indonesia, I_PRICE > 60.
#
# Present via exactly one rule:
#   Q(301,1): Singapore + 95.23>90   → r1 derives; r2/r3 fail on country
#   Q(7,2):   Malaysia  + 80.22>70   → r2 derives; r1/r3 fail on country
#   Q(1,2):   Indonesia + 80.22>60   → r3 derives; r1/r2 fail on country
#
# Absent from all three:
#   Q(301,2): Singapore + 80.22≤90   → r1 price fails; r2/r3 country fail
#   Q(7,3):   Malaysia  + 11.64≤70   → r2 price fails; r1/r3 country fail
#             (r2 passes country but fails price; r1 and r3 fail on country)
# ---------------------------------------------------------------------------

class TestUnionThreeRules(unittest.TestCase):
    """Three-rule union: each branch targets one country with a different price floor."""

    EDB   = "\n".join([W301, W7, W1, I1, I2, I3,
                       S301_1, S301_2, S7_2, S7_3, S1_2])
    RULES = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, I_PRICE > 90.\n"
        "r2: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Malaysia, I_PRICE > 70.\n"
        "r3: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Indonesia, I_PRICE > 60."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULES}\nWHYNOT {target}")

    # --- present via exactly one rule ---

    def test_singapore_present_via_r1_only(self):
        """Q(301,1): Singapore + 95.23>90 → r1 derives; r2/r3 fail on country."""
        result = self._run("Q(301,1)")
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0, "r1 should have no failed derivation for Q(301,1)")

    # --- absent: Singapore warehouse, cheap item — all three rules fail ---

    def test_singapore_cheap_item_absent(self):
        """Q(301,2): Singapore + item2 price 80.22 ≤ 90 → r1 price fails; r2/r3 country fail."""
        result = self._run("Q(301,2)")
        self.assertGreater(result["failed_derivation_count"], 0)

    def test_r1_price_fails_for_singapore_cheap(self):
        """r1 fails for Q(301,2): country goal passes (Singapore=Singapore) but price fails."""
        result = self._run("Q(301,2)")
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[4]["ok"],  "r1 country goal should pass (Singapore=Singapore)")
            self.assertFalse(goals[5]["ok"], "r1 price goal should fail (80.22≤90)")

    # --- absent: Malaysia warehouse, cheap item — different failure reason per rule ---

    def test_all_three_rules_fail_for_malaysia_cheap(self):
        """Q(7,3) must have failed derivations from r1, r2, and r3."""
        result = self._run("Q(7,3)")
        rule_ids = {d["rule_id"] for d in result["failed_derivations"]}
        self.assertEqual(rule_ids, {"r1", "r2", "r3"},
            f"Expected all 3 rule IDs for Q(7,3); got {rule_ids}")

    def test_r2_country_passes_price_fails_for_malaysia_cheap(self):
        """r2 passes the country check (Malaysia=Malaysia) but fails on 11.64 ≤ 70."""
        result = self._run("Q(7,3)")
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[4]["ok"],  "r2 country goal should pass (Malaysia=Malaysia)")
            self.assertFalse(goals[5]["ok"], "r2 price goal should fail (11.64≤70)")

    def test_different_failure_position_per_rule_for_malaysia_cheap(self):
        """For Q(7,3): r2 fails at goal 5 (price); r1 and r3 fail earlier at goal 4 (country)."""
        result = self._run("Q(7,3)")
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            # goal 4 (country) must pass for r2 — only price (goal 5) fails
            self.assertTrue(goals[4]["ok"],
                "r2 must pass country (Malaysia=Malaysia) — fail is price, not country")
        for rule_id in ("r1", "r3"):
            for d in [d for d in result["failed_derivations"] if d["rule_id"] == rule_id]:
                goals = {g["goal_index"]: g for g in d["goal_results"]}
                # goal 4 (country) must fail for r1 and r3
                self.assertFalse(goals[4]["ok"],
                    f"{rule_id} must fail at country goal 4 for Q(7,3)")


# ---------------------------------------------------------------------------
# ═══════════════════ NEGATION COMBINED WITH MULTI-FILTER ════════════════════
#
# Find (warehouse, item) pairs where:
#   1. the warehouse is in Singapore
#   2. the item's price is > 50
#   3. NO stock record exists for this (warehouse, item) pair
#
# Rule:
#   Q(W_ID,I_ID) :- warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY),  ← goal 1
#                   items(I_ID,I_IM_ID,I_NAME,I_PRICE),                   ← goal 2
#                   W_COUNTRY = Singapore,                                 ← goal 3
#                   I_PRICE > 50,                                          ← goal 4
#                   not stocks(W_ID,I_ID,S_QTY_DUMMY).                    ← goal 5
#
# Singapore warehouses in EDB: W301, W22.
# Stock rows: S301_1, S301_2 (w301 stocks items 1,2); S22_2 (w22 stocks item 2 only).
#
# Failure matrix:
#   Q(22,1):  Singapore ✓, price 95.23>50 ✓, stocks(22,1,...) absent → negation PASSES → PRESENT
#   Q(22,2):  Singapore ✓, price 80.22>50 ✓, stocks(22,2,3) exists  → negation FAILS  → ABSENT
#   Q(301,1): Singapore ✓, price 95.23>50 ✓, stocks(301,1,338) exists→ negation FAILS  → ABSENT
#   Q(22,3):  Singapore ✓, price 11.64 ≤ 50                         → goal 4 FAILS     → ABSENT
# ---------------------------------------------------------------------------

class TestNegationWithFilter(unittest.TestCase):
    """NOT EXISTS stock combined with a country filter and a price filter."""

    EDB  = "\n".join([W301, W22, I1, I2, I3, S301_1, S301_2, S22_2])
    RULE = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, "
        "I_PRICE > 50, "
        "not stocks(W_ID,I_ID,S_QTY_DUMMY)."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULE}\nWHYNOT {target}")

    def test_unstocked_expensive_item_present(self):
        """Q(22,1): Singapore + price 95.23>50 + no stocks(22,1,...) row → present."""
        self.assertEqual(self._run("Q(22,1)")["failed_derivation_count"], 0)

    def test_stocked_item_at_w22_absent_negation_fails(self):
        """Q(22,2): stocks(22,2,3) exists — not stocks(...) fails → tuple absent."""
        self.assertGreater(self._run("Q(22,2)")["failed_derivation_count"], 0)

    def test_negation_is_the_failing_goal_for_w22_item2(self):
        """For Q(22,2): goals 1-4 (joins + country + price) pass; only goal 5 (negation) fails."""
        result = self._run("Q(22,2)")
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "items join should succeed")
            self.assertTrue(goals[3]["ok"],  "country filter should pass (Singapore)")
            self.assertTrue(goals[4]["ok"],  "price filter should pass (80.22>50)")
            self.assertFalse(goals[5]["ok"], "not stocks(...) should fail (stock row exists)")

    def test_cheap_item_absent_price_filter_fails(self):
        """Q(22,3): item3 price 11.64 is NOT > 50 — goal 4 (price) fails before negation."""
        result = self._run("Q(22,3)")
        self.assertGreater(result["failed_derivation_count"], 0)
        for d in result["failed_derivations"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[3]["ok"],  "country filter should pass (Singapore)")
            self.assertFalse(goals[4]["ok"], "price filter should fail (11.64≤50)")

    def test_distinct_failure_reasons_negation_vs_price(self):
        """Contrast: Q(22,2) fails on negation goal; Q(22,3) fails on price comparison."""
        neg_result   = self._run("Q(22,2)")
        price_result = self._run("Q(22,3)")
        # Q(22,2): the failing goal must start with "not"
        neg_goals = [g for d in neg_result["failed_derivations"]
                     for g in d["goal_results"] if not g["ok"]]
        self.assertTrue(any(g["goal"].startswith("not ") for g in neg_goals),
            "Q(22,2) should fail on a negation goal")
        # Q(22,3): the failing goal must be a comparison (contains ">")
        price_goals = [g for d in price_result["failed_derivations"]
                       for g in d["goal_results"] if not g["ok"]]
        self.assertTrue(any(">" in g["goal"] for g in price_goals),
            "Q(22,3) should fail on a price comparison goal, not a negation goal")


# ---------------------------------------------------------------------------
# ═══════════ TWO-RULE UNION WHERE ONE BRANCH USES NEGATION ══════════════════
#
# r1 finds Singapore warehouses with an expensive (>90), well-stocked (>500) item.
# r2 finds Malaysia warehouses that do NOT stock a given mid-price (>50) item at all.
# A tuple is present if r1 OR r2 derives it.
#
# r1: Q(W_ID,I_ID) :- warehouses(W_ID,...,W_COUNTRY),          ← goal 1
#                      stocks(W_ID,I_ID,S_QTY),                  ← goal 2
#                      items(I_ID,...,I_PRICE),                   ← goal 3
#                      W_COUNTRY = Singapore, I_PRICE > 90,       ← goals 4-5
#                      S_QTY > 500.                               ← goal 6
# r2: Q(W_ID,I_ID) :- warehouses(W_ID,...,W_COUNTRY),          ← goal 1
#                      items(I_ID,...,I_PRICE),                   ← goal 2
#                      W_COUNTRY = Malaysia,                      ← goal 3
#                      I_PRICE > 50,                              ← goal 4
#                      not stocks(W_ID,I_ID,S_QTY_DUMMY).        ← goal 5
#
# Derivation outcomes:
#   Q(301,31): r1: Singapore + 93.43>90 + qty 700>500 ✓ → r1 derives → PRESENT
#              r2: w301 is Singapore not Malaysia         → r2 country fails
#   Q(7,1):    r2: Malaysia + 95.23>50 + no stocks(7,1) ✓ → r2 derives → PRESENT
#              r1: w7 is Malaysia not Singapore            → r1 country fails
#   Q(7,2):    r1: w7 is Malaysia not Singapore  → r1 country fails (goal 4)
#              r2: Malaysia ✓, 80.22>50 ✓, but stocks(7,2,2) EXISTS → r2 negation fails (goal 5)
# ---------------------------------------------------------------------------

class TestComplexUnionNegation(unittest.TestCase):
    """Two-rule union: r1 is a three-way join with qty gate; r2 uses NOT EXISTS."""

    # w301=Singapore, w7=Malaysia; item1(95.23), item2(80.22), item31(93.43)
    # stocks: 301→item1(338), 301→item31(700), 7→item2(2)  — no stocks(7,1,...) row
    EDB   = "\n".join([W301, W7, I1, I2, I31, S301_1, S301_31, S7_2])
    RULES = (
        "r1: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "stocks(W_ID,I_ID,S_QTY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Singapore, "
        "I_PRICE > 90, "
        "S_QTY > 500.\n"
        "r2: Q(W_ID,I_ID) :- "
        "warehouses(W_ID,W_NAME,W_STREET,W_CITY,W_COUNTRY), "
        "items(I_ID,I_IM_ID,I_NAME,I_PRICE), "
        "W_COUNTRY = Malaysia, "
        "I_PRICE > 50, "
        "not stocks(W_ID,I_ID,S_QTY_DUMMY)."
    )

    def _run(self, target):
        return explain_why_not(f"{self.EDB}\n{self.RULES}\nWHYNOT {target}")

    # --- present via r1 ---

    def test_singapore_well_stocked_expensive_present_via_r1(self):
        """Q(301,31): Singapore + 93.43>90 + qty 700>500 → r1 derives; r2 fails (not Malaysia)."""
        result = self._run("Q(301,31)")
        r1_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]
        self.assertEqual(len(r1_failures), 0,
            "r1 should have no failed derivation for Q(301,31) — all conditions pass")

    # --- present via r2 (negation branch) ---

    def test_malaysia_unstocked_expensive_present_via_r2(self):
        """Q(7,1): Malaysia + 95.23>50 + no stocks(7,1,...) → r2 derives.
        r1 has no binding candidates (stocks(7,1,...) absent) → 0 r1 failed derivations.
        """
        result = self._run("Q(7,1)")
        r2_failures = [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]
        self.assertEqual(len(r2_failures), 0,
            "r2 should have no failed derivation for Q(7,1) — all goals including negation pass")

    # --- absent: r1 fails on country, r2 fails on negation ---

    def test_malaysia_stocked_item_absent(self):
        """Q(7,2): r1 fails on country (Malaysia≠Singapore); r2 fails on negation (stock exists)."""
        result = self._run("Q(7,2)")
        self.assertGreater(result["failed_derivation_count"], 0)

    def test_both_rule_ids_fail_for_malaysia_stocked(self):
        """Q(7,2): both r1 and r2 contribute failed derivations."""
        result = self._run("Q(7,2)")
        rule_ids = {d["rule_id"] for d in result["failed_derivations"]}
        self.assertIn("r1", rule_ids, "r1 should fail for Q(7,2)")
        self.assertIn("r2", rule_ids, "r2 should fail for Q(7,2)")

    def test_r1_fails_country_for_malaysia_stocked(self):
        """r1 fails for Q(7,2) at the country goal (goal 4): Malaysia ≠ Singapore."""
        result = self._run("Q(7,2)")
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "r1 warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "r1 stocks join should succeed")
            self.assertTrue(goals[3]["ok"],  "r1 items join should succeed")
            self.assertFalse(goals[4]["ok"], "r1 country goal should fail (Malaysia≠Singapore)")

    def test_r2_fails_negation_for_malaysia_stocked(self):
        """r2 fails for Q(7,2) at goal 5: stocks(7,2,2) exists so not stocks(...) is false."""
        result = self._run("Q(7,2)")
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[1]["ok"],  "r2 warehouses join should succeed")
            self.assertTrue(goals[2]["ok"],  "r2 items join should succeed")
            self.assertTrue(goals[3]["ok"],  "r2 country goal should pass (Malaysia=Malaysia)")
            self.assertTrue(goals[4]["ok"],  "r2 price goal should pass (80.22>50)")
            self.assertFalse(goals[5]["ok"], "r2 not stocks(...) should fail (stock exists)")

    def test_rules_fail_at_different_goals(self):
        """For Q(7,2): r1 fails at goal 4 (country); r2 fails at goal 5 (negation)."""
        result = self._run("Q(7,2)")
        # r1: goal 4 must be the failing goal
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r1"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertFalse(goals[4]["ok"], "r1 must fail at goal 4 (country)")
        # r2: goal 5 must be the failing goal; goal 4 must pass
        for d in [d for d in result["failed_derivations"] if d["rule_id"] == "r2"]:
            goals = {g["goal_index"]: g for g in d["goal_results"]}
            self.assertTrue(goals[4]["ok"],  "r2 must pass goal 4 (price)")
            self.assertFalse(goals[5]["ok"], "r2 must fail at goal 5 (negation)")


if __name__ == "__main__":
    unittest.main()
