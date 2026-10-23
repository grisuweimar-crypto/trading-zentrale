"""M3 deterministic clustering, anchor-dependence, scenario and CLI export tests."""
import json
import tempfile
import unittest
from pathlib import Path

from scanner.research.zones_m3 import (
    M3_VERSION, MAX_CLUSTER_RELATIVE_WIDTH, build_scenario_plans,
    cluster_zones, enrich_m3, write_readable_tables,
)
from scanner.research.zones_v01 import build_zones
from test_argus_zones_v01 import bars


def anchor(kind, day, price, degree="minor"):
    return {"kind": kind, "pivot_time": day, "confirmed_time": "2026-10-19",
            "price": price, "degree": degree}


LOW = anchor("low", "2026-10-10", 90)
HIGH = anchor("high", "2026-10-12", 120)
BEAR_HIGH = anchor("high", "2026-10-09", 130)
BEAR_LOW = anchor("low", "2026-10-11", 100)


def zone(label, role, side, lower, upper, family, pivots, *, status="watch"):
    return {"zone_id": label, "role": role, "side": side, "lower": lower,
            "upper": upper, "timeframe": "daily", "status": status,
            "known_at": "2026-10-19", "drivers": [{
                "family": family, "anchors": [
                    {"kind": a["kind"], "pivot_time": a["pivot_time"],
                     "price": a["price"], "degree": a["degree"],
                     "known_at": a["confirmed_time"]} for a in pivots
                ]}],
            "evidence_status": "not_validated", "empirical_probability": None}


def demo():
    return [
        zone("s1", "support", "bull", 99, 101, "fib_retracement", [LOW, HIGH]),
        zone("s2", "support", "bull", 99.5, 101.5, "horizontal", [LOW]),
        zone("i1", "invalidation", "bull", 88, 92, "swing_origin", [LOW]),
        zone("t1", "target", "bull", 136, 138, "swing_abc_projection", [LOW, HIGH]),
        zone("r1", "resistance", "bear", 119, 121, "fib_retracement", [BEAR_HIGH, BEAR_LOW]),
        zone("i2", "invalidation", "bear", 128, 132, "swing_origin", [BEAR_HIGH]),
        zone("t2", "target", "bear", 76, 80, "swing_abc_projection", [BEAR_HIGH, BEAR_LOW]),
    ]


class M3ResearchTests(unittest.TestCase):
    def test_clusters_are_order_independent_and_bounded(self):
        data = demo()
        x = cluster_zones(data, 110)
        y = cluster_zones(list(reversed(data)), 110)
        self.assertEqual(x, y)
        self.assertEqual(len(x), 6)
        support = next(c for c in x if c["role"] == "support")
        self.assertEqual(support["source_zone_ids"], ["s1", "s2"])
        self.assertEqual(support["distinct_family_count"], 2)
        self.assertEqual(support["independent_anchor_groups"], 1)
        self.assertEqual(support["dependent_source_count"], 1)
        self.assertIsNone(support["empirical_probability"])
        for c in x:
            self.assertLessEqual(
                (c["upper"] - c["lower"]) / ((c["upper"] + c["lower"]) / 2),
                MAX_CLUSTER_RELATIVE_WIDTH + 1e-10)

    def test_same_physical_swing_multiple_degrees_is_one_anchor_group(self):
        a = zone("a", "support", "bull", 100, 101, "horizontal", [LOW])
        different_degree = anchor("low", "2026-10-10", 90, degree="major")
        b = zone("b", "support", "bull", 100, 101, "fib_retracement", [different_degree])
        c = cluster_zones([a, b], 110)[0]
        self.assertEqual(c["independent_anchor_groups"], 1)

    def test_unrelated_supports_not_falsely_considered_same_anchor(self):
        a = zone("a", "support", "bull", 99, 101, "horizontal", [LOW])
        b = zone("b", "support", "bull", 100, 102, "horizontal", [
            anchor("low", "2026-09-19", 88)])
        c = cluster_zones([a, b], 110)[0]
        self.assertEqual(c["independent_anchor_groups"], 2)
        self.assertEqual(c["distinct_family_count"], 1)

    def test_bull_bear_future_two_step_scenarios(self):
        raw = demo()
        groups = cluster_zones(raw, 110)
        plans = build_scenario_plans(groups, raw, asof="2026-10-21", close=110)
        self.assertEqual({p["side"] for p in plans}, {"bull", "bear"})
        for p in plans:
            self.assertEqual(p["state"], "untriggered")
            self.assertEqual(p["activation"]["valid_from_exclusive"], "2026-10-21")
            self.assertIn("later completed daily bar", p["activation"]["order"])
            self.assertLessEqual(p["known_at"], "2026-10-21")
            self.assertEqual(p["horizon_bars"], 20)
            self.assertIsNone(p["risk_reward_after_costs"])
            self.assertIsNone(p["empirical_probability"])
            self.assertEqual(p["evidence_status"], "not_validated")
            self.assertTrue(p["target_cluster_ids"])
            self.assertTrue(p["invalidation_zone_ids"])

    def test_no_invalidation_or_no_target_yields_no_setup(self):
        raw = [z for z in demo() if z["role"] != "invalidation"]
        self.assertEqual(build_scenario_plans(cluster_zones(raw, 110), raw,
                         asof="2026-10-21", close=110), [])
        raw = [z for z in demo() if z["role"] != "target"]
        # A resistance above the bull entry is a legitimate opposing target,
        # and support below the bear entry is a legitimate opposing target.
        # Remove those too to show that no target is invented.
        raw = [z for z in raw if z["role"] == "invalidation" or
               z["role"] == "support" and z["side"] == "bull"]
        self.assertEqual(build_scenario_plans(cluster_zones(raw, 110), raw,
                         asof="2026-10-21", close=110), [])

    def test_disjoint_roles_must_not_merge_even_at_same_price(self):
        x = zone("a", "support", "bull", 100, 102, "horizontal", [LOW])
        y = zone("b", "invalidation", "bull", 100, 102, "swing_origin", [LOW])
        self.assertEqual(len(cluster_zones([x, y], 110)), 2)

    def test_blocked_inputs_stay_empty_and_no_lookahead(self):
        empty = build_zones([], "DEMO", asof="2026-10-21")
        self.assertEqual(empty["zone_clusters"], [])
        self.assertEqual(empty["scenario_plans"], [])
        self.assertEqual(empty["m3_status"], "no_valid_setup")
        rows = bars()
        cutoff = rows[200]["date"]
        left = build_zones(rows[:201], "DEMO", asof=cutoff)
        right = build_zones(rows, "DEMO", asof=cutoff)
        self.assertEqual(left, right)
        self.assertEqual(left["m3_version"], M3_VERSION)
        self.assertTrue(left["zone_clusters"])
        for p in left["scenario_plans"]:
            self.assertLessEqual(p["known_at"], cutoff)

    def test_csv_and_markdown_export_same_snapshot(self):
        report = {"symbol": "DEMO", "asof": "2026-10-21",
                  "data_quality": "degraded", "asof_raw_close": 110,
                  "zones": demo()}
        enrich_m3(report)
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "DEMO_zones.json"
            output.write_text(json.dumps(report))
            write_readable_tables(report, output)
            self.assertTrue(output.with_name("DEMO_zones_clusters.csv").exists())
            self.assertTrue(output.with_name("DEMO_zones_plans.csv").exists())
            txt = output.with_name("DEMO_zones_M3.md").read_text()
            self.assertIn("Research only", txt)
            self.assertIn("Unabh. Ankergruppen", txt)
            self.assertIn("bull", txt)
            self.assertIn("bear", txt)


if __name__ == "__main__":
    unittest.main()
