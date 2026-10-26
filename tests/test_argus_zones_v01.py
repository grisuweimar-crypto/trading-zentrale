"""Offline safety and reproducibility checks for ARGUS research zones v0.1."""
import math
import unittest
from datetime import date, timedelta

from scanner.research.zones_v01 import VERSION, _zone, build_zones, chronological


def bars(n=240, *, symbol="DEMO"):
    """Deterministic hourly-free synthetic daily OHLCV, *not* market evidence."""
    out = []
    base = date(2025, 1, 1)
    for i in range(n):
        # Enough cyclic turns for causal 2x2 / 3x3 / 5x5 swing confirmation.
        price = 100 + 0.06 * i + 13 * math.sin(i / 6.0)
        out.append({
            "date": (base + timedelta(days=i)).isoformat(),
            "symbol": symbol, "currency": "USD",
            "open": str(price - 0.4), "high": str(price + 1.5),
            "low": str(price - 1.5), "close": str(price),
            "adj_close": str(price), "volume": str(10000 + i),
        })
    return out


class ZoneResearchTests(unittest.TestCase):
    def test_z1_z2_z3_generate_with_causal_provenance(self):
        rows = bars()
        report = build_zones(rows, "DEMO", asof=rows[-1]["date"], exchange="XNAS", asset_id="test:DEMO")
        self.assertEqual(report["model_version"], VERSION)
        self.assertEqual(report["data_quality"], "degraded")
        self.assertEqual(report["zone_status"], "candidate_zones_unvalidated")
        families = {d["family"] for z in report["zones"] for d in z["drivers"]}
        self.assertTrue({"horizontal", "fib_retracement", "swing_abc_projection"}.issubset(families))
        self.assertTrue(report["scenarios"])
        for z in report["zones"]:
            self.assertLessEqual(z["lower"], z["upper"])
            self.assertGreater(z["lower"], 0)
            self.assertLessEqual(z["known_at"], report["last_completed_bar"])
            self.assertIsNone(z["empirical_probability"])
            self.assertEqual(z["evidence_status"], "not_validated")
            for driver in z["drivers"]:
                for anchor in driver["anchors"]:
                    self.assertLessEqual(anchor["pivot_time"], anchor["known_at"])
                    self.assertLessEqual(anchor["known_at"], z["known_at"])

    def test_no_lookahead_after_asof_and_identical_inputs(self):
        rows = bars()
        cutoff = rows[190]["date"]
        original = build_zones(rows[:191], "DEMO", asof=cutoff)
        future_appended = build_zones(rows, "DEMO", asof=cutoff)
        self.assertEqual(original, future_appended)
        self.assertEqual(build_zones(rows, "DEMO", asof=cutoff), future_appended)

    def test_an_incomplete_adjustment_blocks_fibonacci(self):
        rows = bars()
        rows[-10]["adj_close"] = ""
        report = build_zones(rows, "DEMO", asof=rows[-1]["date"])
        self.assertEqual(report["data_quality"], "blocked")
        self.assertEqual(report["zones"], [])
        self.assertEqual(report["zone_status"], "no_qualifying_zone")

    def test_missing_history_never_fabricates_zone(self):
        result = build_zones([], "AMD", asof="2026-10-22")
        self.assertEqual(result["zones"], [])
        self.assertTrue(any("insufficient" in m for m in result["warnings"]))

    def test_raw_split_is_adjusted_coherently_before_pivots(self):
        rows = bars()
        # First half uses 2-for-1 unadjusted raw unit, historical adj_close is 1/2.
        for row in rows[:120]:
            for field in ("open", "high", "low", "close"):
                row[field] = str(float(row[field]) * 2)
        report = build_zones(rows, "DEMO", asof=rows[-1]["date"])
        self.assertNotEqual(report["data_quality"], "blocked")
        self.assertAlmostEqual(report["asof_raw_close"], float(rows[-1]["close"]), places=5)
        self.assertGreater(len(report["zones"]), 0)
        self.assertTrue(all(z["upper"] < 400 for z in report["zones"]))

    def test_zone_created_at_confirmation_not_previous_touch(self):
        anchor = {"degree": "minor", "kind": "low", "pivot_time": "2026-10-20",
                  "confirmed_time": "2026-10-22", "price": 99.0}
        current = {"date": "2026-10-22", "open": 100.0, "high": 101.0,
                   "low": 98.0, "close": 99.0}
        z = _zone("horizontal", "support", "bull", 99.0, 2.0, [anchor], current)
        self.assertEqual(z["status"], "watch")
        current["date"] = "2026-10-23"
        self.assertEqual(_zone("horizontal", "support", "bull", 99.0, 2.0, [anchor], current)["status"], "touched")

    def test_duplicate_date_must_be_rejected(self):
        rows = bars()
        with self.assertRaisesRegex(ValueError, "duplicate_market_sessions"):
            chronological(rows + [rows[0]], rows[-1]["date"])

    def test_synthetic_cases_not_claimed_validated(self):
        rows = bars()
        r = build_zones(rows, "DEMO", asof=rows[-1]["date"])
        for s in r["scenarios"]:
            self.assertEqual(s["evidence_status"], "not_validated")
            self.assertIsNone(s["risk_reward_after_costs"])
            self.assertIn("untriggered", s["activation_rule"])


if __name__ == "__main__":
    unittest.main()
