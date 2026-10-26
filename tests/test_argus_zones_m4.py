"""ARGUS M4 replay: forced negatives, price-gaps, costs and causality."""
import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from scanner.research.zones_m4 import (
    ReplayPolicy, evaluate_frozen, normalize_future_bars, summarize,
)
from scripts.replay_argus_zones_m4 import replay_symbol, run
from test_argus_zones_v01 import bars


ASOF = "2026-10-01"


def setup(side="bull", quality="degraded"):
    bull = side == "bull"
    entry_lo, entry_hi = (99.0, 101.0) if bull else (109.0, 111.0)
    stop, target = (95.0, 120.0) if bull else (115.0, 90.0)
    cluster = [
        {"cluster_id": "entry", "lower": entry_lo, "upper": entry_hi,
         "known_at": ASOF, "role": "support" if bull else "resistance"},
        {"cluster_id": "invalid", "lower": stop, "upper": stop,
         "known_at": ASOF, "role": "invalidation"},
        {"cluster_id": "goal", "lower": target, "upper": target,
         "known_at": ASOF, "role": "target"},
    ]
    p = {"asof": ASOF, "known_at": ASOF, "state": "untriggered",
         "side": side, "scenario_id": "demo-" + side,
         "entry_cluster_id": "entry",
         "invalidation_cluster_id": "invalid",
         "target_cluster_ids": ["goal"]}
    report = {"asof": ASOF, "symbol": "DEMO", "data_hash": "abc123",
              "data_quality": quality, "zone_clusters": cluster}
    return report, p


def d(day, o, h, l, c):
    return {"date": f"2026-10-{day:02d}", "open": o, "high": h, "low": l, "close": c}


def future(*given, to=20):
    values = list(given)
    while len(values) < to:
        i = len(values) + 2
        values.append(d(i, 105, 110, 103, 105))
    return values


class M4Tests(unittest.TestCase):
    def test_bull_touch_next_close_later_fill_target(self):
        rep, p = setup()
        prices = future(d(2, 105, 107, 100, 100),
                        d(3, 101, 104, 99, 102),
                        d(4, 104, 110, 102, 106),
                        d(5, 105, 121, 103, 120))
        e = evaluate_frozen(rep, p, prices)
        self.assertEqual((e["touch_date"], e["confirm_date"], e["fill_date"]),
                         ("2026-10-02", "2026-10-03", "2026-10-04"))
        self.assertEqual(e["outcome"], "target")
        self.assertEqual(e["exit_date"], "2026-10-05")
        self.assertAlmostEqual(e["net_return_pct"],
                               (120 - 104) / 104 * 100 - 0.30, places=5)
        self.assertLess(e["mae_pct"], 0)
        self.assertGreater(e["mfe_pct"], 0)
        self.assertIsNone(e["empirical_probability"])

    def test_stop_first_when_both_reached_same_day(self):
        rep, p = setup()
        seq = future(d(2, 105, 107, 100, 100),
                     d(3, 101, 104, 99, 102),
                     d(4, 104, 121, 92, 110))
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "stop_first_on_same_bar")
        self.assertEqual(e["exit_price"], 95.0)
        self.assertLess(e["net_return_pct"], 0)

    def test_open_gap_through_stop_worse_than_stop_price(self):
        rep, p = setup()
        seq = future(d(2, 105, 107, 100, 100),
                     d(3, 101, 104, 99, 102),
                     d(4, 104, 110, 102, 105),
                     d(5, 90, 96, 88, 92))
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "stop")
        self.assertEqual(e["exit_price"], 90.0)

    def test_bear_side_target_and_fee(self):
        rep, p = setup("bear")
        seq = future(d(2, 108, 110, 106, 110),
                     d(3, 108, 112, 105, 108),
                     d(4, 106, 108, 101, 104),
                     d(5, 101, 106, 88, 90))
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "target")
        self.assertEqual(e["fill_price"], 106.0)
        self.assertAlmostEqual(e["net_return_pct"],
                               (106 - 90) / 106 * 100 - .30, places=5)

    def test_unreached_is_denominator_not_winner(self):
        rep, p = setup()
        seq = future(*[d(i, 107, 110, 103, 106) for i in range(2, 22)])
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "unreached")
        self.assertIsNone(e["net_return_pct"])

    def test_touch_failure_does_not_retroactively_reselect(self):
        rep, p = setup()
        seq = future(d(2, 102, 105, 100, 100),
                     d(3, 101, 105, 99, 100),
                     d(4, 105, 110, 100, 107))
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "touch_not_confirmed")
        self.assertIsNone(e["confirm_date"])

    def test_preentry_breach_blocks_even_if_later_rebounds(self):
        rep, p = setup()
        seq = future(d(2, 98, 100, 93, 94), d(3, 105, 110, 99, 109))
        self.assertEqual(evaluate_frozen(rep, p, seq)["outcome"],
                         "invalidated_before_entry")

    def test_entry_gap_fails_fixed_risk_screen(self):
        rep, p = setup()
        seq = future(d(2, 105, 107, 100, 100),
                     d(3, 101, 104, 99, 102),
                     d(4, 118, 119, 110, 116))
        e = evaluate_frozen(rep, p, seq)
        self.assertEqual(e["outcome"], "risk_rejected")
        self.assertIsNone(e["fill_date"])

    def test_right_censor_and_horizon_exit(self):
        rep, p = setup()
        seq = future(d(2, 105, 107, 100, 100),
                     d(3, 101, 104, 99, 102),
                     d(4, 104, 110, 102, 105))
        censored = evaluate_frozen(rep, p, seq[:3])
        self.assertEqual(censored["outcome"], "right_censored")
        full = evaluate_frozen(rep, p, seq)
        self.assertEqual(full["outcome"], "horizon_close")
        self.assertEqual(full["exit_date"], "2026-10-21")

    def test_no_future_input_to_prediction_and_no_early_fill(self):
        rep, p = setup()
        seq = future(d(2, 105, 107, 100, 100),
                     d(3, 101, 130, 99, 102),
                     d(4, 104, 110, 102, 105))
        # A high of 130 on the confirmation day cannot trigger an exit.
        e = evaluate_frozen(rep, p, seq)
        self.assertNotEqual(e["outcome"], "target")
        self.assertEqual(e["fill_date"], "2026-10-04")
        self.assertEqual(e, evaluate_frozen(rep, p, seq + [d(25, 200, 250, 200, 220)]))

    def test_snapshot_after_cutoff_or_blocked_raises(self):
        rep, p = setup()
        p["known_at"] = "2026-10-02"
        with self.assertRaisesRegex(ValueError, "forecast_known_after_cutoff"):
            evaluate_frozen(rep, p, [])
        rep, p = setup(quality="blocked")
        with self.assertRaisesRegex(ValueError, "blocked_asof_snapshot"):
            evaluate_frozen(rep, p, [])

    def test_scale_and_duplicate_future_fail_closed(self):
        raw = [{"date": "2026-10-02", "open": "50", "high": "52",
                "low": "48", "close": "51", "adj_close": "25.5"}]
        mapped = normalize_future_bars(raw, asof=ASOF, asof_raw_close=100,
                                       asof_adj_close=50)
        self.assertEqual(mapped[0]["close"], 51)
        with self.assertRaisesRegex(ValueError, "duplicate_outcome_bar"):
            normalize_future_bars(raw + raw, asof=ASOF,
                                  asof_raw_close=100, asof_adj_close=50)
        with self.assertRaisesRegex(ValueError, "invalid_future_adj_close"):
            normalize_future_bars([{**raw[0], "adj_close": ""}], asof=ASOF,
                                  asof_raw_close=100, asof_adj_close=50)

    def test_summary_zero_winners_no_synthetic_hit_rate(self):
        rep, p = setup()
        no_touch = evaluate_frozen(rep, p, future(
            *[d(i, 107, 110, 103, 106) for i in range(2, 22)]))
        s = summarize([no_touch])
        self.assertEqual(s["outcome_counts"], {"unreached": 1})
        self.assertIsNone(s["success_probability"])
        self.assertIsNone(s["mean_net_pct_closed_only"])
        self.assertFalse(s["independent_observations"])

    def test_fixed_grid_snapshots_are_source_cutoff_stable(self):
        rows = bars(260)
        cutoff = rows[238]["date"]
        small = replay_symbol(rows[:239], "DEMO", cutoff=cutoff, first_index=150,
                              stride=20, max_snapshots=4)
        appended = replay_symbol(rows, "DEMO", cutoff=cutoff, first_index=150,
                                 stride=20, max_snapshots=4)
        self.assertEqual(small, appended)
        self.assertEqual(len(small["asof_index"]), 4)
        for f, e in zip(small["frozen"], small["outcomes"]):
            self.assertEqual(e["forecast_sha256"], f["forecast_sha256"] if
                             "forecast_sha256" in e else f["forecast_sha256"])
            self.assertEqual(f["scenario"]["asof"], f["asof"])

    def test_full_replay_exports_forecast_before_outcome_and_coverage(self):
        rows = bars(225)
        with tempfile.TemporaryDirectory() as tmp:
            inp = Path(tmp) / "history.csv"
            import csv
            with inp.open("w", newline="") as fp:
                w = csv.DictWriter(fp, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)
            out = Path(tmp) / "m4"
            s = run(inp, ["DEMO", "MISSING"], cutoff=rows[-1]["date"],
                    output_dir=out, first_index=150, stride=20, max_snapshots=2)
            self.assertEqual(s["coverage"]["DEMO"]["snapshots"], 2)
            self.assertEqual(s["coverage"]["MISSING"]["snapshots"], 0)
            self.assertTrue((out / "forecast_ledger.jsonl").exists())
            self.assertTrue((out / "outcome_ledger.jsonl").exists())
            self.assertTrue((out / "asof_coverage.jsonl").exists())
            self.assertTrue((out / "m4_summary.json").exists())
            self.assertTrue(s["research_only"])
            self.assertFalse(s["causal_market_PIT_verified"])


if __name__ == "__main__":
    unittest.main()
