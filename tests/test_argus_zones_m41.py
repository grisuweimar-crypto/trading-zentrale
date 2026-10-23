"""Unit and adversarial tests for the M4.1 range-reaction evaluator."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import unittest

from scanner.research.zones_m41 import (
    ZoneObservationPolicy, evaluate_zone, freeze_zone, matched_control,
    summarize_events, validate_closed_bars,
)

ASOF="2025-01-01"


def report(close=105., asof=ASOF):
    return {"symbol":"DEMO", "asof":asof, "asof_raw_close":close,
            "data_quality":"degraded", "data_hash":"fixed-historical-snapshot",
            "currency":"USD", "exchange":"unknown"}


def zone(role="support", lo=95., hi=100., side="bull", family="fib_retracement"):
    return {"zone_id":"frozen-001", "role":role, "side":side,
            "timeframe":"daily", "lower":lo, "upper":hi,
            "known_at":ASOF, "status":"watch",
            "drivers":[{"family":family,"anchors":[]}], "evidence_status":"not_validated"}


def bar(day_index, o, h, l, c):
    return {"date":(date.fromisoformat(ASOF)+timedelta(days=day_index)).isoformat(),
            "open":o,"high":h,"low":l,"close":c}


def future(rows, n=60, filler=(106,108,104,106)):
    values=list(rows)
    while len(values)<n:
        values.append(bar(len(values)+1,*filler))
    return values


class ZoneM41Tests(unittest.TestCase):
    def setUp(self):
        self.f=freeze_zone(report(),zone(),scope="raw_daily")

    def test_multiday_inside_not_failed_by_next_close(self):
        rows=future([
            bar(1,105,105,97,98),
            bar(2,98,99,96,97),
            bar(3,97,99,94,96),
            bar(4,96,100,95,99),
            bar(5,103,107,102,104),
        ])
        x=evaluate_zone(self.f,rows)
        self.assertEqual(x["first_touch_date"],"2025-01-02")
        self.assertEqual(x["first_favorable_close_after_touch"],"2025-01-06")
        self.assertEqual(x["status"],"rebound_or_rejection")
        self.assertEqual(x["max_penetration_depth_fraction"],1.0)
        self.assertGreaterEqual(x["close_inside_days"],4)
        self.assertEqual(x["longest_consecutive_inside_closes"],4)
        self.assertEqual(x["touch_windows"]["3"]["status"],"observed")
        self.assertIsNone(x["trade_return"])

    def test_retest_separate_touch_episodes(self):
        rows=future([
            bar(1,104,106,98,99),
            bar(2,106,109,105,107),
            bar(3,107,108,98,99),
            bar(4,105,108,101,105),
        ])
        x=evaluate_zone(self.f,rows)
        self.assertEqual(x["touch_episodes"],2)
        self.assertEqual(x["retests"],1)
        self.assertEqual(x["status"],"rebound_or_rejection")

    def test_break_on_contact_followed_by_reclaim(self):
        rows=future([bar(1,102,104,92,93),bar(2,102,106,101,104)])
        x=evaluate_zone(self.f,rows)
        self.assertEqual(x["status"],"break_then_reclaim")
        self.assertEqual(x["first_broken_close"],"2025-01-02")
        self.assertEqual(x["first_reclaim_close"],"2025-01-03")
        self.assertTrue(x["intraday_far_edge_breached"])

    def test_resistance_symmetric_penetration_and_rejection(self):
        f=freeze_zone(report(close=95),zone("resistance",100,105,"bear"),scope="raw_daily")
        rows=future([
            bar(1,97,103,96,102),
            bar(2,101,103,97,98),
        ],filler=(97,99,95,97))
        x=evaluate_zone(f,rows)
        self.assertEqual(x["status"],"rebound_or_rejection")
        self.assertEqual(x["approach_to_first_touch"],"from_below")
        self.assertEqual(x["max_penetration_depth_fraction"],0.6)
        self.assertEqual(x["first_favorable_close_after_touch"],"2025-01-03")
        self.assertEqual(x["touch_windows"]["1"]["favorable_close_outside"],True)

    def test_no_touch_is_not_technical_failure(self):
        x=evaluate_zone(self.f,future([]))
        self.assertEqual(x["status"],"no_touch")
        self.assertIsNone(x["first_touch_date"])
        self.assertFalse(x["asof_windows"]["20"]["touched"])
        self.assertFalse(x["asof_windows"]["60"]["touched"])

    def test_limited_future_is_censored_not_negative(self):
        x=evaluate_zone(self.f, future([bar(1,105,108,97,99)],n=3))
        self.assertEqual(x["status"],"right_censored_after_touch")
        self.assertEqual(x["touch_windows"]["3"]["status"],"right_censored")
        self.assertEqual(x["asof_windows"]["20"]["status"],"right_censored")

    def test_target_is_reach_only_not_fib_reversal(self):
        f=freeze_zone(report(close=105),zone("target",110,115,"bull",
                                           family="swing_abc_projection"),scope="raw_daily")
        rows=future([bar(1,107,114,104,113)])
        x=evaluate_zone(f,rows)
        self.assertEqual(x["status"],"level_reached_no_directional_response_claim")
        self.assertIsNone(x["max_penetration_depth_fraction"])
        self.assertIsNone(x["touch_windows"]["3"]["favorable_close_outside"])

    def test_frozen_source_anchor_and_boundary_guard(self):
        bad=zone()
        bad["known_at"]="2025-01-02"
        with self.assertRaisesRegex(ValueError,"zone_known_after_asof"):
            freeze_zone(report(),bad,scope="raw_daily")
        bad=zone()
        bad["lower"]=101
        with self.assertRaisesRegex(ValueError,"inverted_zone"):
            freeze_zone(report(),bad,scope="raw_daily")

    def test_control_preserves_side_timeframe_width_and_no_signal_contents(self):
        control=matched_control(self.f)
        self.assertIsNotNone(control)
        self.assertEqual(control["upper"]-control["lower"],5.)
        self.assertEqual(control["timeframe"],self.f["timeframe"])
        self.assertEqual(control["role"],"support")
        self.assertEqual(control["source_zone_ids"],[])
        self.assertEqual(control,matched_control(self.f))
        self.assertEqual(control["parent_forecast_id"],self.f["forecast_id"])
        self.assertIn(control["null_distance_multiplier"],(0.7,0.85,1.15,1.3))

    def test_no_null_if_zone_inside_asof_or_wrong_side(self):
        inside=freeze_zone(report(close=98),zone(),scope="raw_daily")
        self.assertIsNone(matched_control(inside))
        under=freeze_zone(report(close=85),zone(),scope="raw_daily")
        self.assertIsNone(matched_control(under))

    def test_invalid_or_duplicate_daily_ohlc_fails_closed(self):
        b=bar(1,102,104,98,100)
        with self.assertRaisesRegex(ValueError,"nonfuture_duplicate"):
            validate_closed_bars([b,b],asof=ASOF)
        with self.assertRaisesRegex(ValueError,"inconsistent_closed_bar"):
            validate_closed_bars([bar(1,102,100,98,101)],asof=ASOF)
        with self.assertRaisesRegex(ValueError,"nonfuture_duplicate"):
            validate_closed_bars([bar(0,105,108,103,105)],asof=ASOF)

    def test_control_ledger_audit_and_counts(self):
        null=matched_control(self.f)
        predictions=[self.f,null]
        out=[evaluate_zone(f,future([])) for f in predictions]
        report=summarize_events(out,predictions)
        self.assertEqual(report["matched_pairs"],1)
        self.assertFalse(report["sample_independent"])
        self.assertIsNone(report["calibrated_probability"])
        with self.assertRaisesRegex(ValueError,"duplicate_frozen_prediction"):
            summarize_events(out,[self.f,self.f])

    def test_outcome_cannot_change_frozen_prediction(self):
        fixed=deepcopy(self.f)
        fut=future([bar(1,106,110,97,98),bar(2,99,105,97,103)])
        a=evaluate_zone(self.f,fut)
        b=evaluate_zone(self.f,future([]))
        self.assertEqual(fixed,self.f)
        self.assertNotEqual(a["status"],b["status"])

    def test_touch_at_last_bar_remains_censored(self):
        filler=[bar(i,106,108,104,106) for i in range(1,60)]
        touched=filler+[bar(60,105,106,97,99)]
        x=evaluate_zone(self.f,touched)
        self.assertEqual(x["status"],"contact_without_outside_reaction")
        self.assertEqual(x["touch_windows"]["1"]["status"],"right_censored")
        self.assertEqual(x["asof_windows"]["60"]["touched"],True)


if __name__=="__main__":
    unittest.main()
