import copy
import csv
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scanner.reports.daily_research import generate_daily_research, validate_daily_research
from scanner.reports.historical_matches import (
    HistoricalMatcher, MatchPolicy, bucket,
)


CURRENT = {"symbol": "AAA", "date": "2026-09-30", "rank": "1", "rank_percentile": "0.05",
           "r_code": "R4", "rs3m": "-0.10", "trend200": "0.10"}


def event(symbol="BBB", day="2026-08-03", **changes):
    return dict(CURRENT, symbol=symbol, date=day, **changes)


def prices(symbol, dates, closes=None):
    closes = closes if closes is not None else [100 + i for i in range(len(dates))]
    return [{"symbol": symbol, "date": day, "close": str(close)} for day, close in zip(dates, closes)]


def daily_dates(count):
    # Explicit synthetic seven-day instrument; no weekday/calendar assumptions.
    return [(date(2026, 8, 3) + timedelta(days=i)).isoformat() for i in range(count)]


class HistoricalMatchTests(unittest.TestCase):
    def match(self, history, market=(), minimum=2, cooldown=5, current=None):
        return HistoricalMatcher(history, market, MatchPolicy(minimum, cooldown)).summary(current or CURRENT)

    def test_level_1_requires_all_four_fields_and_uses_other_symbols(self):
        history = [event("BBB"), event("CCC"), event("wrong_rank", rank_percentile="0.15"),
                   event("wrong_code", r_code="R2"), event("wrong_rs", rs3m="0.05"),
                   event("wrong_trend", trend200="-0.10")]
        result = self.match(history)
        self.assertEqual((result["filter_id"], result["N"]), ("level_1", 2))
        self.assertEqual(result["evaluated_levels"], {"level_1": 2})
        self.assertEqual(result["conditions"], {"rank_percentile_bucket": "top10", "r_code": "R4",
                                              "rs3m_bucket": "[-20%,0%)", "trend200_bucket": "[0%,20%)"})
        self.assertTrue(result["sufficient_matches"])
        self.assertIn("r_code=R4", result["filter_description"])

    def test_level_2_relaxes_only_r_code(self):
        result = self.match([event("BBB"), event("CCC", r_code="R2"),
                             event("rank", rank_percentile="0.15"), event("rs", rs3m="0.10"),
                             event("trend", trend200="-0.1")])
        self.assertEqual((result["filter_id"], result["N"]), ("level_2", 2))
        self.assertEqual(result["evaluated_levels"], {"level_1": 1, "level_2": 2})
        self.assertNotIn("r_code", result["conditions"])
        self.assertEqual(result["conditions"]["rank_percentile_bucket"], "top10")

    def test_level_3_coarsens_rank_but_keeps_rs_and_trend(self):
        history = [event("BBB"), event("CCC", r_code="R2"),
                   event("DDD", rank_percentile="0.15", r_code="R3"),
                   event("rank", rank_percentile="0.3"),
                   event("rs", rank_percentile="0.15", rs3m="0.10"),
                   event("trend", rank_percentile="0.15", trend200="-0.1")]
        result = self.match(history, minimum=3)
        self.assertEqual((result["filter_id"], result["N"]), ("level_3", 3))
        self.assertEqual(result["evaluated_levels"], {"level_1": 1, "level_2": 2, "level_3": 3})
        self.assertEqual(result["conditions"], {"rank_percentile_coarse_bucket": "top20",
                                              "rs3m_bucket": "[-20%,0%)", "trend200_bucket": "[0%,20%)"})

    def test_small_level_3_sample_reports_actual_n_without_claiming_sufficiency(self):
        result = self.match([event()], minimum=20)
        self.assertEqual((result["filter_id"], result["N"]), ("level_3", 1))
        self.assertEqual(result["min_matches"], 20)
        self.assertFalse(result["sufficient_matches"])

    def test_missing_historical_rank_is_never_reconstructed(self):
        history = [event("BBB", rank="", rank_percentile="", score="9999", universe_size="100"),
                   event("CCC", rank="1", rank_percentile="", score="9999", universe_size="100")]
        original = copy.deepcopy(history)
        result = self.match(history, minimum=1)
        self.assertEqual((result["filter_id"], result["N"]), ("none", 0))
        self.assertEqual(history, original)

    def test_legacy_rank_is_derived_from_same_snapshot_scores(self):
        history = [event("BBB", rank="", rank_percentile="", score="90", r_code="", run_id="old"),
               event("CCC", rank="", rank_percentile="", score="80", r_code="", run_id="old")]
        result = self.match(history, minimum=1, current=dict(CURRENT, rank_percentile="0.5"))
        self.assertEqual((result["filter_id"], result["N"]), ("level_2", 1))
        self.assertEqual(result["forward_5t"]["N"], 0)
        self.assertEqual(history[0]["rank_percentile"], "")

    def test_missing_or_invalid_required_fields_never_match(self):
        for field in ("rank_percentile", "rs3m", "trend200"):
            for value in ("", None, "NaN", "Infinity", "-Infinity", "garbage"):
                with self.subTest(field=field, value=value):
                    result = self.match([event(**{field: value})], minimum=1)
                    self.assertEqual((result["filter_id"], result["N"]), ("none", 0))
                    result = self.match([event()], minimum=1, current=dict(CURRENT, **{field: value}))
                    self.assertEqual(result["N"], 0)
        for invalid in ("-0.1", "1.1"):
            self.assertEqual(self.match([event(rank_percentile=invalid)], minimum=1)["N"], 0)

    def test_missing_r_code_can_use_level_2_but_is_not_inferred(self):
        result = self.match([event(r_code="")], minimum=1)
        self.assertEqual(result["filter_id"], "level_2")
        self.assertEqual(result["evaluated_levels"], {"level_1": 0, "level_2": 1})

    def test_stored_r0_is_a_valid_level_1_state(self):
        result = self.match([event(r_code="R0")], minimum=1, current=dict(CURRENT, r_code="R0"))
        self.assertEqual(result["filter_id"], "level_1")
        self.assertEqual(result["conditions"]["r_code"], "R0")

    def test_cooldown_per_symbol_counts_five_observed_sessions_including_holiday_gap(self):
        days = ["2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"]
        history = [event("BBB", day) for day in days] + [event("CCC", days[0])]
        market = prices("BBB", days, [100, 110, 120, 130, 140, 150])
        result = self.match(history, market, minimum=1)
        self.assertEqual(result["N"], 3)  # BBB Friday + following Monday; CCC independently.
        self.assertEqual(result["forward_5t"]["N"], 1)
        self.assertAlmostEqual(result["forward_5t"]["median_return"], 0.5)
        self.assertEqual(self.match(history, market, minimum=1, cooldown=6)["N"], 2)
        self.assertEqual(self.match(history, market, minimum=1, cooldown=0)["N"], 7)

    def test_same_day_duplicates_and_intraday_runs_never_count_twice(self):
        history = [event(run_id="first"), event(run_id="second"), event("CCC")]
        self.assertEqual(self.match(history, minimum=1, cooldown=0)["N"], 2)
        # Later run fields must not repair the first day's missing percentile.
        history[0]["rank_percentile"] = ""
        self.assertEqual(self.match(history, minimum=1, cooldown=0)["N"], 1)

    def test_missing_calendar_does_not_fabricate_cooldown_from_weekdays(self):
        result = self.match([event("BBB", "2026-07-01"), event("BBB", "2026-09-01"), event("CCC")], minimum=1)
        self.assertEqual(result["N"], 2)  # Cannot prove that BBB's cooldown elapsed.
        self.assertEqual(result["forward_5t"]["N"], 0)

    def test_fifth_real_session_ignores_weekend_holiday_and_uses_each_symbols_calendar(self):
        dates_b = ["2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"]
        dates_c = ["2026-09-04", "2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11"]
        market = prices("BBB", dates_b, [100, 101, 102, 103, 104, 150]) + prices("CCC", dates_c, [100, 102, 104, 106, 108, 120])
        result = self.match([event("BBB", dates_b[0]), event("CCC", dates_c[0])], market)
        self.assertEqual(result["forward_5t"]["N"], 2)
        self.assertAlmostEqual(result["forward_5t"]["median_return"], 0.35)
        # At Friday CCC already has its fifth outcome; BBB needs Monday.
        friday = self.match([event("BBB", dates_b[0]), event("CCC", dates_c[0])], market,
                            current=dict(CURRENT, date="2026-09-11"))
        self.assertEqual(friday["forward_5t"]["N"], 1)
        self.assertAlmostEqual(friday["forward_5t"]["median_return"], 0.2)

    def test_missing_20t_price_and_different_n_for_each_horizon(self):
        history, market = [], []
        for symbol, count in (("BBB", 6), ("CCC", 11), ("DDD", 21), ("EEE", 41)):
            history.append(event(symbol))
            market.extend(prices(symbol, daily_dates(count)))
        result = self.match(history, market)
        self.assertEqual(result["N"], 4)
        self.assertEqual([result[f"forward_{h}t"]["N"] for h in (5, 10, 20, 40)], [4, 3, 2, 1])
        for horizon in (5, 10, 20, 40):
            self.assertAlmostEqual(result[f"forward_{horizon}t"]["median_return"], horizon / 100)

    def test_even_median_positive_count_and_rate(self):
        market = prices("BBB", daily_dates(6), [100, 100, 100, 100, 100, 80])
        market += prices("CCC", daily_dates(6), [100, 100, 100, 100, 100, 140])
        result = self.match([event("BBB"), event("CCC")], market)["forward_5t"]
        self.assertEqual(result["N"], 2)
        self.assertAlmostEqual(result["median_return"], 0.10)
        self.assertEqual(result["positive_count"], 1)
        self.assertEqual(result["positive_rate"], 0.5)

    def test_odd_median_and_zero_return_is_not_positive(self):
        history, market = [], []
        for symbol, end in (("BBB", 80), ("CCC", 100), ("DDD", 140)):
            history.append(event(symbol))
            market.extend(prices(symbol, daily_dates(6), [100] * 5 + [end]))
        result = self.match(history, market)["forward_5t"]
        self.assertEqual(result["median_return"], 0)
        self.assertEqual(result["positive_count"], 1)
        self.assertEqual(result["positive_rate"], 1 / 3)

    def test_no_event_day_close_means_no_return_not_nearest_or_scanner_close(self):
        market = prices("BBB", daily_dates(7)[1:])
        result = self.match([event(close="100")], market, minimum=1)
        self.assertEqual(result["N"], 1)
        self.assertEqual(result["forward_5t"], {"N": 0, "median_return": None, "positive_count": 0, "positive_rate": None})

    def test_scanner_weekend_rows_do_not_create_price_sessions(self):
        history = [event("BBB", day, close="100") for day in daily_dates(41)]
        result = self.match(history, minimum=1)
        self.assertEqual(result["N"], 1)
        self.assertEqual([result[f"forward_{h}t"]["N"] for h in (5, 10, 20, 40)], [0, 0, 0, 0])

    def test_invalid_closes_and_duplicate_market_dates_do_not_count_as_sessions(self):
        days = daily_dates(9)
        market = prices("BBB", days, [100, 101, 0, "NaN", -1, 102, 103, 104, 150])
        market += [market[1].copy(), market[-1].copy()]
        result = self.match([event()], market, minimum=1)
        self.assertEqual(result["forward_5t"]["N"], 1)
        self.assertAlmostEqual(result["forward_5t"]["median_return"], 0.5)

    def test_conflicting_prices_are_not_arbitrarily_selected(self):
        market = prices("BBB", daily_dates(6)) + prices("BBB", ["2026-08-03"], [999])
        self.assertEqual(self.match([event()], market, minimum=1)["forward_5t"]["N"], 0)

    def test_future_scanner_fields_and_price_only_rows_cannot_supply_matches(self):
        history = [event("BBB"), event("CCC", rank_percentile=""),
                   event("DDD", observation_type="price_backfill"),
                   event("EEE", "2026-09-30"), event("FFF", "2026-10-01")]
        before = self.match(history, minimum=1)
        self.assertEqual(before["N"], 1)
        # Both a future repair of CCC and an entirely new future symbol are ignored.
        history += [event("CCC", "2026-10-02"), event("GGG", "2026-10-02")]
        self.assertEqual(self.match(history, minimum=1), before)

    def test_outcome_prices_do_not_choose_filter_or_event_sample(self):
        history = [event("BBB"), event("CCC", r_code="R2")]
        with_prices = self.match(history, prices("CCC", daily_dates(41)), minimum=1)
        without_prices = self.match(history, minimum=1)
        self.assertEqual(with_prices, without_prices)  # L1 BBB wins despite CCC having outcomes.
        self.assertEqual(with_prices["filter_id"], "level_1")
        self.assertEqual(with_prices["forward_5t"]["N"], 0)

    def test_future_prices_after_snapshot_do_not_leak_into_outcomes(self):
        result = self.match([event()], prices("BBB", daily_dates(41)), minimum=1,
                            current=dict(CURRENT, date="2026-08-07"))
        self.assertEqual(result["N"], 1)
        self.assertEqual(result["forward_5t"]["N"], 0)

    def test_all_bucket_boundaries_are_deterministic(self):
        for value, label in [(0, "top10"), (.1, "top10"), (.100001, "10..20"), (.2, "10..20"),
                             (.4, "20..40"), (.6, "40..60"), (.8, "60..80"), (.9, "80..90"), (1, "bottom10")]:
            self.assertEqual(bucket("rank_percentile", value), label)
        for value, label in [(0, "top20"), (.2, "top20"), (.200001, "20..60"),
                             (.6, "20..60"), (.600001, "bottom40"), (1, "bottom40")]:
            self.assertEqual(bucket("rank_percentile_coarse", value), label)
        for field, edges, labels in (
            ("rs3m", [-.2, 0, .15, .3], ["<-20%", "[-20%,0%)", "[0%,15%)", "[15%,30%)", ">=30%"]),
            ("trend200", [-.2, 0, .2, .4], ["<-20%", "[-20%,0%)", "[0%,20%)", "[20%,40%)", ">=40%"]),
        ):
            for i, edge in enumerate(edges):
                self.assertEqual(bucket(field, edge - .000001), labels[i])
                self.assertEqual(bucket(field, edge), labels[i + 1])

    def test_invalid_configuration_rejected(self):
        for minimum, cooldown in ((0, 5), (-1, 5), (1, -1), (1.5, 5), (1, True)):
            with self.assertRaises(ValueError):
                MatchPolicy(minimum, cooldown)


class HistoricalMatchPublicationTests(unittest.TestCase):
    def test_generation_validation_provenance_and_semantic_tamper_detection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            research = root / "artifacts/research"
            research.mkdir(parents=True)
            def write_csv(name, rows):
                with (research / name).open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
            write_csv("latest_scanner.csv", [CURRENT])
            write_csv("history_recent.csv", [event("BBB"), event("CCC", r_code="R2")])
            write_csv("price_backfill.csv", prices("BBB", daily_dates(6)))
            metadata = {"snapshot_id": "test-snapshot", "as_of": CURRENT["date"]}
            (research / "history_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
            originals = {path: path.read_bytes() for path in research.glob("*.csv")}
            result = generate_daily_research(root, match_policy=MatchPolicy(1, 5))
            self.assertEqual(result["historical_match_method"]["min_matches"], 1)
            self.assertEqual(result["symbols"]["AAA"]["historical_matches"]["filter_id"], "level_1")
            self.assertEqual(result["symbols"]["AAA"]["historical_matches"]["forward_5t"]["N"], 1)
            self.assertEqual(validate_daily_research(root), result)
            self.assertEqual(originals, {path: path.read_bytes() for path in originals})
            # An updated file hash cannot conceal a wrong mathematical result.
            result["symbols"]["AAA"]["historical_matches"]["forward_5t"]["median_return"] = 99
            raw = json.dumps(result).encode("utf-8")
            (research / "daily_research.json").write_bytes(raw)
            metadata = json.loads((research / "history_metadata.json").read_text(encoding="utf-8"))
            metadata["daily_research"]["sha256"] = hashlib.sha256(raw).hexdigest()
            (research / "history_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "semantics mismatch"):
                validate_daily_research(root)


if __name__ == "__main__":
    unittest.main()
