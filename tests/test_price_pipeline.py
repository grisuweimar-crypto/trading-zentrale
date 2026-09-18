from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from scripts import prefetch_market_history as fetch
from scanner.data.price_history import MARKET_COLUMNS, PRICE_COLUMNS, coverage, merge_prices, validated_rows
from scanner.reports.daily_research import generate_daily_research, validate_daily_research
from scanner.reports.research_views import ValidationPolicy, build_views, encode_csv, parse_csv, refresh_price_backfill
from scanner.reports.research_validation import validate_publication


NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)


def market_rows(symbol, count=6, start=date(2026, 8, 3)):
    return [{"date": (start + timedelta(days=i)).isoformat(), "symbol": symbol, "currency": "USD",
             "open": str(100 + i), "high": str(101 + i), "low": str(99 + i), "close": str(100 + i),
             "volume": "100", "retrieved_at": "2026-09-18T00:00:00+00:00"} for i in range(count)]


class PricePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def write(self, relative, columns, rows):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode_csv(columns, rows))
        return path

    def universe(self, symbols=("AAA", "BBB")):
        rows = [{"symbol": s, "currency": "USD", "date": "2026-09-18", "snapshot_id": "snap"} for s in symbols]
        path = self.write("artifacts/research/latest_scanner.csv", rows[0].keys(), rows)
        meta = {"snapshot_id": "snap", "as_of": "2026-09-18", "latest_run_complete": True,
                "latest_scanner": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}
        (path.parent / "history_metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    def run_fetch(self, rows, **kwargs):
        with patch.object(fetch, "_download", return_value=pd.DataFrame(rows)) as download:
            result = fetch.prefetch_history(self.root, now=NOW, sleep=lambda _: None, **kwargs)
        return result, download

    def test_universe_discovery_bootstraps_every_current_symbol(self):
        self.universe(("AAA", "BBB", "CCC"))
        rows = sum((market_rows(s, 300, date(2025, 8, 1)) for s in ("AAA", "BBB", "CCC")), [])
        result, download = self.run_fetch(rows)
        self.assertEqual(set(result.symbol), {"AAA", "BBB", "CCC"})
        self.assertEqual(download.call_count, 1)
        self.assertEqual(download.call_args.args[0], ["AAA", "BBB", "CCC"])
        self.assertEqual(download.call_args.kwargs["period"], "2y")
        self.assertEqual(result.attrs["coverage"]["complete_symbol_count"], 3)

    def test_existing_history_uses_incremental_start_and_overlap(self):
        self.universe(("AAA",))
        rows = market_rows("AAA", 6)
        self.write(fetch.CACHE, MARKET_COLUMNS, rows)
        result, download = self.run_fetch(market_rows("AAA", 8)[-4:], minimum_days=6)
        self.assertIsNone(download.call_args.kwargs["period"])
        self.assertEqual(download.call_args.kwargs["start"], "2026-08-03")
        self.assertEqual(len(result), 8)
        self.assertEqual(result.attrs["coverage"]["symbols"]["AAA"]["update_mode"], "incremental")

    def test_short_history_is_kept_and_not_fully_reloaded_on_next_day(self):
        self.universe(("AAA",))
        result, download = self.run_fetch(market_rows("AAA", 6), minimum_days=300)
        self.assertEqual([c.kwargs["period"] for c in download.call_args_list], ["2y", "max"])
        self.assertEqual(result.attrs["coverage"]["partial_symbol_count"], 1)
        result, download = self.run_fetch(market_rows("AAA", 7), minimum_days=300)
        self.assertEqual(download.call_count, 1)
        self.assertIsNone(download.call_args.kwargs["period"])
        self.assertEqual(len(result), 7)

    def test_short_period_is_extended_when_deeper_history_is_available(self):
        self.universe(("AAA",))
        def provider(tickers, **kwargs):
            return pd.DataFrame(market_rows("AAA", 6 if kwargs["period"] == "2y" else 41))
        with patch.object(fetch, "_download", side_effect=provider):
            result = fetch.prefetch_history(self.root, minimum_days=40, now=NOW, sleep=lambda _: None)
        self.assertEqual(len(result), 41)
        self.assertEqual(result.attrs["coverage"]["complete_symbol_count"], 1)

    def test_provider_failure_isolated_and_retry_only_failed_ticker(self):
        self.universe()
        calls = []
        def provider(tickers, **kwargs):
            calls.append(tickers)
            frame = pd.DataFrame(market_rows("AAA", 6) if "AAA" in tickers else [], columns=MARKET_COLUMNS)
            frame.attrs["errors"] = {"BBB": "YFRateLimitError: Too Many Requests"}
            return frame
        waits = []
        with patch.object(fetch, "_download", side_effect=provider):
            result = fetch.prefetch_history(self.root, minimum_days=5, now=NOW, sleep=waits.append)
        self.assertEqual(calls, [["AAA", "BBB"], ["BBB"], ["BBB"]])
        self.assertIn(2, waits)
        self.assertIn(4, waits)
        self.assertEqual(result.attrs["coverage"]["covered_symbol_count"], 1)
        self.assertEqual(result.attrs["coverage"]["symbols"]["BBB"]["status"], "provider_error")

    def test_whole_provider_outage_fails_without_destroying_cache(self):
        self.universe(("AAA",))
        path = self.write(fetch.CACHE, MARKET_COLUMNS, market_rows("AAA"))
        before = path.read_bytes()
        with patch.object(fetch, "_download", side_effect=ConnectionError("offline")):
            with self.assertRaisesRegex(RuntimeError, "every requested ticker"):
                fetch.prefetch_history(self.root, minimum_days=5, now=NOW, sleep=lambda _: None)
        self.assertEqual(path.read_bytes(), before)

    def test_mapping_missing_never_guesses_crypto_quote_or_isin(self):
        self.universe(("AAA", "CRYPTO:SOL", "US1234567890"))
        result, download = self.run_fetch(market_rows("AAA"), minimum_days=5)
        self.assertEqual(download.call_args.args[0], ["AAA"])
        for symbol in ("CRYPTO:SOL", "US1234567890"):
            self.assertEqual(result.attrs["coverage"]["symbols"][symbol]["status"], "ticker_mapping_missing")

    def test_existing_crypto_mapping_retains_identity_and_eur_quote(self):
        self.universe(("CRYPTO:SOL",))
        self.write("artifacts/watchlist/watchlist_full.csv", ["asset_id", "YahooSymbol"],
                   [{"asset_id": "CRYPTO:SOL", "YahooSymbol": "SOL-EUR"}])
        result, download = self.run_fetch(market_rows("SOL-EUR"), minimum_days=5)
        self.assertEqual(download.call_args.args[0], ["SOL-EUR"])
        self.assertEqual(set(result.symbol), {"CRYPTO:SOL"})

    def test_exchange_suffixes_are_not_rewritten(self):
        symbols = ["ABC" + suffix for suffix in (".TO", ".V", ".AX", ".L", ".DE", ".PA", ".HK", ".KS", ".T", ".JO", ".MX")]
        mappings = fetch.resolve_mappings(self.root, [{"symbol": s} for s in symbols])
        self.assertEqual(mappings, {s: (s, None) for s in symbols})

    def test_changed_or_conflicting_mapping_is_not_mixed_with_existing_prices(self):
        self.universe()
        self.write(fetch.CACHE, MARKET_COLUMNS, market_rows("AAA"))
        state = {"symbols": {"AAA": {"provider_symbol": "OLD"}}}
        (self.root / fetch.STATE).write_text(json.dumps(state), encoding="utf-8")
        result, download = self.run_fetch(market_rows("BBB"), minimum_days=5)
        self.assertEqual(download.call_args.args[0], ["BBB"])
        self.assertEqual(result.attrs["coverage"]["symbols"]["AAA"]["status"], "ticker_mapping_missing")
        # The rejected mapping must not become accepted on tomorrow's retry.
        result, download = self.run_fetch(market_rows("BBB"), minimum_days=5)
        self.assertEqual(download.call_args.args[0], ["BBB"])
        self.assertEqual(result.attrs["coverage"]["symbols"]["AAA"]["provider_symbol"], "OLD")

    def test_structural_provider_schema_error_is_not_swallowed(self):
        self.universe(("AAA",))
        with patch.object(fetch, "_download", side_effect=fetch.ProviderSchemaError("missing close")):
            with self.assertRaises(fetch.ProviderSchemaError):
                fetch.prefetch_history(self.root, now=NOW, sleep=lambda _: None)

    def test_transient_json_provider_error_is_retried(self):
        self.universe(("AAA",))
        with patch.object(fetch, "_download", side_effect=[json.JSONDecodeError("bad response", "", 0), pd.DataFrame(market_rows("AAA"))]) as download:
            result = fetch.prefetch_history(self.root, minimum_days=5, now=NOW, sleep=lambda _: None)
        self.assertEqual(download.call_count, 2)
        self.assertEqual(result.attrs["coverage"]["covered_symbol_count"], 1)

    def test_bad_snapshot_is_rejected_before_network(self):
        self.universe()
        path = self.root / "artifacts/research/latest_scanner.csv"
        path.write_bytes(path.read_bytes().replace(b"AAA", b"ZZZ"))
        with patch.object(fetch, "_download") as download:
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                fetch.prefetch_history(self.root, now=NOW)
        download.assert_not_called()

    def test_duplicate_and_invalid_prices_and_revision_policy(self):
        original = market_rows("AAA", 1)
        revision = dict(original[0], close="100.5")
        rows, issues = merge_prices(original, [revision] + market_rows("BBB", 1) * 2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["close"], "100")
        self.assertEqual(issues["AAA"]["provider_revision_kept_existing"], 1)
        ambiguous, issues = merge_prices([], original + [revision])
        self.assertEqual(ambiguous, [])
        self.assertEqual(issues["AAA"]["conflicting_duplicate_session"], 1)
        still_ambiguous, _ = merge_prices(original + [revision], original)
        self.assertEqual(still_ambiguous, [])

    def test_invalid_ohlcv_is_excluded(self):
        base = market_rows("AAA", 1)[0]
        for changes in ({"symbol": ""}, {"date": "bad"}, {"close": "NaN"}, {"close": "0"},
                        {"high": "99"}, {"low": "101"}, {"volume": "-1"}, {"open": "Infinity"}):
            rows, issues = validated_rows([dict(base, **changes)])
            self.assertEqual(rows, [])
            self.assertTrue(issues)

    def test_incomplete_current_day_bar_is_not_persisted(self):
        self.universe(("AAA",))
        result, _ = self.run_fetch(market_rows("AAA", 3, date(2026, 9, 17)), minimum_days=1)
        self.assertEqual(result.date.tolist(), ["2026-09-17", "2026-09-18"])

    def test_coverage_counts_are_exact_including_missing_symbols(self):
        rows = market_rows("AAA", 300, date(2025, 1, 1)) + market_rows("BBB", 40)
        report = coverage(["AAA", "BBB", "CCC"], rows + [rows[0]], as_of="2026-09-18")
        self.assertEqual((report["required_symbol_count"], report["covered_symbol_count"], report["complete_symbol_count"],
                          report["partial_symbol_count"], report["unavailable_symbol_count"]), (3, 2, 1, 1, 1))
        self.assertEqual((report["median_sessions"], report["min_sessions"], report["max_sessions"]), (40, 0, 300))
        self.assertEqual(report["symbols_with_40_or_more_sessions"], 2)
        self.assertEqual(report["symbols_with_300_or_more_sessions"], 1)

    def test_different_exchange_sessions_stay_distinct(self):
        rows = market_rows("US", 3, date(2026, 9, 4))
        rows[1]["date"], rows[2]["date"] = "2026-09-08", "2026-09-09"
        rows += market_rows("JP", 3, date(2026, 9, 4))
        rows[-2]["date"], rows[-1]["date"] = "2026-09-07", "2026-09-08"
        merged, _ = merge_prices([], rows)
        self.assertEqual([r["date"] for r in merged if r["symbol"] == "US"], ["2026-09-04", "2026-09-08", "2026-09-09"])
        self.assertEqual([r["date"] for r in merged if r["symbol"] == "JP"], ["2026-09-04", "2026-09-07", "2026-09-08"])

    def test_yahoo_batch_union_does_not_turn_nan_exchange_holidays_into_sessions(self):
        columns = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Volume"], ["AAA", "BBB"]])
        data = pd.DataFrame(100.0, index=pd.to_datetime(["2026-09-04", "2026-09-07", "2026-09-08"]), columns=columns)
        data.loc[pd.Timestamp("2026-09-07"), (slice(None), "AAA")] = float("nan")
        with patch.object(fetch.yf, "download", return_value=data) as download:
            frame = fetch._download(["AAA", "BBB"], period="2y", end="2026-09-09")
        self.assertEqual(frame[frame.symbol == "AAA"].date.tolist(), ["2026-09-04", "2026-09-08"])
        self.assertEqual(frame[frame.symbol == "BBB"].date.tolist(), ["2026-09-04", "2026-09-07", "2026-09-08"])
        self.assertEqual(download.call_args.kwargs["threads"], 4)
        self.assertFalse(download.call_args.kwargs["auto_adjust"])

    def test_batch_size_limits_each_provider_request(self):
        self.universe(("AAA", "BBB", "CCC", "DDD", "EEE"))
        def provider(tickers, **kwargs):
            return pd.DataFrame(sum((market_rows(s) for s in tickers), []))
        with patch.object(fetch, "_download", side_effect=provider) as download:
            fetch.prefetch_history(self.root, minimum_days=5, batch_size=2, now=NOW, sleep=lambda _: None)
        self.assertEqual([len(c.args[0]) for c in download.call_args_list], [2, 2, 1])

    def test_price_refresh_then_daily_produces_all_four_cross_universe_outcomes(self):
        rows = [dict(date="2026-08-03", symbol="BBB", score="50", rank="1", rank_percentile="0.5", r_code="R4", rs3m="-0.1", trend200="0.1"),
                dict(date="2026-09-18", symbol="AAA", score="50", rank="1", rank_percentile="0.5", r_code="R4", rs3m="-0.1", trend200="0.1"),
                dict(date="2026-09-18", symbol="BBB", score="40", rank="2", rank_percentile="1", r_code="R4", rs3m="-0.1", trend200="0.1")]
        self.write("artifacts/snapshots/score_history.csv", rows[0].keys(), rows)
        self.write("artifacts/research/history_analysis.csv", rows[0].keys(), rows[:1])
        meta = build_views(self.root, now=NOW, policy=ValidationPolicy(expected_symbol_count=2))
        protected = {path: path.read_bytes() for path in (self.root / "artifacts/research").glob("*.csv") if path.name != "price_backfill.csv"}
        market = market_rows("AAA", 41) + market_rows("BBB", 41)
        for row in market:
            row.update(score="666", rank="666", r_code="R1", rs3m="666", trend200="666", confidence="666", cycle="666")
        self.run_fetch(market, minimum_days=40)
        refreshed = refresh_price_backfill(self.root)
        self.assertEqual(refreshed["snapshot_id"], meta["snapshot_id"])
        self.assertEqual(refreshed["price_coverage"]["covered_symbol_count"], 2)
        columns, exported = parse_csv((self.root / "artifacts/research/price_backfill.csv").read_bytes())
        self.assertEqual(columns, PRICE_COLUMNS)
        self.assertEqual(len(exported), 82)
        self.assertEqual(protected, {path: path.read_bytes() for path in protected})
        daily = generate_daily_research(self.root)
        for h in (5, 10, 20, 40):
            stats = daily["symbols"]["AAA"]["historical_matches"][f"forward_{h}t"]
            self.assertEqual(stats["N"], 1)
            self.assertAlmostEqual(stats["median_return"], h / 100)
            self.assertEqual(daily["historical_outcome_coverage"][f"symbols_with_forward_{h}t_samples"], 1)
        self.assertEqual(daily["historical_outcome_coverage"]["price_symbols_available"], 2)
        validate_daily_research(self.root)
        validate_publication(self.root)
        # Coverage is derived, not a free-form success claim.
        meta_path = self.root / "artifacts/research/history_metadata.json"
        saved = json.loads(meta_path.read_text(encoding="utf-8"))
        saved["price_coverage"]["covered_symbol_count"] = 999
        meta_path.write_text(json.dumps(saved), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "price coverage"):
            validate_publication(self.root)


if __name__ == "__main__":
    unittest.main()
