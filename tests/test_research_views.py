"""Offline integrity tests; fixture observations never enter project artifacts."""
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scanner.reports import research_views as views


class ResearchViewsTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.now = datetime(2026, 9, 17, tzinfo=timezone.utc)
        self.columns = ["date", "symbol", "score", "run_id", "confidence_label"]
        self.rows = [dict(zip(self.columns, ["2026-09-17", symbol, score, "run1", "high"]))
                     for symbol, score in [("A", "50.00"), ("B", "50"), ("C", "10")]]
        self.write(views.SOURCE, self.columns, self.rows)
        self.policy = views.ValidationPolicy(expected_symbol_count=3)

    def write(self, relative, columns, rows):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(views.encode_csv(columns, rows))
        return path

    def build(self, **kwargs):
        return views.build_views(self.root, now=self.now, policy=kwargs.pop("policy", self.policy), **kwargs)

    def path(self, name):
        return self.root / views.OUTPUT / (name + ".csv")

    def read(self, name):
        return views.parse_csv(self.path(name).read_bytes())[1]

    def test_complete_ranks_ties_percentiles_identity_hashes_and_utc(self):
        meta = self.build()
        self.assertTrue(meta["latest_run_complete"])
        latest = self.read("latest_scanner")
        self.assertEqual([r["rank"] for r in latest], ["1", "1", "3"])
        self.assertEqual([r["rank_percentile"] for r in latest], [str(1/3), str(1/3), "1.0"])
        self.assertEqual({r["universe_size"] for r in latest}, {"3"})
        self.assertEqual({r["confidence_label"] for r in latest}, {"high"})
        for name in ("latest_scanner", "history_recent"):
            self.assertEqual({r["snapshot_id"] for r in self.read(name)}, {meta["snapshot_id"]})
            self.assertEqual({r["generated_at"] for r in self.read(name)}, {meta["generated_at"]})
        self.assertEqual(datetime.fromisoformat(meta["generated_at"]).utcoffset(), timedelta(0))
        for name in ("history_analysis", "latest_scanner", "history_recent", "price_backfill"):
            self.assertEqual(meta[name]["sha256"], hashlib.sha256(self.path(name).read_bytes()).hexdigest())

    def test_partial_preserves_views_archive_and_published_identity(self):
        first = self.build()
        protected = {self.path(n): self.path(n).read_bytes() for n in ("latest_scanner", "history_recent", "history_analysis")}
        self.write(views.SOURCE, self.columns, self.rows[:1])
        meta = self.build()
        self.assertFalse(meta["latest_run_complete"])
        self.assertIn("symbol_count_below_threshold", meta["latest_run_error"])
        self.assertEqual(meta["snapshot_id"], first["snapshot_id"])
        self.assertNotEqual(meta["attempt_id"], first["attempt_id"])
        self.assertEqual(protected, {p: p.read_bytes() for p in protected})

    def test_recent_calendar_cutoff_derived_and_legacy_prices_preserved(self):
        cols = self.columns + ["observation_type", "data_source"]
        old = [dict(self.rows[0], date=dt, observation_type="observed_scanner", data_source="scanner_run")
               for dt in ("2026-05-19", "2026-05-20", "2026-05-21")]
        old += [dict(old[-1]), dict(old[-1], symbol="PRICE", observation_type="market_data", data_source="yahoo_ohlcv")]
        self.write(views.OUTPUT + "/history_analysis.csv", cols, old)
        meta = self.build()
        archive = self.read("history_analysis")
        self.assertEqual(archive[:len(old)], old)
        recent = self.read("history_recent")
        self.assertEqual(len(recent), 6)
        self.assertNotIn("2026-05-19", {r["date"] for r in recent})
        self.assertEqual({r["observation_type"] for r in recent}, {"observed_scanner"})
        self.assertTrue(meta["validation"]["warnings"])
        for row in recent:
            self.assertIn({k: v for k, v in row.items() if k not in views.VIEW_COLUMNS}, archive)

    def test_existing_values_duplicates_and_ranks_never_rewritten(self):
        cols = self.columns + ["rank"]
        old = dict(self.rows[0], date="2026-01-01", score="1.12345678901234567890", rank="")
        self.write(views.OUTPUT + "/history_analysis.csv", cols, [old, old])
        self.build()
        archive = self.read("history_analysis")
        self.assertEqual([{c: r[c] for c in cols} for r in archive[:2]], [old, old])
        before = self.path("history_analysis").read_bytes()
        self.build()
        self.assertEqual(before, self.path("history_analysis").read_bytes())

    def test_price_allowlist_no_scanner_metrics_and_immutable_values(self):
        cols = views.PRICE_COLUMNS[:8] + ["score", "rank", "retrieved_at"]
        row = dict(zip(cols, ["2026-01-01", "P", "USD", "1", "2", "0.5", "1.50", "20", "99", "1", "2026-09-16T10:00:00+00:00"]))
        self.write(views.MARKET, cols, [row])
        self.build()
        prices = self.read("price_backfill")
        self.assertEqual(set(prices[0]), set(views.PRICE_COLUMNS))
        self.assertEqual(prices[0]["retrieved_at"], row["retrieved_at"])
        before = self.path("price_backfill").read_bytes()
        self.write(views.MARKET, cols, [dict(row, close="999")])
        self.build()
        self.assertEqual(before, self.path("price_backfill").read_bytes())
        self.assertNotIn("P", {r["symbol"] for r in self.read("history_analysis")})

    def test_missing_duplicate_nonfinite_aborted_and_malformed_scans(self):
        self.build()
        before = self.path("latest_scanner").read_bytes()
        cases = [(["date", "symbol"], self.rows, "missing_required_columns"),
                 (self.columns, self.rows + self.rows[:1], "duplicate_symbols"),
                 (self.columns, [dict(r, score="NaN") for r in self.rows], "invalid_score_data"),
                 (self.columns + ["scan_status"], [dict(r, scan_status="aborted") for r in self.rows], "scan_aborted"),
                 (self.columns + ["observation_type"], [dict(r, observation_type="price_backfill") for r in self.rows], "non_scanner_observations")]
        for cols, rows, error in cases:
            with self.subTest(error=error):
                self.write(views.SOURCE, cols, rows)
                meta = self.build()
                self.assertIn(error, meta["latest_run_error"])
                self.assertEqual(before, self.path("latest_scanner").read_bytes())
        (self.root / views.SOURCE).write_bytes(b'date,symbol,score\n2026-09-17,A,"broken')
        self.assertFalse(self.build()["latest_run_complete"])
        self.assertEqual(before, self.path("latest_scanner").read_bytes())

    def test_baseline_from_previous_source_and_bootstrap_fails_closed(self):
        meta = self.build(policy=views.ValidationPolicy())
        self.assertIn("missing_completeness_baseline", meta["latest_run_error"])
        self.assertFalse(self.path("latest_scanner").exists())
        older = [dict(r, date="2026-09-16", run_id="old") for r in self.rows]
        self.write(views.SOURCE, self.columns, older + self.rows)
        self.assertTrue(self.build(policy=views.ValidationPolicy())["latest_run_complete"])

    def test_same_day_run_selection_does_not_combine_partial_runs(self):
        self.build()
        self.write(views.SOURCE, self.columns, self.rows + [dict(self.rows[0], run_id="run2")])
        self.assertFalse(self.build()["latest_run_complete"])

    def test_numeric_universe_excludes_missing_score_with_configured_coverage(self):
        self.write(views.SOURCE, self.columns, self.rows[:2] + [dict(self.rows[2], score="")])
        self.build(policy=views.ValidationPolicy(expected_symbol_count=3, min_score_ratio=0.6))
        latest = self.read("latest_scanner")
        self.assertEqual({r["universe_size"] for r in latest}, {"2"})
        self.assertEqual(latest[2]["rank"], "")
        self.assertEqual(latest[2]["rank_percentile"], "")

    def test_metadata_is_last_and_write_failure_cannot_claim_success(self):
        self.build()
        manifest = self.root / views.OUTPUT / "history_metadata.json"
        before = manifest.read_bytes()
        calls = []
        real = views.atomic_write
        def write(path, content):
            calls.append(path)
            if path == self.path("latest_scanner"):
                raise OSError("simulated disk failure")
            real(path, content)
        with patch.object(views, "atomic_write", side_effect=write):
            with self.assertRaises(OSError):
                self.build()
        self.assertEqual(before, manifest.read_bytes())
        self.assertNotIn(manifest, calls)
        self.assertFalse((manifest.parent / ".research_views.lock").exists())
        self.build()  # Recovery after failed publication.

    def test_new_complete_scan_replaces_latest_and_preserves_old_observations(self):
        first = self.build()
        old = self.read("history_analysis")
        self.write(views.SOURCE, self.columns, [dict(r, run_id="run2", score="20") for r in self.rows])
        second = self.build()
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertEqual({r["score"] for r in self.read("latest_scanner")}, {"20"})
        self.assertEqual(self.read("history_analysis")[:3], old)
        self.assertEqual(len(self.read("history_analysis")), 6)

    def test_stale_source_missing_newline_and_missing_file_fail_closed(self):
        self.build()
        self.write(views.SOURCE, self.columns, [dict(r, date="2026-09-16") for r in self.rows])
        self.assertIn("scan_older_than_last_complete", self.build()["latest_run_error"])
        path = self.write(views.SOURCE, self.columns, self.rows)
        path.write_bytes(path.read_bytes().rstrip(b"\n"))
        self.assertFalse(self.build()["latest_run_complete"])
        path.unlink()
        self.assertIn("missing_source", self.build()["latest_run_error"])

    def test_publication_order_and_source_mutation_guard(self):
        calls = []
        real = views.atomic_write
        def write(path, content):
            calls.append(path.name)
            real(path, content)
        with patch.object(views, "atomic_write", side_effect=write):
            self.build()
        self.assertEqual(calls[-1], "history_metadata.json")
        before = (self.root / views.OUTPUT / "history_metadata.json").read_bytes()
        real_validate = views.validate_run
        def validate(*args):
            (self.root / views.SOURCE).write_bytes(b"changed")
            return real_validate(*args)
        with patch.object(views, "validate_run", side_effect=validate):
            with self.assertRaisesRegex(ValueError, "Input changed"):
                self.build()
        self.assertEqual(before, (self.root / views.OUTPUT / "history_metadata.json").read_bytes())

    def test_unknown_schema_reported_and_invalid_price_schema_rejected(self):
        self.write(views.SOURCE, self.columns + ["new_field"], self.rows)
        meta = self.build()
        self.assertIn("unknown_source_columns: new_field", meta["validation"]["warnings"])
        self.write(views.OUTPUT + "/price_backfill.csv", views.PRICE_COLUMNS + ["score"], [])
        with self.assertRaisesRegex(ValueError, "price_backfill_schema_mismatch"):
            self.build()

    def test_partial_cannot_publish_inconsistent_retained_views(self):
        self.build()
        recent = self.path("history_recent")
        recent.write_bytes(recent.read_bytes().replace(b"high", b"changed"))
        self.write(views.SOURCE, self.columns, self.rows[:1])
        with self.assertRaisesRegex(ValueError, "inconsistent_retained_views"):
            self.build()


if __name__ == "__main__":
    unittest.main()
