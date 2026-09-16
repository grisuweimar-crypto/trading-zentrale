"""Offline research-export regression tests, using only temporary fixtures."""

import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/generate_history_research.py"
SPEC = importlib.util.spec_from_file_location("research_script", SCRIPT)
research = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(research)


class ResearchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / research.SOURCE
        self.source.parent.mkdir(parents=True)
        self.output = self.root / research.OUTPUT
        self.now = datetime(2026, 9, 5, 1, 17, tzinfo=timezone.utc)
        self.columns = ["date", "symbol", "score", "scoring_version", "extra"]

    def fixture(self, start=date(2026, 8, 1), end=date(2026, 9, 4)):
        return [[(start + timedelta(days=i)).isoformat(), "001", "2.123456789012345678", "v1", "NA"]
                for i in range((end - start).days + 1)]

    def write(self, rows, columns=None):
        with self.source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(self.columns if columns is None else columns)
            writer.writerows(rows)

    def read(self, path):
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.reader(handle))

    def test_month_selection_values_metadata_and_protected_files(self):
        rows = self.fixture()
        rows.append(["2026-08-31", "AVAV", "50.00", "v2", "quoted,\ntext"])
        self.write(rows)
        protected = [self.source]
        for rel in ("artifacts/snapshots/score_history_recent.csv",
                    "artifacts/reports/history_delta.csv",
                    "artifacts/watchlist/watchlist_ALL.csv"):
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"existing\r\nbytes\r\n")
            protected.append(path)
        before = {p: p.read_bytes() for p in protected}
        meta = research.build_research(self.root, self.now)
        self.assertEqual(meta["validation"]["status"], "passed")
        self.assertEqual(self.read(self.output / "history_research.csv"), [self.columns] + rows)
        month = self.read(self.output / "monthly/history_monthly_2026-08.csv")
        self.assertEqual(month, [self.columns] + [r for r in rows if r[0].startswith("2026-08")])
        self.assertEqual([r[0] for r in month[1:] if r[1] == "AVAV"], ["2026-08-31"])
        self.assertEqual(before, {p: p.read_bytes() for p in protected})
        self.assertEqual(meta["source_sha256"], hashlib.sha256(before[self.source]).hexdigest())
        self.assertEqual(meta["source_row_count"], len(rows))
        self.assertEqual(meta["research_row_count"], len(rows))
        self.assertEqual(meta["unique_symbols"], 2)
        self.assertEqual(meta["source_oldest_date"], "2026-08-01")
        self.assertEqual(meta["source_newest_date"], "2026-09-04")
        self.assertEqual(meta["latest_completed_month"], "2026-08")
        self.assertEqual(meta["scoring_versions"], ["v1", "v2"])
        self.assertEqual(meta["universe_versions"], [])
        self.assertEqual(meta["config_versions"], [])
        self.assertEqual(meta["columns"], self.columns)
        self.assertEqual(meta["missing_expected_columns"],
                         [c for c in research.EXPECTED_COLUMNS if c not in self.columns])
        self.assertEqual(meta["research_size_bytes"], (self.output / "history_research.csv").stat().st_size)
        self.assertEqual(meta, json.loads((self.output / "history_research_metadata.json").read_text()))
        first = {p: p.read_bytes() for p in self.output.rglob("*") if p.is_file()}
        research.build_research(self.root, self.now)
        self.assertEqual(first, {p: p.read_bytes() for p in first})

    def test_calendar_boundaries_and_leap_year(self):
        for today, expected in [(date(2027, 1, 5), (date(2026, 12, 1), date(2026, 12, 31))),
                                (date(2024, 3, 1), (date(2024, 2, 1), date(2024, 2, 29))),
                                (date(2026, 10, 1), (date(2026, 9, 1), date(2026, 9, 30)))]:
            with self.subTest(today=today):
                self.assertEqual(research.previous_month(today), expected)
                self.write(self.fixture(expected[0], today))
                now = datetime.combine(today, datetime.min.time(), timezone.utc)
                meta = research.build_research(self.root, now)
                self.assertEqual(meta["validation"]["status"], "passed")
                self.assertTrue((self.output / f"monthly/history_monthly_{expected[0]:%Y-%m}.csv").exists())

    def test_berlin_month_boundary_summer_and_winter(self):
        for now, start, end, month in [
            (datetime(2026, 8, 31, 22, 30, tzinfo=timezone.utc), date(2026, 8, 1), date(2026, 8, 31), "2026-08"),
            (datetime(2026, 12, 31, 23, 30, tzinfo=timezone.utc), date(2026, 12, 1), date(2026, 12, 31), "2026-12")]:
            with self.subTest(now=now):
                self.write(self.fixture(start, end))
                meta = research.build_research(self.root, now)
                self.assertEqual(meta["latest_completed_month"], month)
                self.assertEqual(meta["validation"]["status"], "passed")

    def test_incomplete_month_blocks_snapshot_preserves_existing(self):
        for missing in ("2026-08-01", "2026-08-06", "2026-08-31"):
            with self.subTest(missing=missing):
                self.write([r for r in self.fixture() if r[0] != missing])
                meta = research.build_research(self.root, self.now)
                self.assertEqual(meta["validation"]["status"], "blocked")
                self.assertEqual(meta["validation"]["previous_month_missing_dates"], [missing])
                self.assertFalse((self.output / "monthly").exists())
        existing = self.output / "monthly/history_monthly_2026-08.csv"
        existing.parent.mkdir()
        existing.write_bytes(b"previous publication")
        research.build_research(self.root, self.now)
        self.assertEqual(existing.read_bytes(), b"previous publication")

    def test_missing_month_and_stale_history(self):
        for rows in (self.fixture(date(2026, 9, 1), date(2026, 9, 4)),
                     self.fixture(date(2026, 8, 1), date(2026, 8, 31))):
            self.write(rows)
            meta = research.build_research(self.root, self.now)
            self.assertEqual(meta["validation"]["status"], "blocked")
            self.assertFalse((self.output / "monthly").exists())
        self.assertIn("stale", meta["validation"]["errors"][0])

    def test_duplicates_are_preserved_without_synthetic_rows(self):
        rows = self.fixture()
        rows += [rows[0].copy(), ["2026-08-01", "001", "99", "v2", ""]]
        self.write(rows)
        meta = research.build_research(self.root, self.now)
        self.assertEqual(meta["duplicate_date_symbol_rows"], 2)
        self.assertEqual(self.read(self.output / "history_research.csv")[1:], rows)

    def test_minimal_optional_schema(self):
        rows = [r[:2] for r in self.fixture()]
        self.write(rows, ["date", "symbol"])
        meta = research.build_research(self.root, self.now)
        self.assertEqual(meta["columns"], ["date", "symbol"])
        self.assertIn("rank", meta["missing_expected_columns"])
        self.assertEqual(self.read(self.output / "history_research.csv"), [["date", "symbol"]] + rows)

    def test_backfills_only_complete_closed_months(self):
        rows = self.fixture(date(2026, 6, 15), date(2026, 9, 4))
        self.write(rows)
        meta = research.build_research(self.root, self.now)
        self.assertEqual(meta["monthly_exports"], ["2026-07", "2026-08"])
        self.assertIn("2026-06", meta["skipped_incomplete_months"])
        self.assertEqual(self.read(self.output / "history_research.csv")[1:], rows)

    def test_invalid_or_unreadable_input_creates_no_outputs(self):
        cases = ["", "date,symbol\n", "wrong,schema\nx,y\n", "date,date,symbol\nx,x,A\n",
                 "date,symbol\nbad,A\n", "date,symbol\n2026-08-01,\n",
                 "date,symbol\n2026-09-06,A\n", "date,symbol\n2026-08-01,A,extra\n"]
        for content in cases:
            with self.subTest(content=content):
                self.source.write_text(content)
                with self.assertRaises(ValueError):
                    research.build_research(self.root, self.now)
                self.assertFalse(self.output.exists())
        self.source.unlink()
        with self.assertRaises(FileNotFoundError):
            research.build_research(self.root, self.now)
        self.write(self.fixture())
        with patch.object(Path, "read_bytes", side_effect=PermissionError("unreadable")):
            with self.assertRaises(PermissionError):
                research.build_research(self.root, self.now)

    def test_cli_failure_and_summary(self):
        summary = self.root / "summary.md"
        with patch.object(research, "build_research", side_effect=ValueError("broken history")), \
             patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}), \
             patch("builtins.print"):
            self.assertEqual(research.main(), 1)
        self.assertIn("broken history", summary.read_text())
        self.write([r for r in self.fixture() if r[0] != "2026-08-06"])
        meta = research.build_research(self.root, self.now)
        with patch.object(research, "build_research", return_value=meta), patch("builtins.print"):
            self.assertEqual(research.main(), 1)


if __name__ == "__main__":
    unittest.main()
