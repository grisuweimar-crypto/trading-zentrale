"""Offline regression tests: python -m unittest discover -s tests -v."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from scanner.reports.history_delta import write_recent_score_history


class RecentHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.full = self.root / "score_history.csv"
        self.recent = self.root / "score_history_recent.csv"

    def write(self, rows):
        self.full.write_text("date,symbol,score,extra\n" + rows, encoding="utf-8")

    def test_window_values_archive_and_repeat(self):
        self.write("2020-01-07,OLD,1,\n2020-04-01,001,2.123456789012345678,NA\n"
                   "2020-01-08,EDGE,3.00,\n2020-03-02,A,4,x\n")
        before = self.full.read_bytes()
        result = write_recent_score_history(self.full, self.recent)
        self.assertEqual(result.date.min(), "2020-01-08")
        self.assertEqual(result.date.max(), "2020-04-01")
        self.assertEqual(len(result), 3)
        self.assertIn("001,2.123456789012345678,NA", self.recent.read_text())
        source = pd.read_csv(self.full, dtype=str, keep_default_na=False)
        self.assertEqual(list(result.columns), list(source.columns))
        self.assertTrue(set(map(tuple, result.values)) <= set(map(tuple, source.values)))
        first = self.recent.read_bytes()
        write_recent_score_history(self.full, self.recent)
        self.assertEqual(first, self.recent.read_bytes())
        self.assertEqual(before, self.full.read_bytes())
        self.assertFalse(result.duplicated(["date", "symbol"]).any())

    def test_short_history_and_header_only(self):
        for rows, count in [("2020-04-01,A,1,\n2020-04-03,B,2,\n", 2), ("", 0)]:
            with self.subTest(rows=rows):
                self.write(rows)
                result = write_recent_score_history(self.full, self.recent)
                self.assertEqual(len(result), count)
                self.assertEqual(self.full.read_bytes(), self.recent.read_bytes())

    def test_existing_duplicates_keep_actual_last_row(self):
        self.write("2020-04-01,A,1,x\n2020-04-01,A,2,y\n")
        before = self.full.read_bytes()
        result = write_recent_score_history(self.full, self.recent)
        self.assertEqual(result.score.tolist(), ["2"])
        self.assertEqual(before, self.full.read_bytes())

    def test_invalid_input_and_archive_overwrite_fail(self):
        for rows in ["bad,A,1,\n", ",A,1,\n", "2020-04-01,,1,\n"]:
            with self.subTest(rows=rows):
                self.write(rows)
                with self.assertRaises(ValueError):
                    write_recent_score_history(self.full, self.recent)
        with self.assertRaises(ValueError):
            write_recent_score_history(self.full, self.full)
        self.full.write_text("wrong,schema\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            write_recent_score_history(self.full, self.recent)
        with self.assertRaises(FileNotFoundError):
            write_recent_score_history(self.root / "missing.csv", self.recent)

    def test_regular_history_step_and_error_propagation(self):
        script = Path(__file__).resolve().parents[1] / "scripts/generate_history_delta.py"
        spec = importlib.util.spec_from_file_location("history_script", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.write("2020-01-01,OLD,1,x\n2020-04-01,A,2,y\n")
        watchlist = self.root / "watchlist.csv"
        # Match the current watchlist schema consumed by the snapshot builder.
        watchlist.write_text(
            "market_date,symbol,asset_id,name,score,cycle,score_status,trend_ok,liquidity_ok\n"
            "2020-04-02,A,A,Asset,3,50,ok,True,True\n",
            encoding="utf-8",
        )
        with patch.object(module, "artifacts_dir", return_value=self.root), \
             patch.object(module, "resolve_score_history_path", return_value=self.full), \
             patch("scanner.reports.history_delta.artifacts_dir", return_value=self.root), \
             patch("sys.argv", [str(script), "--watchlist", str(watchlist)]):
            self.assertEqual(module.main(), 0)
            first = self.full.read_bytes()
            self.assertEqual(module.main(), 0)
            self.assertEqual(first, self.full.read_bytes())
            full = pd.read_csv(self.full)
            self.assertEqual(len(full), 3)
            self.assertEqual(full.date.min(), "2020-01-01")
            self.assertFalse(full.duplicated(["date", "symbol"]).any())
            latest = full.loc[full["date"] == "2020-04-02"].iloc[0]
            self.assertEqual(latest["symbol"], "A")
            self.assertEqual(latest["cycle"], 50)
            recent = pd.read_csv(self.root / "snapshots/score_history_recent.csv")
            self.assertEqual(len(recent), 2)
            self.assertEqual(recent.date.max(), full.date.max())
            self.assertTrue((self.root / "reports/history_delta.json").exists())
            self.assertTrue((self.root / "reports/history_delta.csv").exists())
            with patch.object(module, "write_recent_score_history", side_effect=RuntimeError("bug")):
                with self.assertRaisesRegex(RuntimeError, "bug"):
                    module.main()
            with patch("scanner.reports.history_delta.to_csv_safely", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    write_recent_score_history(self.full, self.recent)


if __name__ == "__main__":
    unittest.main()
