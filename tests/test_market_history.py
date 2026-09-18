import csv
from pathlib import Path
import tempfile
import unittest

import pandas as pd
from unittest.mock import patch

from scripts.prefetch_market_history import prefetch_history
from scripts.publish_history_analysis import publish_history_analysis
from scanner.reports.research_views import ValidationPolicy


class MarketHistoryTests(unittest.TestCase):
    def test_merges_without_overwriting_or_duplicate_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "data/inputs/universe_master.csv"
            universe.parent.mkdir(parents=True)
            universe.write_text("symbol,currency\nAVAV,USD\nROL,USD\n", encoding="utf-8")
            target = root / "artifacts/market_data/yahoo_ohlcv.csv"
            target.parent.mkdir(parents=True)
            with target.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["date", "symbol", "currency", "open", "high", "low", "close", "volume"])
                writer.writerow(["2026-01-02", "AVAV", "USD", "99", "100", "98", "99", "10"])

            downloaded = pd.DataFrame([
                {"date": "2026-01-02", "symbol": "AVAV", "open": 1, "high": 2, "low": 0.5, "close": 1, "volume": 10},
                {"date": "2026-01-05", "symbol": "AVAV", "open": 2, "high": 3, "low": 1, "close": 2, "volume": 11},
                {"date": "2026-01-02", "symbol": "ROL", "open": 4, "high": 5, "low": 3, "close": 4, "volume": 12},
            ])
            with patch("scripts.prefetch_market_history._download", return_value=downloaded):
                result = prefetch_history(root, ("AVAV", "ROL"), minimum_days=1, output=target)

            avav = result[result["symbol"] == "AVAV"]
            self.assertEqual(len(avav), 2)
            self.assertEqual(float(avav.loc[avav["date"] == "2026-01-02", "close"].iloc[0]), 99)
            self.assertFalse(result.duplicated(["date", "symbol"]).any())

    def test_second_prefetch_does_not_rewrite_existing_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "data/inputs/universe_master.csv"
            universe.parent.mkdir(parents=True)
            universe.write_text("symbol,currency\nAVAV,USD\n", encoding="utf-8")
            target = root / "artifacts/market_data/yahoo_ohlcv.csv"
            target.parent.mkdir(parents=True)
            target.write_text(
                "date,symbol,currency,open,high,low,close,volume\n"
                "2026-01-02,AVAV,USD,99,100,98,99,10\n",
                encoding="utf-8",
            )
            before = target.read_bytes()
            downloaded = pd.DataFrame([{
                "date": "2026-01-02", "symbol": "AVAV", "open": 9,
                "high": 9, "low": 9, "close": 9, "volume": 9,
            }])
            with patch("scripts.prefetch_market_history._download", return_value=downloaded):
                prefetch_history(root, ("AVAV",), minimum_days=1, output=target)
            self.assertEqual(before, target.read_bytes())

    def test_public_analysis_export_has_provenance_and_stable_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scanner = root / "artifacts/snapshots/score_history.csv"
            scanner.parent.mkdir(parents=True)
            scanner.write_text(
                "date,symbol,score\n2026-01-02,AAPL,50\n",
                encoding="utf-8",
            )
            source = root / "artifacts/market_data/yahoo_ohlcv.csv"
            source.parent.mkdir(parents=True)
            source.write_text(
                "date,symbol,currency,open,high,low,close,volume\n"
                "2026-01-02,AVAV,USD,1,2,0.5,1.5,10\n",
                encoding="utf-8",
            )
            target = publish_history_analysis(root, policy=ValidationPolicy(expected_symbol_count=1))
            exported = pd.read_csv(target)
            self.assertEqual(exported["symbol"].tolist(), ["AAPL"])
            self.assertEqual(set(exported["observation_type"]), {"observed_scanner"})
            self.assertEqual(set(exported["data_source"]), {"scanner_run"})
            self.assertIn("score", exported.columns)
            prices = pd.read_csv(root / "artifacts/research/price_backfill.csv")
            self.assertEqual(prices["symbol"].tolist(), ["AVAV"])
            self.assertIn("open", prices.columns)
            self.assertNotIn("score", prices.columns)


if __name__ == "__main__":
    unittest.main()
