import json
import tempfile
import unittest
from pathlib import Path

from scanner.reports.daily_research import generate_daily_research
from scanner.research.query_api import HistoryQueryService


class DailyResearchAndQueryTests(unittest.TestCase):
    def test_daily_research_written_for_current_symbols_and_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            research = root / "artifacts" / "research"
            research.mkdir(parents=True)

            latest = [
                {
                    "symbol": "AAA",
                    "score": "12.0",
                    "rank": "3",
                    "rank_percentile": "0.3",
                    "r_code": "R2",
                    "rs3m": "0.05",
                    "trend200": "0.10",
                    "cycle": "7.5",
                    "confidence": "62",
                    "confidence_label": "MED",
                    "close": "99.0",
                    "currency": "USD",
                    "sector": "Technology",
                    "cluster_official": "AI"
                },
                {
                    "symbol": "BBB",
                    "score": "20.0",
                    "rank": "1",
                    "rank_percentile": "0.1",
                    "r_code": "R1",
                    "rs3m": "0.20",
                    "trend200": "-0.10",
                    "cycle": "-2.0",
                    "confidence": "75",
                    "confidence_label": "HIGH",
                    "close": "77.5",
                    "currency": "USD",
                    "sector": "Healthcare",
                    "cluster_official": "Biotech"
                },
            ]
            history = [
                {
                    "date": "2026-09-01",
                    "symbol": "AAA",
                    "score": "11.0",
                    "rank": "4",
                    "rank_percentile": "0.4",
                    "r_code": "R3",
                    "rs3m": "-0.02",
                    "trend200": "0.08",
                    "close": "90.0",
                },
                {
                    "date": "2026-09-02",
                    "symbol": "AAA",
                    "score": "12.0",
                    "rank": "3",
                    "rank_percentile": "0.3",
                    "r_code": "R2",
                    "rs3m": "0.05",
                    "trend200": "0.10",
                    "close": "99.0",
                },
                {
                    "date": "2026-09-03",
                    "symbol": "BBB",
                    "score": "18.0",
                    "rank": "2",
                    "rank_percentile": "0.2",
                    "r_code": "R2",
                    "rs3m": "0.10",
                    "trend200": "-0.20",
                    "close": "70.0",
                },
                {
                    "date": "2026-09-04",
                    "symbol": "BBB",
                    "score": "20.0",
                    "rank": "1",
                    "rank_percentile": "0.1",
                    "r_code": "R1",
                    "rs3m": "0.20",
                    "trend200": "-0.10",
                    "close": "77.5",
                },
            ]
            (research / "latest_scanner.csv").write_text(
                "symbol,score,rank,rank_percentile,r_code,rs3m,trend200,cycle,confidence,confidence_label,close,currency,sector,cluster_official\n"
                + "AAA,12.0,3,0.3,R2,0.05,0.10,7.5,62,MED,99.0,USD,Technology,AI\n"
                + "BBB,20.0,1,0.1,R1,0.20,-0.10,-2.0,75,HIGH,77.5,USD,Healthcare,Biotech\n",
                encoding="utf-8",
            )
            (research / "history_recent.csv").write_text(
                "date,symbol,score,rank,rank_percentile,r_code,rs3m,trend200,close\n"
                + "2026-09-01,AAA,11.0,4,0.4,R3,-0.02,0.08,90.0\n"
                + "2026-09-02,AAA,12.0,3,0.3,R2,0.05,0.10,99.0\n"
                + "2026-09-03,BBB,18.0,2,0.2,R2,0.10,-0.20,70.0\n"
                + "2026-09-04,BBB,20.0,1,0.1,R1,0.20,-0.10,77.5\n",
                encoding="utf-8",
            )
            (research / "history_metadata.json").write_text(json.dumps({
                "snapshot_id": "snap-123",
                "history_recent": {"path": "artifacts/research/history_recent.csv"},
            }), encoding="utf-8")

            result = generate_daily_research(root)
            self.assertEqual(result["snapshot_id"], "snap-123")
            self.assertEqual(set(result["symbols"].keys()), {"AAA", "BBB"})
            self.assertIn("current", result["symbols"]["AAA"])
            self.assertIn("dynamics", result["symbols"]["AAA"])
            self.assertIn("historical_matches", result["symbols"]["AAA"])
            self.assertEqual(result["universe_size"], 2)

            metadata = json.loads((research / "history_metadata.json").read_text(encoding="utf-8"))
            self.assertIn("daily_research", metadata)
            self.assertEqual(metadata["daily_research"]["snapshot_id"], "snap-123")

    def test_history_query_filters_and_rejects_invalid_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "history_recent.csv"
            csv_path.write_text(
                "date,symbol,score,rank,rank_percentile,r_code,rs3m,trend200,close\n"
                + "2026-06-01,RACE,10,5,0.5,R3,-0.10,0.12,50\n"
                + "2026-06-02,RACE,11,4,0.4,R2,-0.05,0.14,53\n"
                + "2026-06-01,UEC,9,6,0.6,R3,-0.15,0.05,22\n",
                encoding="utf-8",
            )
            service = HistoryQueryService(csv_path)

            payload = service.query(symbol="RACE", days=30)
            self.assertEqual(payload["count"], 2)
            self.assertEqual(payload["data"][0]["symbol"], "RACE")

            multi = service.query(symbols="RACE,UEC", days=30)
            self.assertEqual(len(multi["data"]), 3)

            invalid = service.query(symbol="RACE", fields=["date", "not_allowed"])
            self.assertEqual(invalid["error"], "invalid_fields")

            large = service.query(symbols=",".join([f"SYM{i}" for i in range(40)]), days=1000)
            self.assertEqual(large["error"], "request_too_large")
