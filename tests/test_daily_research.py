"""Daily integration tests run unchanged on Windows and GitHub's Ubuntu runner."""
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scanner.reports.daily_research import WATCHLIST, begin_daily, generate_daily
from scanner.reports.research_validation import NAMES, validate_publication
from scanner.reports.research_views import OUTPUT, SOURCE, ValidationPolicy, encode_csv, parse_csv

PROJECT = Path(__file__).resolve().parents[1]


class DailyResearchTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.receipt = self.root / "scanner-run.json"
        self.policy = ValidationPolicy(expected_symbol_count=3)
        self.columns = "market_date symbol asset_id name score OpportunityScore RiskScore confidence ConfidenceLabel rs3m trend200 cycle score_status trend_ok liquidity_ok".split()
        self.rows = [dict(zip(self.columns, ["2026-09-16", s, s, s, v, "62.00", "28.123456789012345", "60", "MED", "0.1", "0.2", "50", "OK", "True", "True"]))
                     for s, v in (("001", "50.00"), ("B", "50"), ("C", "10"))]

    def write_watchlist(self, rows=None):
        path = self.root / WATCHLIST
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode_csv(self.columns, self.rows if rows is None else rows))
        # Keep tests deterministic on filesystems with coarse timestamp resolution.
        if self.receipt.exists():
            started = datetime.fromisoformat(json.loads(self.receipt.read_text())["started_at"]).timestamp()
            os.utime(path, (started + 1, started + 1))

    def generate(self, **kwargs):
        return generate_daily(self.root, self.receipt, policy=self.policy, **kwargs)

    def read(self, name):
        return parse_csv((self.root / OUTPUT / (name + ".csv")).read_bytes())[1]

    def test_complete_daily_all_files_current_fields_ranks_hashes_and_history(self):
        begin_daily(self.root, self.receipt, run_id="daily1")
        self.write_watchlist()
        meta = self.generate()
        self.assertTrue(meta["latest_run_complete"])
        self.assertEqual(meta["as_of"], "2026-09-16")
        self.assertNotEqual(meta["as_of"], meta["generated_at"])
        self.assertEqual(meta["source"]["file"], WATCHLIST)
        for name in NAMES:
            path = self.root / OUTPUT / (name + ".csv")
            self.assertTrue(path.exists())
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), meta[name]["sha256"])
        latest = self.read("latest_scanner")
        self.assertEqual([r["rank"] for r in latest], ["1", "1", "3"])
        self.assertEqual([r["rank_percentile"] for r in latest], [str(1/3), str(1/3), "1.0"])
        self.assertEqual(latest[0]["symbol"], "001")
        self.assertEqual(latest[0]["opportunity"], "62.00")
        self.assertEqual(latest[0]["risk"], "28.123456789012345")
        self.assertEqual(latest[0]["confidence_label"], "MED")
        self.assertTrue(latest[0]["r_code"].startswith("R"))
        self.assertEqual({r["snapshot_id"] for r in latest + self.read("history_recent")}, {meta["snapshot_id"]})
        self.assertEqual(self.read("history_analysis")[0]["rank"], "1")
        self.assertEqual(len(parse_csv((self.root / SOURCE).read_bytes())[1]), 3)
        validate_publication(self.root)

    def test_partial_then_failed_step_preserve_both_histories_latest_and_last_complete(self):
        begin_daily(self.root, self.receipt)
        self.write_watchlist()
        first = self.generate()
        protected = [self.root / SOURCE] + [self.root / OUTPUT / (n + ".csv") for n in ("history_analysis", "history_recent", "latest_scanner")]
        before = {p: p.read_bytes() for p in protected}
        begin_daily(self.root, self.receipt)
        self.write_watchlist(self.rows[:1])
        with patch("scanner.reports.daily_research.DailyInput.enrich", side_effect=AssertionError("ranks must not be calculated")):
            meta = self.generate()
        self.assertFalse(meta["latest_run_complete"])
        self.assertEqual(meta["last_complete_scan"], first["last_complete_scan"])
        self.assertEqual(meta["snapshot_id"], first["snapshot_id"])
        self.assertEqual(before, {p: p.read_bytes() for p in protected})
        begin_daily(self.root, self.receipt)
        self.write_watchlist()
        self.assertFalse(self.generate(scanner_status="failure")["latest_run_complete"])
        self.assertEqual(before, {p: p.read_bytes() for p in protected})

    def test_unmodified_watchlist_and_same_day_mixed_dates_are_rejected(self):
        self.write_watchlist()
        begin_daily(self.root, self.receipt)
        self.assertFalse(self.generate()["latest_run_complete"])
        self.write_watchlist(self.rows[:2] + [dict(self.rows[2], market_date="2026-09-15")])
        self.assertIn("invalid_as_of", self.generate()["latest_run_error"])

    def test_same_day_reruns_preserve_original_scanner_observations(self):
        begin_daily(self.root, self.receipt, run_id="first")
        self.write_watchlist()
        self.generate()
        before = self.read("history_analysis")
        source_before = parse_csv((self.root / SOURCE).read_bytes())[1]
        begin_daily(self.root, self.receipt, run_id="second")
        self.write_watchlist([dict(r, score="25.00") for r in self.rows])
        self.generate()
        self.assertEqual(self.read("history_analysis")[:3], before)
        self.assertEqual(parse_csv((self.root / SOURCE).read_bytes())[1][:3], source_before)
        self.assertEqual(len(self.read("history_analysis")), 6)
        self.generate()  # Same receipt must not duplicate observations.
        self.assertEqual(len(self.read("history_analysis")), 6)

    def test_integrity_gate_rejects_tampering_even_with_updated_hash(self):
        begin_daily(self.root, self.receipt)
        self.write_watchlist()
        self.generate()
        path = self.root / OUTPUT / "latest_scanner.csv"
        cols, rows = parse_csv(path.read_bytes())
        rows[0]["rank"] = "99"
        path.write_bytes(encode_csv(cols, rows))
        mp = path.parent / "history_metadata.json"
        meta = json.loads(mp.read_text())
        meta["latest_scanner"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        mp.write_text(json.dumps(meta))
        with self.assertRaisesRegex(ValueError, "rank mismatch"):
            validate_publication(self.root)

    def test_cli_pipeline_and_git_add_include_all_research_files(self):
        env = dict(os.environ, PYTHONPATH=str(PROJECT / "src"), PYTHONIOENCODING="utf-8")
        env.pop("GITHUB_OUTPUT", None)
        env.pop("GITHUB_STEP_SUMMARY", None)
        script = str(PROJECT / "scripts/generate_research_views.py")
        def cli(*args):
            result = subprocess.run([sys.executable, script, "--root", str(self.root), *args], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result
        cli("--begin-run", "--receipt", str(self.receipt))
        self.write_watchlist()
        cli("--receipt", str(self.receipt), "--expected-symbol-count", "3")
        cli("--validate-only")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, capture_output=True)
        (self.root / ".gitignore").write_bytes((PROJECT / ".gitignore").read_bytes())
        subprocess.run(["git", "-C", str(self.root), "add", "artifacts/"], check=True, capture_output=True)
        tracked = subprocess.check_output(["git", "-C", str(self.root), "diff", "--cached", "--name-only"], text=True).splitlines()
        for name in NAMES:
            self.assertIn(f"{OUTPUT}/{name}.csv", tracked)
        self.assertIn(f"{OUTPUT}/history_metadata.json", tracked)

    def test_workflow_orders_and_gates_production_publication(self):
        workflow = yaml.load((PROJECT / ".github/workflows/run_scanner.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        steps = workflow["jobs"]["build"]["steps"]
        def index(text):
            return next(i for i, step in enumerate(steps) if text in step.get("run", ""))
        self.assertLess(index("unittest discover"), index("prefetch_market_history"))
        self.assertLess(index("--scanner-status"), index("prefetch_market_history"))
        self.assertLess(index("prefetch_market_history"), index("--refresh-prices"))
        self.assertLess(index("--refresh-prices"), index("generate_daily_research.py"))
        self.assertNotIn("AVAV ROL", steps[index("prefetch_market_history")]["run"])
        for command in ("prefetch_market_history", "--refresh-prices"):
            self.assertEqual(steps[index(command)]["if"], "steps.research.outputs.complete == 'true'")
        self.assertLess(index("--begin-run"), index("python -m scanner.app.run_daily"))
        self.assertLess(index("python -m scanner.app.run_daily"), index("--scanner-status"))
        self.assertLess(index("--scanner-status"), index("generate_history_delta.py --report-only"))
        self.assertLess(index("--scanner-status"), index("generate_daily_research.py"))
        self.assertEqual(steps[index("generate_daily_research.py")]["if"], "steps.research.outputs.complete == 'true'")
        self.assertLess(index("--validate-only"), index("git add artifacts/"))
        self.assertLess(index("git add artifacts/"), index("exit 1"))
        self.assertNotIn("publish_history_analysis.py", "\n".join(s.get("run", "") for s in steps))
        self.assertEqual(steps[index("python -m scanner.app.run_daily")]["continue-on-error"], "true")
        for step in steps[index("generate_history_delta.py --report-only"):index("--validate-only")]:
            self.assertEqual(step["if"], "steps.research.outputs.complete == 'true'")

    def test_report_only_does_not_upsert_or_mix_same_day_runs(self):
        from scripts import generate_history_delta as script
        source = self.root / SOURCE
        source.parent.mkdir(parents=True)
        columns = ["date", "symbol", "score", "run_id"]
        rows = [{"date": d, "symbol": s, "score": "50", "run_id": run}
                for d, run, symbols in (("2026-09-15", "old", ["A", "B"]),
                                        ("2026-09-16", "first", ["A", "B"]),
                                        ("2026-09-16", "second", ["A"])) for s in symbols]
        source.write_bytes(encode_csv(columns, rows))
        before = source.read_bytes()
        with patch.object(script, "resolve_score_history_path", return_value=source), \
             patch.object(script, "artifacts_dir", return_value=self.root / "artifacts"), \
             patch.object(script, "compute_history_delta", return_value=(None, {})) as compute, \
             patch.object(script, "write_history_delta_outputs"), \
             patch.object(script, "upsert_daily_snapshot", side_effect=AssertionError("No upsert allowed")), \
             patch("sys.argv", ["generate_history_delta.py", "--report-only"]):
            self.assertEqual(script.main(), 0)
        selected = compute.call_args[0][0]
        self.assertEqual(selected[selected["date"] == "2026-09-16"]["symbol"].tolist(), ["A"])
        self.assertEqual(before, source.read_bytes())


if __name__ == "__main__":
    unittest.main()
