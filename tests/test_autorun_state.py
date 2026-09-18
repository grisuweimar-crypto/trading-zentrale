from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

from scripts.autorun_state import STATE, should_run


class AutorunStateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / STATE).parent.mkdir(parents=True)
        self.now = datetime.fromisoformat("2026-09-18T18:37:00+02:00")

    def record(self, timestamp):
        (self.root / STATE).write_text(json.dumps({"published_at": timestamp}), encoding="utf-8")

    def test_missing_or_invalid_marker_allows_retry(self):
        self.assertTrue(should_run(self.root, "schedule", self.now))
        for content in ("broken", "{}", "null", "[]", '{"published_at": "invalid"}'):
            (self.root / STATE).write_text(content, encoding="utf-8")
            self.assertTrue(should_run(self.root, "schedule", self.now))

    def test_morning_previous_day_future_or_naive_marker_allows_retry(self):
        for timestamp in ("2026-09-18T10:38:00+00:00", "2026-09-17T18:00:00+00:00",
                          "2026-09-18T20:00:00+00:00", "2026-09-18T17:30:00"):
            self.record(timestamp)
            self.assertTrue(should_run(self.root, "schedule", self.now))

    def test_evening_publication_skips_retry_but_manual_run_is_allowed(self):
        self.record("2026-09-18T15:07:00+00:00")
        self.assertFalse(should_run(self.root, "schedule", self.now))
        self.assertTrue(should_run(self.root, "workflow_dispatch", self.now))

    def test_winter_uses_berlin_time(self):
        now = datetime.fromisoformat("2026-12-18T18:37:00+01:00")
        self.record("2026-12-18T15:30:00+00:00")
        self.assertTrue(should_run(self.root, "schedule", now))
        self.record("2026-12-18T16:07:00+00:00")
        self.assertFalse(should_run(self.root, "schedule", now))

    def test_cli_record_and_github_output(self):
        project = Path(__file__).resolve().parents[1]
        command = [sys.executable, str(project / "scripts/autorun_state.py"), "--root", str(self.root)]
        output = self.root / "github_output"
        env = dict(os.environ, GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_OUTPUT=str(output), GITHUB_RUN_ID="123")
        subprocess.run(command + ["--record"], env=env, check=True, capture_output=True)
        state = json.loads((self.root / STATE).read_text(encoding="utf-8"))
        self.assertEqual(state["run_id"], "123")
        self.assertIsNotNone(datetime.fromisoformat(state["published_at"]).tzinfo)
        subprocess.run(command, env=env, check=True, capture_output=True)
        self.assertEqual(output.read_text(encoding="utf-8"), "run=true\n")

    def test_workflow_serializes_checks_and_publishes_marker_only_after_validation(self):
        project = Path(__file__).resolve().parents[1]
        workflow = yaml.load((project / ".github/workflows/run_scanner.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        self.assertEqual(workflow["concurrency"]["cancel-in-progress"], "false")
        build = workflow["jobs"]["build"]
        self.assertEqual(build["needs"], "autorun")
        self.assertEqual(build["if"], "needs.autorun.outputs.run == 'true'")
        for job in workflow["jobs"].values():
            self.assertEqual(job["steps"][0]["with"]["ref"], "${{ github.ref }}")
        steps = build["steps"]
        marker = next(i for i, step in enumerate(steps) if "autorun_state.py --record" in step.get("run", ""))
        validation = next(i for i, step in enumerate(steps) if "--validate-only" in step.get("run", ""))
        publication = next(i for i, step in enumerate(steps) if "git add artifacts/" in step.get("run", ""))
        self.assertLess(validation, marker)
        self.assertLess(marker, publication)
        self.assertEqual(steps[marker]["if"], "steps.research.outputs.complete == 'true'")


if __name__ == "__main__":
    unittest.main()
