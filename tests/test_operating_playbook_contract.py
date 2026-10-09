"""Prevent stale or fictitious operating commands returning to the playbook."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
PLAYBOOK = ROOT / "docs" / "OPERATING_PLAYBOOK.md"


class OperatingPlaybookContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = PLAYBOOK.read_text(encoding="utf-8")

    def test_real_scanner_score_scale_not_legacy(self):
        self.assertIn("Score: 0–100", self.text)
        self.assertNotIn("Score (0-200)", self.text)
        self.assertNotIn("Score (0–200)", self.text)
        self.assertNotIn("≥100", self.text)

    def test_all_referenced_scripts_and_workflows_exist(self):
        cited = set(re.findall(
            r"(?:scripts/[A-Za-z0-9_./-]+\.py|\.github/workflows/[A-Za-z0-9_./-]+\.yml)",
            self.text,
        ))
        # Must not silently regress into a descriptive but unactionable guide.
        self.assertGreaterEqual(len(cited), 7, cited)
        for relative_path in sorted(cited):
            with self.subTest(path=relative_path):
                self.assertTrue((ROOT / relative_path).is_file(), relative_path)

    def test_no_retired_commands_or_unapproved_trading_operation(self):
        for retired in (
            "scripts/run_daily.py",
            "scripts/health_report.py",
            "scripts/calibrate_light.py",
            "scripts/telegram_test.py",
            "--skip_rebalance",
        ):
            with self.subTest(retired=retired):
                self.assertNotIn(retired, self.text)
        for required in (
            "history_metadata.json",
            "latest_scanner.csv",
            "daily_research.json",
            "decision_snapshot_w10.json",
            "watch_runtime/manifest.json",
            "EFFECTIVENESS",
        ):
            # EFFECTIVENESS is not an operational status in this German
            # playbook; check below with German wording instead.
            if required == "EFFECTIVENESS":
                self.assertIn("Wirksamkeit", self.text)
            else:
                self.assertIn(required, self.text)

    def test_score_policy_and_private_data_controls_are_mentioned(self):
        self.assertIn("min_score_ratio", self.text)
        self.assertIn("ceil(symbol_count * min_score_ratio)", self.text)
        self.assertIn("20 % Reserve", self.text)
        self.assertIn("2.000.000 Bytes", self.text)
        self.assertIn("Point-in-Time", self.text)
        self.assertIn("Private Depotpositionen", self.text)


if __name__ == "__main__":
    unittest.main()
