"""Tests of first real L14-authorized, independent research initiation."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"/"pattern_discovery"))
from run_initial_real_discovery import (
    CUTOFF, FAMILY_FIELDS, RESEARCH_OBSERVATION_PATH, clean_number, preregistration
)
from scanner.research.pattern_discovery import normalize_preregistration


class InitialRealDiscoveryTests(unittest.TestCase):
    def test_exact_pre_registered_families_and_no_score(self):
        p=preregistration("2026-11-17T14:00:00+00:00","test-commit")
        self.assertEqual(set(FAMILY_FIELDS),{"rs3m","trend200","cycle"})
        self.assertEqual(p["targets"],["return_20t_gt_0","peer_excess_20t_gt_0"])
        self.assertEqual(p["multiple_testing"]["primary_method"],"HOLM_FWER")
        self.assertEqual(p["pattern_complexity"]["max_atomic_conditions"],3)
        self.assertEqual(p["data_cutoff"],CUTOFF)
        self.assertNotIn("score",FAMILY_FIELDS)
        self.assertNotIn("rank",FAMILY_FIELDS)
        self.assertIn(RESEARCH_OBSERVATION_PATH,p["data_sources"])
        self.assertTrue(normalize_preregistration(p)["data_cutoff"].startswith("2026-11-17T00:00:00"))

    def test_future_cutoff_fails_closed(self):
        p=preregistration("2026-11-16T12:00:00+00:00","test-commit")
        with self.assertRaisesRegex(Exception,"data_cutoff_after_declared_start"):
            normalize_preregistration(p)

    def test_nonfinite_data_is_missing_not_imputed(self):
        self.assertIsNone(clean_number(None))
        self.assertIsNone(clean_number("NaN"))
        self.assertIsNone(clean_number(float("inf")))
        self.assertAlmostEqual(clean_number("0.045"),.045)
        self.assertIsNone(clean_number("invalid"))

    def test_no_executable_buy_or_claim_authority(self):
        p=preregistration("2026-11-17T14:00:00+00:00","test-commit")
        self.assertIn("overlapping_events_not_independent",p["dependency_rules"])
        self.assertIn("mature_adjusted_price_labels_only",p["pit_rules"])
        self.assertNotIn("buy_signal",p)
        self.assertNotIn("rank",p["allowed_transformations"])


if __name__=="__main__":unittest.main()
