"""ARGUS presentation contract: comprehensible, read-only, original evidence retained."""
from pathlib import Path
import json
import unittest

ROOT = Path(__file__).resolve().parents[1]
RESEARCH = ROOT / "argus-preview/v7/research-lab.js"
QUALITY = ROOT / "argus-preview/v7/qm-monitor.js"
OBSERVATIONS = ROOT / "argus-preview/observations/external-thesis-watch.js"
HELP = ROOT / "argus-preview/observations/argus-help.js"


class ArgusClarityTests(unittest.TestCase):
    def test_scientific_terms_have_context_and_originals_remain(self):
        text = RESEARCH.read_text(encoding="utf-8")
        for phrase in (
            "So liest du diese Seite",
            "Was wissen wir über unsere Handelsmuster?",
            "Festgelegte Muster",
            "Beobachtungen sammeln",
            "Originalkennungen",
            "unterschiedliche Datenstände",
            "Ein bestandener Techniktest",
            "Originalbericht des Forschungszyklus",
        ):
            self.assertIn(phrase.lower(), text.lower(), phrase)
        self.assertIn("const stageNames=", text)
        self.assertIn("const stageHelp=", text)
        self.assertIn("c.laboratory_state", text)
        self.assertIn("c.work_items", text)
        self.assertIn("meta.snapshot_id", text)
        self.assertIn("fetchJson(cyclePath)", text)
        self.assertIn("fail(e.name===", text)

    def test_quality_labels_do_not_assert_empirical_proof(self):
        text = QUALITY.read_text(encoding="utf-8")
        self.assertIn("Welche Fehler wurden gefunden und behoben?", text)
        self.assertIn("Maßnahme umgesetzt", text)
        self.assertIn("formell abgeschlossen", text)
        self.assertIn("states[f.status]", text)
        self.assertIn("replayLedger", text)
        self.assertIn("Originalstatus", text)
        self.assertIn("keine Freigabe", text)

    def test_external_candidates_are_not_pattern_candidates(self):
        text = OBSERVATIONS.read_text(encoding="utf-8")
        self.assertIn("Aktienkandidaten", text)
        self.assertIn("Musterkandidaten", text)
        self.assertIn("originalen archivvermerk", text.lower())
        self.assertNotIn("Die damalige externe E-Mail-Begründung ist hier noch nicht hinterlegt.", text)
        self.assertIn("Eigen", text)
        self.assertIn("readablePattern", text)
        self.assertIn("checkInput", text)
        self.assertIn("localStorage", text)
        self.assertIn("candidateElliott", text)
        self.assertIn("scanneraufnahme unverändert", text.lower())

    def test_glossary_is_available_on_both_preview_pages(self):
        text = HELP.read_text(encoding="utf-8")
        standalone = (ROOT / "argus-preview/v7/index.html").read_text(encoding="utf-8")
        obs = OBSERVATIONS.read_text(encoding="utf-8")
        self.assertIn("argus-help.js", standalone)
        self.assertIn("argus-help.js", obs)
        self.assertIn("Begriffe verstehen", text)
        self.assertIn("Musterkandidat", text)
        self.assertIn("Aktienkandidat", text)
        self.assertIn("Point-in-Time", text)
        self.assertIn("research", RESEARCH.name)

    def test_public_reference_and_private_source_boundary_unchanged(self):
        reference = json.loads((ROOT / "argus-preview/observations/external_thesis_watch_reference.json").read_text())
        candidates = json.loads((ROOT / "argus-preview/observations/external_thesis_watch_candidates.json").read_text())
        self.assertEqual(len(reference["items"]), 10)
        self.assertEqual(len(candidates["items"]), 1)
        self.assertEqual(candidates["items"][0]["zone_reference"], "E01/Z1–Z2")
        self.assertEqual(candidates["items"][0]["source_family"], "Externe Chartanalyse · Quelle X")
        self.assertIsNone(candidates["items"][0]["source_date"])
        self.assertTrue(candidates["items"][0]["private_source_and_price_levels_not_published"])


if __name__ == "__main__":
    unittest.main()
