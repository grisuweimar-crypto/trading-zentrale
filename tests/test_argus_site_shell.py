"""Presentation-only regressions for ARGUS shell, theme and About illustration."""
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/"argus-preview"


class ArgusSiteShellTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.home=(BASE/"index.html").read_text(encoding="utf-8")
        cls.v7=(BASE/"v7/index.html").read_text(encoding="utf-8")
        cls.script=(BASE/"argus-shell.js").read_text(encoding="utf-8")
        cls.css=(BASE/"argus-theme.css").read_text(encoding="utf-8")

    def test_exactly_one_footer_in_main_html(self):
        self.assertEqual(self.home.count('<footer class="footer">'),1)
        self.assertNotIn('<div class="smallfoot">',self.home)
        self.assertIn("Keine Anlageberatung",self.home)

    def test_about_uses_only_original_approved_brand(self):
        # No new owl, temple, or revised icon is an authorized ARGUS logo.
        self.assertNotIn('about-argus-artwork.svg', self.home)
        self.assertNotIn('about-argus-artwork-mobile.svg', self.home)
        self.assertIn('<div class="aboutPicture" aria-hidden="true"></div>', self.home)
        self.assertIn('alt="Unverändertes Original-Eulenaugenmotiv"', self.home)
        self.assertIn('data:image/png;base64,', self.home)
        self.assertIn("approvedEye.cloneNode(true)", self.script)
        self.assertIn("aboutPicture.replaceWith(panel)", self.script)
        self.assertIn("ARGUS", self.script)
        self.assertIn("PANOPTES", self.script)
        self.assertIn("Research & Decision Intelligence System", self.script)
        self.assertIn(".argusApprovedIdentity", self.css)
        self.assertNotIn("Dies ist nur eine lokale Designprobe", self.home)

    def test_about_labels_are_plain_german(self):
        about=self.home.split('<section id="about"',1)[1].split('</section>',1)[0]
        for expected in ("DATENBASIS","EINORDNUNG","VERÄNDERUNGEN","FORSCHUNG UND QUALITÄT"):
            self.assertIn(expected,about)
        for old in ("OR B I S / O C U L I","CEREBRUM / VIGILIA","MEMORIA / SPECULUM","INVENTIO / SUBTILITAS"):
            self.assertNotIn(old,about)

    def test_dark_light_system_are_reused_on_both_pages(self):
        for page in (self.home,self.v7):
            self.assertIn("argus-theme.css",page)
            self.assertIn("argus-shell.js",page)
        for opt in ("['dark'","['light'","['system'"):
            self.assertIn(opt,self.script)
        self.assertIn("localStorage.setItem(KEY,chosen)",self.script)
        self.assertIn("prefers-color-scheme: dark",self.script)
        self.assertIn("scrollIntoView",self.script)
        self.assertIn("about:'Über ARGUS'",self.script)
        self.assertIn("about:'Über'",self.script)
        self.assertIn("tab.title=full",self.script)
        self.assertIn("aria-label','Scanner-Daten aktualisieren",self.script)
        self.assertIn('data-argus-theme="light"',self.css)
        self.assertIn("#about .argusApprovedIdentity",self.css)
        self.assertIn("prefers-reduced-motion",self.css)


if __name__=="__main__":
    unittest.main()
