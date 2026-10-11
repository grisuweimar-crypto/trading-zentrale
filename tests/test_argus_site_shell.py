"""Presentation regression for user's approved ARGUS dark-only rollback."""
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

    def test_exactly_one_footer(self):
        self.assertEqual(self.home.count('<footer class="footer">'),1)
        self.assertNotIn('<div class="smallfoot">',self.home)

    def test_legacy_artwork_is_exactly_restored(self):
        self.assertIn('about-argus-original-before-pr301.png',self.home)
        artwork=BASE/"assets/about-argus-original-before-pr301.png"
        self.assertTrue(artwork.exists())
        self.assertGreater(artwork.stat().st_size,100000)
        self.assertNotIn('about-argus-artwork.svg',self.home)
        self.assertNotIn('about-argus-artwork-mobile.svg',self.home)
        self.assertIn('alt="Unverändertes Original-Eulenaugenmotiv"',self.home)
        self.assertNotIn("argusApprovedIdentity",self.script)
        self.assertNotIn("cloneNode",self.script)

    def test_one_dark_theme_without_switch(self):
        for page in (self.home,self.v7):
            self.assertIn("argus-theme.css",page)
            self.assertIn("argus-shell.js",page)
        self.assertNotIn("argusThemeChoice",self.script)
        self.assertNotIn("argusThemeControl",self.script)
        self.assertNotIn("localStorage.setItem",self.script)
        self.assertNotIn('data-argus-theme="light"',self.css)
        self.assertNotIn('data-argus-theme="light"',self.home)
        self.assertNotIn("argus-theme.js",self.home)
        self.assertIn("style.colorScheme='dark'",self.script)
        self.assertIn("color-scheme:dark",self.css)

    def test_mobile_nav_remains_accessible(self):
        self.assertIn("scrollIntoView",self.script)
        self.assertIn("about:'Über ARGUS'",self.script)
        self.assertIn("about:'Über'",self.script)
        self.assertIn('aria-label',self.script)
        self.assertIn("grid-template-columns:repeat(5,minmax(0,1fr))",self.css)

    def test_no_changes_to_factual_about_labels(self):
        about=self.home.split('<section id="about"',1)[1].split('</section>',1)[0]
        for label in ("DATENBASIS","EINORDNUNG","VERÄNDERUNGEN","FORSCHUNG UND QUALITÄT"):
            self.assertIn(label,about)


if __name__=="__main__":
    unittest.main()
