from datetime import datetime,timezone
from email.utils import format_datetime
import pytest
from scripts.argus_external_feed_capture import parse_feed, collect

NOW=datetime(2026,10,13,11,30,tzinfo=timezone.utc)
RSS=b"""<rss><channel><item><title>Gold and bonds</title><link>https://finanzmarktwelt.de/test-gold/</link><pubDate>Tue, 13 Oct 2026 09:00:00 GMT</pubDate></item></channel></rss>"""
ATOM=b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Crypto Wave Review</title><published>2026-10-13T09:00:00Z</published><link href="https://www.youtube.com/watch?v=abc123"/></entry></feed>"""

def test_rss_produces_undirected_context_not_advice():
    records=parse_feed("finanzmarktwelt",RSS,NOW)
    assert len(records)==1
    assert records[0]["status"]=="UNKNOWN"
    assert records[0]["symbol"]=="__GLOBAL__"
    assert records[0]["ingested_at"]==NOW.isoformat()
    assert "decision" not in records[0] and "score" not in records[0]

def test_atom_publication_timestamp_is_preserved():
    records=parse_feed("MCO",ATOM,NOW)
    assert records[0]["published_at"]=="2026-10-13T09:00:00+00:00"

def test_future_headline_is_suppressed():
    future=RSS.replace(b"09:00:00",b"13:00:00")
    assert parse_feed("finanzmarktwelt",future,NOW)==[]

def test_reject_different_host_behind_rss():
    invalid=RSS.replace(b"finanzmarktwelt.de",b"evil.example")
    assert parse_feed("finanzmarktwelt",invalid,NOW)==[]

def test_absent_source_marked_unknown_but_other_source_kept():
    def fetcher(url):
        if "youtube" in url:
            raise OSError("blocked")
        return RSS
    r=collect(now=NOW,fetcher=fetcher)
    assert len(r["observations"])==1
    assert r["source_status"]["MCO"].startswith("UNAVAILABLE_")
    assert r["research_only"] is True
