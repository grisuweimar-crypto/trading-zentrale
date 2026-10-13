#!/usr/bin/env python3
"""PIT-safe RSS/Atom headline intake into research-only external Watch context.

No full articles, position data, inferred thesis, or trading recommendation.
A source headline is NOT an established market fact.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
from pathlib import Path
import urllib.request
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

FEEDS = {
    "finanzmarktwelt": "https://finanzmarktwelt.de/feed/",
    "MCO": "https://www.youtube.com/feeds/videos.xml?channel_id=UCngIhBkikUe6e7tZTjpKK7Q",
}
ATOM = "{http://www.w3.org/2005/Atom}"
YT = "{http://www.youtube.com/xml/schemas/2015}"
MAX_ITEMS = 12


def _instant(value: str) -> datetime:
    if not value or not value.strip():
        raise ValueError("missing_publication_timestamp")
    try:
        d = parsedate_to_datetime(value) if "," in value else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_publication_timestamp") from exc
    if d.tzinfo is None or d.utcoffset() is None:
        raise ValueError("publication_timezone_required")
    return d.astimezone(timezone.utc)


def _observation(source: str, title: str, url: str, published: str, captured: datetime) -> dict:
    pub = _instant(published)
    if pub > captured:
        raise ValueError("future_publication_forbidden")
    host = (urlsplit(url).hostname or "").lower()
    allowed = ("finanzmarktwelt.de", "www.finanzmarktwelt.de") if source == "finanzmarktwelt" else ("www.youtube.com", "youtube.com")
    if not url.startswith("https://") or host not in allowed:
        raise ValueError("untrusted_source_link")
    if not title.strip():
        raise ValueError("empty_headline")
    stable = sha256((source + "\n" + url).encode("utf-8")).hexdigest()[:24]
    return {
        "id": source.lower() + ":" + stable,
        "symbol": "__GLOBAL__",
        "source": source,
        "source_url": url,
        "published_at": pub.isoformat(),
        "ingested_at": captured.isoformat(),
        "status": "UNKNOWN",
        "summary": "Unverifizierte Quellenüberschrift: " + title.strip()[:160],
    }


def parse_feed(source: str, xml: bytes, captured: datetime) -> list[dict]:
    if captured.tzinfo is None:
        raise ValueError("capture_timezone_required")
    root = ET.fromstring(xml)
    candidates: list[tuple[str, str, str]] = []
    if root.tag == "rss":
        for item in root.findall("./channel/item"):
            candidates.append((item.findtext("title") or "", item.findtext("link") or "", item.findtext("pubDate") or ""))
    elif root.tag == ATOM + "feed":
        for item in root.findall(ATOM + "entry"):
            link = item.find(ATOM + "link")
            candidates.append((
                item.findtext(ATOM + "title") or "",
                link.get("href", "") if link is not None else "",
                item.findtext(ATOM + "published") or "",
            ))
    else:
        raise ValueError("unexpected_feed_xml")
    result: list[dict] = []
    seen: set[str] = set()
    for title, url, published in candidates:
        try:
            item = _observation(source, title, url, published, captured)
        except ValueError:
            continue
        if item["id"] in seen or _instant(item["published_at"]) < captured - timedelta(days=14):
            continue
        seen.add(item["id"])
        result.append(item)
        if len(result) >= MAX_ITEMS:
            break
    return result


def fetch_feed(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "ARGUS-Research-RSS/1.0", "Accept": "application/rss+xml, application/atom+xml, application/xml"})
    with urllib.request.urlopen(req, timeout=25) as response:
        data = response.read(2_000_001)
    if len(data) > 2_000_000:
        raise ValueError("feed_size_limit")
    return data


def collect(*, now: datetime, fetcher=fetch_feed) -> dict:
    now = now.astimezone(timezone.utc)
    rows: list[dict] = []
    statuses: dict[str, str] = {}
    for source, url in FEEDS.items():
        try:
            candidates = parse_feed(source, fetcher(url), now)
            rows.extend(candidates)
            statuses[source] = "AVAILABLE" if candidates else "EMPTY_OR_UNUSABLE"
        except (OSError, ValueError, ET.ParseError) as exc:
            statuses[source] = "UNAVAILABLE_" + type(exc).__name__
    return {
        "schema_version": "argus_watch_external_context_v1",
        "research_only": True,
        "capture_at": now.isoformat(),
        "source_status": statuses,
        "observations": rows,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    report = collect(now=datetime.now(timezone.utc))
    # No fake success if there are no dated, valid records.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False)+"\n", encoding="utf-8")
    print(json.dumps({"research_only": True, "candidate_count": len(report["observations"]), "source_status": report["source_status"]},sort_keys=True))
    return 0 if report["observations"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
