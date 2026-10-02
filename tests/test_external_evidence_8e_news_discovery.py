from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.news_discovery_8e import (
    NewsDiscovery8EError,
    build_news_discovery_snapshot,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_news_discovery_v1.json"


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _write_rss(path: Path) -> None:
    path.write_text(
        """<?xml version='1.0' encoding='UTF-8'?>
<rss version='2.0'><channel>
  <item>
    <title>Authority press release</title>
    <link>https://www.ftc.gov/news-events/news/press-releases/example</link>
    <guid>https://www.ftc.gov/news-events/news/press-releases/example</guid>
    <pubDate>Fri, 25 Sep 2026 16:00:00 -0400</pubDate>
    <description>Official release.</description>
  </item>
</channel></rss>
""",
        encoding="utf-8",
    )


def test_news_feed_remains_discovery_only(tmp_path: Path) -> None:
    source = tmp_path / "feed.xml"
    _write_rss(source)
    payload = build_news_discovery_snapshot(
        xml_path=source,
        feed_kind="FTC_COMPETITION_PRESS_RELEASES",
        ingested_at="2026-09-26T18:00:00+00:00",
        config=_config(),
    )
    assert payload["status"] == "DISCOVERY_ONLY_NOT_PROMOTION_ELIGIBLE"
    assert payload["row_count"] == 1
    row = payload["rows"][0]
    assert row["discovery_only"] is True
    assert row["promotion_eligible"] is False
    assert row["canonical_event_key"] is None
    assert row["event_type"] is None
    assert row["market_direction"] == "UNASSIGNED"
    assert payload["guards"]["canonical_events_created"] is False
    assert payload["guards"]["generic_sentiment_enabled"] is False


def test_news_discovery_rejects_naive_ingested_at(tmp_path: Path) -> None:
    source = tmp_path / "feed.xml"
    _write_rss(source)
    with pytest.raises(NewsDiscovery8EError, match="timezone-aware"):
        build_news_discovery_snapshot(
            xml_path=source,
            feed_kind="FTC_COMPETITION_PRESS_RELEASES",
            ingested_at="2026-09-26T18:00:00",
            config=_config(),
        )
