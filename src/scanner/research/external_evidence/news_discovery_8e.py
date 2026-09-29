from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8e_news_discovery_v1"


class NewsDiscovery8EError(ValueError):
    pass


def _aware_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise NewsDiscovery8EError(f"invalid ingested_at: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NewsDiscovery8EError("ingested_at must be timezone-aware")
    return parsed


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _sha256_bytes(raw)


def _item_text(item: ET.Element, name: str) -> str:
    child = item.find(name)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _reported_pubdate(value: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError) as exc:
        raise NewsDiscovery8EError(f"invalid RSS pubDate: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NewsDiscovery8EError("RSS pubDate must be timezone-aware")
    return parsed.isoformat()


def build_news_discovery_snapshot(
    *,
    xml_path: Path,
    feed_kind: str,
    ingested_at: str | datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise NewsDiscovery8EError("unsupported news discovery config")
    hard = config.get("hard_boundaries") or {}
    if hard.get("generic_sentiment_enabled") is not False:
        raise NewsDiscovery8EError("generic sentiment must remain disabled")
    if hard.get("market_outcomes_may_be_read") is not False:
        raise NewsDiscovery8EError("news discovery must remain outcome-blind")

    feeds = config.get("feeds") or {}
    key = str(feed_kind or "").strip().upper()
    if key not in feeds:
        raise NewsDiscovery8EError(f"unsupported discovery feed kind: {feed_kind!r}")
    feed = feeds[key]
    source_url = str(feed.get("url") or "").strip()
    if key.startswith("FTC_") and not source_url.startswith("https://www.ftc.gov/"):
        raise NewsDiscovery8EError("FTC discovery feed must use ftc.gov")
    if key.startswith("DOJ_") and not source_url.startswith("https://www.justice.gov/"):
        raise NewsDiscovery8EError("DOJ discovery feed must use justice.gov")

    observed_at = _aware_datetime(ingested_at)
    raw_xml = xml_path.read_bytes()
    raw_sha = _sha256_bytes(raw_xml)
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise NewsDiscovery8EError("cannot parse discovery RSS XML") from exc

    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in root.findall(".//item"):
        title = _item_text(item, "title")
        link = _item_text(item, "link")
        guid = _item_text(item, "guid")
        description = _item_text(item, "description")
        pubdate = _reported_pubdate(_item_text(item, "pubDate"))
        source_item_id = guid or link
        if not source_item_id:
            raise NewsDiscovery8EError("discovery RSS item requires guid or link")
        if source_item_id in seen_ids:
            raise NewsDiscovery8EError(f"duplicate discovery RSS identity: {source_item_id}")
        seen_ids.add(source_item_id)
        source_record = {
            "guid": guid or None,
            "link": link or None,
            "title": title or None,
            "description": description or None,
            "reported_pubdate": pubdate,
        }
        rows.append(
            {
                "source_item_id": source_item_id,
                "source_id": key.lower(),
                "provider": str(feed.get("provider") or ""),
                "source_class": "REPUTABLE_NEWS_DISCOVERY_ONLY",
                "source_url": source_url,
                "title": title or None,
                "link": link or None,
                "reported_published_at": pubdate,
                "ingested_at": observed_at.isoformat(),
                "content_sha256": _canonical_hash(source_record),
                "discovery_only": True,
                "promotion_eligible": False,
                "canonical_event_key": None,
                "event_type": None,
                "market_direction": "UNASSIGNED",
                "source_record": source_record,
            }
        )

    rows.sort(key=lambda row: (str(row.get("reported_published_at") or ""), row["source_item_id"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B4_news_discovery",
        "status": "DISCOVERY_ONLY_NOT_PROMOTION_ELIGIBLE",
        "feed_kind": key,
        "source_url": source_url,
        "raw_xml_sha256": raw_sha,
        "ingested_at": observed_at.isoformat(),
        "row_count": len(rows),
        "rows": rows,
        "guards": {
            "canonical_events_created": False,
            "event_types_assigned": False,
            "generic_sentiment_enabled": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_news_discovery_snapshot(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
