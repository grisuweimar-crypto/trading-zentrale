from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8e_doj_antitrust_rss_v1"


class DOJAntitrust8EError(ValueError):
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
            raise DOJAntitrust8EError(f"invalid ingested_at: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DOJAntitrust8EError("ingested_at must be timezone-aware")
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


def _reported_pubdate(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError) as exc:
        raise DOJAntitrust8EError(f"invalid RSS pubDate: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DOJAntitrust8EError("RSS pubDate must be timezone-aware")
    return parsed


def build_doj_antitrust_case_filing_evidence(
    *,
    xml_path: Path,
    feed_kind: str,
    ingested_at: str | datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise DOJAntitrust8EError("unsupported DOJ antitrust RSS config")
    hard = config.get("hard_boundaries") or {}
    if hard.get("market_outcomes_may_be_read") is not False:
        raise DOJAntitrust8EError("DOJ adapter must remain outcome-blind")
    if hard.get("phase7_integration_enabled") is not False:
        raise DOJAntitrust8EError("DOJ adapter may not enable Phase-7 integration")

    feeds = config.get("feeds") or {}
    key = str(feed_kind or "").strip().upper()
    if key not in feeds:
        raise DOJAntitrust8EError(f"unsupported DOJ feed kind: {feed_kind!r}")
    feed = feeds[key]
    source_url = str(feed.get("url") or "").strip()
    if not source_url.startswith("https://www.justice.gov/"):
        raise DOJAntitrust8EError("DOJ feed URL must be an official justice.gov URL")

    observed_at = _aware_datetime(ingested_at)
    raw_xml = xml_path.read_bytes()
    raw_sha = _sha256_bytes(raw_xml)
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise DOJAntitrust8EError("cannot parse DOJ RSS XML") from exc

    items = root.findall(".//item")
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in items:
        title = _item_text(item, "title")
        link = _item_text(item, "link")
        guid = _item_text(item, "guid")
        description = _item_text(item, "description")
        pubdate_text = _item_text(item, "pubDate")
        source_event_id = guid or link
        if not source_event_id:
            raise DOJAntitrust8EError("DOJ RSS item requires guid or link")
        if source_event_id in seen_ids:
            raise DOJAntitrust8EError(f"duplicate DOJ RSS item identity: {source_event_id}")
        seen_ids.add(source_event_id)
        if link and not link.startswith("https://www.justice.gov/"):
            raise DOJAntitrust8EError(f"DOJ RSS item link is not justice.gov: {link}")

        reported = _reported_pubdate(pubdate_text)
        identity_hash = hashlib.sha256(source_event_id.encode("utf-8")).hexdigest()
        source_record = {
            "feed_kind": key,
            "guid": guid or None,
            "link": link or None,
            "title": title or None,
            "description": description or None,
            "pubDate": pubdate_text or None,
        }
        rows.append(
            {
                "canonical_event_key": f"DOJ:LITIGATION_FILED:{key}:{identity_hash}",
                "event_type": str(feed["event_type"]).upper(),
                "subject_id": f"DOJ_ANTITRUST_MATTER:{identity_hash}",
                "source_id": f"doj_antitrust_{key.lower()}",
                "source_class": "PUBLIC_AUTHORITY_PRIMARY",
                "source_event_id": source_event_id,
                "event_state": str(feed["event_state"]).upper(),
                "authority_scope": "DOJ_ANTITRUST_CASE_FILING",
                "published_at": reported.isoformat() if reported else None,
                "valid_from": observed_at.isoformat(),
                "ingested_at": observed_at.isoformat(),
                "content_sha256": _canonical_hash(source_record),
                "strict_pit_eligible": True,
                "discovery_only": False,
                "public_release_proof_status": "INGESTION_ONLY",
                "historical_publication_time_independently_proven": False,
                "source_reported_pubdate": reported.isoformat() if reported else None,
                "title": title or None,
                "link": link or None,
                "source_record": source_record,
            }
        )

    rows.sort(key=lambda row: (str(row.get("source_reported_pubdate") or ""), row["source_event_id"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B3_doj_antitrust_case_filings",
        "status": "AUTHORITATIVE_CASE_FILINGS_PIT_FROM_INGESTION_ONLY",
        "feed_kind": key,
        "source_url": source_url,
        "raw_xml_sha256": raw_sha,
        "ingested_at": observed_at.isoformat(),
        "row_count": len(rows),
        "rows": rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "rss_title_semantic_reclassification_enabled": False,
            "rss_pubdate_used_as_valid_from": False,
            "historical_first_public_release_reconstructed": False,
            "company_name_to_ticker_mapping_enabled": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_doj_antitrust_evidence(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
