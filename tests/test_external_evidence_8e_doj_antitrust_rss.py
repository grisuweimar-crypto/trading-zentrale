from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scanner.research.external_evidence.doj_antitrust_rss_8e import (
    DOJAntitrust8EError,
    build_doj_antitrust_case_filing_evidence,
)
from scanner.research.external_evidence.structured_events_8e import build_event_ledger


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_doj_antitrust_rss_v1.json"
EVENT_CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _event_config() -> dict:
    return json.loads(EVENT_CONFIG_PATH.read_text(encoding="utf-8"))


def _write_rss(path: Path, *, duplicate: bool = False) -> None:
    item = """
      <item>
        <title>U.S. v. Example Corp.</title>
        <link>https://www.justice.gov/atr/case/us-v-example-corp</link>
        <guid>https://www.justice.gov/atr/case/us-v-example-corp</guid>
        <pubDate>Fri, 25 Sep 2026 14:30:00 -0400</pubDate>
        <description>Complaint filed.</description>
      </item>
    """
    path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?><rss version='2.0'><channel>"
        + item
        + (item if duplicate else "")
        + "</channel></rss>",
        encoding="utf-8",
    )


def test_doj_case_filing_is_strict_pit_only_from_ingestion(tmp_path: Path) -> None:
    source = tmp_path / "civil.xml"
    _write_rss(source)
    ingested_at = "2026-09-26T17:00:00+00:00"
    payload = build_doj_antitrust_case_filing_evidence(
        xml_path=source,
        feed_kind="CIVIL_CASE_FILINGS",
        ingested_at=ingested_at,
        config=_config(),
    )
    assert payload["row_count"] == 1
    row = payload["rows"][0]
    assert row["event_type"] == "LITIGATION_FILED"
    assert row["event_state"] == "FILED"
    assert row["published_at"] == "2026-09-25T14:30:00-04:00"
    assert row["valid_from"] == ingested_at
    assert row["public_release_proof_status"] == "INGESTION_ONLY"
    assert row["historical_publication_time_independently_proven"] is False
    assert payload["guards"]["rss_pubdate_used_as_valid_from"] is False


def test_doj_case_filing_does_not_fake_first_public_release(tmp_path: Path) -> None:
    source = tmp_path / "civil.xml"
    _write_rss(source)
    payload = build_doj_antitrust_case_filing_evidence(
        xml_path=source,
        feed_kind="CIVIL_CASE_FILINGS",
        ingested_at="2026-09-26T17:00:00+00:00",
        config=_config(),
    )
    event_config = _event_config()
    source_ranks = {
        key: int(value["authority_rank"])
        for key, value in event_config["source_classes"].items()
    }
    ledger = build_event_ledger(
        evidence_rows=payload["rows"],
        as_of=datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc),
        allowed_event_types=event_config["initial_event_taxonomy"],
        source_ranks=source_ranks,
    )
    event = ledger["events"][0]
    assert event["status"] == "INSUFFICIENT_FIRST_PUBLIC_RELEASE_PROOF"
    assert event["first_public_release_at"] is None
    assert event["valid_from"] == "2026-09-26T17:00:00+00:00"


def test_doj_adapter_rejects_duplicate_item_identity(tmp_path: Path) -> None:
    source = tmp_path / "civil.xml"
    _write_rss(source, duplicate=True)
    with pytest.raises(DOJAntitrust8EError, match="duplicate DOJ RSS item identity"):
        build_doj_antitrust_case_filing_evidence(
            xml_path=source,
            feed_kind="CIVIL_CASE_FILINGS",
            ingested_at="2026-09-26T17:00:00+00:00",
            config=_config(),
        )


def test_doj_adapter_rejects_naive_ingested_at(tmp_path: Path) -> None:
    source = tmp_path / "civil.xml"
    _write_rss(source)
    with pytest.raises(DOJAntitrust8EError, match="timezone-aware"):
        build_doj_antitrust_case_filing_evidence(
            xml_path=source,
            feed_kind="CIVIL_CASE_FILINGS",
            ingested_at="2026-09-26T17:00:00",
            config=_config(),
        )
