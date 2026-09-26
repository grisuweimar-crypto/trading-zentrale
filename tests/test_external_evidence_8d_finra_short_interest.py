from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.finra_short_interest import (
    HISTORICAL_MODE,
    PROSPECTIVE_MODE,
    FinraShortInterestError,
    build_finra_snapshot,
    publication_valid_from,
)
from scanner.research.external_evidence.finra_short_interest_acquisition import (
    FinraShortInterestAcquisitionError,
    collect_finra_publication_snapshot,
)


def _row(symbol: str = "TEST", settlement: str = "2026-09-15") -> dict:
    return {
        "symbolCode": symbol,
        "settlementDate": settlement,
        "currentShortPositionQuantity": 1200,
        "previousShortPositionQuantity": 1000,
        "changePreviousNumber": 200,
        "changePercent": 20.0,
        "averageDailyVolumeQuantity": 400,
        "daysToCoverQuantity": 3.0,
        "revisionFlag": None,
        "stockSplitFlag": None,
        "marketClassCode": "N",
        "issueName": "Test Issuer",
    }


def _write_rows(path: Path, rows: list[dict]) -> Path:
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def test_publication_valid_from_respects_new_york_dst() -> None:
    assert publication_valid_from("2026-09-25") == "2026-09-25T20:40:00Z"
    assert publication_valid_from("2026-12-24") == "2026-12-24T21:40:00Z"


def test_historical_backfill_is_never_strict_pit(tmp_path: Path) -> None:
    source = _write_rows(tmp_path / "finra.json", [_row()])
    payload = build_finra_snapshot(
        source_path=source,
        publication_date="2026-09-25",
        ingested_at="2026-09-26T08:00:00+00:00",
        source_mode=HISTORICAL_MODE,
    )
    assert payload["strict_pit_eligible"] is False
    assert payload["vintage_status"] == "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
    assert payload["rows"][0]["strict_pit_eligible"] is False
    assert payload["guards"]["market_outcomes_read"] is False
    assert payload["guards"]["market_direction_assigned"] is False
    assert payload["rows"][0]["market_direction"] == "UNASSIGNED"


def test_prospective_snapshot_requires_ingestion_after_publication(tmp_path: Path) -> None:
    source = _write_rows(tmp_path / "finra.json", [_row()])
    with pytest.raises(FinraShortInterestError, match="precedes FINRA publication"):
        build_finra_snapshot(
            source_path=source,
            publication_date="2026-09-25",
            ingested_at="2026-09-25T20:39:59+00:00",
            source_mode=PROSPECTIVE_MODE,
        )

    payload = build_finra_snapshot(
        source_path=source,
        publication_date="2026-09-25",
        ingested_at="2026-09-25T20:40:00+00:00",
        source_mode=PROSPECTIVE_MODE,
    )
    assert payload["strict_pit_eligible"] is True
    assert payload["rows"][0]["published_at"] == "2026-09-25T20:40:00Z"
    assert payload["rows"][0]["event_time"] == "2026-09-15"


def test_finra_normalizer_marks_partial_and_detects_inconsistent_changes(tmp_path: Path) -> None:
    row = _row()
    row["changePreviousNumber"] = 999
    row["changePercent"] = None
    source = _write_rows(tmp_path / "finra.json", [row])
    payload = build_finra_snapshot(
        source_path=source,
        publication_date="2026-09-25",
        ingested_at="2026-09-25T21:00:00+00:00",
        source_mode=PROSPECTIVE_MODE,
    )
    observed = payload["rows"][0]
    assert observed["feature_status"] == "PARTIAL"
    assert "REPORTED_CHANGE_QUANTITY_INCONSISTENT_WITH_POSITIONS" in observed["reason_codes"]


class _Response:
    def __init__(self, rows: list[dict], *, total: int, request_id: str):
        self.status_code = 200
        self.content = json.dumps(rows).encode("utf-8")
        self.text = self.content.decode("utf-8")
        self.headers = {
            "Record-Total": str(total),
            "FINRA-api-request-id": request_id,
        }

    def json(self):
        return json.loads(self.content.decode("utf-8"))


def test_prospective_collector_pages_and_preserves_raw_hashes(tmp_path: Path) -> None:
    source_rows = [
        _row("AAA"),
        _row("BBB"),
        _row("CCC"),
    ]
    calls: list[dict] = []

    def fake_post(url: str, *, json: dict, headers: dict, timeout: float):
        assert url.startswith("https://api.finra.org/")
        assert headers["Accept"] == "application/json"
        assert json["dateRangeFilters"] == [
            {
                "fieldName": "settlementDate",
                "startDate": "2026-09-15",
                "endDate": "2026-09-15",
            }
        ]
        calls.append(dict(json))
        offset = int(json["offset"])
        limit = int(json["limit"])
        rows = source_rows[offset : offset + limit]
        return _Response(rows, total=3, request_id=f"req-{offset}")

    manifest = collect_finra_publication_snapshot(
        settlement_date="2026-09-15",
        publication_date="2026-09-25",
        output_dir=tmp_path / "out",
        collected_at="2026-09-25T20:45:00+00:00",
        limit=2,
        minimum_interval_seconds=0,
        post_fn=fake_post,
        sleep_fn=lambda _: None,
    )
    assert len(calls) == 2
    assert manifest["raw_row_count"] == 3
    assert manifest["record_total_header"] == 3
    assert manifest["strict_pit_eligible"] is True
    assert len(manifest["pages"]) == 2
    assert all(len(page["sha256"]) == 64 for page in manifest["pages"])
    snapshot = json.loads((tmp_path / "out" / "snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["row_count"] == 3
    assert snapshot["strict_pit_eligible"] is True


def test_prospective_collector_fails_closed_before_publication(tmp_path: Path) -> None:
    called = False

    def fake_post(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("network must not be called before publication")

    with pytest.raises(FinraShortInterestAcquisitionError, match="before 16:40 ET"):
        collect_finra_publication_snapshot(
            settlement_date="2026-09-15",
            publication_date="2026-09-25",
            output_dir=tmp_path / "out",
            collected_at="2026-09-25T20:39:59+00:00",
            post_fn=fake_post,
        )
    assert called is False


def test_prospective_collector_rejects_wrong_settlement_date(tmp_path: Path) -> None:
    wrong = _row("BAD", settlement="2026-09-16")

    def fake_post(url: str, *, json: dict, headers: dict, timeout: float):
        return _Response([wrong], total=1, request_id="req-bad")

    with pytest.raises(FinraShortInterestAcquisitionError, match="unexpected settlement dates"):
        collect_finra_publication_snapshot(
            settlement_date="2026-09-15",
            publication_date="2026-09-25",
            output_dir=tmp_path / "out",
            collected_at="2026-09-25T20:45:00+00:00",
            minimum_interval_seconds=0,
            post_fn=fake_post,
            sleep_fn=lambda _: None,
        )
