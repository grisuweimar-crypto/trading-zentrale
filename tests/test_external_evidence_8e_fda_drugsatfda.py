from __future__ import annotations

import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scanner.research.external_evidence.fda_drugsatfda_8e import (
    FDAApproval8EError,
    build_fda_approval_evidence,
)
from scanner.research.external_evidence.structured_events_8e import build_event_ledger


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_fda_approval_v1.json"
EVENT_CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _event_config() -> dict:
    return json.loads(EVENT_CONFIG_PATH.read_text(encoding="utf-8"))


def _write_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "strange_app_filename.txt",
            "ApplNo\tApplType\tApplPublicNotes\tSponsorName\n"
            "000001\tNDA\t\tSponsor One\n"
            "000002\tANDA\t\tSponsor Two\n",
        )
        zf.writestr(
            "submissions_any_name.txt",
            "ApplNo\tSubmissionClassCodeID\tSubmissionType\tSubmissionNo\tSubmissionStatus\tSubmissionStatusDate\tSubmissionsPublicNotes\tReviewPriority\n"
            "000001\t\tORIG\t1\tAP\t2026-09-10\t\tPriority\n"
            "000002\t\tORIG\t1\tTA\t2026-09-11\t\tStandard\n",
        )
        zf.writestr(
            "actions.txt",
            "ActionTypes_LookupID\tActionTypes_LookupDescription\tSupplCategoryLevel1Code\tSupplCategoryLevel2Code\n"
            "10\tApproval\t\t\n"
            "11\tTentative Approval\t\t\n",
        )
        zf.writestr(
            "join.txt",
            "J_SubmissionActionTypeID\tSubmissionNo\tSubmissionType\tApplNo\tActionTypes_LookupID\n"
            "1\t1\tORIG\t000001\t10\n"
            "2\t1\tORIG\t000002\t11\n",
        )


def test_fda_bulk_uses_exact_approval_action_and_ingestion_for_pit(tmp_path: Path) -> None:
    source = tmp_path / "drugsatfda.zip"
    _write_zip(source)
    ingested_at = "2026-09-26T16:00:00+00:00"

    payload = build_fda_approval_evidence(
        zip_path=source,
        ingested_at=ingested_at,
        config=_config(),
    )

    assert payload["row_count"] == 1
    row = payload["rows"][0]
    assert row["application_number"] == "000001"
    assert row["action_type_description"] == "Approval"
    assert row["event_type"] == "REGULATORY_APPROVAL"
    assert row["event_state"] == "APPROVED"
    assert row["fda_action_date"] == "2026-09-10"
    assert row["published_at"] is None
    assert row["valid_from"] == ingested_at
    assert row["ingested_at"] == ingested_at
    assert row["public_release_proof_status"] == "INGESTION_ONLY"
    assert row["historical_publication_time_independently_proven"] is False
    assert row["strict_pit_eligible"] is True
    assert payload["guards"]["submission_status_date_used_as_published_at"] is False
    assert payload["guards"]["submission_status_date_used_as_valid_from"] is False
    assert payload["guards"]["historical_first_public_release_reconstructed"] is False


def test_tentative_approval_is_not_emitted_as_regulatory_approval(tmp_path: Path) -> None:
    source = tmp_path / "drugsatfda.zip"
    _write_zip(source)
    payload = build_fda_approval_evidence(
        zip_path=source,
        ingested_at="2026-09-26T16:00:00+00:00",
        config=_config(),
    )
    assert all(row["application_number"] != "000002" for row in payload["rows"])


def test_fda_ingestion_only_evidence_does_not_fake_first_public_release(tmp_path: Path) -> None:
    source = tmp_path / "drugsatfda.zip"
    _write_zip(source)
    payload = build_fda_approval_evidence(
        zip_path=source,
        ingested_at="2026-09-26T16:00:00+00:00",
        config=_config(),
    )
    event_config = _event_config()
    source_ranks = {
        key: int(value["authority_rank"])
        for key, value in event_config["source_classes"].items()
    }
    ledger = build_event_ledger(
        evidence_rows=payload["rows"],
        as_of=datetime(2026, 9, 26, 17, 0, tzinfo=timezone.utc),
        allowed_event_types=event_config["initial_event_taxonomy"],
        source_ranks=source_ranks,
    )
    assert ledger["event_count"] == 1
    event = ledger["events"][0]
    assert event["status"] == "INSUFFICIENT_FIRST_PUBLIC_RELEASE_PROOF"
    assert event["first_public_release_at"] is None
    assert event["valid_from"] == "2026-09-26T16:00:00+00:00"


def test_fda_adapter_rejects_naive_ingestion_time(tmp_path: Path) -> None:
    source = tmp_path / "drugsatfda.zip"
    _write_zip(source)
    with pytest.raises(FDAApproval8EError, match="timezone-aware"):
        build_fda_approval_evidence(
            zip_path=source,
            ingested_at="2026-09-26T16:00:00",
            config=_config(),
        )
