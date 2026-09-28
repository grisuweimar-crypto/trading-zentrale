from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scanner.research.external_evidence.macro_ledger_8f import (
    MacroLedger8FError,
    build_macro_ledger,
    merge_macro_observations_append_only,
)


def _row(**overrides):
    row = {
        "series_id": "TEST_SERIES",
        "factor_id": "oil",
        "observation_date": "2026-09-24",
        "value": "91.0",
        "units": "dollars_per_barrel",
        "realtime_start": "2026-09-26",
        "realtime_end": "9999-12-31",
        "revision_id": "TEST_SERIES:2026-09-24:snapshot:1",
        "source_id": "test_source",
        "source_record_sha256": "a" * 64,
        "license_status": "USABLE",
        "status": "KNOWN",
        "ingested_at": "2026-09-26T18:00:00+00:00",
        "valid_from": "2026-09-26T18:00:00+00:00",
        "historical_vintage_independently_proven": False,
        "historical_publication_time_independently_proven": False,
        "published_at": None,
        "availability_proof_type": "ACTUAL_PROSPECTIVE_INGESTION",
    }
    row.update(overrides)
    return row


def test_append_only_ledger_deduplicates_exact_identity_without_overwrite():
    merged = merge_macro_observations_append_only(
        existing_rows=[_row()],
        new_rows=[_row()],
        allowed_series_ids={"TEST_SERIES"},
    )
    assert len(merged) == 1
    assert merged[0]["value"] == 91.0


def test_append_only_ledger_rejects_changed_content_under_same_revision_identity():
    with pytest.raises(MacroLedger8FError, match="identity collision"):
        merge_macro_observations_append_only(
            existing_rows=[_row()],
            new_rows=[_row(value="99.0")],
            allowed_series_ids={"TEST_SERIES"},
        )


def test_later_revision_is_preserved_as_separate_row():
    merged = merge_macro_observations_append_only(
        existing_rows=[_row()],
        new_rows=[
            _row(
                value="91.5",
                revision_id="TEST_SERIES:2026-09-24:snapshot:2",
                ingested_at="2026-09-27T18:00:00+00:00",
                valid_from="2026-09-27T18:00:00+00:00",
                realtime_start="2026-09-27",
                source_record_sha256="b" * 64,
            )
        ],
        allowed_series_ids={"TEST_SERIES"},
    )
    assert len(merged) == 2
    assert [row["value"] for row in merged] == [91.0, 91.5]


def test_coverage_audit_uses_only_rows_knowable_at_asof():
    ledger = build_macro_ledger(
        existing_rows=[],
        new_rows=[
            _row(),
            _row(
                observation_date="2026-09-25",
                value="92.0",
                revision_id="TEST_SERIES:2026-09-25:snapshot:future",
                ingested_at="2026-09-27T18:00:00+00:00",
                valid_from="2026-09-27T18:00:00+00:00",
                realtime_start="2026-09-27",
                source_record_sha256="c" * 64,
            ),
        ],
        allowed_series_ids={"TEST_SERIES"},
        as_of=datetime(2026, 9, 26, 20, 0, tzinfo=timezone.utc),
    )
    assert ledger["row_count"] == 2
    assert ledger["knowable_row_count"] == 1
    assert ledger["future_row_count"] == 1
    assert ledger["coverage"]["row_count"] == 1
    assert ledger["coverage"]["series_count"] == 1
    assert ledger["coverage"]["factor_counts"] == {"oil": 1}
    assert ledger["coverage"]["availability_proof_counts"] == {
        "ACTUAL_PROSPECTIVE_INGESTION": 1
    }
    assert ledger["guards"]["market_outcomes_read"] is False
