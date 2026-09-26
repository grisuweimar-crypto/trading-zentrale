from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scanner.research.external_evidence.macro_exposure_8f import (
    MacroExposure8FError,
    build_macro_context,
    validate_exposure_mapping,
    validate_macro_observation,
    validate_phase8f_contract,
)


ROOT = Path(__file__).resolve().parents[1]


def _macro_row(**overrides):
    row = {
        "series_id": "TEST_SERIES",
        "factor_id": "rates_policy",
        "observation_date": "2026-09-24",
        "value": "4.25",
        "units": "percent",
        "realtime_start": "2026-09-25",
        "realtime_end": "9999-12-31",
        "revision_id": "TEST_SERIES:2026-09-24:2026-09-25",
        "source_id": "fred_alfred_realtime",
        "source_record_sha256": "a" * 64,
        "license_status": "USABLE",
        "status": "KNOWN",
        "ingested_at": "2026-09-26T12:00:00+00:00",
        "valid_from": "2026-09-26T00:00:00+00:00",
        "historical_vintage_independently_proven": True,
    }
    row.update(overrides)
    return row


def _mapping(**overrides):
    row = {
        "mapping_id": "MAP:ABC:rates_policy:1",
        "map_version": "8F_EXPOSURE_MAP_2026-09-26_V1",
        "subject_id": "ABC",
        "factor_id": "rates_policy",
        "relationship_class": "FINANCING_SENSITIVITY",
        "evidence_type": "ISSUER_FILING",
        "evidence_reference": "issuer-filing-accession-1",
        "evidence_sha256": "b" * 64,
        "evidence_valid_from": "2026-09-25T15:00:00+00:00",
        "human_reviewed": True,
        "reviewed_at": "2026-09-26T10:00:00+00:00",
        "review_status": "ACTIVE",
        "valid_from": "2026-09-26T10:00:00+00:00",
        "valid_to": None,
    }
    row.update(overrides)
    return row


def test_historical_day_level_vintage_uses_next_utc_day_floor():
    normalized = validate_macro_observation(
        _macro_row(), allowed_series_ids={"TEST_SERIES"}
    )
    assert normalized["valid_from"].isoformat() == "2026-09-26T00:00:00+00:00"
    assert normalized["historical_vintage_independently_proven"] is True


def test_historical_day_level_vintage_cannot_be_used_same_day():
    with pytest.raises(MacroExposure8FError, match="next UTC day"):
        validate_macro_observation(
            _macro_row(valid_from="2026-09-25T23:59:59+00:00"),
            allowed_series_ids={"TEST_SERIES"},
        )


def test_prospective_macro_record_cannot_backdate_before_ingestion():
    with pytest.raises(MacroExposure8FError, match="cannot precede actual ingestion"):
        validate_macro_observation(
            _macro_row(
                historical_vintage_independently_proven=False,
                valid_from="2026-09-26T11:59:59+00:00",
            ),
            allowed_series_ids={"TEST_SERIES"},
        )


def test_non_known_macro_state_cannot_smuggle_numeric_neutral_value():
    with pytest.raises(MacroExposure8FError, match="must not carry a numeric value"):
        validate_macro_observation(
            _macro_row(status="UNKNOWN", value=0),
            allowed_series_ids={"TEST_SERIES"},
        )


def test_exposure_mapping_requires_human_review_and_no_retrojection():
    with pytest.raises(MacroExposure8FError, match="human-reviewed"):
        validate_exposure_mapping(
            _mapping(human_reviewed=False), allowed_factor_ids={"rates_policy"}
        )

    with pytest.raises(MacroExposure8FError, match="retrojected"):
        validate_exposure_mapping(
            _mapping(valid_from="2026-09-26T09:59:59+00:00"),
            allowed_factor_ids={"rates_policy"},
        )


def test_direction_weight_threshold_and_outcome_fields_are_forbidden():
    for field, value in (
        ("direction", "POSITIVE"),
        ("weight", 1.0),
        ("threshold", 0.5),
        ("forward_return", 0.12),
    ):
        with pytest.raises(MacroExposure8FError, match="forbids direction/outcome/weight/threshold"):
            validate_exposure_mapping(
                _mapping(**{field: value}), allowed_factor_ids={"rates_policy"}
            )


def test_context_uses_latest_revision_knowable_at_asof_only():
    observations = [
        _macro_row(
            value="4.25",
            realtime_start="2026-09-24",
            revision_id="r1",
            valid_from="2026-09-25T00:00:00+00:00",
            ingested_at="2026-09-26T12:00:00+00:00",
        ),
        _macro_row(
            value="4.10",
            realtime_start="2026-09-25",
            revision_id="r2",
            valid_from="2026-09-26T00:00:00+00:00",
            ingested_at="2026-09-26T12:00:00+00:00",
        ),
        _macro_row(
            value="3.90",
            realtime_start="2026-09-26",
            revision_id="future-r3",
            valid_from="2026-09-27T00:00:00+00:00",
            ingested_at="2026-09-27T12:00:00+00:00",
        ),
    ]
    result = build_macro_context(
        observations=observations,
        mappings=[_mapping()],
        as_of=datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc),
        allowed_series_ids={"TEST_SERIES"},
        allowed_factor_ids={"rates_policy"},
    )
    assert result["context_count"] == 1
    assert result["contexts"][0]["status"] == "KNOWN"
    assert result["contexts"][0]["macro_observation"]["revision_id"] == "r2"
    assert result["contexts"][0]["macro_observation"]["value"] == 4.10
    assert result["guards"]["market_outcomes_read"] is False
    assert result["guards"]["market_direction_assigned"] is False


def test_empty_versioned_exposure_map_is_valid_foundation_and_contract_is_frozen_fail_closed():
    macro_config = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )
    exposure_map = json.loads(
        (ROOT / "configs/external_evidence_8f_exposure_map_v1.json").read_text(encoding="utf-8")
    )
    result = validate_phase8f_contract(macro_config, exposure_map)
    assert result["status"] == "PASS_FOUNDATION_CONTRACT"
    assert result["factor_count"] == 11
    assert result["mapping_count"] == 0
    assert result["outcomes_read"] is False

    macro_config["principles"]["market_outcomes_may_be_read"] = True
    with pytest.raises(MacroExposure8FError, match="hard guards"):
        validate_phase8f_contract(macro_config, exposure_map)
