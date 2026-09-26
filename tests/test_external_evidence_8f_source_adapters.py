from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scanner.research.external_evidence.bls_cpi_8f import (
    BLSCPI8FError,
    SERIES_MOM_SA,
    SERIES_YOY_NSA,
    build_bls_cpi_macro_observations,
    parse_bls_cpi_release,
)
from scanner.research.external_evidence.eia_energy_8f import (
    EIAEnergy8FError,
    build_eia_prospective_macro_observations,
)
from scanner.research.external_evidence.macro_exposure_8f import (
    MacroExposure8FError,
    validate_macro_observation,
)


BLS_RELEASE = """
Consumer Price Index News Release
Transmission of material in this release is embargoed until
8:30 a.m. (ET) Friday, September 11, 2026 USDL-26-1496

CONSUMER PRICE INDEX - AUGUST 2026

The Consumer Price Index for All Urban Consumers (CPI-U) increased 0.4 percent on a seasonally adjusted basis in August after rising 0.1 percent in July.
Over the last 12 months, the all items index increased 3.4 percent before seasonal adjustment.
"""


def test_bls_archive_parser_extracts_exact_release_availability_and_values():
    parsed = parse_bls_cpi_release(BLS_RELEASE)
    assert parsed["release_id"] == "USDL-26-1496"
    assert parsed["published_at"] == "2026-09-11T08:30:00-04:00"
    assert parsed["reference_period"] == "2026-08"
    assert parsed["observation_date"] == "2026-08-01"
    assert parsed["mom_sa_percent"] == 0.4
    assert parsed["yoy_nsa_percent"] == 3.4
    assert len(parsed["source_record_sha256"]) == 64


def test_bls_archive_records_validate_as_exact_historical_pit():
    rows = build_bls_cpi_macro_observations(
        BLS_RELEASE,
        ingested_at=datetime(2026, 9, 26, 18, 30, tzinfo=timezone.utc),
    )
    assert {row["series_id"] for row in rows} == {SERIES_MOM_SA, SERIES_YOY_NSA}

    normalized = [
        validate_macro_observation(
            row,
            allowed_series_ids={SERIES_MOM_SA, SERIES_YOY_NSA},
        )
        for row in rows
    ]
    assert all(row["historical_vintage_independently_proven"] for row in normalized)
    assert all(row["historical_publication_time_independently_proven"] for row in normalized)
    assert all(row["valid_from"].isoformat() == "2026-09-11T08:30:00-04:00" for row in normalized)


def test_exact_timestamp_proof_cannot_backdate_before_archived_release_time():
    row = build_bls_cpi_macro_observations(
        BLS_RELEASE,
        ingested_at=datetime(2026, 9, 26, 18, 30, tzinfo=timezone.utc),
    )[0]
    row["valid_from"] = "2026-09-11T08:29:59-04:00"
    with pytest.raises(MacroExposure8FError, match="cannot precede independently proven exact publication time"):
        validate_macro_observation(row, allowed_series_ids={SERIES_MOM_SA})


def test_bls_parser_fails_closed_when_release_timestamp_is_missing():
    with pytest.raises(BLSCPI8FError, match="release timestamp"):
        parse_bls_cpi_release(BLS_RELEASE.replace("8:30 a.m. (ET) Friday, September 11, 2026", ""))


def test_eia_current_history_is_prospective_snapshot_not_retrojected_history():
    payload = {
        "response": {
            "data": [
                {"period": "2026-09-21", "value": "96.97"},
                {"period": "2026-09-22", "value": 96.41},
            ]
        }
    }
    ingested_at = datetime(2026, 9, 26, 18, 45, tzinfo=timezone.utc)
    rows = build_eia_prospective_macro_observations(
        payload,
        series_id="PET.RWTC.D",
        ingested_at=ingested_at,
    )
    assert len(rows) == 2
    assert all(row["factor_id"] == "oil" for row in rows)
    assert all(row["valid_from"] == ingested_at.isoformat() for row in rows)
    assert all(row["historical_vintage_independently_proven"] is False for row in rows)
    assert all(row["availability_proof_type"] == "ACTUAL_PROSPECTIVE_INGESTION" for row in rows)
    for row in rows:
        validate_macro_observation(row, allowed_series_ids={"PET.RWTC.D"})


def test_eia_unknown_value_stays_unknown_not_numeric_neutral():
    payload = {"response": {"data": [{"period": "2026-09-23", "value": None}]}}
    row = build_eia_prospective_macro_observations(
        payload,
        series_id="NG.RNGWHHD.D",
        ingested_at=datetime(2026, 9, 26, 19, 0, tzinfo=timezone.utc),
    )[0]
    assert row["status"] == "UNKNOWN"
    assert row["value"] is None
    validate_macro_observation(row, allowed_series_ids={"NG.RNGWHHD.D"})


def test_eia_unregistered_series_fails_closed():
    with pytest.raises(EIAEnergy8FError, match="not predeclared"):
        build_eia_prospective_macro_observations(
            {"response": {"data": []}},
            series_id="INVENTED.SERIES.D",
            ingested_at=datetime(2026, 9, 26, 19, 0, tzinfo=timezone.utc),
        )
