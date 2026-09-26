from __future__ import annotations

import pytest

from scanner.research.external_evidence.sec_insider_identity_grid import (
    SecInsiderIdentityGridError,
    verified_feature_grid,
)


def _payload() -> dict:
    return {
        "schema_version": "external_evidence_8d_insider_asof_identity_v1",
        "guards": {
            "market_outcomes_read": False,
            "current_ticker_retrojection_enabled": False,
        },
        "rows": [
            {
                "issuer_cik": "0000000001",
                "as_of": "2026-05-01T20:00:00+00:00",
                "identity_status": "VERIFIED_FAMILY_PIT_IDENTITY",
            },
            {
                "issuer_cik": None,
                "as_of": "2026-05-01T20:00:00+00:00",
                "identity_status": "UNKNOWN_NO_PIT_IDENTITY",
            },
            {
                "issuer_cik": None,
                "as_of": "2026-05-02T20:00:00+00:00",
                "identity_status": "AMBIGUOUS_PIT_IDENTITY",
            },
            {
                "issuer_cik": "0000000001",
                "as_of": "2026-05-01T20:00:00+00:00",
                "identity_status": "VERIFIED_FAMILY_PIT_IDENTITY",
            },
            {
                "issuer_cik": "0000000002",
                "as_of": "2026-05-03T20:00:00+00:00",
                "identity_status": "VERIFIED_FAMILY_PIT_IDENTITY",
            },
        ],
    }


def test_grid_contains_only_verified_identity_and_deduplicates() -> None:
    rows = verified_feature_grid(_payload())
    assert rows == [
        {"issuer_cik": "0000000001", "as_of": "2026-05-01T20:00:00+00:00"},
        {"issuer_cik": "0000000002", "as_of": "2026-05-03T20:00:00+00:00"},
    ]


def test_grid_refuses_outcome_contaminated_identity_payload() -> None:
    payload = _payload()
    payload["guards"]["market_outcomes_read"] = True
    with pytest.raises(SecInsiderIdentityGridError, match="outcome-blind"):
        verified_feature_grid(payload)


def test_grid_refuses_current_ticker_retrojection() -> None:
    payload = _payload()
    payload["guards"]["current_ticker_retrojection_enabled"] = True
    with pytest.raises(SecInsiderIdentityGridError, match="retrojection"):
        verified_feature_grid(payload)
