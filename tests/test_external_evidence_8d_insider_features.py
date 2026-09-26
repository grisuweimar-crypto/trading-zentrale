from __future__ import annotations

from scanner.research.external_evidence.sec_insider_features import (
    SecInsiderFeatureError,
    build_insider_features,
)


def _validation(decision: str = "PASS_SOURCE_SEMANTICS_VALIDATION") -> dict:
    return {
        "schema_version": "external_evidence_8d_insider_validation_result_v1",
        "decision": decision,
        "guards": {
            "market_outcomes_read_by_pipeline": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def _row(
    key: str,
    code: str,
    transaction_date: str,
    valid_from: str,
    *,
    shares: float = 10.0,
    price: float = 2.0,
    amendment: bool = False,
    candidate_status: str = "P_S_HIGH_PRECISION_DISCRETIONARY_CANDIDATE",
    owners: list[dict] | None = None,
) -> dict:
    return {
        "issuer_cik": "0000000001",
        "accession_number": f"0000000001-26-{key}",
        "transaction_key": key,
        "document_type": "4/A" if amendment else "4",
        "amendment": amendment,
        "transaction_date": transaction_date,
        "transaction_code": code,
        "transaction_shares": shares,
        "transaction_price_per_share": price,
        "transaction_value_when_price_known": shares * price,
        "valid_from": valid_from,
        "strict_pit_eligible": True,
        "candidate_status": candidate_status,
        "aff10b5one": False,
        "equity_swap_involved": False,
        "reporting_owners": owners
        if owners is not None
        else [{"reporting_owner_cik": f"0000009{key[-3:]}"}],
    }


def _evidence() -> dict:
    return {
        "schema_version": "external_evidence_8d_sec_insider_bulk_v1",
        "source_quarter": "2026Q2",
        "rows": [
            _row("000001", "P", "2026-04-15", "2026-04-16T16:00:00-04:00", shares=100, price=5),
            _row("000002", "S", "2026-06-01", "2026-06-02T16:00:00-04:00", shares=20, price=10),
            _row("000003", "P", "2026-06-15", "2026-07-02T16:00:00-04:00", shares=30, price=3),
            _row("000004", "P", "2026-06-20", "2026-06-21T16:00:00-04:00", amendment=True),
            _row(
                "000005",
                "S",
                "2026-06-25",
                "2026-06-26T16:00:00-04:00",
                candidate_status="EXCLUDED_10B5_1_PLAN",
            ),
        ],
        "guards": {"market_outcomes_read": False},
    }


def test_b4_requires_source_semantics_pass() -> None:
    try:
        build_insider_features(
            evidence_payloads=[_evidence()],
            validation_result=_validation("REMAIN_CHALLENGER"),
            asof_grid=[{"issuer_cik": "0000000001", "as_of": "2026-06-30T20:00:00+00:00"}],
        )
    except SecInsiderFeatureError as exc:
        assert "PASS_SOURCE_SEMANTICS_VALIDATION" in str(exc)
    else:
        raise AssertionError("B4 must refuse non-passing B3 validation")


def test_b4_primary_and_robustness_windows_are_pit_safe() -> None:
    payload = build_insider_features(
        evidence_payloads=[_evidence()],
        validation_result=_validation(),
        asof_grid=[{"issuer_cik": "0000000001", "as_of": "2026-06-30T20:00:00+00:00"}],
        primary_days=30,
        robustness_days=(90,),
    )
    assert payload["guards"]["market_outcomes_read"] is False
    assert payload["guards"]["numeric_features_promoted"] is False
    rows = {row["window_days"]: row for row in payload["rows"]}

    primary = rows[30]
    assert primary["window_role"] == "PRIMARY"
    assert primary["coverage_status"] == "KNOWN"
    assert primary["purchase_count"] == 0  # future-known P and amendment are excluded
    assert primary["sale_count"] == 1
    assert primary["sale_shares"] == 20.0
    assert primary["sale_value_when_price_known"] == 200.0

    robustness = rows[90]
    assert robustness["window_role"] == "ROBUSTNESS_ONLY"
    assert robustness["coverage_status"] == "KNOWN"
    assert robustness["purchase_count"] == 1
    assert robustness["sale_count"] == 1
    assert robustness["purchase_shares"] == 100.0


def test_b4_incomplete_source_window_is_explicit_not_zero() -> None:
    payload = build_insider_features(
        evidence_payloads=[_evidence()],
        validation_result=_validation(),
        asof_grid=[{"issuer_cik": "0000000001", "as_of": "2026-05-15T20:00:00+00:00"}],
        primary_days=30,
        robustness_days=(90,),
    )
    rows = {row["window_days"]: row for row in payload["rows"]}
    assert rows[30]["coverage_status"] in {"KNOWN", "KNOWN_ZERO"}
    assert rows[90]["coverage_status"] == "INSUFFICIENT_SOURCE_WINDOW"
    assert rows[90]["purchase_count"] is None
    assert rows[90]["sale_count"] is None


def test_b4_owner_counts_fail_closed_on_multi_owner_transactions() -> None:
    evidence = _evidence()
    evidence["rows"].append(
        _row(
            "000006",
            "P",
            "2026-06-28",
            "2026-06-29T16:00:00-04:00",
            owners=[
                {"reporting_owner_cik": "0000000101"},
                {"reporting_owner_cik": "0000000102"},
            ],
        )
    )
    payload = build_insider_features(
        evidence_payloads=[evidence],
        validation_result=_validation(),
        asof_grid=[{"issuer_cik": "0000000001", "as_of": "2026-06-30T20:00:00+00:00"}],
        primary_days=30,
        robustness_days=(90,),
    )
    primary = next(row for row in payload["rows"] if row["window_days"] == 30)
    assert primary["purchase_count"] == 1
    assert primary["owner_identity_status"] == "PARTIAL_OWNER_IDENTITY"
    assert primary["distinct_buyer_count"] is None
    assert primary["distinct_seller_count"] is None
