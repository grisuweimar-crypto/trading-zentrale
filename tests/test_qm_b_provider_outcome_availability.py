from datetime import date, datetime, timezone

from scanner.research.governance.qm_b_provider_outcome_availability import (
    classify_current_provider_reachability,
    classify_forward_outcome,
    load_provider_outcome_contract,
)


def test_provider_reachability_distinguishes_pre_and_post_snapshot_attempts():
    snapshot = datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc)
    base = {"status": "price_data_ok", "provider_symbol": "AAPL", "sessions": 500}
    before = classify_current_provider_reachability(
        {**base, "last_attempt_at": "2026-09-29T19:59:00+00:00"},
        snapshot_generated_at=snapshot,
    )
    after = classify_current_provider_reachability(
        {**base, "last_attempt_at": "2026-09-29T20:01:00+00:00"},
        snapshot_generated_at=snapshot,
    )
    assert before["classification"] == "PIT_OBSERVED_AT_OR_BEFORE_SNAPSHOT"
    assert before["snapshot_pit"] is True
    assert after["classification"] == "POST_SNAPSHOT_OBSERVED"
    assert after["snapshot_pit"] is False


def test_provider_attempt_without_price_coverage_is_not_positive():
    result = classify_current_provider_reachability(
        {
            "status": "price_data_unavailable",
            "provider_symbol": "MISSING",
            "sessions": 0,
            "last_attempt_at": "2026-09-29T19:59:00+00:00",
        },
        snapshot_generated_at=datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc),
    )
    assert result["classification"] == "ATTEMPT_WITHOUT_PRICE_COVERAGE"
    assert result["snapshot_pit"] is False


def test_forward_outcome_requires_exact_start_and_horizon_target():
    days = [date(2026, 9, d) for d in (1, 2, 3, 4, 5, 8)]
    price_dates = {"AAA": days}
    closes = {("AAA", day): 100.0 + i for i, day in enumerate(days)}
    available = classify_forward_outcome(
        symbol="AAA",
        event_date=days[0],
        horizon=5,
        price_dates=price_dates,
        closes=closes,
        audit_as_of=date(2026, 9, 30),
    )
    assert available["availability_status"] == "AVAILABLE"
    assert available["available_from"] == "2026-09-08"
    assert available["denominator_eligible"] is True
    assert abs(available["forward_return"] - 0.05) < 1e-12

    missing = classify_forward_outcome(
        symbol="AAA",
        event_date=date(2026, 9, 7),
        horizon=5,
        price_dates=price_dates,
        closes=closes,
        audit_as_of=date(2026, 9, 30),
    )
    assert missing["availability_status"] == "MISSING"
    assert missing["denominator_eligible"] is False


def test_unresolved_target_fails_closed_as_unknown_not_not_yet_available():
    days = [date(2026, 9, d) for d in (21, 22, 23)]
    result = classify_forward_outcome(
        symbol="AAA",
        event_date=days[0],
        horizon=5,
        price_dates={"AAA": days},
        closes={("AAA", day): 100.0 for day in days},
        audit_as_of=date(2026, 9, 30),
    )
    assert result["availability_status"] == "UNKNOWN"
    assert result["denominator_eligible"] is False
    assert "NOT_YET_AVAILABLE_NOT_INFERRED_WITHOUT_CALENDAR_EVIDENCE" in result["reason_codes"]


def test_contract_forbids_retrojection_promotion_and_imputation():
    contract = load_provider_outcome_contract("configs/qm_b_provider_outcome_availability_v1.json")
    assert contract["provider_coverage"]["strict_ledger_promotion_enabled"] is False
    assert contract["outcome_availability"]["strict_ledger_promotion_enabled"] is False
    assert contract["outcome_availability"]["neutral_imputation_allowed"] is False
    assert contract["pit_rules"]["historical_retrojection_permitted"] is False
    assert contract["pit_rules"]["missing_outcome_may_not_enter_denominator"] is True
    assert contract["pit_rules"]["unknown_outcome_may_not_enter_denominator"] is True
