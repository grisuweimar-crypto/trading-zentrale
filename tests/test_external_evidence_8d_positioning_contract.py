from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8d_positioning_crowding_v1.json"


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_8d_starts_outcome_blind_and_without_decision_integration() -> None:
    contract = _contract()
    principles = contract["principles"]
    gate = contract["research_gate"]
    assert principles["market_outcomes_may_be_read"] is False
    assert principles["market_direction_may_be_assigned"] is False
    assert principles["absolute_high_or_low_positioning_implies_direction"] is False
    assert gate["outcome_research_enabled"] is False
    assert gate["production_external_evidence_enabled"] is False
    assert gate["phase7_integration_enabled"] is False
    assert gate["interaction_research_enabled"] is False


def test_finra_short_interest_separates_observation_from_publication_time() -> None:
    family = _contract()["families"]["SHORT_INTEREST"]
    assert family["event_time_policy"] == "settlementDate"
    assert family["valid_from_policy"] == "FINRA_PUBLICATION_TIME_NOT_SETTLEMENT_DATE"
    assert family["vintage_policy"]["historical_backfill"] == (
        "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
    )
    assert family["vintage_policy"]["strict_pit_eligibility"] == (
        "PROSPECTIVE_SNAPSHOTS_OR_OTHERWISE_PROVEN_ORIGINAL_VINTAGE_ONLY"
    )
    assert family["disabled_features"]["short_interest_percent_float"].startswith("DISABLED_")
    assert family["disabled_features"]["daily_short_interest"].startswith("DISABLED_")
    assert family["direction"] == "UNASSIGNED"


def test_insider_first_slice_is_only_high_precision_p_and_s() -> None:
    family = _contract()["families"]["INSIDER_ACTIVITY"]
    assert family["forms"] == ["4", "4/A"]
    assert family["event_time_policy"] == "transaction_date"
    assert family["valid_from_policy"] == "SEC_ACCEPTANCE_TIMESTAMP"
    assert set(family["initial_transaction_scope"]) == {"P", "S"}
    assert family["direction"] == "UNASSIGNED"
    excluded = " ".join(family["excluded_from_discretionary_buy_sell_scope"])
    for code in ("A_", "D_", "F_", "G_", "M_", "J_", "K_", "V_"):
        assert code in excluded


def test_borrow_rate_remains_deferred_and_no_proxy_substitution_is_allowed() -> None:
    family = _contract()["families"]["BORROW_RATE"]
    assert family["status"] == "SOURCE_GAP_DEFERRED"
    assert family["direction"] == "UNASSIGNED"
    substitutions = set(family["prohibited_substitutions"])
    assert "FAILS_TO_DELIVER_AS_BORROW_RATE" in substitutions
    assert "SHORT_SALE_VOLUME_AS_BORROW_RATE" in substitutions
    assert "CURRENT_BORROW_FEE_RETROJECTED_HISTORICALLY" in substitutions


def test_global_pit_identity_guards_are_fail_closed() -> None:
    guards = _contract()["identity_and_pit_guards"]
    assert guards["asof_universe_join_required"] is True
    assert guards["current_ticker_may_be_used_as_historical_stable_id"] is False
    assert guards["symbol_changes_must_be_explicit"] is True
    assert guards["settlement_date_is_not_publication_time"] is True
    assert guards["transaction_date_is_not_filing_availability_time"] is True
    assert guards["revised_history_may_silently_replace_original_vintage"] is False
    assert guards["unknown_or_uncovered_status_must_be_explicit"] is True
