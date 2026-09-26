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


def test_finra_short_interest_separates_event_publication_and_actual_observation_time() -> None:
    family = _contract()["families"]["SHORT_INTEREST"]
    assert family["event_time_policy"] == "settlementDate"
    assert family["published_at_policy"] == "FINRA_PUBLICATION_DATE_AT_16_40_AMERICA_NEW_YORK"
    assert family["valid_from_policy"] == (
        "ACTUAL_INGESTED_AT_UNLESS_ORIGINAL_PUBLICATION_VINTAGE_IS_INDEPENDENTLY_PROVEN"
    )
    assert family["vintage_policy"]["historical_backfill"] == (
        "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
    )
    assert family["vintage_policy"]["prospective_valid_from"] == "INGESTED_AT"
    assert family["vintage_policy"]["strict_pit_eligibility"] == (
        "PROSPECTIVE_SNAPSHOT_FROM_ACTUAL_INGESTION_FORWARD_OR_OTHERWISE_PROVEN_ORIGINAL_VINTAGE_ONLY"
    )
    assert family["disabled_features"]["short_interest_percent_float"].startswith("DISABLED_")
    assert family["disabled_features"]["daily_short_interest"].startswith("DISABLED_")
    assert family["direction"] == "UNASSIGNED"


def test_finra_a1_acquisition_contract_is_fail_closed() -> None:
    family = _contract()["families"]["SHORT_INTEREST"]
    acquisition = family["acquisition_contract"]
    assert family["status"] == "SOURCE_ACCEPTED_A1_A2_IMPLEMENTED_WITH_VINTAGE_LIMITATION"
    assert acquisition["prospective_only_for_strict_pit"] is True
    assert acquisition["one_settlement_date_per_snapshot"] is True
    assert acquisition["request_method"] == "FILTERED_POST_WITH_PAGINATION"
    assert acquisition["synchronous_record_limit_per_request"] == 5000
    assert acquisition["record_total_header_must_reconcile_when_present"] is True
    assert acquisition["raw_page_sha256_required"] is True
    assert acquisition["canonical_raw_snapshot_sha256_required"] is True
    assert acquisition["publication_time_retrojection_enabled"] is False
    assert acquisition["ci_live_network_access"] is False


def test_finra_a2_coverage_contract_forbids_identity_guessing() -> None:
    coverage = _contract()["families"]["SHORT_INTEREST"]["coverage_contract"]
    assert coverage["single_finra_settlement_date_required"] is True
    assert coverage["match_method"] == "EXACT_SYMBOL_ONLY"
    assert coverage["ticker_suffix_stripping_enabled"] is False
    assert coverage["adr_substitution_enabled"] is False
    assert coverage["fuzzy_name_matching_enabled"] is False
    assert coverage["ambiguous_market_class_auto_selection_enabled"] is False
    assert coverage["missing_evidence_status"] == "UNKNOWN_NOT_IN_FINRA_SNAPSHOT"
    assert coverage["ambiguous_evidence_status"] == "AMBIGUOUS_MULTIPLE_FINRA_ROWS"
    assert coverage["missing_evidence_may_default_to_neutral"] is False


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
    assert guards["publication_time_is_not_automatically_valid_from_for_later_retrievals"] is True
    assert guards["transaction_date_is_not_filing_availability_time"] is True
    assert guards["revised_history_may_silently_replace_original_vintage"] is False
    assert guards["unknown_or_uncovered_status_must_be_explicit"] is True
