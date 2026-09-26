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


def test_finra_short_interest_separates_observation_publication_and_ingestion() -> None:
    family = _contract()["families"]["SHORT_INTEREST"]
    assert family["event_time_policy"] == "settlementDate"
    assert family["published_at_policy"] == "FINRA_PUBLICATION_DATE_AT_16_40_AMERICA_NEW_YORK"
    assert family["valid_from_policy"].startswith("ACTUAL_INGESTED_AT")
    assert family["vintage_policy"]["historical_backfill"] == (
        "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
    )
    assert family["vintage_policy"]["prospective_valid_from"] == "INGESTED_AT"
    assert family["disabled_features"]["short_interest_percent_float"].startswith("DISABLED_")
    assert family["disabled_features"]["daily_short_interest"].startswith("DISABLED_")
    assert family["direction"] == "UNASSIGNED"


def test_finra_a1_a2_contract_is_fail_closed() -> None:
    family = _contract()["families"]["SHORT_INTEREST"]
    acquisition = family["acquisition_contract"]
    coverage = family["coverage_contract"]
    assert family["status"] == "SOURCE_ACCEPTED_A1_A2_IMPLEMENTED_WITH_VINTAGE_LIMITATION"
    assert acquisition["prospective_only_for_strict_pit"] is True
    assert acquisition["one_settlement_date_per_snapshot"] is True
    assert acquisition["request_method"] == "FILTERED_POST_WITH_PAGINATION"
    assert acquisition["publication_time_retrojection_enabled"] is False
    assert acquisition["raw_page_sha256_required"] is True
    assert acquisition["canonical_raw_snapshot_sha256_required"] is True
    assert acquisition["ci_live_network_access"] is False
    assert coverage["match_method"] == "EXACT_SYMBOL_ONLY"
    assert coverage["ticker_suffix_stripping_enabled"] is False
    assert coverage["adr_substitution_enabled"] is False
    assert coverage["fuzzy_name_matching_enabled"] is False
    assert coverage["ambiguous_market_class_auto_selection_enabled"] is False
    assert coverage["missing_evidence_may_default_to_neutral"] is False


def test_insider_b1_b2_is_exact_accession_high_precision_challenger() -> None:
    family = _contract()["families"]["INSIDER_ACTIVITY"]
    contract = family["b1_b2_contract"]
    assert family["status"] == "SOURCE_ACCEPTED_B1_B2_IMPLEMENTED_HIGH_PRECISION_CHALLENGER"
    assert family["forms"] == ["4", "4/A"]
    assert family["event_time_policy"] == "TRANS_DATE"
    assert family["valid_from_policy"] == "EXACT_ACCESSION_JOIN_TO_SEC_SUBMISSIONS_ACCEPTANCE_METADATA"
    assert set(family["initial_transaction_scope"]) == {"P", "S"}
    assert contract["official_quarterly_bulk_only"] is True
    assert contract["verified_sec_bundle_required"] is True
    assert contract["exact_accession_join_required"] is True
    assert contract["exact_issuer_cik_join_required"] is True
    assert contract["current_ticker_used_as_historical_identity"] is False
    assert contract["transaction_form_type_4_required_for_discretionary_candidate"] is True
    assert contract["p_requires_acquired_code_a"] is True
    assert contract["s_requires_disposed_code_d"] is True
    assert contract["equity_swaps_excluded_from_discretionary_candidate"] is True
    assert contract["aff10b5one_true_excluded_from_discretionary_candidate"] is True
    assert contract["aff10b5one_unknown_not_assumed_discretionary"] is True
    assert contract["ci_live_network_access"] is False
    assert family["direction"] == "UNASSIGNED"


def test_insider_excluded_codes_are_not_silently_discretionary() -> None:
    excluded = set(
        _contract()["families"]["INSIDER_ACTIVITY"][
            "excluded_from_discretionary_buy_sell_scope"
        ]
    )
    required = {
        "A_GRANT_AWARD_OR_COMPANY_ACQUISITION",
        "D_SALE_OR_TRANSFER_BACK_TO_COMPANY",
        "F_EXERCISE_PRICE_OR_TAX_WITHHOLDING",
        "G_GIFT",
        "M_DERIVATIVE_EXERCISE_OR_CONVERSION",
        "J_OTHER_UNLESS_SEPARATELY_VALIDATED",
        "K_SWAP_OR_HEDGE",
        "EQUITY_SWAP_INVOLVED",
        "AFF10B5ONE_TRUE",
        "TRANS_FORM_TYPE_NOT_4",
        "P_S_ACQUIRED_DISPOSED_CONFLICT",
    }
    assert required <= excluded


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
    assert guards[
        "publication_time_is_not_automatically_valid_from_for_later_retrievals"
    ] is True
    assert guards["transaction_date_is_not_filing_availability_time"] is True
    assert guards["revised_history_may_silently_replace_original_vintage"] is False
    assert guards["unknown_or_uncovered_status_must_be_explicit"] is True
