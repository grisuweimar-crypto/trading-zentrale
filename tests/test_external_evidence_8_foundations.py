import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "external_evidence_8_contract_v1.json").read_text(encoding="utf-8"))


def load_conflict_matrix():
    return json.loads((ROOT / "configs" / "external_conflict_matrix_v1.json").read_text(encoding="utf-8"))


def test_external_evidence_stays_separate_family():
    c = load_contract()
    s = c["external_evidence_semantics"]
    assert s["must_remain_separate_family"] is True
    assert s["may_rewrite_selection"] is False
    assert s["may_rewrite_timing"] is False
    assert s["may_rewrite_probability"] is False
    assert s["may_rewrite_risk"] is False
    assert s["may_rewrite_confidence"] is False
    assert s["may_rewrite_elliott"] is False


def test_unknown_and_quality_failures_never_become_neutral():
    c = load_contract()
    s = c["status_model"]
    assert s["unknown_may_be_treated_as_neutral"] is False
    assert s["low_coverage_may_be_treated_as_neutral"] is False
    assert s["stale_may_be_treated_as_neutral"] is False
    assert s["conflicting_sources_may_be_treated_as_neutral"] is False


def test_full_pit_provenance_is_required():
    c = load_contract()
    required = set(c["pit_contract"]["common_fields"])
    assert {"event_time", "published_at", "ingested_at", "valid_from", "revision_id", "source", "license", "restatement_policy", "vintage"}.issubset(required)
    assert c["pit_contract"]["current_value_retrojection_forbidden"] is True


def test_revisions_require_historical_consensus_snapshots():
    c = load_contract()
    rules = set(c["pit_contract"]["family_rules"]["revisions"])
    assert "historical_consensus_snapshot_required" in rules
    assert "current_consensus_retrojection_forbidden" in rules


def test_short_interest_publication_guard_exists():
    c = load_contract()
    rules = set(c["pit_contract"]["family_rules"]["positioning"])
    assert "settlement_date_separate_from_publication_date" in rules
    assert "usable_no_earlier_than_publication" in rules


def test_all_promotion_gates_are_required():
    c = load_contract()
    gates = set(c["promotion_gates"]["required"])
    assert {"pit", "coverage", "incremental", "stability", "robustness", "cost", "multiple_testing"}.issubset(gates)
    assert c["promotion_gates"]["all_required_for_integration"] is True


def test_phase7_baseline_is_bound_to_frozen_7i_contract():
    c = load_contract()
    b = c["baseline_policy"]
    assert b["baseline"] == "frozen_phase7_core"
    assert b["phase7_validation_contract"] == "configs/decision_validation_promotion_v1.json"
    assert b["phase7_frozen_on"] == "2026-09-25"
    assert b["phase7_prospective_unspent_from"] == "2026-09-26"


def test_single_family_research_precedes_joint_testing():
    c = load_contract()
    b = c["baseline_policy"]
    assert b["single_family_first"] is True
    assert b["multi_family_joint_test_before_single_family_promotion_allowed"] is False
    assert b["baseline"] == "frozen_phase7_core"


def test_research_governance_guards_survivorship_and_human_evidence_consumption():
    c = load_contract()
    g = c["research_governance"]
    assert g["as_of_universe_ledger_required"] is True
    assert g["historical_membership_and_exclusion_reasons_required"] is True
    assert g["survivorship_by_current_membership_forbidden"] is True
    assert g["hypothesis_family_registry_required"] is True
    assert g["confirmatory_family_freeze_required_before_outcome_inspection"] is True
    assert g["human_outcome_inspection_log_required"] is True
    assert g["outcome_driven_design_change_marks_evidence_spent_for_design"] is True
    assert g["preventive_qa_change_without_future_outcome_inspection_may_preserve_unspent_status"] is True


def test_external_relation_is_descriptive_not_trade_policy():
    c = load_contract()
    assert c["relation_to_phase7"]["descriptive_not_policy"] is True
    assert c["relation_to_phase7"]["external_direction_alone_is_trade_decision"] is False


def test_conflict_matrix_is_complete_and_preserves_unknown_and_conflict():
    m = load_conflict_matrix()
    rows = {(x["core"], x["external"]): x["relation"] for x in m["relations"]}
    expected_pairs = {(core, external) for core in m["core_states"] for external in m["external_states"]}
    assert set(rows) == expected_pairs
    assert len(m["relations"]) == len(expected_pairs)
    assert rows[("POSITIVE", "NEGATIVE")] == "CONFLICTING"
    assert rows[("POSITIVE", "UNKNOWN")] == "UNKNOWN"
    assert rows[("NEGATIVE", "POSITIVE")] == "CONFLICTING"
    assert rows[("CONFLICTED", "UNKNOWN")] == "UNKNOWN"
    assert rows[("INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EXTERNAL")] == "INSUFFICIENT_EXTERNAL"
    assert m["policy_boundary"]["relation_may_change_phase7_stance_directly"] is False


def test_decision_reliability_extension_cannot_overwrite_phase7():
    c = load_contract()
    r = c["decision_reliability_extension"]
    assert r["phase7_reliability_must_not_be_overwritten"] is True
    assert r["integration_only_after_promotion"] is True


def test_no_orders_or_direct_portfolio_action_replacement():
    c = load_contract()
    s = c["external_evidence_semantics"]
    assert s["may_directly_replace_portfolio_action"] is False
    assert s["may_generate_order"] is False


def test_forbidden_shortcuts_cover_survivorship_and_spent_evidence_reuse():
    c = load_contract()
    shortcuts = set(c["forbidden_shortcuts"])
    assert "use_current_universe_membership_as_historical_membership" in shortcuts
    assert "reuse_outcome_inspected_evidence_as_unspent_design_confirmation" in shortcuts
    assert "ignore_license_restrictions" in shortcuts
