from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.interaction_eligibility_8h import (
    BOUND_STATE,
    NO_PROMOTED_STATE,
    WAITING_STATE,
    ExternalEvidence8HEligibilityError,
    bind_interaction_eligibility,
    validate_binding_contract,
)
from scanner.research.external_evidence.promotion_review_8g import (
    build_promotion_review_packet,
    finalize_promotion_review,
    record_manual_review,
    source_governance_review,
)
from scanner.research.external_evidence.prospective_8g import PROSPECTIVE_COMPLETION_SCHEMA
from scanner.research.external_evidence.research_8g import ACTIVE_FACTORS, HORIZONS


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_eligibility_binding_v1.json").read_text())
CHALLENGER = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
PROMOTION_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_promotion_review_v1.json").read_text())
SOURCE_REGISTRY = json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text())
COLLECTION = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8f_freeze_2026-09-28" / "external_evidence_8f_collection_latest.json").read_text())
GATE_8F = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8f_freeze_2026-09-28" / "external_evidence_8f_completion_gate.json").read_text())


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _rehash(value: dict, field: str) -> dict:
    row = deepcopy(value)
    row.pop(field, None)
    row[field] = _digest(row)
    return row


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    payload = f"blob {len(data)}\0".encode("ascii") + data
    return hashlib.sha1(payload).hexdigest()


def _family() -> list[str]:
    return [f"{factor}_x_{horizon}t" for factor in ACTIVE_FACTORS for horizon in HORIZONS]


def _prospective(*, confirmed: tuple[str, ...] = ("rates_policy_x_5t",)) -> dict:
    states = {hypothesis_id: "NOT_ELIGIBLE_FROM_HOLDOUT" for hypothesis_id in _family()}
    for hypothesis_id in confirmed:
        states[hypothesis_id] = "PROSPECTIVE_CONFIRMED"
    row = {
        "schema_version": PROSPECTIVE_COMPLETION_SCHEMA,
        "phase": "8G-G",
        "state": "PROSPECTIVE_COMPLETE",
        "empirically_complete": True,
        "eligible_hypotheses": list(confirmed),
        "pending_hypotheses": [],
        "hypothesis_states": states,
        "holm_family_size": len(states),
        "automatic_promotion_authorized": False,
        "next_subphase": "8G-H_PROMOTION_REVIEW",
    }
    row["completion_sha256"] = _digest(row)
    return row


def _all_pass() -> dict[str, dict]:
    return {
        criterion["id"]: {
            "status": "PASS",
            "evidence_sha256": _digest({"criterion": criterion["id"]}),
        }
        for criterion in PROMOTION_PROTOCOL["promotion_criteria"]
    }


def _registry_with_frozen_sources() -> dict:
    registry = deepcopy(SOURCE_REGISTRY)
    registry["sources"] = list(registry["sources"]) + [
        {
            "source_id": "FED_H15",
            "family": "macro_exposure",
            "provider": "Federal Reserve Board H.15",
            "pit_status": "SAFE",
            "license_status": "USABLE",
            "documentation_url": "https://www.federalreserve.gov/releases/h15/",
            "promotion_eligible": False,
        },
        {
            "source_id": "ECB_EXR",
            "family": "macro_exposure",
            "provider": "ECB Data Portal EXR",
            "pit_status": "SAFE",
            "license_status": "USABLE",
            "documentation_url": "https://data.ecb.europa.eu/",
            "promotion_eligible": False,
        },
    ]
    return registry


def _approved_chain(hypothesis_id: str = "rates_policy_x_5t") -> tuple[dict, dict, dict, dict, dict]:
    prospective = _prospective(confirmed=(hypothesis_id,))
    factor_id = hypothesis_id.rsplit("_x_", 1)[0]
    registry = _registry_with_frozen_sources()
    governance = source_governance_review(
        factor_id=factor_id,
        source_registry=registry,
        collection_receipt=COLLECTION,
        completion_gate_8f=GATE_8F,
        protocol=PROMOTION_PROTOCOL,
    )
    packet = build_promotion_review_packet(
        hypothesis_id=hypothesis_id,
        prospective_completion=prospective,
        criteria_evidence=_all_pass(),
        source_governance=governance,
        protocol=PROMOTION_PROTOCOL,
    )
    decision = record_manual_review(
        packet=packet,
        decision="APPROVE_FOR_8H_RESEARCH_ONLY",
        reviewer_identity="synthetic-test-reviewer",
        reviewed_at="2027-06-01T12:00:00Z",
        rationale="Synthetic contract test: all frozen upstream gates passed.",
        protocol=PROMOTION_PROTOCOL,
    )
    completion = finalize_promotion_review(
        prospective_completion=prospective,
        decisions={hypothesis_id: decision},
        protocol=PROMOTION_PROTOCOL,
    )
    return prospective, packet, decision, completion, registry


def test_8h_b_contract_is_bound_to_exact_8h_a_and_8g_parent_blobs() -> None:
    validate_binding_contract(CONTRACT, CHALLENGER)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "95afed1c46ca313d97af8b7e4f9dee8032d0c886"
    bound = {
        "8h_a_contract": "8h_a_contract_git_blob_sha",
        "8g_challenger_specs": "8g_challenger_specs_git_blob_sha",
        "8g_promotion_review": "8g_promotion_review_git_blob_sha",
        "8g_promotion_review_code": "8g_promotion_review_code_git_blob_sha",
        "8g_prospective_code": "8g_prospective_code_git_blob_sha",
        "source_registry": "source_registry_git_blob_sha",
    }
    for path_key, sha_key in bound.items():
        path = ROOT / parent[path_key]
        assert path.exists(), parent[path_key]
        assert _git_blob_sha(path) == parent[sha_key]


def test_current_repository_state_waits_instead_of_claiming_no_promotion_evidence() -> None:
    result = bind_interaction_eligibility(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
    )
    assert CONTRACT["current_repository_state"]["8g_h_completion_artifact_present"] is False
    assert result["state"] == WAITING_STATE
    assert result["wait_reason"] == "8G_H_COMPLETION_ARTIFACT_MISSING"
    assert result["eligible_factor_horizon_ids"] == []
    assert result["interaction_spec_freeze_authorized"] is False
    assert result["interaction_outcome_access_authorized"] is False
    assert result["empirical_interaction_research_enabled"] is False
    assert result["concrete_interaction_specs"] == []


def test_orphan_decision_without_8g_h_completion_fails_closed() -> None:
    with pytest.raises(ExternalEvidence8HEligibilityError, match="orphan_8g_h_review_artifacts_without_completion"):
        bind_interaction_eligibility(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            source_registry=SOURCE_REGISTRY,
            decisions={"rates_policy_x_5t": {"state": "APPROVED_FOR_8H_RESEARCH_ONLY"}},
        )


def test_incomplete_promotion_review_never_authorizes_even_if_partial_approval_is_listed() -> None:
    completion = {
        "schema_version": "external_evidence_8g_promotion_review_completion_v1",
        "phase": "8G-H",
        "state": "PROMOTION_REVIEW_INCOMPLETE",
        "empirically_complete": False,
        "eligible_hypotheses": ["rates_policy_x_5t", "fx_x_5t"],
        "pending_hypotheses": ["fx_x_5t"],
        "approved_for_8h_research": ["rates_policy_x_5t"],
        "dispositions": {
            "rates_policy_x_5t": "APPROVED_FOR_8H_RESEARCH_ONLY",
            "fx_x_5t": "PENDING_MANUAL_REVIEW",
        },
        "automatic_promotion_authorized": False,
        "production_external_evidence_enabled": False,
        "next_phase": "8H_CROSS_FACTOR_INTERACTION",
    }
    completion["completion_sha256"] = _digest(completion)
    result = bind_interaction_eligibility(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
        promotion_completion=completion,
    )
    assert result["state"] == WAITING_STATE
    assert result["eligible_factor_horizon_ids"] == []
    assert result["interaction_spec_freeze_authorized"] is False
    assert result["interaction_outcome_access_authorized"] is False


def test_terminal_no_candidate_completion_maps_to_no_promoted_factors() -> None:
    prospective = _prospective(confirmed=())
    completion = finalize_promotion_review(
        prospective_completion=prospective,
        decisions={},
        protocol=PROMOTION_PROTOCOL,
    )
    result = bind_interaction_eligibility(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
        promotion_completion=completion,
        prospective_completion=prospective,
    )
    assert result["state"] == NO_PROMOTED_STATE
    assert result["upstream_promotion_review_empirically_complete"] is True
    assert result["eligible_factor_horizon_ids"] == []
    assert result["interaction_spec_freeze_authorized"] is False
    assert result["empirical_interaction_research_enabled"] is False
    assert result["hard_rule"] == "NO_PROMOTED_FACTORS -> NO_EMPIRICAL_INTERACTION_RESEARCH"


def test_valid_manual_approval_binds_exact_factor_horizon_but_keeps_outcomes_closed() -> None:
    prospective, packet, decision, completion, registry = _approved_chain()
    result = bind_interaction_eligibility(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=registry,
        promotion_completion=completion,
        prospective_completion=prospective,
        review_packets={"rates_policy_x_5t": packet},
        decisions={"rates_policy_x_5t": decision},
    )
    assert result["state"] == BOUND_STATE
    assert result["eligible_factor_horizon_ids"] == ["rates_policy_x_5t"]
    assert result["interaction_spec_freeze_authorized"] is True
    assert result["interaction_outcome_access_authorized"] is False
    assert result["empirical_interaction_research_enabled"] is False
    assert result["concrete_interaction_specs"] == []
    component = result["eligible_components"][0]
    assert component["factor_id"] == "rates_policy"
    assert component["horizon_sessions"] == 5
    assert component["eligibility_state"] == "ELIGIBLE_FOR_8H_INTERACTION_SPEC_FREEZE_ONLY"
    assert component["factor_spec_source_id"] == "FED_H15"
    assert component["factor_spec_feature_fields"] == ["rates_policy_level_pct", "rates_policy_delta_pp"]
    assert component["interaction_outcome_access_authorized"] is False
    assert component["production_authorized"] is False
    assert component["phase7_mutation_authorized"] is False
    assert component["phase8i_integration_authorized"] is False
    assert component["orders_or_trades_authorized"] is False


def test_decision_digest_tampering_is_rejected_even_when_state_text_still_says_approved() -> None:
    prospective, packet, decision, completion, registry = _approved_chain()
    tampered = deepcopy(decision)
    tampered["reviewer_identity"] = "different-reviewer-without-rehash"
    with pytest.raises(ExternalEvidence8HEligibilityError, match="promotion_decision_digest_mismatch"):
        bind_interaction_eligibility(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            source_registry=registry,
            promotion_completion=completion,
            prospective_completion=prospective,
            review_packets={"rates_policy_x_5t": packet},
            decisions={"rates_policy_x_5t": tampered},
        )


def test_rehashed_forged_completion_approved_list_is_rederived_and_rejected() -> None:
    prospective, packet, decision, completion, registry = _approved_chain()
    forged = deepcopy(completion)
    forged["approved_for_8h_research"] = []
    forged = _rehash(forged, "completion_sha256")
    with pytest.raises(ExternalEvidence8HEligibilityError, match="approved_list_does_not_match_dispositions"):
        bind_interaction_eligibility(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            source_registry=registry,
            promotion_completion=forged,
            prospective_completion=prospective,
            review_packets={"rates_policy_x_5t": packet},
            decisions={"rates_policy_x_5t": decision},
        )


def test_valid_upstream_packet_cannot_bypass_bound_source_registry_identity() -> None:
    prospective, packet, decision, completion, _ = _approved_chain()
    with pytest.raises(ExternalEvidence8HEligibilityError, match="approved_source_not_resolved_in_bound_registry:FED_H15"):
        bind_interaction_eligibility(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            source_registry=SOURCE_REGISTRY,
            promotion_completion=completion,
            prospective_completion=prospective,
            review_packets={"rates_policy_x_5t": packet},
            decisions={"rates_policy_x_5t": decision},
        )


def test_cross_horizon_packet_mutation_is_rejected_even_with_recomputed_hash_chain() -> None:
    prospective, packet, decision, completion, registry = _approved_chain()
    mutated_packet = deepcopy(packet)
    mutated_packet["horizon_sessions"] = 20
    mutated_packet = _rehash(mutated_packet, "packet_sha256")
    mutated_decision = deepcopy(decision)
    mutated_decision["packet_sha256"] = mutated_packet["packet_sha256"]
    mutated_decision = _rehash(mutated_decision, "decision_sha256")
    with pytest.raises(ExternalEvidence8HEligibilityError, match="promotion_packet_factor_horizon_mismatch"):
        bind_interaction_eligibility(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            source_registry=registry,
            promotion_completion=completion,
            prospective_completion=prospective,
            review_packets={"rates_policy_x_5t": mutated_packet},
            decisions={"rates_policy_x_5t": mutated_decision},
        )


def test_8h_b_never_selects_interactions_or_enables_outcome_access() -> None:
    boundary = CONTRACT["outcome_blind_boundary"]
    auth = CONTRACT["authorization_boundary"]
    assert boundary["interaction_outcomes_may_be_read_in_8h_b"] is False
    assert boundary["interaction_candidates_may_be_selected_in_8h_b"] is False
    assert boundary["economic_theory_ranking_allowed"] is False
    assert boundary["automatic_cartesian_product_generation_allowed"] is False
    assert boundary["interaction_spec_freeze_belongs_to"] == "8H-C"
    assert auth["maximum_8h_b_authorization"] == "ELIGIBLE_FOR_8H_INTERACTION_SPEC_FREEZE_ONLY"
    assert auth["eligible_component_allows_interaction_outcome_access"] is False
    assert auth["concrete_interaction_specs_in_8h_b"] == []
    assert CONTRACT["8h_b_completion_gate"]["next_step_after_pass"] == "8H-C_OUTCOME_BLIND_INTERACTION_SPEC_FREEZE"
