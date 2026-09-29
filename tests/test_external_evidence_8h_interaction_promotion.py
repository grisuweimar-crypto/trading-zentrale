from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.interaction_holdout_8h import (
    HOLDOUT_COMPLETION_SCHEMA,
    HOLDOUT_FAMILY_COMPLETE,
    HOLDOUT_LEDGER_SCHEMA,
    HOLDOUT_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_promotion_8h import (
    APPROVED_STATE,
    NO_PROMOTION_CANDIDATES,
    PROMOTION_REVIEW_COMPLETE,
    PROMOTION_REVIEW_INCOMPLETE,
    WAITING_FOR_8H_G,
    ExternalEvidence8HPromotionError,
    build_promotion_review_packet,
    finalize_promotion_review,
    promotion_review_status,
    record_manual_review,
    validate_promotion_contract,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_promotion_review_v1.json").read_text())


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode("ascii") + data).hexdigest()


def _spec(spec_id: str = "ix_h", horizon: int = 5) -> dict:
    row = {
        "interaction_spec_id": spec_id,
        "interaction_class": "CORE_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {
                "kind": "CORE_NUMERIC",
                "component_id": "score",
                "resolved_field": "score",
                "horizon_sessions": horizon,
            },
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"rates_policy_x_{horizon}t",
                "factor_id": "rates_policy",
                "horizon_sessions": horizon,
                "feature_field": "rates_policy_level_pct",
                "component_identity_sha256": _digest({"factor": "rates_policy", "horizon": horizon}),
            },
        ],
        "operator": "ELEMENTWISE_PRODUCT",
        "eligibility_binding_sha256": _digest({"eligibility": spec_id}),
        "candidate_manifest_sha256": _digest({"manifest": spec_id}),
        "main_effects_retained": True,
        "exactly_one_new_interaction_term": True,
        "outcome_values_read_during_spec_freeze": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["interaction_spec_sha256"] = _digest(row)
    return row


def _freeze(specs: list[dict]) -> dict:
    ordered = sorted(specs, key=lambda x: x["interaction_spec_id"])
    family = [
        {
            "hypothesis_id": f"{spec['interaction_spec_id']}__peer_excess_{spec['horizon_sessions']}t",
            "interaction_spec_id": spec["interaction_spec_id"],
            "horizon_sessions": spec["horizon_sessions"],
            "primary_outcome": "peer_excess",
        }
        for spec in ordered
    ]
    row = {
        "schema_version": INTERACTION_FREEZE_RESULT_SCHEMA,
        "phase": "8H-C",
        "state": SPECS_FROZEN,
        "eligibility_binding_sha256": _digest({"eligibility": "bound"}),
        "candidate_manifest_sha256": _digest({"manifest": "frozen"}),
        "frozen_at": "2027-06-30T10:30:00+00:00",
        "frozen_interaction_specs": ordered,
        "confirmatory_family": family,
        "interaction_dataset_model_construction_authorized": True,
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "automatic_promotion_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-D_DATASET_MODEL_CONSTRUCTION",
        "wait_reason": None,
    }
    row["freeze_sha256"] = _digest(row)
    return row


def _holdout_chain(specs: list[dict], confirmed_ids: set[str]) -> tuple[dict, dict, dict]:
    freeze = _freeze(specs)
    family = freeze["confirmatory_family"]
    results = []
    states = {}
    confirmed = []
    for member in family:
        hypothesis = member["hypothesis_id"]
        state = "HOLDOUT_CONFIRMED" if hypothesis in confirmed_ids else "HOLDOUT_NOT_CONFIRMED"
        states[hypothesis] = state
        if state == "HOLDOUT_CONFIRMED":
            confirmed.append(hypothesis)
        row = {
            "interaction_spec_id": member["interaction_spec_id"],
            "hypothesis_id": hypothesis,
            "horizon_sessions": member["horizon_sessions"],
            "split": "HOLDOUT",
            "status": "HOLDOUT_EVALUATED",
            "paired_n": 40,
            "snapshot_n": 40,
            "temporal_support_regions": 2,
            "minimum_evidence_met": True,
            "primary_effect": 0.25 if state == "HOLDOUT_CONFIRMED" else -0.1,
            "primary_ci_95": [0.05, 0.45] if state == "HOLDOUT_CONFIRMED" else [-0.3, 0.1],
            "primary_p_value_two_sided": 0.01 if state == "HOLDOUT_CONFIRMED" else 0.5,
            "holm_adjusted_p": 0.02 if state == "HOLDOUT_CONFIRMED" else 0.5,
            "holm_positive": state == "HOLDOUT_CONFIRMED",
            "terminal_state": state,
            "model_pair_sha256": _digest({"model": member["interaction_spec_id"]}),
            "model_refit": False,
            "family_membership_changed": False,
        }
        row["evaluation_sha256"] = _digest(row)
        results.append(row)
    completion = {
        "schema_version": HOLDOUT_COMPLETION_SCHEMA,
        "phase": "8H-G",
        "empirically_complete": True,
        "8h_f_validation_sha256": _digest({"validation": "frozen"}),
        "8h_f_validation_family_receipt_sha256": _digest({"receipt": "frozen"}),
        "holdout_authorization_sha256": _digest({"authorization": "one-shot"}),
        "ledger_before_sha256": _digest({"ledger": "armed"}),
        "confirmatory_family": family,
        "hypothesis_states": states,
        "confirmed_hypotheses": confirmed,
        "holdout_result_sha256s": [row["evaluation_sha256"] for row in results],
        "holm_method": "Holm",
        "family_wise_alpha": 0.05,
        "model_refit": False,
        "family_shrunk_or_expanded": False,
        "zero_confirmed_hypotheses_is_valid_terminal_result": True,
        "automatic_promotion_authorized": False,
        "maximum_future_approval_scope": "APPROVED_FOR_8I_RESEARCH_ONLY",
        "8h_h_promotion_review_eligible": True,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-H_INTERACTION_PROMOTION_REVIEW",
    }
    completion["completion_sha256"] = _digest(completion)
    holdout = {
        "schema_version": HOLDOUT_RESULT_SCHEMA,
        "phase": "8H-G",
        "state": HOLDOUT_FAMILY_COMPLETE,
        "8h_c_freeze_sha256": freeze["freeze_sha256"],
        "8h_d_construction_sha256": _digest({"design": "frozen"}),
        "8h_e_discovery_sha256": _digest({"discovery": "frozen"}),
        "8h_f_validation_sha256": completion["8h_f_validation_sha256"],
        "8h_f_validation_family_receipt_sha256": completion["8h_f_validation_family_receipt_sha256"],
        "split_manifest_sha256": _digest({"split": "frozen"}),
        "holdout_authorization_sha256": completion["holdout_authorization_sha256"],
        "confirmatory_family": family,
        "holdout_results": results,
        "hypothesis_states": states,
        "confirmed_hypotheses": confirmed,
        "holdout_completion": completion,
        "full_family_holdout_complete": True,
        "holm_applied_to_full_family": True,
        "8h_h_promotion_review_eligible": True,
        "holdout_outcomes_opened": True,
        "holdout_open_count": 1,
        "model_refit": False,
        "family_shrunk_or_expanded": False,
        "outcome_access_log": {
            "opened_by": "synthetic-test",
            "opened_at": "2027-10-24T11:30:00+00:00",
            "human_outcome_inspection_occurred": False,
            "human_inspector_identity": None,
            "inspection_note": None,
        },
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-H_INTERACTION_PROMOTION_REVIEW",
    }
    holdout["holdout_sha256"] = _digest(holdout)
    ledger = {
        "schema_version": HOLDOUT_LEDGER_SCHEMA,
        "phase": "8H-G",
        "append_only": True,
        "authorization_sha256": completion["holdout_authorization_sha256"],
        "state": "CONSUMED",
        "holdout_open_count": 1,
        "terminal_result_sha256": holdout["holdout_sha256"],
        "completion_sha256": completion["completion_sha256"],
    }
    ledger["ledger_sha256"] = _digest(ledger)
    return freeze, holdout, ledger


def _criteria(spec: dict, holdout: dict, ledger: dict, hypothesis: str) -> dict[str, dict]:
    evaluation = next(row for row in holdout["holdout_results"] if row["hypothesis_id"] == hypothesis)
    rows = {cid: {"status": "PASS", "note": cid} for cid in [x["id"] for x in CONTRACT["promotion_criteria"]]}
    rows["UPSTREAM_ELIGIBILITY_INTEGRITY"]["eligibility_binding_sha256"] = spec["eligibility_binding_sha256"]
    rows["PIT_AND_FRESH_EVIDENCE_CLEAN"].update({"pit_clean": True, "fresh_evidence_after_spec_freeze": True})
    rows["INCREMENTAL_HOLDOUT_VALUE"]["holdout_evaluation_sha256"] = evaluation["evaluation_sha256"]
    rows["MULTIPLE_TESTING_PROTECTED"]["holdout_evaluation_sha256"] = evaluation["evaluation_sha256"]
    rows["SPEC_MODEL_IMMUTABILITY"]["model_refit"] = False
    rows["PROVENANCE_AND_SOURCE_IDENTITY_CLEAR"]["source_identity_clear"] = True
    rows["EVIDENCE_CONSUMPTION_DOCUMENTED"]["consumed_ledger_sha256"] = ledger["ledger_sha256"]
    for row in rows.values():
        row["evidence_sha256"] = _digest({k: v for k, v in row.items() if k != "evidence_sha256"})
    return rows


def test_8h_h_contract_binds_exact_8h_g_parent_blobs() -> None:
    validate_promotion_contract(CONTRACT)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "3e3feb970a953e6fdd493fc26b99f7eb189f5580"
    for path_key, sha_key in {
        "8h_a_contract": "8h_a_contract_git_blob_sha",
        "8h_g_contract": "8h_g_contract_git_blob_sha",
        "8h_g_implementation": "8h_g_implementation_git_blob_sha",
        "8h_g_tests": "8h_g_tests_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]


def test_current_repository_state_waits_without_claiming_no_candidates() -> None:
    result = promotion_review_status(contract=CONTRACT)
    assert result["state"] == WAITING_FOR_8H_G
    assert result["eligible_hypotheses"] == []
    assert result["approved_for_8i_research"] == []
    assert result["empirically_complete"] is False
    assert result["8i_research_handoff_authorized"] is False
    assert result["phase8i_integration_authorized"] is False


def test_only_holdout_confirmed_hypothesis_can_receive_packet() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids=set())
    with pytest.raises(ExternalEvidence8HPromotionError, match="only_holdout_confirmed"):
        build_promotion_review_packet(
            contract=CONTRACT,
            holdout_result=holdout,
            consumed_ledger=ledger,
            freeze_result=freeze,
            hypothesis_id=hypothesis,
            criteria_evidence=_criteria(spec, holdout, ledger, hypothesis),
        )


def test_valid_manual_approval_authorizes_8i_research_design_only() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids={hypothesis})
    packet = build_promotion_review_packet(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        freeze_result=freeze,
        hypothesis_id=hypothesis,
        criteria_evidence=_criteria(spec, holdout, ledger, hypothesis),
    )
    assert packet["all_10_criteria_pass"] is True
    assert packet["governance_clear"] is True
    decision = record_manual_review(
        contract=CONTRACT,
        packet=packet,
        decision="APPROVE_FOR_8I_RESEARCH_ONLY",
        reviewer_identity="manual-reviewer",
        reviewed_at="2027-10-25T12:00:00+00:00",
        rationale="All frozen criteria and one-shot Holdout bindings verified.",
    )
    assert decision["state"] == APPROVED_STATE
    assert decision["authorizes_8i_research_design"] is True
    assert decision["authorizes_8i_integration"] is False
    assert decision["authorizes_production"] is False
    complete = finalize_promotion_review(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        packets={hypothesis: packet},
        decisions={hypothesis: decision},
    )
    assert complete["state"] == PROMOTION_REVIEW_COMPLETE
    assert complete["empirically_complete"] is True
    assert complete["approved_for_8i_research"] == [hypothesis]
    assert complete["8i_research_handoff_authorized"] is True
    assert complete["phase8i_integration_authorized"] is False
    assert complete["orders_or_trades_authorized"] is False


def test_failed_criterion_blocks_manual_approval() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids={hypothesis})
    evidence = _criteria(spec, holdout, ledger, hypothesis)
    evidence["STABILITY_ROBUSTNESS"]["status"] = "FAIL"
    evidence["STABILITY_ROBUSTNESS"]["evidence_sha256"] = _digest({k: v for k, v in evidence["STABILITY_ROBUSTNESS"].items() if k != "evidence_sha256"})
    packet = build_promotion_review_packet(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        freeze_result=freeze,
        hypothesis_id=hypothesis,
        criteria_evidence=evidence,
    )
    assert packet["governance_clear"] is False
    with pytest.raises(ExternalEvidence8HPromotionError, match="approval_requires_all_10_criteria"):
        record_manual_review(
            contract=CONTRACT,
            packet=packet,
            decision="APPROVE_FOR_8I_RESEARCH_ONLY",
            reviewer_identity="reviewer",
            reviewed_at="2027-10-25T12:00:00+00:00",
            rationale="Attempted approval must fail.",
        )


def test_tampered_consumption_ledger_is_rejected() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids={hypothesis})
    tampered = deepcopy(ledger)
    tampered["holdout_open_count"] = 2
    with pytest.raises(ExternalEvidence8HPromotionError, match="consumed_ledger_digest_mismatch"):
        build_promotion_review_packet(
            contract=CONTRACT,
            holdout_result=holdout,
            consumed_ledger=tampered,
            freeze_result=freeze,
            hypothesis_id=hypothesis,
            criteria_evidence=_criteria(spec, holdout, ledger, hypothesis),
        )


def test_rejection_never_authorizes_inverse_or_8i_handoff() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids={hypothesis})
    packet = build_promotion_review_packet(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        freeze_result=freeze,
        hypothesis_id=hypothesis,
        criteria_evidence=_criteria(spec, holdout, ledger, hypothesis),
    )
    decision = record_manual_review(
        contract=CONTRACT,
        packet=packet,
        decision="REJECT",
        reviewer_identity="reviewer",
        reviewed_at="2027-10-25T12:00:00+00:00",
        rationale="Governance reviewer rejects research promotion.",
    )
    assert decision["rejection_authorizes_inverse_signal"] is False
    complete = finalize_promotion_review(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        packets={hypothesis: packet},
        decisions={hypothesis: decision},
    )
    assert complete["state"] == PROMOTION_REVIEW_COMPLETE
    assert complete["approved_for_8i_research"] == []
    assert complete["8i_research_handoff_authorized"] is False
    assert complete["rejection_authorizes_inverse_signal"] is False


def test_zero_confirmed_hypotheses_is_valid_terminal_completion() -> None:
    spec = _spec()
    _, holdout, ledger = _holdout_chain([spec], confirmed_ids=set())
    result = finalize_promotion_review(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        packets={},
        decisions={},
    )
    assert result["state"] == NO_PROMOTION_CANDIDATES
    assert result["eligible_hypotheses"] == []
    assert result["empirically_complete"] is True
    assert result["8h_complete"] is True
    assert result["8i_research_handoff_authorized"] is False


def test_missing_manual_disposition_keeps_8h_h_incomplete() -> None:
    spec = _spec()
    hypothesis = f"{spec['interaction_spec_id']}__peer_excess_5t"
    freeze, holdout, ledger = _holdout_chain([spec], confirmed_ids={hypothesis})
    packet = build_promotion_review_packet(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        freeze_result=freeze,
        hypothesis_id=hypothesis,
        criteria_evidence=_criteria(spec, holdout, ledger, hypothesis),
    )
    result = finalize_promotion_review(
        contract=CONTRACT,
        holdout_result=holdout,
        consumed_ledger=ledger,
        packets={hypothesis: packet},
        decisions={},
    )
    assert result["state"] == PROMOTION_REVIEW_INCOMPLETE
    assert result["pending_manual_review"] == [hypothesis]
    assert result["empirically_complete"] is False
    assert result["8i_research_handoff_authorized"] is False
