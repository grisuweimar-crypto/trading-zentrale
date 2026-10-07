from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import pytest

import scanner.research.pattern_discovery.promotion_gate as gate
from scanner.research.pattern_discovery.promotion_gate import (
    PromotionGateError,
    build_promotion_review,
    record_promotion_decision,
    validate_admission_current,
    verify_promotion_decision,
    verify_promotion_review,
)
from scanner.research.pattern_discovery.promotion_registry import (
    PromotionRegistry,
)


def canon(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest(value):
    return sha256(canon(value).encode("utf-8")).hexdigest()


def frozen_pattern(
    *,
    pattern_id="PAT-L12-A",
    pattern_type="RELATIVE_ALPHA",
    version="v1",
):
    spec = {
        "semantics": {
            "pattern_type": pattern_type,
            "natural_language_description": "L12 fixture",
        },
        "forecast": {
            "target_id": "PEER_EXCESS",
            "expected_direction": "POSITIVE",
            "horizon_sessions": 20,
            "baseline": "PIT_PEER_BASELINE",
        },
    }
    record = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": pattern_id,
        "pattern_version": version,
        "pattern_spec_hash": digest(spec),
        "natural_language_description": "L12 fixture",
        "pattern_spec": spec,
        "freeze_timestamp": "2026-10-01T12:00:00Z",
        "data_cutoff": "2026-10-01T11:00:00Z",
        "code_version": "test",
        "universe_version": "u-test",
        "feature_library_version": "f-test",
        "candidate_id": "CAND-L12",
        "discovery_run_id": "DISC-L12",
        "l4_evidence_hash": "a" * 64,
        "discovery_evidence": {
            "direction_probability": 0.99,
            "probability_advantage_lift": 0.49,
        },
        "qm_c_handoff": {
            "hypothesis_record": {
                "hypothesis_family_id": "HF-L12-FAMILY"
            }
        },
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    record["frozen_record_hash"] = digest(record)
    return record


def rating_history(p, *, rating="B", suffix="b"):
    return {
        "pattern_id": p["pattern_id"],
        "pattern_version": p["pattern_version"],
        "pattern_spec_hash": p["pattern_spec_hash"],
        "history_hash": suffix * 64,
        "current_rating": rating,
        "transition_count": 2,
    }


def l9_report(
    p,
    *,
    result_class="SUPPORTED",
    day=5,
    look_hash_char="c",
    final=True,
):
    return {
        "look_hash": look_hash_char * 64,
        "confirmation_evidence_hash": "d" * 64,
        "evaluated_at": f"2026-10-{day:02d}T20:00:00Z",
        "family_decision": "FINAL_COMPLETE" if final else "CONTINUE",
        "qm_governance": {
            "is_final_look": final,
            "monitoring_plan_id": "MON-L12",
            "monitoring_plan_version": "v1",
            "look_id": "FINAL" if final else "LOOK_1",
        },
        "pattern_results": [
            {
                "pattern_id": p["pattern_id"],
                "pattern_version": p["pattern_version"],
                "pattern_spec_hash": p["pattern_spec_hash"],
                "result_class": result_class,
                "result_reasons": [],
                "prospective_evidence": {
                    "direction_probability": 0.68,
                    "baseline_probability": 0.50,
                    "probability_advantage_lift": 0.18,
                    "mean_aligned_outcome": 0.041,
                    "effect_size_vs_baseline": 0.029,
                    "effective_n": 11,
                    "support_region_count": 6,
                    "robust_uncertainty": {
                        "aligned_effect_interval_95": [0.01, 0.07],
                        "probability_lift_interval_95": [0.03, 0.29],
                    },
                },
            }
        ],
    }


def graph(p, *, other=None, severity="NONE", relationship="RELATED", char="e"):
    nodes = [
        {
            "node_id": "N1",
            "pattern_id": p["pattern_id"],
            "pattern_version": p["pattern_version"],
            "pattern_spec_hash": p["pattern_spec_hash"],
        }
    ]
    edges = []
    if other is not None:
        nodes.append(
            {
                "node_id": "N2",
                "pattern_id": other["pattern_id"],
                "pattern_version": other["pattern_version"],
                "pattern_spec_hash": other["pattern_spec_hash"],
            }
        )
        edges.append(
            {
                "source_node_id": "N1",
                "target_node_id": "N2",
                "relationship": relationship,
                "dependency_severity": severity,
                "provenance": {
                    "structural": {
                        "feature_overlap": 0.8,
                        "condition_overlap": 0.7,
                    },
                    "empirical": {"event_overlap": 0.6},
                },
            }
        )
    return {
        "graph_id": "PDG-L12",
        "graph_hash": char * 64,
        "nodes": nodes,
        "edges": edges,
    }


def policy(*, dependency="INDEPENDENT_IF_NO_HIGH_DEPENDENCY"):
    return {
        "integration_mode": "SHADOW_CHALLENGER",
        "dependency_handling": dependency,
        "conflict_handling": "DEFER_TO_L13_RELATION_GRAPH",
    }


@pytest.fixture(autouse=True)
def isolate_upstream_verifiers(monkeypatch):
    monkeypatch.setattr(
        gate, "verify_rating_history", lambda value: {"valid": True}
    )
    monkeypatch.setattr(
        gate, "verify_confirmation_look", lambda value: {"valid": True}
    )
    monkeypatch.setattr(
        gate, "verify_dependency_graph", lambda value: {"valid": True}
    )


def test_rating_b_alone_never_auto_promotes():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p, rating="B"),
        [],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert "LATEST_TERMINAL_L9_RESULT_MUST_BE_SUPPORTED" in (
        review["promotion_eligibility"]["blockers"]
    )
    assert review["promotion_eligibility"][
        "automatic_promotion_performed"
    ] is False


def test_supported_b_pattern_becomes_review_eligible_not_active():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p, rating="B"),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert review["promotion_eligibility"]["status"] == "ELIGIBLE_FOR_REVIEW"
    assert review["probability_attachment"]["values"][
        "direction_probability"
    ] == 0.68
    assert review["probability_attachment"][
        "discovery_probability_used"
    ] is False
    assert review["requested_integration_contract"][
        "directional_authority"
    ] == "SHADOW_ONLY"
    assert review["requested_integration_contract"][
        "decision_layer_effect_active"
    ] is False


def test_approval_binds_exact_pattern_version_and_requires_l13():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T15:10:00Z",
        reason_codes=["SEPARATE_REVIEW_APPROVED"],
    )
    assert decision["promotion_status"] == "ADMITTED"
    admission = decision["admission_contract"]
    assert admission["admitted_pattern_id"] == p["pattern_id"]
    assert admission["admitted_pattern_version"] == p["pattern_version"]
    assert admission["admitted_pattern_spec_hash"] == p["pattern_spec_hash"]
    assert admission["vote_counting_allowed"] is False
    assert admission["decision_layer_effect_active"] is False
    assert admission["requires_l13_before_activation"] is True
    assert verify_promotion_decision(decision)["valid"] is True


def test_blocked_review_cannot_be_approved():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p, rating="C"),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    with pytest.raises(
        PromotionGateError, match="promotion_approval_requires_eligibility"
    ):
        record_promotion_decision(
            review,
            decision="APPROVE",
            reviewer_id="reviewer",
            reviewer_role="research_governance",
            reviewed_at="2026-10-07T15:10:00Z",
            reason_codes=["SHOULD_FAIL"],
        )


def test_high_dependency_requires_nonindependent_handling():
    p = frozen_pattern()
    other = frozen_pattern(pattern_id="PAT-L12-B")
    review = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p, other=other, severity="HIGH"),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert "HIGH_DEPENDENCY_REQUIRES_NONINDEPENDENT_HANDLING" in (
        review["promotion_eligibility"]["blockers"]
    )

    acceptable = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p, other=other, severity="HIGH"),
        requested_policy=policy(dependency="CORRELATED_NO_COUNTING"),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert acceptable["promotion_eligibility"]["status"] == "ELIGIBLE_FOR_REVIEW"


def test_modifier_requires_separate_contract():
    p = frozen_pattern(pattern_type="CONTEXT_MODIFIER")
    review = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert "MODIFIER_REQUIRES_SEPARATE_ADMISSION_CONTRACT" in (
        review["promotion_eligibility"]["blockers"]
    )


def test_latest_terminal_negative_blocks_old_supported_result():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p),
        [
            l9_report(p, day=4, result_class="SUPPORTED", look_hash_char="c"),
            l9_report(
                p,
                day=6,
                result_class="FALSIFIED",
                look_hash_char="f",
            ),
        ],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert review["probability_attachment"] is None


def test_admission_revalidation_fails_closed_when_binding_changes():
    p = frozen_pattern()
    history = rating_history(p)
    report = l9_report(p)
    dep = graph(p)
    review = build_promotion_review(
        p,
        history,
        [report],
        dep,
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T15:10:00Z",
        reason_codes=["SEPARATE_REVIEW_APPROVED"],
    )
    assert validate_admission_current(
        decision, p, history, [report], dep
    )["status"] == "CURRENT"

    changed = rating_history(p, rating="A", suffix="9")
    result = validate_admission_current(
        decision, p, changed, [report], dep
    )
    assert result["valid"] is False
    assert result["status"] == "STALE_REVIEW_REQUIRED"
    assert "RATING_HISTORY_CHANGED_SINCE_REVIEW" in result["reasons"]


def test_review_and_decision_are_tamper_evident():
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    assert verify_promotion_review(review)["valid"] is True
    tampered = deepcopy(review)
    tampered["requested_integration_contract"][
        "decision_layer_effect_active"
    ] = True
    with pytest.raises(PromotionGateError):
        verify_promotion_review(tampered)


def test_registry_is_append_only_reversible_and_idempotent(tmp_path):
    p = frozen_pattern()
    review = build_promotion_review(
        p,
        rating_history(p),
        [l9_report(p)],
        graph(p),
        requested_policy=policy(),
        opened_at="2026-10-07T15:00:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T15:10:00Z",
        reason_codes=["SEPARATE_REVIEW_APPROVED"],
    )
    registry = PromotionRegistry(tmp_path / "promotion.jsonl")
    first = registry.record_decision(decision)
    second = registry.record_decision(decision)
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert registry.current_status(
        decision["pattern_identity"]
    )["status"] == "ADMITTED"

    reversed_state = registry.reverse(
        pattern_identity=decision["pattern_identity"],
        action="ROLLBACK",
        reason_codes=["GOVERNANCE_REVIEW"],
        actor_id="reviewer",
        actor_role="research_governance",
        observed_at="2026-10-07T15:20:00Z",
        evidence_hash="8" * 64,
    )
    assert reversed_state["status"] == "ROLLED_BACK"
    assert registry.verify_integrity()["event_count"] == 2
    assert registry.current_status(
        decision["pattern_identity"]
    )["status"] == "ROLLED_BACK"
