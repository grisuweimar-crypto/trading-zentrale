from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import pytest

import scanner.research.pattern_discovery.promotion_gate as gate_module
from scanner.research.pattern_discovery.promotion_gate import (
    PromotionGateError,
    PromotionRegistry,
    build_promotion_review,
    persist_promotion_review,
    record_promotion_decision,
    validate_admission_current,
    verify_promotion_decision,
    verify_promotion_review,
)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest(value):
    return sha256(canonical(value).encode("utf-8")).hexdigest()


def frozen_pattern(
    *,
    pattern_id="PAT-L12-A",
    version="v1",
    pattern_type="RELATIVE_ALPHA",
    discovery_probability=0.95,
):
    spec = {
        "identity": {
            "pattern_id": pattern_id,
            "pattern_version": version,
        },
        "semantics": {
            "pattern_type": pattern_type,
            "natural_language_description": "L12 synthetic fixture",
        },
        "forecast": {
            "target_id": "PEER_EXCESS",
            "expected_direction": "POSITIVE",
            "horizon_sessions": 20,
            "baseline": "PIT_PEER_BASELINE",
        },
    }
    body = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": pattern_id,
        "pattern_version": version,
        "pattern_spec_hash": digest(spec),
        "natural_language_description": "L12 synthetic fixture",
        "pattern_spec": spec,
        "freeze_timestamp": "2026-10-01T12:00:00Z",
        "data_cutoff": "2026-10-01T11:00:00Z",
        "code_version": "l12-test",
        "universe_version": "u-test",
        "feature_library_version": "f-test",
        "candidate_id": "CAND-L12",
        "discovery_run_id": "DISC-L12",
        "l4_evidence_hash": "1" * 64,
        "discovery_evidence": {
            "direction_probability": discovery_probability,
        },
        "qm_c_handoff": {
            "hypothesis_record": {
                "hypothesis_family_id": "HF-L12-ALPHA"
            }
        },
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    body["frozen_record_hash"] = digest(body)
    return body


def rating_history(pattern, rating="B", suffix="a"):
    return {
        "pattern_id": pattern["pattern_id"],
        "pattern_version": pattern["pattern_version"],
        "pattern_spec_hash": pattern["pattern_spec_hash"],
        "current_rating": rating,
        "transition_count": 2,
        "history_hash": suffix * 64,
    }


def l9_report(
    pattern,
    *,
    result_class="SUPPORTED",
    look_hash=None,
    day=6,
    family_decision="FINAL_COMPLETE",
    is_final=True,
    prospective_probability=0.67,
):
    look_hash = look_hash or digest(
        {
            "pattern": pattern["pattern_id"],
            "version": pattern["pattern_version"],
            "day": day,
            "result": result_class,
        }
    )
    return {
        "look_hash": look_hash,
        "confirmation_look_id": f"LOOK-L12-{day}",
        "confirmation_evidence_hash": digest(
            {"evidence": look_hash}
        ),
        "evaluated_at": f"2026-10-{day:02d}T19:30:00Z",
        "family_decision": family_decision,
        "qm_governance": {
            "is_final_look": is_final,
            "monitoring_plan_id": "MON-L12",
            "monitoring_plan_version": "v1",
            "look_id": "FINAL" if is_final else "LOOK_1",
        },
        "pattern_results": [
            {
                "pattern_id": pattern["pattern_id"],
                "pattern_version": pattern["pattern_version"],
                "pattern_spec_hash": pattern["pattern_spec_hash"],
                "result_class": result_class,
                "result_reasons": [],
                "prospective_evidence": {
                    "direction_probability": prospective_probability,
                    "baseline_probability": 0.49,
                    "probability_advantage_lift": (
                        prospective_probability - 0.49
                    ),
                    "mean_aligned_outcome": 0.04,
                    "effect_size_vs_baseline": 0.025,
                    "effective_n": 9,
                    "support_region_count": 5,
                    "robust_uncertainty": {
                        "aligned_effect_interval_95": [0.01, 0.07],
                        "probability_lift_interval_95": [0.02, 0.31],
                    },
                },
            }
        ],
    }


def dependency_graph(pattern, *, severity=None, relationship="RELATED"):
    nodes = [
        {
            "node_id": "NODE-A",
            "pattern_id": pattern["pattern_id"],
            "pattern_version": pattern["pattern_version"],
            "pattern_spec_hash": pattern["pattern_spec_hash"],
        }
    ]
    edges = []
    if severity is not None:
        nodes.append(
            {
                "node_id": "NODE-B",
                "pattern_id": "PAT-L12-B",
                "pattern_version": "v1",
                "pattern_spec_hash": "b" * 64,
            }
        )
        edges.append(
            {
                "source_node_id": "NODE-A",
                "target_node_id": "NODE-B",
                "relationship": relationship,
                "dependency_severity": severity,
                "feature_overlap": 0.8,
                "event_overlap": 0.6,
            }
        )
    return {
        "graph_id": "PDG-L12",
        "graph_hash": digest(
            {
                "pattern": pattern["pattern_id"],
                "severity": severity,
                "relationship": relationship,
            }
        ),
        "nodes": nodes,
        "edges": edges,
    }


def policy(
    *,
    integration="SHADOW_CHALLENGER",
    dependency="INDEPENDENT_IF_NO_HIGH_DEPENDENCY",
    conflict="DEFER_TO_L13_RELATION_GRAPH",
):
    return {
        "integration_mode": integration,
        "dependency_handling": dependency,
        "conflict_handling": conflict,
    }


def eligible_review(
    pattern=None,
    *,
    rating="B",
    graph=None,
    requested=None,
):
    pattern = pattern or frozen_pattern()
    graph = graph or dependency_graph(pattern)
    requested = requested or policy()
    return build_promotion_review(
        pattern,
        rating_history(pattern, rating=rating),
        [l9_report(pattern)],
        graph,
        requested_policy=requested,
        opened_at="2026-10-07T15:30:00Z",
    )


@pytest.fixture(autouse=True)
def isolate_upstream_verifiers(monkeypatch):
    # L6/L9/L10 are run as full regression suites by the L12 CI.
    monkeypatch.setattr(
        gate_module,
        "verify_rating_history",
        lambda value: {"valid": True},
    )
    monkeypatch.setattr(
        gate_module,
        "verify_confirmation_look",
        lambda value: {"valid": True},
    )
    monkeypatch.setattr(
        gate_module,
        "verify_dependency_graph",
        lambda value: {"valid": True},
    )


def test_b_or_a_is_only_eligible_for_review_not_auto_promoted():
    pattern = frozen_pattern()
    review = eligible_review(pattern)

    assert review["review_status"] == "OPEN"
    assert (
        review["promotion_eligibility"]["status"]
        == "ELIGIBLE_FOR_REVIEW"
    )
    assert (
        review["promotion_eligibility"][
            "automatic_promotion_performed"
        ]
        is False
    )
    assert review.get("admission_contract") is None


def test_probability_attachment_uses_l9_not_discovery_probability():
    pattern = frozen_pattern(discovery_probability=0.95)
    review = build_promotion_review(
        pattern,
        rating_history(pattern, "B"),
        [l9_report(pattern, prospective_probability=0.67)],
        dependency_graph(pattern),
        requested_policy=policy(),
        opened_at="2026-10-07T15:30:00Z",
    )

    attachment = review["probability_attachment"]
    assert attachment["values"]["direction_probability"] == 0.67
    assert attachment["discovery_probability_used"] is False
    assert attachment["recalibration_performed"] is False


def test_non_ba_rating_and_missing_terminal_confirmation_block_approval():
    pattern = frozen_pattern()
    review = build_promotion_review(
        pattern,
        rating_history(pattern, "C"),
        [],
        dependency_graph(pattern),
        requested_policy=policy(),
        opened_at="2026-10-07T15:30:00Z",
    )
    blockers = review["promotion_eligibility"]["blockers"]

    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert "RATING_NOT_PROMOTION_ELIGIBLE:C" in blockers
    assert "TERMINAL_SUPPORTED_L9_CONFIRMATION_REQUIRED" in blockers
    with pytest.raises(
        PromotionGateError,
        match="promotion_approval_requires_eligibility",
    ):
        record_promotion_decision(
            review,
            decision="APPROVE",
            reviewer_id="reviewer",
            reviewer_role="research_governance",
            reviewed_at="2026-10-07T16:00:00Z",
            reason_codes=["APPROVED_FOR_SHADOW"],
        )


def test_high_or_duplicate_dependency_cannot_be_treated_independent():
    pattern = frozen_pattern()
    high_graph = dependency_graph(
        pattern,
        severity="HIGH",
        relationship="DUPLICATE",
    )
    blocked = eligible_review(
        pattern,
        graph=high_graph,
        requested=policy(
            dependency="INDEPENDENT_IF_NO_HIGH_DEPENDENCY"
        ),
    )
    assert blocked["promotion_eligibility"]["status"] == "BLOCKED"
    assert (
        "HIGH_DEPENDENCY_REQUIRES_NONINDEPENDENT_HANDLING"
        in blocked["promotion_eligibility"]["blockers"]
    )
    assert (
        "DUPLICATE_REQUIRES_NONINDEPENDENT_HANDLING"
        in blocked["promotion_eligibility"]["blockers"]
    )

    handled = eligible_review(
        pattern,
        graph=high_graph,
        requested=policy(dependency="CORRELATED_NO_COUNTING"),
    )
    assert (
        handled["promotion_eligibility"]["status"]
        == "ELIGIBLE_FOR_REVIEW"
    )
    assert (
        handled["requested_integration_contract"][
            "vote_counting_allowed"
        ]
        is False
    )


def test_modifier_requires_separate_admission_contract():
    pattern = frozen_pattern(pattern_type="CONTEXT_MODIFIER")
    review = eligible_review(pattern)

    assert review["promotion_eligibility"]["status"] == "BLOCKED"
    assert (
        "MODIFIER_REQUIRES_SEPARATE_ADMISSION_CONTRACT"
        in review["promotion_eligibility"]["blockers"]
    )


def test_explicit_approval_binds_exact_version_and_stays_shadow_only():
    pattern = frozen_pattern()
    review = eligible_review(pattern)
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )
    admission = decision["admission_contract"]

    assert decision["promotion_status"] == "ADMITTED"
    assert admission["admitted_pattern_id"] == pattern["pattern_id"]
    assert admission["admitted_pattern_version"] == "v1"
    assert (
        admission["admitted_pattern_spec_hash"]
        == pattern["pattern_spec_hash"]
    )
    assert admission["admitted_pattern_family_id"] == "HF-L12-ALPHA"
    assert admission["horizon_sessions"] == 20
    assert admission["integration_mode"] == "SHADOW_CHALLENGER"
    assert admission["directional_authority"] == "SHADOW_ONLY"
    assert admission["vote_counting_allowed"] is False
    assert admission["decision_layer_effect_active"] is False
    assert admission["requires_l13_before_activation"] is True
    assert verify_promotion_decision(decision)["valid"] is True


def test_reject_is_retained_even_when_review_is_blocked():
    pattern = frozen_pattern()
    review = build_promotion_review(
        pattern,
        rating_history(pattern, "U"),
        [],
        dependency_graph(pattern),
        requested_policy=policy(),
        opened_at="2026-10-07T15:30:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="REJECT",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["INSUFFICIENT_CONFIRMATION"],
    )
    assert decision["promotion_status"] == "REJECTED"
    assert decision["admission_contract"] is None


def test_admission_becomes_stale_when_bound_rating_changes():
    pattern = frozen_pattern()
    rating = rating_history(pattern, "B", suffix="a")
    report = l9_report(pattern)
    graph = dependency_graph(pattern)
    review = build_promotion_review(
        pattern,
        rating,
        [report],
        graph,
        requested_policy=policy(),
        opened_at="2026-10-07T15:30:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )

    current = validate_admission_current(
        decision,
        pattern,
        rating,
        [report],
        graph,
    )
    assert current["valid"] is True
    assert current["status"] == "CURRENT"

    changed = rating_history(pattern, "B", suffix="c")
    stale = validate_admission_current(
        decision,
        pattern,
        changed,
        [report],
        graph,
    )
    assert stale["valid"] is False
    assert stale["status"] == "STALE_REVIEW_REQUIRED"
    assert "RATING_HISTORY_CHANGED_SINCE_REVIEW" in stale["reasons"]


def test_admission_becomes_stale_on_new_l9_or_dependency_binding():
    pattern = frozen_pattern()
    rating = rating_history(pattern, "B")
    report = l9_report(pattern, day=6)
    graph = dependency_graph(pattern)
    review = build_promotion_review(
        pattern,
        rating,
        [report],
        graph,
        requested_policy=policy(),
        opened_at="2026-10-07T15:30:00Z",
    )
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )

    newer = l9_report(pattern, day=7)
    stale_l9 = validate_admission_current(
        decision,
        pattern,
        rating,
        [report, newer],
        graph,
    )
    assert "CONFIRMATION_EVIDENCE_CHANGED_SINCE_REVIEW" in (
        stale_l9["reasons"]
    )

    changed_graph = deepcopy(graph)
    changed_graph["graph_hash"] = "9" * 64
    stale_graph = validate_admission_current(
        decision,
        pattern,
        rating,
        [report],
        changed_graph,
    )
    assert "DEPENDENCY_GRAPH_CHANGED_SINCE_REVIEW" in (
        stale_graph["reasons"]
    )


def test_exact_pattern_version_binding_fails_closed():
    pattern = frozen_pattern()
    review = eligible_review(pattern)
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )
    v2 = frozen_pattern(version="v2")

    with pytest.raises(
        PromotionGateError,
        match="admission_pattern_identity_mismatch",
    ):
        validate_admission_current(
            decision,
            v2,
            rating_history(v2, "B"),
            [l9_report(v2)],
            dependency_graph(v2),
        )


def test_registry_is_append_only_idempotent_and_reversible(tmp_path):
    pattern = frozen_pattern()
    review = eligible_review(pattern)
    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )
    registry = PromotionRegistry(tmp_path / "promotion.jsonl")

    first = registry.record_decision(decision)
    second = registry.record_decision(decision)
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert registry.current_status(
        decision["pattern_identity"]
    )["status"] == "ADMITTED"

    reversed_result = registry.reverse(
        pattern_identity=decision["pattern_identity"],
        action="ROLLBACK",
        reason_codes=["INCREMENTAL_VALUE_NOT_CONFIRMED"],
        actor_id="reviewer",
        actor_role="research_governance",
        observed_at="2026-10-08T12:00:00Z",
        evidence_hash="4" * 64,
    )
    assert reversed_result["status"] == "ROLLED_BACK"
    assert registry.current_status(
        decision["pattern_identity"]
    )["status"] == "ROLLED_BACK"
    assert registry.verify_integrity()["event_count"] == 2


def test_review_and_decision_are_tamper_evident():
    review = eligible_review()
    assert verify_promotion_review(review)["valid"] is True

    tampered_review = deepcopy(review)
    tampered_review["promotion_eligibility"]["status"] = "BLOCKED"
    with pytest.raises(
        PromotionGateError,
        match="promotion_review_hash_mismatch",
    ):
        verify_promotion_review(tampered_review)

    decision = record_promotion_decision(
        review,
        decision="APPROVE",
        reviewer_id="reviewer",
        reviewer_role="research_governance",
        reviewed_at="2026-10-07T16:00:00Z",
        reason_codes=["APPROVED_FOR_SHADOW"],
    )
    tampered_decision = deepcopy(decision)
    tampered_decision["admission_contract"][
        "decision_layer_effect_active"
    ] = True
    with pytest.raises(
        PromotionGateError,
        match="promotion_decision_layer_effect_forbidden",
    ):
        verify_promotion_decision(tampered_decision)


def test_persist_review_is_immutable_and_idempotent(tmp_path):
    review = eligible_review()
    first = persist_promotion_review(tmp_path, review)
    second = persist_promotion_review(tmp_path, review)

    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert (tmp_path / first["review_path"]).exists()
