from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from scanner.research.external_evidence.promotion_review_8g import (
    ExternalEvidence8GPromotionError,
    build_promotion_review_packet,
    finalize_promotion_review,
    record_manual_review,
    source_governance_review,
    validate_promotion_protocol,
)
from scanner.research.external_evidence.prospective_8g import PROSPECTIVE_COMPLETION_SCHEMA


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_promotion_review_v1.json").read_text())
SOURCE_REGISTRY = json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text())
COLLECTION = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8f_freeze_2026-09-28" / "external_evidence_8f_collection_latest.json").read_text())
GATE_8F = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8f_freeze_2026-09-28" / "external_evidence_8f_completion_gate.json").read_text())


def _completion(*, state: str = "PROSPECTIVE_CONFIRMED") -> dict:
    family = [
        "rates_policy_x_5t", "rates_policy_x_20t", "rates_policy_x_40t", "rates_policy_x_60t",
        "yield_curve_x_5t", "yield_curve_x_20t", "yield_curve_x_40t", "yield_curve_x_60t",
        "fx_x_5t", "fx_x_20t", "fx_x_40t", "fx_x_60t",
    ]
    states = {h: "NOT_ELIGIBLE_FROM_HOLDOUT" for h in family}
    states[family[0]] = state
    return {
        "schema_version": PROSPECTIVE_COMPLETION_SCHEMA,
        "phase": "8G-G",
        "state": "PROSPECTIVE_COMPLETE",
        "empirically_complete": True,
        "eligible_hypotheses": [family[0]],
        "pending_hypotheses": [],
        "hypothesis_states": states,
        "holm_family_size": 12,
        "automatic_promotion_authorized": False,
        "next_subphase": "8G-H_PROMOTION_REVIEW",
        "completion_sha256": "c" * 64,
    }


def _all_pass() -> dict[str, dict]:
    return {
        row["id"]: {"status": "PASS", "evidence_sha256": (row["id"].encode().hex()[:64]).ljust(64, "0")}
        for row in PROTOCOL["promotion_criteria"]
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


def _clear_source_review(factor: str = "rates_policy") -> dict:
    return source_governance_review(
        factor_id=factor,
        source_registry=_registry_with_frozen_sources(),
        collection_receipt=COLLECTION,
        completion_gate_8f=GATE_8F,
        protocol=PROTOCOL,
    )


def test_protocol_preserves_exact_10_plan_criteria_and_manual_only_promotion() -> None:
    validate_promotion_protocol(PROTOCOL)
    assert PROTOCOL["phase"] == "8G-H"
    assert len(PROTOCOL["promotion_criteria"]) == 10
    assert [x["plan_criterion"] for x in PROTOCOL["promotion_criteria"]] == list(range(1, 11))
    assert PROTOCOL["automatic_promotion_enabled"] is False
    assert PROTOCOL["productive_integration_enabled"] is False
    assert PROTOCOL["completion_gate"]["next_phase"] == "8H_CROSS_FACTOR_INTERACTION"


def test_current_registry_blocks_frozen_source_id_resolution_instead_of_silent_promotion() -> None:
    review = source_governance_review(
        factor_id="rates_policy",
        source_registry=SOURCE_REGISTRY,
        collection_receipt=COLLECTION,
        completion_gate_8f=GATE_8F,
        protocol=PROTOCOL,
    )
    assert review["all_sources_clear"] is False
    assert "SOURCE_NOT_RESOLVED_IN_REGISTRY:FED_H15" in review["blockers"]


def test_manual_clearance_cannot_replace_missing_registry_identity() -> None:
    review = source_governance_review(
        factor_id="rates_policy",
        source_registry=SOURCE_REGISTRY,
        collection_receipt=COLLECTION,
        completion_gate_8f=GATE_8F,
        protocol=PROTOCOL,
        explicit_source_clearances={
            "FED_H15": {
                "registry_resolution": True,
                "pit_status": "SAFE",
                "license_status": "CLEARED",
                "provenance_status": "CLEARED",
            }
        },
    )
    assert review["all_sources_clear"] is False
    assert "SOURCE_NOT_RESOLVED_IN_REGISTRY:FED_H15" in review["blockers"]


def test_registered_safe_usable_source_and_8f_provenance_clear_source_gate() -> None:
    review = _clear_source_review()
    assert review["all_sources_clear"] is True
    assert review["blockers"] == []
    assert review["raw_snapshot_provenance_pass"] is True
    assert review["8f_completion_gate_pass"] is True


def test_statistical_confirmation_alone_does_not_auto_promote() -> None:
    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=_completion(),
        criteria_evidence=_all_pass(),
        source_governance=_clear_source_review(),
        protocol=PROTOCOL,
    )
    assert packet["promotion_eligible_from_8g_g"] is True
    assert packet["all_10_criteria_pass"] is True
    assert packet["governance_clear"] is True
    assert packet["automatic_promotion_authorized"] is False
    assert packet["production_authorized"] is False
    assert packet["manual_review_required"] is True


def test_missing_one_plan_criterion_blocks_approval() -> None:
    evidence = _all_pass()
    evidence["ASOF_UNIVERSE_SURVIVORSHIP_PROVEN"] = {"status": "MISSING"}
    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=_completion(),
        criteria_evidence=evidence,
        source_governance=_clear_source_review(),
        protocol=PROTOCOL,
    )
    with pytest.raises(ExternalEvidence8GPromotionError, match="approval_requires_all_10_criteria"):
        record_manual_review(
            packet=packet,
            decision="APPROVE_FOR_8H_RESEARCH_ONLY",
            reviewer_identity="reviewer",
            reviewed_at="2027-06-01T12:00:00Z",
            rationale="All other gates passed; survivorship evidence missing.",
            protocol=PROTOCOL,
        )


def test_source_governance_blocker_prevents_approval_even_if_10_evidence_flags_pass() -> None:
    blocked = source_governance_review(
        factor_id="rates_policy",
        source_registry=SOURCE_REGISTRY,
        collection_receipt=COLLECTION,
        completion_gate_8f=GATE_8F,
        protocol=PROTOCOL,
    )
    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=_completion(),
        criteria_evidence=_all_pass(),
        source_governance=blocked,
        protocol=PROTOCOL,
    )
    assert packet["all_10_criteria_pass"] is True
    assert packet["governance_clear"] is False
    with pytest.raises(ExternalEvidence8GPromotionError, match="approval_requires_no_governance_blockers"):
        record_manual_review(
            packet=packet,
            decision="APPROVE_FOR_8H_RESEARCH_ONLY",
            reviewer_identity="reviewer",
            reviewed_at="2027-06-01T12:00:00Z",
            rationale="Attempted approval while source registry is unresolved.",
            protocol=PROTOCOL,
        )


def test_manual_approval_authorizes_only_8h_research() -> None:
    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=_completion(),
        criteria_evidence=_all_pass(),
        source_governance=_clear_source_review(),
        protocol=PROTOCOL,
    )
    decision = record_manual_review(
        packet=packet,
        decision="APPROVE_FOR_8H_RESEARCH_ONLY",
        reviewer_identity="human-reviewer",
        reviewed_at="2027-06-01T12:00:00Z",
        rationale="All ten frozen Phase-8 family promotion criteria passed.",
        protocol=PROTOCOL,
    )
    assert decision["state"] == "APPROVED_FOR_8H_RESEARCH_ONLY"
    assert decision["authorizes_8h_research"] is True
    assert decision["authorizes_production"] is False
    assert decision["authorizes_phase7_mutation"] is False
    assert decision["authorizes_8i_integration"] is False
    assert decision["authorizes_orders_or_trades"] is False


def test_non_confirmed_hypothesis_can_be_reviewed_but_never_approved() -> None:
    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=_completion(state="PROSPECTIVE_NOT_CONFIRMED"),
        criteria_evidence=_all_pass(),
        source_governance=_clear_source_review(),
        protocol=PROTOCOL,
    )
    assert packet["promotion_eligible_from_8g_g"] is False
    with pytest.raises(ExternalEvidence8GPromotionError, match="approval_requires_prospective_confirmation"):
        record_manual_review(
            packet=packet,
            decision="APPROVE_FOR_8H_RESEARCH_ONLY",
            reviewer_identity="reviewer",
            reviewed_at="2027-06-01T12:00:00Z",
            rationale="Should not approve an unconfirmed hypothesis.",
            protocol=PROTOCOL,
        )


def test_finalize_requires_disposition_for_each_prospective_confirmed_hypothesis() -> None:
    completion = _completion()
    pending = finalize_promotion_review(
        prospective_completion=completion,
        decisions={},
        protocol=PROTOCOL,
    )
    assert pending["empirically_complete"] is False
    assert pending["state"] == "PROMOTION_REVIEW_INCOMPLETE"

    packet = build_promotion_review_packet(
        hypothesis_id="rates_policy_x_5t",
        prospective_completion=completion,
        criteria_evidence=_all_pass(),
        source_governance=_clear_source_review(),
        protocol=PROTOCOL,
    )
    decision = record_manual_review(
        packet=packet,
        decision="DEFER",
        reviewer_identity="human-reviewer",
        reviewed_at="2027-06-01T12:00:00Z",
        rationale="Governance follow-up requested before top-level 8H.",
        protocol=PROTOCOL,
    )
    final = finalize_promotion_review(
        prospective_completion=completion,
        decisions={"rates_policy_x_5t": decision},
        protocol=PROTOCOL,
    )
    assert final["empirically_complete"] is True
    assert final["state"] == "PROMOTION_REVIEW_COMPLETE"
    assert final["approved_for_8h_research"] == []
    assert final["next_phase"] == "8H_CROSS_FACTOR_INTERACTION"


def test_empty_prospective_confirmed_set_is_valid_completion() -> None:
    completion = _completion(state="PROSPECTIVE_NOT_CONFIRMED")
    final = finalize_promotion_review(
        prospective_completion=completion,
        decisions={},
        protocol=PROTOCOL,
    )
    assert final["empirically_complete"] is True
    assert final["state"] == "NO_PROMOTION_CANDIDATES"
