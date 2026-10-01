from __future__ import annotations

from pathlib import Path

import pytest

from scanner.research.governance.qm_h_capa import (
    CapaLedger,
    CapaLedgerError,
    build_identity_resolvers,
    load_qm_h_contract,
)


def make_ledger(tmp_path: Path, *, identity_resolvers=None) -> CapaLedger:
    return CapaLedger(tmp_path / "qm_h.jsonl", identity_resolvers=identity_resolvers)


def register(
    ledger: CapaLedger,
    *,
    finding_id: str = "QM-H-DEF-001",
    category: str = "DEFECT",
    evidence_impact: str = "NO_KNOWN_EVIDENCE_IMPACT",
    identity_refs=None,
) -> None:
    ledger.register_finding(
        finding_id=finding_id,
        category=category,
        title="Historical matcher defect",
        description="Test finding for the QM-H governance layer.",
        source_reference="test://qm-h/fixture",
        evidence_impact=evidence_impact,
        identity_refs=identity_refs or [],
        actor_id="reviewer-1",
        actor_role="qm-reviewer",
    )


def to_triaged(ledger: CapaLedger, finding_id: str = "QM-H-DEF-001") -> None:
    ledger.transition(
        finding_id=finding_id,
        to_status="TRIAGED",
        actor_id="reviewer-1",
        actor_role="qm-reviewer",
        details={"severity": "HIGH", "scope": "historical research", "triage_rationale": "impact requires CAPA"},
    )


def to_root_cause(ledger: CapaLedger, finding_id: str = "QM-H-DEF-001") -> None:
    ledger.transition(
        finding_id=finding_id,
        to_status="ROOT_CAUSE_IDENTIFIED",
        actor_id="reviewer-1",
        actor_role="qm-reviewer",
        details={"cause": "session semantics were underspecified", "cause_classification": "METHOD"},
    )


def to_action_plan(ledger: CapaLedger, finding_id: str = "QM-H-DEF-001") -> None:
    ledger.transition(
        finding_id=finding_id,
        to_status="ACTION_PLANNED",
        actor_id="owner-1",
        actor_role="capa-owner",
        details={
            "capa_id": "QM-H-CAPA-001",
            "corrective_action": "repair the matcher contract",
            "preventive_action": "add regression coverage",
            "action_owner": "owner-1",
        },
    )


def to_implemented(ledger: CapaLedger, finding_id: str = "QM-H-DEF-001") -> None:
    ledger.transition(
        finding_id=finding_id,
        to_status="IMPLEMENTED",
        actor_id="owner-1",
        actor_role="capa-owner",
        details={
            "capa_id": "QM-H-CAPA-001",
            "implementation_reference": "commit:test-1",
            "implementation_summary": "contract and regression test implemented",
        },
    )


def to_verified(ledger: CapaLedger, finding_id: str = "QM-H-DEF-001") -> None:
    ledger.transition(
        finding_id=finding_id,
        to_status="EFFECTIVENESS_VERIFIED",
        actor_id="reviewer-2",
        actor_role="qm-reviewer",
        details={
            "capa_id": "QM-H-CAPA-001",
            "effectiveness_evidence_reference": "test://regression/pass",
            "effectiveness_result": "EFFECTIVE",
        },
    )


def test_contract_is_continuous_research_only_and_not_productive():
    contract = load_qm_h_contract()
    assert contract["module"] == "QM-H"
    assert contract["continuous_control"] is True
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["principles"]["qm_a_remains_evidence_consumption_authority"] is True
    assert contract["principles"]["qm_h_may_not_mutate_qm_a_state"] is True


def test_four_finding_categories_are_distinct():
    assert load_qm_h_contract()["finding_categories"] == [
        "DEFECT",
        "NEAR_MISS",
        "METHODOLOGY_FINDING",
        "EXTERNAL_EVIDENCE_GAP",
    ]


def test_upstream_identity_references_require_exact_stable_fields(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    with pytest.raises(CapaLedgerError, match="identity_ref_missing:QM_C_RESULT"):
        register(
            ledger,
            identity_refs=[{"kind": "QM_C_RESULT", "identity": {"result_id": "R-1", "result_version": "v1"}}],
        )


def test_complete_upstream_identity_requires_authoritative_resolver(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    with pytest.raises(CapaLedgerError, match="identity_ref_resolver_required:QM_C_RESULT"):
        register(
            ledger,
            identity_refs=[
                {
                    "kind": "QM_C_RESULT",
                    "identity": {"result_id": "R-1", "result_version": "v1", "result_hash": "hash-1"},
                }
            ],
        )


def test_upstream_identity_is_compared_with_authoritative_registry_record(tmp_path: Path):
    def result_resolver(identity):
        assert identity["result_id"] == "R-1"
        return {"result_id": "R-1", "result_version": "v1", "result_hash": "canonical-hash"}

    ledger = make_ledger(tmp_path, identity_resolvers={"QM_C_RESULT": result_resolver})
    bad_ref = [
        {
            "kind": "QM_C_RESULT",
            "identity": {"result_id": "R-1", "result_version": "v1", "result_hash": "fabricated-hash"},
        }
    ]
    with pytest.raises(CapaLedgerError, match="identity_ref_registry_mismatch:QM_C_RESULT:result_hash"):
        register(ledger, identity_refs=bad_ref)

    good_ref = [
        {
            "kind": "QM_C_RESULT",
            "identity": {"result_id": "R-1", "result_version": "v1", "result_hash": "canonical-hash"},
        }
    ]
    register(ledger, identity_refs=good_ref)
    finding = ledger.get_finding("QM-H-DEF-001")
    assert finding["identity_refs"][0]["identity"]["result_hash"] == "canonical-hash"


def test_identity_resolver_builder_uses_existing_registry_getters():
    class FakeResultRegistry:
        def get_result(self, result_id, result_version):
            assert (result_id, result_version) == ("R-1", "v1")
            return {"result_id": "R-1", "result_version": "v1", "result_hash": "canonical-hash"}

    resolvers = build_identity_resolvers(result_registry=FakeResultRegistry())
    resolved = resolvers["QM_C_RESULT"](
        {"result_id": "R-1", "result_version": "v1", "result_hash": "canonical-hash"}
    )
    assert resolved["result_hash"] == "canonical-hash"


def test_known_qm_b_external_blocker_can_be_referenced_but_not_relabelled(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(
        ledger,
        finding_id="QM-H-EXT-001",
        category="EXTERNAL_EVIDENCE_GAP",
        evidence_impact="PROMOTION_BLOCKED",
        identity_refs=[
            {
                "kind": "QM_B_EXTERNAL_BLOCKER",
                "identity": {"blocker_id": "LISTING_EVIDENCE", "state": "UNKNOWN_FAIL_CLOSED"},
            }
        ],
    )
    with pytest.raises(CapaLedgerError, match="qm_b_external_blocker_state_mismatch"):
        ledger.register_finding(
            finding_id="QM-H-EXT-002",
            category="EXTERNAL_EVIDENCE_GAP",
            title="Bad blocker state",
            description="Must fail closed.",
            source_reference="test://bad-state",
            evidence_impact="PROMOTION_BLOCKED",
            identity_refs=[
                {
                    "kind": "QM_B_EXTERNAL_BLOCKER",
                    "identity": {"blocker_id": "LISTING_EVIDENCE", "state": "RESOLVED"},
                }
            ],
            actor_id="reviewer-1",
            actor_role="qm-reviewer",
        )


def test_illegal_lifecycle_jump_fails_closed(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    with pytest.raises(CapaLedgerError, match="transition_forbidden:OPEN->ACTION_PLANNED"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="ACTION_PLANNED",
            actor_id="owner-1",
            actor_role="capa-owner",
            details={
                "capa_id": "QM-H-CAPA-001",
                "corrective_action": "x",
                "preventive_action": "y",
                "action_owner": "owner-1",
            },
        )


def test_complete_capa_requires_effectiveness_verification(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    to_triaged(ledger)
    to_root_cause(ledger)
    to_action_plan(ledger)
    to_implemented(ledger)
    with pytest.raises(CapaLedgerError, match="transition_forbidden:IMPLEMENTED->CLOSED"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="CLOSED",
            actor_id="reviewer-2",
            actor_role="qm-reviewer",
            details={
                "capa_id": "QM-H-CAPA-001",
                "closure_reference": "review://close",
                "closure_rationale": "too early",
            },
        )
    to_verified(ledger)
    ledger.transition(
        finding_id="QM-H-DEF-001",
        to_status="CLOSED",
        actor_id="reviewer-2",
        actor_role="qm-reviewer",
        details={
            "capa_id": "QM-H-CAPA-001",
            "closure_reference": "review://close",
            "closure_rationale": "effectiveness verified",
        },
    )
    finding = ledger.get_finding("QM-H-DEF-001")
    assert finding["status"] == "CLOSED"
    assert finding["capa_id"] == "QM-H-CAPA-001"


def test_nonzero_evidence_impact_requires_disposition_reference_to_close(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger, evidence_impact="EVIDENCE_REVIEW_REQUIRED")
    to_triaged(ledger)
    to_root_cause(ledger)
    to_action_plan(ledger)
    to_implemented(ledger)
    to_verified(ledger)
    with pytest.raises(CapaLedgerError, match="closure_requires_evidence_disposition_reference"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="CLOSED",
            actor_id="reviewer-2",
            actor_role="qm-reviewer",
            details={
                "capa_id": "QM-H-CAPA-001",
                "closure_reference": "review://close",
                "closure_rationale": "missing upstream evidence disposition",
            },
        )


def test_closure_uses_prospective_evidence_impact_not_stale_value(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger, evidence_impact="NO_KNOWN_EVIDENCE_IMPACT")
    to_triaged(ledger)
    to_root_cause(ledger)
    to_action_plan(ledger)
    to_implemented(ledger)
    to_verified(ledger)
    with pytest.raises(CapaLedgerError, match="closure_requires_evidence_disposition_reference"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="CLOSED",
            actor_id="reviewer-2",
            actor_role="qm-reviewer",
            details={
                "capa_id": "QM-H-CAPA-001",
                "closure_reference": "review://close",
                "closure_rationale": "impact discovered during closure",
                "evidence_impact": "PROMOTION_BLOCKED",
                "evidence_impact_change_reference": "review://impact/1",
                "evidence_impact_change_rationale": "late evidence audit found a blocker",
            },
        )


def test_evidence_impact_change_requires_traceable_reference_and_rationale(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    with pytest.raises(CapaLedgerError, match="evidence_impact_change_requires_reference"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="TRIAGED",
            actor_id="reviewer-1",
            actor_role="qm-reviewer",
            details={
                "severity": "HIGH",
                "scope": "historical research",
                "triage_rationale": "new evidence impact",
                "evidence_impact": "EVIDENCE_REVIEW_REQUIRED",
            },
        )


def test_evidence_impact_downgrade_requires_disposition_reference(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger, evidence_impact="PROMOTION_BLOCKED")
    with pytest.raises(CapaLedgerError, match="evidence_impact_downgrade_requires_disposition_reference"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="TRIAGED",
            actor_id="reviewer-1",
            actor_role="qm-reviewer",
            details={
                "severity": "HIGH",
                "scope": "historical research",
                "triage_rationale": "attempt unsafe downgrade",
                "evidence_impact": "NO_KNOWN_EVIDENCE_IMPACT",
                "evidence_impact_change_reference": "review://impact/2",
                "evidence_impact_change_rationale": "claimed resolution",
            },
        )


def test_external_evidence_gap_requires_explicit_resolution_evidence_to_close(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(
        ledger,
        finding_id="QM-H-EXT-001",
        category="EXTERNAL_EVIDENCE_GAP",
        evidence_impact="PROMOTION_BLOCKED",
    )
    to_triaged(ledger, "QM-H-EXT-001")
    to_root_cause(ledger, "QM-H-EXT-001")
    to_action_plan(ledger, "QM-H-EXT-001")
    to_implemented(ledger, "QM-H-EXT-001")
    to_verified(ledger, "QM-H-EXT-001")
    with pytest.raises(CapaLedgerError, match="external_evidence_gap_requires_resolution_reference"):
        ledger.transition(
            finding_id="QM-H-EXT-001",
            to_status="CLOSED",
            actor_id="reviewer-2",
            actor_role="qm-reviewer",
            details={
                "capa_id": "QM-H-CAPA-001",
                "closure_reference": "review://close",
                "closure_rationale": "resolution not proven",
                "evidence_disposition_reference": "qm-a://disposition/1",
            },
        )


def test_capa_id_cannot_silently_change(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    to_triaged(ledger)
    to_root_cause(ledger)
    to_action_plan(ledger)
    with pytest.raises(CapaLedgerError, match="capa_id_cannot_change"):
        ledger.transition(
            finding_id="QM-H-DEF-001",
            to_status="IMPLEMENTED",
            actor_id="owner-1",
            actor_role="capa-owner",
            details={
                "capa_id": "QM-H-CAPA-999",
                "implementation_reference": "commit:wrong",
                "implementation_summary": "attempted rekey",
            },
        )


def test_duplicate_finding_id_is_rejected(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    with pytest.raises(CapaLedgerError, match="finding_already_registered"):
        register(ledger)


def test_hash_chain_detects_in_place_tampering(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    original = ledger.path.read_text(encoding="utf-8")
    ledger.path.write_text(original.replace("Historical matcher defect", "tampered defect"), encoding="utf-8")
    with pytest.raises(CapaLedgerError, match="ledger_entry_hash_invalid"):
        ledger.verify_integrity()


def test_integrity_report_lists_open_findings(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    result = ledger.verify_integrity()
    assert result["valid"] is True
    assert result["event_count"] == 1
    assert result["finding_count"] == 1
    assert result["states"]["QM-H-DEF-001"] == "OPEN"
    assert result["open_findings"] == ["QM-H-DEF-001"]
    assert len(result["head_hash"]) == 64
