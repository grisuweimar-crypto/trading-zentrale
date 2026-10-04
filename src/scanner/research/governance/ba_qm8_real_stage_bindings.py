"""BA-QM8 real-artifact stage binding inventory.

This audit reads the checked-in current Scanner-vNext artifacts and binds them to
BA-QM8 stages without inventing missing provenance. Expected privacy boundaries
remain explicit. Coverage gaps are reported as gaps instead of being neutralized
or guessed.
"""
from __future__ import annotations

from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from scanner.reports.daily_research import validate_daily_research
from scanner.reports.scanner_provenance import validate_bound_provenance
from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest


SCHEMA_VERSION = "ba_qm8_real_stage_binding_receipt_v1"
_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_W10_PATH = _ROOT / "artifacts" / "research" / "decision_snapshot_w10.json"
DEFAULT_PACKET_SET_PATH = _ROOT / "artifacts" / "research" / "current_decision_packets_7a.json"
EXTERNAL_CONTRACT_PATH = _ROOT / "configs" / "external_evidence_8_contract_v1.json"
EXTERNAL_8C_COMPLETION_PATH = (
    _ROOT / "artifacts" / "research" / "external_evidence_8c_completion.json"
)


class BAQM8RealBindingError(ValueError):
    """Raised when current real artifacts violate an already explicit contract."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM8RealBindingError(f"real_binding_input_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM8RealBindingError(f"real_binding_input_must_be_object:{path}")
    return value


def _sha256(path: Path) -> str:
    digest = sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise BAQM8RealBindingError(f"real_binding_input_unreadable:{path}") from exc
    return digest.hexdigest()


def _stage(
    *,
    status: str,
    evidence: Mapping[str, Any],
    gaps: list[str] | None = None,
    expected_boundary: bool = False,
) -> dict[str, Any]:
    return {
        "status": status,
        "evidence": dict(evidence),
        "gaps": list(gaps or []),
        "expected_boundary": expected_boundary,
    }


def audit_real_stage_bindings(root: str | Path = _ROOT) -> dict[str, Any]:
    root = Path(root).resolve()
    daily = validate_daily_research(root)
    w10_path = root / "artifacts" / "research" / "decision_snapshot_w10.json"
    packet_path = root / "artifacts" / "research" / "current_decision_packets_7a.json"
    external_contract_path = root / "configs" / "external_evidence_8_contract_v1.json"
    external_8c_path = root / "artifacts" / "research" / "external_evidence_8c_completion.json"

    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise BAQM8RealBindingError("real_binding_daily_snapshot_id_required")

    w10 = validate_sealed_manifest(
        _read_json(w10_path),
        expected_snapshot_id=snapshot_id,
    )
    packet_set = _read_json(packet_path)
    if packet_set.get("schema_version") != "decision_current_packet_set_7a_v1":
        raise BAQM8RealBindingError("real_binding_packet_set_schema_invalid")
    if str(packet_set.get("snapshot_id") or "") != snapshot_id:
        raise BAQM8RealBindingError("real_binding_packet_snapshot_mismatch")

    stages_w10 = w10.get("stages")
    if not isinstance(stages_w10, Mapping):
        raise BAQM8RealBindingError("real_binding_w10_stages_required")
    final_7a = stages_w10.get("final_7a")
    scanner_w10 = stages_w10.get("scanner_daily_research")
    if not isinstance(final_7a, Mapping) or not isinstance(scanner_w10, Mapping):
        raise BAQM8RealBindingError("real_binding_w10_core_stages_missing")
    stored_packet_hash = str(final_7a.get("artifact_sha256") or "")
    current_packet_hash = _sha256(packet_path)
    if not stored_packet_hash or stored_packet_hash != current_packet_hash:
        raise BAQM8RealBindingError("real_binding_final_7a_hash_mismatch")

    raw_packets = packet_set.get("packets")
    if not isinstance(raw_packets, list) or not raw_packets:
        raise BAQM8RealBindingError("real_binding_packets_required")

    family_counts: Counter[str] = Counter()
    claim_ids: set[str] = set()
    selection_per_packet: list[int] = []
    packet_snapshots: set[str] = set()
    invalid_pit_claims: list[str] = []
    for raw in raw_packets:
        if not isinstance(raw, Mapping):
            raise BAQM8RealBindingError("real_binding_packet_must_be_object")
        packet = validate_input_packet(raw)
        packet_snapshots.add(str(packet.get("source_snapshot_id") or ""))
        evidence = packet.get("evidence")
        if not isinstance(evidence, list):
            raise BAQM8RealBindingError("real_binding_packet_evidence_required")
        selection_count = 0
        for row in evidence:
            if not isinstance(row, Mapping):
                raise BAQM8RealBindingError("real_binding_evidence_must_be_object")
            family = str(row.get("family") or "")
            claim_id = str(row.get("claim_id") or "")
            family_counts[family] += 1
            if family == "selection":
                selection_count += 1
            if claim_id in claim_ids:
                raise BAQM8RealBindingError(
                    f"real_binding_duplicate_claim_id:{claim_id}"
                )
            claim_ids.add(claim_id)
            if row.get("pit_state") not in {"verified", "partial"}:
                invalid_pit_claims.append(claim_id)
        selection_per_packet.append(selection_count)

    if packet_snapshots != {snapshot_id}:
        raise BAQM8RealBindingError("real_binding_packet_source_snapshot_mismatch")
    if any(count != 1 for count in selection_per_packet):
        raise BAQM8RealBindingError("real_binding_selection_claim_count_invalid")
    if invalid_pit_claims:
        raise BAQM8RealBindingError(
            "real_binding_pit_claims_invalid:" + ",".join(sorted(invalid_pit_claims))
        )

    semantics = packet_set.get("semantics")
    validation = packet_set.get("validation")
    if not isinstance(semantics, Mapping) or not isinstance(validation, Mapping):
        raise BAQM8RealBindingError("real_binding_packet_contract_sections_missing")

    stage_results: dict[str, dict[str, Any]] = {}

    daily_provenance = daily.get("scanner_input_provenance")
    validated_provenance = None
    data_gaps: list[str] = []
    if isinstance(daily_provenance, Mapping):
        try:
            validated_provenance = validate_bound_provenance(
                root,
                daily_provenance,
                expected_snapshot_id=snapshot_id,
            )
        except Exception as exc:
            raise BAQM8RealBindingError(
                f"real_binding_scanner_provenance_invalid:{exc}"
            ) from exc
    else:
        data_gaps.append("scanner_input_provenance_missing")

    stage_results["DATA"] = _stage(
        status="PASS" if validated_provenance is not None else "PARTIAL",
        evidence={
            "scanner_input_provenance_present": validated_provenance is not None,
            "provenance_path": (
                daily_provenance.get("path")
                if isinstance(daily_provenance, Mapping)
                else None
            ),
            "provenance_sha256": (
                daily_provenance.get("sha256")
                if isinstance(daily_provenance, Mapping)
                else None
            ),
            "direct_scoring_input_sha256": (
                daily_provenance.get("direct_scoring_input_sha256")
                if isinstance(daily_provenance, Mapping)
                else None
            ),
            "scoring_universe_sha256": (
                daily_provenance.get("scoring_universe_sha256")
                if isinstance(daily_provenance, Mapping)
                else None
            ),
            "historical_backfill": (
                daily_provenance.get("historical_backfill")
                if isinstance(daily_provenance, Mapping)
                else None
            ),
        },
        gaps=data_gaps,
    )

    scanner_gaps: list[str] = []
    w10_provenance = scanner_w10.get("scanner_input_provenance")
    if isinstance(daily_provenance, Mapping):
        if not isinstance(w10_provenance, Mapping):
            scanner_gaps.append("w10_scanner_input_provenance_missing")
        elif dict(w10_provenance) != dict(daily_provenance):
            raise BAQM8RealBindingError(
                "real_binding_w10_scanner_provenance_mismatch"
            )
    stage_results["SCANNER"] = _stage(
        status="PASS" if not scanner_gaps else "PARTIAL",
        evidence={
            "schema_version": daily.get("schema_version"),
            "snapshot_id": snapshot_id,
            "source_snapshot_id": daily.get("source_snapshot_id"),
            "generated_at": daily.get("generated_at"),
            "w10_snapshot_identity_state": scanner_w10.get("snapshot_identity_state"),
            "scanner_input_provenance_bound_in_w10": (
                isinstance(daily_provenance, Mapping)
                and isinstance(w10_provenance, Mapping)
                and dict(w10_provenance) == dict(daily_provenance)
            ),
        },
        gaps=scanner_gaps,
    )

    stage_results["SELECTION"] = _stage(
        status="PASS",
        evidence={
            "packet_count": len(raw_packets),
            "selection_claim_count": family_counts["selection"],
            "exactly_one_selection_per_packet": True,
            "source_snapshot_id": snapshot_id,
        },
    )

    timing_count = family_counts["timing"]
    stage_results["TIMING"] = _stage(
        status="PASS" if timing_count > 0 else "PARTIAL",
        evidence={
            "timing_claim_count": timing_count,
            "timing_direction_source": semantics.get("timing_direction_source"),
            "timing_match_from_pit_features": semantics.get(
                "timing_match_from_pit_features"
            ),
        },
        gaps=[] if timing_count > 0 else ["no_current_timing_claims"],
    )

    probability_ok = (
        semantics.get("probability_reconstructed") is False
        and semantics.get("phase2_probability_source_preserved") is True
    )
    stage_results["PROBABILITY"] = _stage(
        status="PASS" if probability_ok else "FAIL",
        evidence={
            "probability_claim_count": family_counts["probability"],
            "probability_reconstructed": semantics.get("probability_reconstructed"),
            "phase2_probability_source_preserved": semantics.get(
                "phase2_probability_source_preserved"
            ),
            "w10_stage_status": (stages_w10.get("phase2_probability") or {}).get(
                "status"
            ),
        },
        gaps=[] if probability_ok else ["probability_source_or_semantics_not_preserved"],
    )

    risk_ok = (
        semantics.get("risk_reconstructed") is False
        and semantics.get("phase3_risk_source_preserved") is True
        and semantics.get("risk_missing_values_backfilled") is False
    )
    stage_results["RISK"] = _stage(
        status="PASS" if risk_ok else "FAIL",
        evidence={
            "risk_claim_count": family_counts["risk"],
            "risk_reconstructed": semantics.get("risk_reconstructed"),
            "phase3_risk_source_preserved": semantics.get(
                "phase3_risk_source_preserved"
            ),
            "risk_missing_values_backfilled": semantics.get(
                "risk_missing_values_backfilled"
            ),
            "w10_stage_status": (stages_w10.get("phase3_risk") or {}).get("status"),
        },
        gaps=[] if risk_ok else ["risk_source_or_missingness_semantics_invalid"],
    )

    confidence_ok = (
        semantics.get("confidence_reconstructed") is False
        and semantics.get("phase4_confidence_annotations_attached") is True
        and semantics.get("confidence_is_directional_vote") is False
        and semantics.get("confidence_encodes_attractiveness") is False
        and validation.get("phase4_same_snapshot_verified") is True
        and str(validation.get("phase4_snapshot_id") or "") == snapshot_id
    )
    stage_results["CONFIDENCE"] = _stage(
        status="PASS" if confidence_ok else "FAIL",
        evidence={
            "confidence_claim_count": family_counts["confidence"],
            "phase4_same_snapshot_verified": validation.get(
                "phase4_same_snapshot_verified"
            ),
            "confidence_is_directional_vote": semantics.get(
                "confidence_is_directional_vote"
            ),
            "w10_stage_status": (stages_w10.get("phase4_confidence") or {}).get(
                "status"
            ),
        },
        gaps=[] if confidence_ok else ["confidence_snapshot_or_semantics_invalid"],
    )

    phase5 = stages_w10.get("phase5_governance")
    phase5 = phase5 if isinstance(phase5, Mapping) else {}
    learning_ok = (
        phase5.get("status") == "available"
        and bool(str(phase5.get("source_commit") or ""))
        and phase5.get("snapshot_identity_state") == "not_market_snapshot_evidence"
        and semantics.get("phase5_shadow_context_attached") is True
        and semantics.get("phase5_current_symbol_policy_evaluated") is False
        and semantics.get("phase5_production_change_performed") is False
        and semantics.get("phase5_unpromoted_changes_decision") is False
    )
    stage_results["LEARNING"] = _stage(
        status="PASS" if learning_ok else "FAIL",
        evidence={
            "w10_stage_status": phase5.get("status"),
            "source_commit": phase5.get("source_commit"),
            "snapshot_identity_state": phase5.get("snapshot_identity_state"),
            "phase5_shadow_context_attached": semantics.get(
                "phase5_shadow_context_attached"
            ),
            "phase5_unpromoted_changes_decision": semantics.get(
                "phase5_unpromoted_changes_decision"
            ),
        },
        gaps=[] if learning_ok else ["phase5_governance_boundary_invalid"],
    )

    phase6 = stages_w10.get("phase6_elliott")
    if not isinstance(phase6, Mapping):
        raise BAQM8RealBindingError("real_binding_phase6_stage_missing")
    phase6_status = str(phase6.get("status") or "")
    if phase6_status == "not_supplied":
        elliott_ok = (
            phase6.get("missing_is_neutral_evidence") is False
            and phase6.get("decision_effect") == "none"
            and semantics.get("elliott_unpromoted_changes_decision") is False
        )
        elliott_boundary = "explicit_missing_not_neutral"
    elif phase6_status == "available":
        elliott_ok = (
            phase6.get("multi_degree_reducer_used") is False
            and phase6.get("elliott_direction_used_as_vote") is False
            and phase6.get("decision_effect") == "review_only_downstream_of_7d_7e"
            and semantics.get("elliott_unpromoted_changes_decision") is False
        )
        elliott_boundary = "review_only"
    else:
        elliott_ok = False
        elliott_boundary = "invalid"
    stage_results["ELLIOTT"] = _stage(
        status="PASS_BOUNDARY" if elliott_ok else "FAIL",
        evidence={
            "w10_stage_status": phase6_status,
            "boundary": elliott_boundary,
            "decision_effect": phase6.get("decision_effect"),
            "missing_is_neutral_evidence": phase6.get(
                "missing_is_neutral_evidence"
            ),
        },
        gaps=[] if elliott_ok else ["elliott_boundary_invalid"],
        expected_boundary=phase6_status == "not_supplied",
    )

    downstream = w10.get("downstream_contract")
    if not isinstance(downstream, Mapping):
        raise BAQM8RealBindingError("real_binding_w10_downstream_contract_missing")
    decision_contract_ok = (
        list(downstream.get("chain") or []) == ["7D", "7E", "7F", "7G", "7H"]
        and downstream.get("requires_same_snapshot_id") is True
        and downstream.get("private_position_data_persisted") is False
        and bool(str(downstream.get("private_runner") or ""))
    )
    stage_results["DECISION_LAYER"] = _stage(
        status="PASS_PRIVACY_BOUNDARY" if decision_contract_ok else "FAIL",
        evidence={
            "final_7a_hash_verified": True,
            "downstream_chain": downstream.get("chain"),
            "requires_same_snapshot_id": downstream.get("requires_same_snapshot_id"),
            "private_position_data_persisted": downstream.get(
                "private_position_data_persisted"
            ),
            "private_runner": downstream.get("private_runner"),
            "public_current_private_receipt_expected": False,
        },
        gaps=[] if decision_contract_ok else ["decision_private_runtime_contract_invalid"],
        expected_boundary=True,
    )

    external_contract = _read_json(external_contract_path)
    external_8c = _read_json(external_8c_path)
    external_semantics = external_contract.get("external_evidence_semantics")
    external_hard = external_8c.get("hard_boundaries")
    if not isinstance(external_semantics, Mapping) or not isinstance(
        external_hard, Mapping
    ):
        raise BAQM8RealBindingError("real_binding_external_contract_sections_missing")
    external_ok = (
        semantics.get("external_evidence_phase8_activated") is False
        and external_hard.get("production_external_evidence_enabled") is False
        and external_hard.get("phase7_integration_enabled") is False
        and external_hard.get("market_direction_assigned") is False
        and external_semantics.get("may_directly_replace_portfolio_action") is False
        and external_semantics.get("may_generate_order") is False
    )
    stage_results["EXTERNAL_EVIDENCE"] = _stage(
        status="PASS_DISABLED_BOUNDARY" if external_ok else "FAIL",
        evidence={
            "phase8_activated_in_7a": semantics.get(
                "external_evidence_phase8_activated"
            ),
            "production_external_evidence_enabled": external_hard.get(
                "production_external_evidence_enabled"
            ),
            "phase7_integration_enabled": external_hard.get(
                "phase7_integration_enabled"
            ),
            "may_generate_order": external_semantics.get("may_generate_order"),
        },
        gaps=[] if external_ok else ["external_evidence_boundary_invalid"],
        expected_boundary=True,
    )

    hard_failures = sorted(
        stage for stage, result in stage_results.items() if result["status"] == "FAIL"
    )
    coverage_gaps = {
        stage: result["gaps"]
        for stage, result in stage_results.items()
        if result["gaps"]
    }
    if hard_failures:
        raise BAQM8RealBindingError(
            "real_binding_hard_stage_failure:" + ",".join(hard_failures)
        )

    closure_blockers = sorted(
        stage
        for stage, result in stage_results.items()
        if result["status"] == "PARTIAL"
    )
    status = (
        "REAL_STAGE_BINDINGS_PASS"
        if not closure_blockers
        else "REAL_STAGE_BINDINGS_PARTIAL_GAPS_OPEN"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "snapshot_id": snapshot_id,
        "snapshot_as_of": daily.get("as_of"),
        "w10_status": w10.get("status"),
        "final_7a_artifact_sha256": current_packet_hash,
        "packet_count": len(raw_packets),
        "claim_count": len(claim_ids),
        "family_claim_counts": dict(sorted(family_counts.items())),
        "stage_results": stage_results,
        "coverage_gaps": coverage_gaps,
        "closure_blockers": closure_blockers,
        "closure_eligible": not closure_blockers,
        "missing_treated_as_neutral": False,
        "guessed_lineage_added": False,
        "private_position_data_persisted": False,
        "external_evidence_productively_integrated": False,
    }
