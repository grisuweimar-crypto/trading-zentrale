"""BA-QM7 / QM-J Decision-Layer and isolated end-to-end falsification.

Research-only. The module attacks two separate invariants:
1) unadmitted placebo sidecar evidence must have zero Decision/Watch effect;
2) an effective destruction of admitted Selection/Timing direction must change at
   least one affected Universal Stance, otherwise the pipeline is insensitive to
   the information it claims to interpret.

No productive artifact, Decision rule, position data or order path is mutated.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.w11_end_to_end import validate_w11_end_to_end
from scanner.research.governance.qm_j_negative_controls import (
    NegativeControlError,
    content_hash,
    destroyed_information_control,
    placebo_evidence_control,
)

SCHEMA_VERSION = "qm_j_decision_e2e_falsification_v1"
RESULT_SCHEMA_VERSION = "qm_j_decision_e2e_falsification_result_v1"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_j_decision_e2e_falsification_v1.json"
DIRECTIONAL_FAMILIES = {"selection", "timing"}
ELIGIBLE_COVERAGE = {"available", "limited"}
ELIGIBLE_PIT = {"verified", "partial"}
ELIGIBLE_MATURITY = {"robust", "directional_but_immature", "not_applicable"}


@dataclass(frozen=True)
class DecisionE2EPlan:
    frozen_against_main_commit: str
    placebo_seed: str
    destroyed_seed: str
    trigger_status: str
    nontrigger_status: str


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise NegativeControlError(f"qm_j_decision_e2e_{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise NegativeControlError(f"qm_j_decision_e2e_invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_plan(path: str | Path | None = None) -> DecisionE2EPlan:
    target = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NegativeControlError(f"qm_j_decision_e2e_config_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise NegativeControlError("qm_j_decision_e2e_config_schema_invalid")
    if payload.get("status") != "frozen_before_execution":
        raise NegativeControlError("qm_j_decision_e2e_plan_not_frozen")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise NegativeControlError("qm_j_decision_e2e_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise NegativeControlError("qm_j_decision_e2e_execution_forbidden")
    placebo = payload.get("placebo_sidecar_test") or {}
    destroyed = payload.get("destroyed_information_test") or {}
    gate = destroyed.get("control_effectiveness_gate") or {}
    if placebo.get("primary_rule") != "ZERO_DECISION_EFFECT" or placebo.get("trigger_on_any_change") is not True:
        raise NegativeControlError("qm_j_decision_placebo_rule_invalid")
    if destroyed.get("primary_rule") != "MUST_BE_GREATER_THAN_ZERO_AFTER_EFFECTIVE_DESTRUCTION":
        raise NegativeControlError("qm_j_destroyed_information_rule_invalid")
    if int(gate.get("minimum_changed_directional_claims", 0)) != 1 or int(gate.get("minimum_changed_directional_symbols", 0)) != 1:
        raise NegativeControlError("qm_j_destroyed_information_effectiveness_gate_invalid")
    commit = str(payload.get("frozen_against_main_commit") or "").strip().lower()
    if len(commit) != 40:
        raise NegativeControlError("qm_j_decision_e2e_frozen_commit_invalid")
    return DecisionE2EPlan(
        frozen_against_main_commit=commit,
        placebo_seed=str(placebo.get("seed") or ""),
        destroyed_seed=str(destroyed.get("seed") or ""),
        trigger_status=str(payload.get("trigger_status") or ""),
        nontrigger_status=str(payload.get("nontrigger_status") or ""),
    )


def _current_packet_indices(
    archive_packets: Sequence[Mapping[str, object]], snapshot_id: str
) -> list[int]:
    indices: list[int] = []
    seen: set[str] = set()
    for index, raw in enumerate(archive_packets):
        packet = validate_input_packet(raw)
        if str(packet.get("source_snapshot_id") or "") != snapshot_id:
            continue
        symbol = str(packet.get("symbol") or "")
        if symbol in seen:
            raise NegativeControlError(f"qm_j_duplicate_current_packet_symbol:{symbol}")
        seen.add(symbol)
        indices.append(index)
    if not indices:
        raise NegativeControlError("qm_j_current_snapshot_packets_missing")
    return indices


def _decision_as_of(archive_packets: Sequence[Mapping[str, object]], indices: Sequence[int]) -> str:
    times = {_utc(archive_packets[index].get("as_of"), "packet_as_of") for index in indices}
    if len(times) != 1:
        raise NegativeControlError("qm_j_current_snapshot_decision_time_not_unique")
    return next(iter(times)).isoformat()


def build_synthetic_long_position_book(
    daily: Mapping[str, object], archive_packets: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise NegativeControlError("qm_j_daily_snapshot_id_required")
    symbols_payload = daily.get("symbols")
    if not isinstance(symbols_payload, Mapping):
        raise NegativeControlError("qm_j_daily_symbols_required")
    indices = _current_packet_indices(archive_packets, snapshot_id)
    decision_as_of = _decision_as_of(archive_packets, indices)
    symbols = sorted(
        str(archive_packets[index].get("symbol") or "")
        for index in indices
        if str(archive_packets[index].get("symbol") or "") in symbols_payload
    )
    if not symbols:
        raise NegativeControlError("qm_j_synthetic_position_universe_empty")
    positions = [{
        "schema_version": "decision_position_snapshot_v1",
        "symbol": symbol,
        "source_snapshot_id": f"qm-j-synthetic-long:{snapshot_id}:{symbol}",
        "as_of": decision_as_of,
        "position_state": "long",
        "quantity": 1,
        "can_add": None,
        "remaining_adds": None,
    } for symbol in symbols]
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": f"qm-j-synthetic-long:{snapshot_id}",
        "as_of": decision_as_of,
        "positions": positions,
    }


def _watch_signature(watch: Mapping[str, object]) -> dict[str, dict[str, object]]:
    """Read preserved 7D/7F semantics from the validated 7H Watch once.

    `build_orchestrated_depot_watch` already builds the canonical bundle set and
    then copies Universal Stance and Portfolio Action into each decision row.
    Reading those fields here avoids a second, semantically redundant full
    `build_decision_bundle_set` pass without changing the tested Decision path.
    """
    rows = watch.get("rows")
    if not isinstance(rows, list):
        raise NegativeControlError("qm_j_watch_rows_invalid")
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise NegativeControlError("qm_j_watch_row_invalid")
        symbol = str(row.get("symbol") or "")
        decision = row.get("decision")
        if not isinstance(decision, Mapping):
            raise NegativeControlError(f"qm_j_watch_decision_missing:{symbol}")
        result[symbol] = {
            "universal_stance_state": decision.get("universal_stance_state"),
            "universal_stance_direction": decision.get("universal_stance_direction"),
            "portfolio_action_state": decision.get("portfolio_action_state"),
            "portfolio_action_reason_code": decision.get("portfolio_action_reason_code"),
            "presentation_group": row.get("presentation_group"),
            "attention_required": row.get("attention_required"),
        }
    return result


def _run_chain(
    *,
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    packets: Sequence[Mapping[str, object]],
    manifest: Mapping[str, object],
) -> dict[str, object]:
    watch, diagnostics = build_orchestrated_depot_watch(daily, position_book, packets)
    receipt = validate_w11_end_to_end(
        manifest=manifest,
        archive_packets=packets,
        watch=watch,
        diagnostics=diagnostics,
    )
    return {
        "watch_signature": _watch_signature(watch),
        "watch_status": watch.get("watch_status"),
        "w11_receipt": receipt,
        "diagnostics": diagnostics,
    }


def _changed_symbols(
    left: Mapping[str, Mapping[str, object]],
    right: Mapping[str, Mapping[str, object]],
    fields: Sequence[str],
) -> list[str]:
    if set(left) != set(right):
        raise NegativeControlError("qm_j_comparison_symbol_grid_changed")
    changed: list[str] = []
    for symbol in sorted(left):
        if any(left[symbol].get(field) != right[symbol].get(field) for field in fields):
            changed.append(symbol)
    return changed


def _eligible_direction_records(
    archive_packets: Sequence[Mapping[str, object]], current_indices: Sequence[int]
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for packet_index in current_indices:
        packet = validate_input_packet(archive_packets[packet_index])
        evidence = packet.get("evidence")
        assert isinstance(evidence, list)
        for evidence_index, row in enumerate(evidence):
            assert isinstance(row, Mapping)
            if row.get("family") not in DIRECTIONAL_FAMILIES:
                continue
            if row.get("coverage_state") not in ELIGIBLE_COVERAGE or row.get("pit_state") not in ELIGIBLE_PIT:
                continue
            if row.get("maturity_state") not in ELIGIBLE_MATURITY:
                continue
            payload = row.get("payload")
            direction = str(payload.get("direction") or "").strip().lower() if isinstance(payload, Mapping) else ""
            if direction not in {"positive", "negative"}:
                continue
            records.append({
                "packet_index": packet_index,
                "evidence_index": evidence_index,
                "symbol": str(packet.get("symbol") or ""),
                "claim_id": str(row.get("claim_id") or ""),
                "family": str(row.get("family") or ""),
                "direction": direction,
            })
    return records


def _destroy_current_directions(
    archive_packets: Sequence[Mapping[str, object]],
    *,
    snapshot_id: str,
    seed: str,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    copied = [deepcopy(dict(packet)) for packet in archive_packets]
    current_indices = _current_packet_indices(copied, snapshot_id)
    records = _eligible_direction_records(copied, current_indices)
    if len(records) < 2 or len({str(row["direction"]) for row in records}) < 2:
        return copied, {
            "effective": False,
            "reason": "insufficient_directional_variation_for_permutation",
            "eligible_directional_claims": len(records),
            "changed_directional_claims": 0,
            "changed_directional_symbols": [],
            "control_artifact_hash": None,
        }
    artifact = destroyed_information_control(
        records,
        source_snapshot_id=snapshot_id,
        predictive_fields=["direction"],
        identity_fields=["symbol", "claim_id"],
        protected_fields=["packet_index", "evidence_index", "symbol", "claim_id", "family"],
        seed=seed,
    )
    transformed = artifact["records"]
    changed_symbols: set[str] = set()
    changed_claims = 0
    for original, changed in zip(records, transformed):
        if changed["direction"] == original["direction"]:
            continue
        packet_index = int(changed["packet_index"])
        evidence_index = int(changed["evidence_index"])
        payload = copied[packet_index]["evidence"][evidence_index]["payload"]
        if not isinstance(payload, dict):
            raise NegativeControlError("qm_j_direction_payload_not_mutable_copy")
        payload["direction"] = changed["direction"]
        changed_claims += 1
        changed_symbols.add(str(changed["symbol"]))
    return copied, {
        "effective": changed_claims >= 1 and bool(changed_symbols),
        "reason": None if changed_claims else "deterministic_permutation_changed_no_direction_values",
        "eligible_directional_claims": len(records),
        "changed_directional_claims": changed_claims,
        "changed_directional_symbols": sorted(changed_symbols),
        "control_artifact_hash": artifact.get("artifact_hash"),
        "source_content_hash": artifact.get("source_content_hash"),
        "control_content_hash": artifact.get("control_content_hash"),
    }


def run_falsification(
    *,
    daily: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
    manifest: Mapping[str, object],
    plan: DecisionE2EPlan | None = None,
) -> dict[str, object]:
    plan = plan or load_plan()
    packets = [deepcopy(dict(packet)) for packet in archive_packets]
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    position_book = build_synthetic_long_position_book(daily, packets)
    baseline = _run_chain(daily=daily, position_book=position_book, packets=packets, manifest=manifest)

    placebo_artifact = placebo_evidence_control(
        packets,
        source_snapshot_id=snapshot_id,
        identity_fields=["symbol", "source_snapshot_id", "as_of"],
        evidence_field="qm_j_placebo_evidence_sidecar",
        seed=plan.placebo_seed,
    )
    placebo_packets = placebo_artifact["records"]
    placebo = _run_chain(daily=daily, position_book=position_book, packets=placebo_packets, manifest=manifest)
    placebo_stance_changes = _changed_symbols(
        baseline["watch_signature"], placebo["watch_signature"],
        ["universal_stance_state", "universal_stance_direction"],
    )
    placebo_action_changes = _changed_symbols(
        baseline["watch_signature"], placebo["watch_signature"],
        ["portfolio_action_state", "portfolio_action_reason_code"],
    )
    placebo_watch_changes = _changed_symbols(
        baseline["watch_signature"], placebo["watch_signature"],
        ["universal_stance_state", "universal_stance_direction", "portfolio_action_state", "portfolio_action_reason_code", "presentation_group", "attention_required"],
    )
    placebo_trigger = bool(placebo_stance_changes or placebo_action_changes or placebo_watch_changes)

    destroyed_packets, destruction = _destroy_current_directions(
        packets, snapshot_id=snapshot_id, seed=plan.destroyed_seed
    )
    destroyed: dict[str, object] | None = None
    destroyed_stance_changes: list[str] = []
    destroyed_action_changes: list[str] = []
    destroyed_watch_changes: list[str] = []
    destroyed_trigger = False
    if destruction["effective"]:
        destroyed = _run_chain(daily=daily, position_book=position_book, packets=destroyed_packets, manifest=manifest)
        affected = set(str(symbol) for symbol in destruction["changed_directional_symbols"])
        all_stance_changes = _changed_symbols(
            baseline["watch_signature"], destroyed["watch_signature"],
            ["universal_stance_state", "universal_stance_direction"],
        )
        destroyed_stance_changes = sorted(affected & set(all_stance_changes))
        destroyed_action_changes = _changed_symbols(
            baseline["watch_signature"], destroyed["watch_signature"],
            ["portfolio_action_state", "portfolio_action_reason_code"],
        )
        destroyed_watch_changes = _changed_symbols(
            baseline["watch_signature"], destroyed["watch_signature"],
            ["universal_stance_state", "universal_stance_direction", "portfolio_action_state", "portfolio_action_reason_code", "presentation_group", "attention_required"],
        )
        destroyed_trigger = len(destroyed_stance_changes) == 0

    triggered = placebo_trigger or destroyed_trigger
    if not destruction["effective"] and not placebo_trigger:
        overall_status = "BLOCKED_CONTROL_INEFFECTIVE"
    else:
        overall_status = plan.trigger_status if triggered else plan.nontrigger_status
    affected_count = len(destruction["changed_directional_symbols"])
    preservation_rate = None
    if destruction["effective"] and affected_count:
        preservation_rate = 1.0 - len(destroyed_stance_changes) / affected_count

    result: dict[str, object] = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "application": "DECISION_LAYER_AND_ISOLATED_END_TO_END_FALSIFICATION",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "snapshot_id": snapshot_id,
        "plan": {
            "frozen_against_main_commit": plan.frozen_against_main_commit,
            "placebo_seed": plan.placebo_seed,
            "destroyed_seed": plan.destroyed_seed,
        },
        "synthetic_position_book": {
            "position_state": "long",
            "position_count": len(position_book["positions"]),
            "real_private_positions_used": False,
            "persisted": False,
        },
        "baseline": {
            "bundle_count": len(baseline["watch_signature"]),
            "watch_status": baseline["watch_status"],
            "w11_status": baseline["w11_receipt"]["status"],
            "semantic_signature_hash": content_hash(baseline["watch_signature"]),
        },
        "placebo_sidecar": {
            "control_artifact_hash": placebo_artifact.get("artifact_hash"),
            "stance_changed_symbols": placebo_stance_changes,
            "portfolio_action_changed_symbols": placebo_action_changes,
            "watch_semantics_changed_symbols": placebo_watch_changes,
            "zero_decision_effect": not placebo_trigger,
            "w11_status": placebo["w11_receipt"]["status"],
            "triggered": placebo_trigger,
        },
        "destroyed_information": {
            **destruction,
            "affected_universal_stance_change_count": len(destroyed_stance_changes),
            "affected_universal_stance_changed_symbols": destroyed_stance_changes,
            "directional_stance_preservation_rate": preservation_rate,
            "portfolio_action_change_count": len(destroyed_action_changes),
            "portfolio_action_changed_symbols": destroyed_action_changes,
            "watch_action_or_stance_change_count": len(destroyed_watch_changes),
            "watch_changed_symbols": destroyed_watch_changes,
            "w11_status": None if destroyed is None else destroyed["w11_receipt"]["status"],
            "triggered": destroyed_trigger,
        },
        "status": overall_status,
        "promotion_blocked_by_qm_j": triggered,
        "investigation_required": triggered,
        "capa_required": triggered,
        "diagnostic_classes_if_triggered": ["PIPELINE_BIAS"] if triggered else [],
        "w11_acceptance_interpretation": "INTEGRITY_ONLY_NOT_PREDICTIVE_VALIDATION",
        "production_artifacts_mutated": False,
        "promotion_performed": False,
    }
    result["result_hash"] = content_hash(result)
    return result
