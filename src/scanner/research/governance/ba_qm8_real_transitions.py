"""BA-QM8 real transition observations for the current sealed Scanner-vNext path.

This adapter converts already validated repository artifacts into canonical
transition observations and sends them through the seven-class fail-closed
guard. It does not create investment evidence. Audit IDs are deterministic
digests of existing claim IDs / artifact hashes so ancestry remains explicit.

For historical snapshots that predate scanner_input_provenance, DATA->SCANNER
is reported blocked rather than reconstructed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from scanner.reports.daily_research import validate_daily_research
from scanner.reports.scanner_provenance import validate_bound_provenance
from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest
from scanner.research.governance.ba_qm8_end_to_end import load_contract
from scanner.research.governance.ba_qm8_transition_guard import (
    BAQM8TransitionError,
    validate_transition_observation,
)


SCHEMA_VERSION = "ba_qm8_real_transition_audit_v1"
_ROOT = Path(__file__).resolve().parents[4]


class BAQM8RealTransitionError(ValueError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM8RealTransitionError(f"real_transition_input_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM8RealTransitionError(f"real_transition_input_must_be_object:{path}")
    return value


def _file_sha(path: Path) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise BAQM8RealTransitionError(f"real_transition_input_unreadable:{path}") from exc


def _aware(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise BAQM8RealTransitionError(f"{field}_required")
    if len(text) == 10:
        text += "T00:00:00+00:00"
    elif text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise BAQM8RealTransitionError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _digest(value: object) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def _stage_roles() -> dict[str, str]:
    contract = load_contract()
    return {
        str(row["stage_id"]): str(row["semantic_role"])
        for row in contract["stage_chain"]
    }


def _family_summary(
    packets: list[dict[str, Any]],
    family: str,
    *,
    fallback_time: datetime,
) -> dict[str, Any]:
    claims: list[dict[str, Any]] = []
    missing_symbols: list[str] = []
    multiplicity = False
    for packet in packets:
        symbol = str(packet.get("symbol") or "")
        rows = [
            row for row in packet.get("evidence", [])
            if isinstance(row, Mapping) and row.get("family") == family
        ]
        if not rows:
            missing_symbols.append(symbol)
        if len(rows) > 1:
            multiplicity = True
        for row in rows:
            claims.append({
                "claim_id": str(row.get("claim_id") or ""),
                "as_of": str(row.get("as_of") or ""),
                "available_from": str(row.get("available_from") or ""),
                "source_version": str(row.get("source_version") or ""),
                "claim_ref": str(row.get("claim_ref") or "") or None,
                "integration_mode": str(row.get("integration_mode") or ""),
                "pit_state": str(row.get("pit_state") or ""),
            })

    claim_ids = [row["claim_id"] for row in claims]
    if any(not value for value in claim_ids):
        raise BAQM8RealTransitionError(f"real_transition_blank_claim_id:{family}")
    if len(set(claim_ids)) != len(claim_ids):
        raise BAQM8RealTransitionError(f"real_transition_duplicate_claim_id:{family}")

    if claims:
        native_id = f"claims:{family}:{_digest(claims)}"
        as_of = max(_aware(row["as_of"], f"{family}_as_of") for row in claims)
        available = max(
            _aware(row["available_from"], f"{family}_available_from")
            for row in claims
        )
    else:
        native_id = f"claims:{family}:explicit-none:{_digest(missing_symbols)}"
        as_of = fallback_time
        available = fallback_time

    missing_ids: list[str] = []
    if missing_symbols:
        missing_ids.append(
            f"missing:{family}:symbols:{_digest(sorted(missing_symbols))}"
        )
    return {
        "native_ids": [native_id],
        "claim_count": len(claims),
        "missing_symbol_count": len(missing_symbols),
        "missing_input_ids": missing_ids,
        "as_of": as_of,
        "available_from": available,
        "multiplicity_applicable": multiplicity,
        "multiplicity_control_state": (
            "FROZEN_PREDECLARED" if multiplicity else "NOT_APPLICABLE"
        ),
    }


def _artifact_native(
    label: str,
    block: Mapping[str, Any],
    *,
    fallback_time: datetime,
) -> dict[str, Any]:
    artifact_hash = str(block.get("artifact_sha256") or "").strip()
    status = str(block.get("status") or "")
    if status == "not_supplied":
        return {
            "native_ids": [f"{label}:explicit-not-supplied"],
            "missing_input_ids": [f"missing:{label}:not-supplied"],
            "as_of": fallback_time,
            "available_from": _aware(
                block.get("resolved_at") or fallback_time.isoformat(),
                f"{label}_resolved_at",
            ),
            "multiplicity_applicable": False,
            "multiplicity_control_state": "NOT_APPLICABLE",
        }
    if not artifact_hash:
        raise BAQM8RealTransitionError(f"{label}_artifact_hash_required")
    available = _aware(
        block.get("available_from") or fallback_time.isoformat(),
        f"{label}_available_from",
    )
    return {
        "native_ids": [f"artifact:{label}:{artifact_hash}"],
        "missing_input_ids": [],
        "as_of": fallback_time,
        "available_from": available,
        "multiplicity_applicable": False,
        "multiplicity_control_state": "NOT_APPLICABLE",
    }


def _stage_descriptor(
    stage_id: str,
    *,
    roles: Mapping[str, str],
    snapshot_id: str,
    prior: Mapping[str, Any] | None,
    native: Mapping[str, Any],
) -> dict[str, Any]:
    prior_evidence = list(prior.get("material_evidence_ids", [])) if prior else []
    native_ids = list(native.get("native_ids", []))
    evidence = prior_evidence + native_ids
    if len(set(evidence)) != len(evidence):
        raise BAQM8RealTransitionError(f"real_transition_evidence_id_collision:{stage_id}")

    native_as_of = native["as_of"]
    native_available = native["available_from"]
    if prior:
        as_of = max(_aware(prior["as_of"], "prior_as_of"), native_as_of)
        available = max(
            _aware(prior["available_from"], "prior_available_from"),
            native_available,
        )
    else:
        as_of = native_as_of
        available = native_available

    return {
        "stage_id": stage_id,
        "semantic_role": roles[stage_id],
        "snapshot_id": snapshot_id,
        "source_snapshot_id": snapshot_id,
        "as_of": _iso(as_of),
        "available_from": _iso(available),
        "material_evidence_ids": evidence,
        "native_evidence_ids": native_ids,
        "missing_input_ids": list(native.get("missing_input_ids", [])),
        "multiplicity_applicable": bool(native.get("multiplicity_applicable", False)),
        "multiplicity_control_state": str(
            native.get("multiplicity_control_state") or "NOT_APPLICABLE"
        ),
    }


def _transition_observation(
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    *,
    double_counting_resolution: str,
    snapshot_binding_required: bool = True,
) -> dict[str, Any]:
    return {
        "schema_version": "ba_qm8_transition_observation_v1",
        "transition_id": (
            f"real:{source['stage_id']}->{target['stage_id']}:"
            f"{target['snapshot_id']}"
        ),
        "from_stage": source["stage_id"],
        "to_stage": target["stage_id"],
        "source": {
            "semantic_role": source["semantic_role"],
            "snapshot_id": source["snapshot_id"],
            "as_of": source["as_of"],
            "available_from": source["available_from"],
            "material_evidence_ids": list(source["material_evidence_ids"]),
        },
        "target": {
            "semantic_role": target["semantic_role"],
            "source_snapshot_id": target["source_snapshot_id"],
            "as_of": target["as_of"],
            "available_from": target["available_from"],
            "declared_input_stage": source["stage_id"],
            "material_evidence_ids": list(target["material_evidence_ids"]),
            "declared_input_evidence_ids": list(source["material_evidence_ids"]),
            "target_native_evidence_ids": list(target["native_evidence_ids"]),
        },
        "controls": {
            "backdating_allowed": False,
            "retrojected_value_ids": [],
            "double_counting_resolution": double_counting_resolution,
            "semantic_contract_match": True,
            "missing_input_ids": list(target["missing_input_ids"]),
            "neutralized_missing_input_ids": [],
            "missing_is_neutral": False,
            "multiplicity_applicable": target["multiplicity_applicable"],
            "multiplicity_control_state": target["multiplicity_control_state"],
            "snapshot_binding_required": snapshot_binding_required,
        },
    }


def audit_real_transitions(root: str | Path = _ROOT) -> dict[str, Any]:
    root = Path(root).resolve()
    roles = _stage_roles()
    daily = validate_daily_research(root)
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise BAQM8RealTransitionError("real_transition_snapshot_id_required")
    scanner_time = _aware(daily.get("generated_at"), "daily_generated_at")

    w10 = validate_sealed_manifest(
        _read_json(root / "artifacts/research/decision_snapshot_w10.json"),
        expected_snapshot_id=snapshot_id,
    )
    stages_w10 = w10.get("stages")
    if not isinstance(stages_w10, Mapping):
        raise BAQM8RealTransitionError("real_transition_w10_stages_required")

    packet_set_path = root / "artifacts/research/current_decision_packets_7a.json"
    packet_set = _read_json(packet_set_path)
    if packet_set.get("schema_version") != "decision_current_packet_set_7a_v1":
        raise BAQM8RealTransitionError("real_transition_packet_set_schema_invalid")
    if str(packet_set.get("snapshot_id") or "") != snapshot_id:
        raise BAQM8RealTransitionError("real_transition_packet_snapshot_mismatch")
    raw_packets = packet_set.get("packets")
    if not isinstance(raw_packets, list) or not raw_packets:
        raise BAQM8RealTransitionError("real_transition_packets_required")

    packets: list[dict[str, Any]] = []
    for raw in raw_packets:
        if not isinstance(raw, Mapping):
            raise BAQM8RealTransitionError("real_transition_packet_must_be_object")
        packets.append(validate_input_packet(raw))

    scanner_native = {
        "native_ids": [
            f"artifact:daily_research:{_file_sha(root / 'artifacts/research/daily_research.json')}",
            f"snapshot:{snapshot_id}",
        ],
        "missing_input_ids": [],
        "as_of": scanner_time,
        "available_from": scanner_time,
        "multiplicity_applicable": False,
        "multiplicity_control_state": "NOT_APPLICABLE",
    }
    stage_map: dict[str, dict[str, Any]] = {}
    stage_map["SCANNER"] = _stage_descriptor(
        "SCANNER",
        roles=roles,
        snapshot_id=snapshot_id,
        prior=None,
        native=scanner_native,
    )

    prior = stage_map["SCANNER"]
    family_stages = (
        ("SELECTION", "selection"),
        ("TIMING", "timing"),
        ("PROBABILITY", "probability"),
        ("RISK", "risk"),
        ("CONFIDENCE", "confidence"),
    )
    family_summaries: dict[str, dict[str, Any]] = {}
    for stage_id, family in family_stages:
        summary = _family_summary(packets, family, fallback_time=scanner_time)
        family_summaries[family] = summary
        stage_map[stage_id] = _stage_descriptor(
            stage_id,
            roles=roles,
            snapshot_id=snapshot_id,
            prior=prior,
            native=summary,
        )
        prior = stage_map[stage_id]

    phase5 = stages_w10.get("phase5_governance")
    if not isinstance(phase5, Mapping):
        raise BAQM8RealTransitionError("real_transition_phase5_missing")
    learning_native = _artifact_native(
        "phase5_governance",
        phase5,
        fallback_time=scanner_time,
    )
    stage_map["LEARNING"] = _stage_descriptor(
        "LEARNING",
        roles=roles,
        snapshot_id=snapshot_id,
        prior=prior,
        native=learning_native,
    )
    prior = stage_map["LEARNING"]

    phase6 = stages_w10.get("phase6_elliott")
    if not isinstance(phase6, Mapping):
        raise BAQM8RealTransitionError("real_transition_phase6_missing")
    elliott_claims = _family_summary(packets, "elliott", fallback_time=scanner_time)
    phase6_native = _artifact_native(
        "phase6_elliott",
        phase6,
        fallback_time=scanner_time,
    )
    elliott_native = {
        "native_ids": list(dict.fromkeys(
            elliott_claims["native_ids"] + phase6_native["native_ids"]
        )),
        "missing_input_ids": list(dict.fromkeys(
            elliott_claims["missing_input_ids"] + phase6_native["missing_input_ids"]
        )),
        "as_of": max(elliott_claims["as_of"], phase6_native["as_of"]),
        "available_from": max(
            elliott_claims["available_from"],
            phase6_native["available_from"],
        ),
        "multiplicity_applicable": elliott_claims["multiplicity_applicable"],
        "multiplicity_control_state": elliott_claims["multiplicity_control_state"],
    }
    stage_map["ELLIOTT"] = _stage_descriptor(
        "ELLIOTT",
        roles=roles,
        snapshot_id=snapshot_id,
        prior=prior,
        native=elliott_native,
    )
    prior = stage_map["ELLIOTT"]

    final7a = stages_w10.get("final_7a")
    archive7a = stages_w10.get("phase7a_archive")
    if not isinstance(final7a, Mapping) or not isinstance(archive7a, Mapping):
        raise BAQM8RealTransitionError("real_transition_decision_artifacts_missing")
    decision_native = {
        "native_ids": [
            f"artifact:final_7a:{final7a.get('artifact_sha256')}",
            f"artifact:phase7a_archive:{archive7a.get('artifact_sha256')}",
        ],
        "missing_input_ids": [],
        "as_of": _aware(
            packet_set.get("as_of") or final7a.get("available_from"),
            "decision_as_of",
        ),
        "available_from": max(
            _aware(final7a.get("available_from"), "final7a_available_from"),
            _aware(archive7a.get("available_from"), "archive7a_available_from"),
        ),
        "multiplicity_applicable": False,
        "multiplicity_control_state": "NOT_APPLICABLE",
    }
    if any(value.endswith(":None") for value in decision_native["native_ids"]):
        raise BAQM8RealTransitionError("real_transition_decision_hash_missing")
    stage_map["DECISION_LAYER"] = _stage_descriptor(
        "DECISION_LAYER",
        roles=roles,
        snapshot_id=snapshot_id,
        prior=prior,
        native=decision_native,
    )
    prior = stage_map["DECISION_LAYER"]

    external_contract_path = root / "configs/external_evidence_8_contract_v1.json"
    external_completion_path = (
        root / "artifacts/research/external_evidence_8c_completion.json"
    )
    external_contract = _read_json(external_contract_path)
    external_completion = _read_json(external_completion_path)
    hard = external_completion.get("hard_boundaries")
    semantics = external_contract.get("external_evidence_semantics")
    if not isinstance(hard, Mapping) or not isinstance(semantics, Mapping):
        raise BAQM8RealTransitionError("real_transition_external_sections_missing")
    if (
        hard.get("production_external_evidence_enabled") is not False
        or hard.get("phase7_integration_enabled") is not False
        or semantics.get("may_generate_order") is not False
    ):
        raise BAQM8RealTransitionError("real_transition_external_boundary_invalid")
    external_native = {
        "native_ids": [
            f"artifact:external_contract:{_file_sha(external_contract_path)}",
            f"artifact:external_8c_completion:{_file_sha(external_completion_path)}",
        ],
        "missing_input_ids": [],
        "as_of": _aware(
            external_completion.get("completed_at"),
            "external_completed_at",
        ),
        "available_from": _aware(
            external_completion.get("completed_at"),
            "external_completed_at",
        ),
        "multiplicity_applicable": False,
        "multiplicity_control_state": "NOT_APPLICABLE",
    }
    stage_map["EXTERNAL_EVIDENCE"] = _stage_descriptor(
        "EXTERNAL_EVIDENCE",
        roles=roles,
        snapshot_id=snapshot_id,
        prior=prior,
        native=external_native,
    )

    ordered = [
        "SCANNER",
        "SELECTION",
        "TIMING",
        "PROBABILITY",
        "RISK",
        "CONFIDENCE",
        "LEARNING",
        "ELLIOTT",
        "DECISION_LAYER",
        "EXTERNAL_EVIDENCE",
    ]
    receipts: list[dict[str, Any]] = []
    for source_id, target_id in zip(ordered, ordered[1:]):
        resolution = (
            "REVIEWED_NON_ADDITIVE"
            if target_id in {"PROBABILITY", "RISK", "CONFIDENCE", "ELLIOTT"}
            else "NO_TRIGGER"
        )
        observation = _transition_observation(
            stage_map[source_id],
            stage_map[target_id],
            double_counting_resolution=resolution,
        )
        receipts.append(validate_transition_observation(observation))

    blocked: list[dict[str, Any]] = []
    provenance_ref = daily.get("scanner_input_provenance")
    if isinstance(provenance_ref, Mapping):
        try:
            provenance = validate_bound_provenance(
                root,
                provenance_ref,
                expected_snapshot_id=snapshot_id,
            )
        except Exception as exc:
            raise BAQM8RealTransitionError(
                f"real_transition_scanner_provenance_invalid:{exc}"
            ) from exc
        pre = provenance.get("pre_run")
        runtime = provenance.get("runtime")
        if not isinstance(pre, Mapping) or not isinstance(runtime, Mapping):
            raise BAQM8RealTransitionError("real_transition_data_provenance_sections_missing")
        inputs = pre.get("inputs")
        if not isinstance(inputs, list):
            raise BAQM8RealTransitionError("real_transition_pre_run_inputs_missing")
        data_ids = [
            f"input:{row.get('role')}:{row.get('sha256')}"
            for row in inputs
            if isinstance(row, Mapping) and row.get("sha256")
        ]
        scoring = runtime.get("scoring_rows_input")
        yahoo = runtime.get("yahoo_enrichment")
        if isinstance(scoring, Mapping) and scoring.get("sha256"):
            data_ids.append(f"input:scoring_rows:{scoring['sha256']}")
        if isinstance(yahoo, Mapping) and yahoo.get("provider_frame_sha256"):
            data_ids.append(
                f"input:yahoo_provider_frame:{yahoo['provider_frame_sha256']}"
            )
        if not data_ids:
            raise BAQM8RealTransitionError("real_transition_data_evidence_missing")
        captured = max(
            _aware(pre.get("captured_at"), "pre_run_captured_at"),
            _aware(runtime.get("captured_at"), "runtime_captured_at"),
        )
        data_stage = {
            "stage_id": "DATA",
            "semantic_role": roles["DATA"],
            "snapshot_id": "",
            "source_snapshot_id": "",
            "as_of": _iso(captured),
            "available_from": _iso(captured),
            "material_evidence_ids": data_ids,
            "native_evidence_ids": data_ids,
            "missing_input_ids": [],
            "multiplicity_applicable": False,
            "multiplicity_control_state": "NOT_APPLICABLE",
        }
        scanner_with_data = dict(stage_map["SCANNER"])
        scanner_with_data["material_evidence_ids"] = (
            data_ids + list(scanner_with_data["material_evidence_ids"])
        )
        observation = _transition_observation(
            data_stage,
            scanner_with_data,
            double_counting_resolution="NO_TRIGGER",
            snapshot_binding_required=False,
        )
        receipts.insert(0, validate_transition_observation(observation))
    else:
        blocked.append({
            "from_stage": "DATA",
            "to_stage": "SCANNER",
            "reason": "scanner_input_provenance_missing",
            "historical_backfill_attempted": False,
        })

    expected_passes = 10 - len(blocked)
    if len(receipts) != expected_passes:
        raise BAQM8RealTransitionError("real_transition_receipt_count_invalid")

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "REAL_TRANSITIONS_PASS"
            if not blocked
            else "REAL_TRANSITIONS_PARTIAL_DATA_PROVENANCE_BLOCKED"
        ),
        "snapshot_id": snapshot_id,
        "transition_pass_count": len(receipts),
        "transition_blocked_count": len(blocked),
        "receipts": receipts,
        "blocked_transitions": blocked,
        "all_executed_transitions_cover_all_seven_classes": all(
            receipt.get("all_required_error_classes_passed") is True
            for receipt in receipts
        ),
        "historical_lineage_backfilled": False,
        "missing_treated_as_neutral": False,
        "closure_eligible": not blocked and len(receipts) == 10,
        "family_claim_counts": {
            family: summary["claim_count"]
            for family, summary in family_summaries.items()
        },
    }
