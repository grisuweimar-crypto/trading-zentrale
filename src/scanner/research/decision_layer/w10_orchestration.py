"""W10 snapshot orchestration and no-backdating contract.

This module does not compute investment evidence.  It records when already
existing phase artifacts became available to one orchestration run, freezes the
upstream set before Phase 7A, and seals the resulting 7A packet/archive pair.

The timestamps recorded for runtime-produced stages are created by this module.
Callers cannot pass an earlier timestamp for those stages.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping


SCHEMA_VERSION = "decision_snapshot_orchestration_w10_v1"
REQUIRED_PRE_7A_STAGES = (
    "scanner_daily_research",
    "phase2_probability",
    "phase3_risk",
    "phase4_confidence",
    "phase5_governance",
    "phase6_elliott",
)


class W10OrchestrationError(ValueError):
    """Raised when the W10 snapshot chain cannot be frozen without inference."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise W10OrchestrationError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise W10OrchestrationError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise W10OrchestrationError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise W10OrchestrationError("now_timezone_required")
    return current.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        raise W10OrchestrationError(f"artifact_missing:{path}")
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise W10OrchestrationError(f"invalid_json_artifact:{path}") from exc
    if not isinstance(value, dict):
        raise W10OrchestrationError(f"artifact_must_be_object:{path}")
    return value


def _snapshot_candidates(payload: Mapping[str, object]) -> set[str]:
    candidates: set[str] = set()
    direct = payload.get("snapshot_id")
    if direct:
        candidates.add(str(direct))
    for key in ("source", "validation", "provenance"):
        nested = payload.get(key)
        if isinstance(nested, Mapping) and nested.get("snapshot_id"):
            candidates.add(str(nested["snapshot_id"]))
    return candidates


def _assert_snapshot_if_expressed(
    payload: Mapping[str, object],
    expected_snapshot_id: str,
    *,
    stage: str,
) -> str:
    candidates = _snapshot_candidates(payload)
    if not candidates:
        return "not_expressed_by_artifact"
    if candidates != {expected_snapshot_id}:
        raise W10OrchestrationError(
            f"snapshot_mismatch:{stage}:{','.join(sorted(candidates))}:{expected_snapshot_id}"
        )
    return "verified"


def begin_manifest(
    daily: Mapping[str, object],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Start one W10 chain from the authoritative daily_research snapshot."""
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    snapshot_as_of = str(daily.get("as_of") or "").strip()
    scanner_available = _utc(daily.get("generated_at"), "daily_generated_at")
    started = _now(now)
    if scanner_available > started:
        raise W10OrchestrationError("daily_research_not_yet_available")
    if not snapshot_id:
        raise W10OrchestrationError("snapshot_id_required")
    if not snapshot_as_of:
        raise W10OrchestrationError("snapshot_as_of_required")
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "snapshot_as_of": snapshot_as_of,
        "status": "collecting",
        "started_at": _iso(started),
        "pre_7a_frozen_at": None,
        "sealed_at": None,
        "stages": {
            "scanner_daily_research": {
                "status": "available",
                "available_from": _iso(scanner_available),
                "availability_source": "daily_research.generated_at",
                "snapshot_identity_state": "verified",
            }
        },
        "guards": {
            "final_7a_requires_all_upstream_stages_resolved": True,
            "runtime_stage_available_from_is_recorded_not_supplied": True,
            "later_evidence_backdating_allowed": False,
            "phase6_missing_is_neutral_evidence": False,
            "private_position_data_persisted": False,
        },
    }


def _mutable_stages(manifest: Mapping[str, object]) -> dict[str, object]:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise W10OrchestrationError("unsupported_w10_manifest")
    if manifest.get("status") != "collecting":
        raise W10OrchestrationError("upstream_manifest_already_frozen")
    stages = manifest.get("stages")
    if not isinstance(stages, dict):
        raise W10OrchestrationError("stages_required")
    return stages


def record_runtime_artifact(manifest: Mapping[str, object], *, stage: str, artifact_path: Path, now: datetime | None = None) -> dict[str, object]:
    """Record an artifact immediately after this orchestration produced it."""
    out = deepcopy(dict(manifest))
    stages = _mutable_stages(out)
    if stage in stages:
        raise W10OrchestrationError(f"stage_already_recorded:{stage}")
    recorded = _now(now)
    payload = _load_object(artifact_path)
    identity = _assert_snapshot_if_expressed(payload, str(out["snapshot_id"]), stage=stage)
    stages[stage] = {
        "status": "available",
        "available_from": _iso(recorded),
        "availability_source": "w10_runtime_recorded_after_stage",
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
        "snapshot_identity_state": identity,
    }
    return out


def record_preexisting_artifact(manifest: Mapping[str, object], *, stage: str, artifact_path: Path, source_commit: str, source_available_from: str, require_snapshot_match: bool) -> dict[str, object]:
    """Record persisted evidence using its repository-derived availability."""
    out = deepcopy(dict(manifest))
    stages = _mutable_stages(out)
    if stage in stages:
        raise W10OrchestrationError(f"stage_already_recorded:{stage}")
    available = _utc(source_available_from, f"{stage}_source_available_from")
    started = _utc(out["started_at"], "started_at")
    if available > started:
        raise W10OrchestrationError(f"future_preexisting_artifact:{stage}")
    commit = str(source_commit or "").strip()
    if len(commit) < 12:
        raise W10OrchestrationError(f"source_commit_required:{stage}")
    payload = _load_object(artifact_path)
    if require_snapshot_match:
        identity = _assert_snapshot_if_expressed(payload, str(out["snapshot_id"]), stage=stage)
        if identity != "verified":
            raise W10OrchestrationError(f"snapshot_identity_not_expressed:{stage}")
    else:
        identity = "not_market_snapshot_evidence"
    stages[stage] = {
        "status": "available",
        "available_from": _iso(available),
        "availability_source": "git_commit_time",
        "source_commit": commit,
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
        "snapshot_identity_state": identity,
    }
    return out


def resolve_phase6(manifest: Mapping[str, object], *, artifact_path: Path | None, now: datetime | None = None) -> dict[str, object]:
    """Resolve Phase 6 before 7A; absence is explicit and never neutral evidence."""
    out = deepcopy(dict(manifest))
    stages = _mutable_stages(out)
    stage = "phase6_elliott"
    if stage in stages:
        raise W10OrchestrationError(f"stage_already_recorded:{stage}")
    resolved = _now(now)
    if artifact_path is None:
        stages[stage] = {
            "status": "not_supplied",
            "resolved_at": _iso(resolved),
            "availability_source": None,
            "snapshot_identity_state": "not_applicable",
            "decision_effect": "none",
            "missing_is_neutral_evidence": False,
        }
        return out
    payload = _load_object(artifact_path)
    if payload.get("schema_version") != "decision_elliott_6h_source_v1":
        raise W10OrchestrationError("unsupported_phase6_source_schema")
    available = _utc(payload.get("available_from"), "phase6_available_from")
    if available > resolved:
        raise W10OrchestrationError("future_phase6_source")
    commit = str(payload.get("source_commit") or "").strip()
    if len(commit) < 12:
        raise W10OrchestrationError("phase6_source_commit_required")
    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        raise W10OrchestrationError("phase6_outputs_must_be_list")
    stages[stage] = {
        "status": "available",
        "available_from": _iso(available),
        "resolved_at": _iso(resolved),
        "availability_source": "decision_elliott_6h_source_v1.available_from",
        "source_commit": commit,
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256(artifact_path),
        "snapshot_identity_state": "date_bound_review_context",
        "output_count": len(outputs),
        "decision_effect": "review_only_downstream_of_7d_7e",
    }
    return out


def freeze_pre_7a(manifest: Mapping[str, object], *, now: datetime | None = None) -> dict[str, object]:
    """Freeze all upstream stages before the final 7A packet may be built."""
    out = deepcopy(dict(manifest))
    stages = _mutable_stages(out)
    missing = [stage for stage in REQUIRED_PRE_7A_STAGES if stage not in stages]
    if missing:
        raise W10OrchestrationError("upstream_stage_unresolved:" + ",".join(missing))
    freeze_time = _now(now)
    scanner_time = _utc(stages["scanner_daily_research"]["available_from"], "scanner_available_from")
    if freeze_time < scanner_time:
        raise W10OrchestrationError("freeze_before_scanner_publication")
    for stage in REQUIRED_PRE_7A_STAGES:
        entry = stages[stage]
        if not isinstance(entry, Mapping):
            raise W10OrchestrationError(f"invalid_stage_entry:{stage}")
        status = entry.get("status")
        if status not in {"available", "not_supplied"}:
            raise W10OrchestrationError(f"stage_not_resolved:{stage}")
        if status == "available":
            available = _utc(entry.get("available_from"), f"{stage}_available_from")
            if available > freeze_time:
                raise W10OrchestrationError(f"future_stage_at_freeze:{stage}")
    out["status"] = "pre_7a_frozen"
    out["pre_7a_frozen_at"] = _iso(freeze_time)
    return out


def seal_final_7a(manifest: Mapping[str, object], *, packet_set_path: Path, archive_path: Path, now: datetime | None = None) -> dict[str, object]:
    """Seal the final 7A packet/archive after the upstream set is immutable."""
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise W10OrchestrationError("unsupported_w10_manifest")
    if manifest.get("status") != "pre_7a_frozen":
        raise W10OrchestrationError("pre_7a_freeze_required")
    out = deepcopy(dict(manifest))
    stages = out.get("stages")
    if not isinstance(stages, dict):
        raise W10OrchestrationError("stages_required")
    packet_set = _load_object(packet_set_path)
    if str(packet_set.get("snapshot_id") or "") != str(out.get("snapshot_id") or ""):
        raise W10OrchestrationError("final_7a_snapshot_mismatch")
    if not isinstance(packet_set.get("packets"), list) or not packet_set["packets"]:
        raise W10OrchestrationError("final_7a_packets_required")
    decision_time = _utc(packet_set.get("as_of"), "final_7a_as_of")
    frozen = _utc(out.get("pre_7a_frozen_at"), "pre_7a_frozen_at")
    sealed = _now(now)
    if decision_time < frozen:
        raise W10OrchestrationError("final_7a_predates_upstream_freeze")
    if decision_time > sealed:
        raise W10OrchestrationError("final_7a_from_future")
    stages["final_7a"] = {
        "status": "available",
        "available_from": _iso(decision_time),
        "availability_source": "current_decision_packets_7a.as_of",
        "artifact_path": str(packet_set_path),
        "artifact_sha256": _sha256(packet_set_path),
        "snapshot_identity_state": "verified",
    }
    stages["phase7a_archive"] = {
        "status": "available",
        "available_from": _iso(sealed),
        "availability_source": "w10_runtime_recorded_after_archive_write",
        "artifact_path": str(archive_path),
        "artifact_sha256": _sha256(archive_path),
        "snapshot_identity_state": "inherits_final_7a",
    }
    out["status"] = "sealed"
    out["sealed_at"] = _iso(sealed)
    out["downstream_contract"] = {
        "private_runner": "scripts/run_depot_watch_orchestrated.py",
        "chain": ["7D", "7E", "7F", "7G", "7H"],
        "requires_same_snapshot_id": True,
        "phase6_effect_boundary": "7F_review_context_only",
        "private_position_data_persisted": False,
    }
    return out


def validate_sealed_manifest(manifest: Mapping[str, object], *, expected_snapshot_id: str | None = None) -> dict[str, object]:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise W10OrchestrationError("unsupported_w10_manifest")
    if manifest.get("status") != "sealed":
        raise W10OrchestrationError("w10_manifest_not_sealed")
    snapshot_id = str(manifest.get("snapshot_id") or "")
    if not snapshot_id:
        raise W10OrchestrationError("snapshot_id_required")
    if expected_snapshot_id is not None and snapshot_id != str(expected_snapshot_id):
        raise W10OrchestrationError("w10_manifest_snapshot_mismatch")
    stages = manifest.get("stages")
    if not isinstance(stages, Mapping):
        raise W10OrchestrationError("stages_required")
    required = set(REQUIRED_PRE_7A_STAGES) | {"final_7a", "phase7a_archive"}
    missing = sorted(required - set(map(str, stages.keys())))
    if missing:
        raise W10OrchestrationError("sealed_stage_missing:" + ",".join(missing))
    freeze = _utc(manifest.get("pre_7a_frozen_at"), "pre_7a_frozen_at")
    seal = _utc(manifest.get("sealed_at"), "sealed_at")
    if seal < freeze:
        raise W10OrchestrationError("seal_before_freeze")
    for stage, entry in stages.items():
        if not isinstance(entry, Mapping):
            raise W10OrchestrationError(f"invalid_stage_entry:{stage}")
        available_text = entry.get("available_from")
        if available_text:
            available = _utc(available_text, f"{stage}_available_from")
            if stage in REQUIRED_PRE_7A_STAGES and available > freeze:
                raise W10OrchestrationError(f"upstream_evidence_after_freeze:{stage}")
            if available > seal:
                raise W10OrchestrationError(f"evidence_after_seal:{stage}")
    guards = manifest.get("guards")
    if not isinstance(guards, Mapping):
        raise W10OrchestrationError("guards_required")
    if guards.get("later_evidence_backdating_allowed") is not False:
        raise W10OrchestrationError("no_backdating_guard_missing")
    if guards.get("private_position_data_persisted") is not False:
        raise W10OrchestrationError("private_position_guard_invalid")
    return deepcopy(dict(manifest))
