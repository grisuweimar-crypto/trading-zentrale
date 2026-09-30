"""QM-B durable prospective project-universe membership capture.

Research-only. Captures exact universe_master bytes at an actual observation time,
creates one normalized prospective membership snapshot, and appends it to the
hash-chained membership ledger only when the universe content hash is new.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping

from scanner.research.governance.qm_b_prospective_identity_match import load_universe_snapshot
from scanner.research.governance.qm_b_prospective_membership import (
    ProspectiveMembershipError,
    append_membership_snapshot,
    build_membership_snapshot,
    verify_membership_ledger,
)

CAPTURE_SCHEMA_VERSION = "qm_b_durable_membership_capture_result_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_durable_membership_capture_v1.json"


class DurableMembershipCaptureError(ValueError):
    """Raised when durable prospective membership capture cannot proceed safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise DurableMembershipCaptureError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DurableMembershipCaptureError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise DurableMembershipCaptureError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _atomic_write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".tmp.{os.getpid()}")
    try:
        temp.write_bytes(value)
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    data = (json.dumps(dict(value), indent=2, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write_bytes(path, data)


def load_capture_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DurableMembershipCaptureError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_durable_membership_capture_v1":
        raise DurableMembershipCaptureError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise DurableMembershipCaptureError("contract_scope_invalid")
    dedup = payload.get("deduplication")
    if not isinstance(dedup, Mapping) or dedup.get("key") != "universe_sha256":
        raise DurableMembershipCaptureError("deduplication_contract_invalid")
    if dedup.get("repeat_observation_for_same_hash") is not False:
        raise DurableMembershipCaptureError("same_hash_repeat_must_be_disabled")
    archive = payload.get("archive")
    if not isinstance(archive, Mapping) or archive.get("retain_raw_bytes_exactly") is not True:
        raise DurableMembershipCaptureError("raw_archive_contract_invalid")
    return payload


def _validated_commit_sha(value: Any, accepted_lengths: set[int]) -> str:
    commit = _clean(value).lower()
    if len(commit) not in accepted_lengths or re.fullmatch(r"[0-9a-f]+", commit) is None:
        raise DurableMembershipCaptureError("source_commit_sha_invalid")
    return commit


def _ledger_source_hashes(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    verify_membership_ledger(path)
    result: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DurableMembershipCaptureError(f"ledger_invalid_json:{line_number}") from exc
        snapshot = event.get("snapshot") if isinstance(event, Mapping) else None
        if not isinstance(snapshot, Mapping):
            raise DurableMembershipCaptureError(f"ledger_snapshot_missing:{line_number}")
        digest = _clean(snapshot.get("universe_sha256")).lower()
        if digest:
            result[digest] = dict(snapshot)
    return result


def capture_universe_membership(
    universe_path: str | Path,
    *,
    repo_root: str | Path,
    observed_at: str,
    source_commit_sha: str,
    trigger: str,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Persist a new prospective membership state, or return NO_CHANGE for same content.

    The caller must pass the exact universe bytes from the triggering commit.  This
    function never reads a later universe state as a substitute for that evidence.
    """
    contract = load_capture_contract(contract_path)
    observed = _utc(observed_at, field="observed_at")
    observed_iso = observed.isoformat()
    accepted_lengths = {int(v) for v in contract["source_commit"]["accepted_hex_lengths"]}
    commit_sha = _validated_commit_sha(source_commit_sha, accepted_lengths)
    trigger_text = _clean(trigger)
    if not trigger_text:
        raise DurableMembershipCaptureError("trigger_required")

    source = Path(universe_path)
    try:
        raw_bytes = source.read_bytes()
    except OSError as exc:
        raise DurableMembershipCaptureError(f"universe_source_unreadable:{source}") from exc
    rows, universe_sha256 = load_universe_snapshot(source)

    root = Path(repo_root).resolve()
    archive_root = (root / str(contract["archive_root"])).resolve()
    try:
        archive_root.relative_to(root)
    except ValueError as exc:
        raise DurableMembershipCaptureError("archive_root_outside_repo") from exc
    ledger = archive_root / str(contract["ledger_filename"])

    existing = _ledger_source_hashes(ledger)
    if universe_sha256 in existing:
        prior = existing[universe_sha256]
        return {
            "schema_version": CAPTURE_SCHEMA_VERSION,
            "capture_status": "NO_CHANGE",
            "reason_code": "UNIVERSE_SHA256_ALREADY_ARCHIVED",
            "source_commit_sha": commit_sha,
            "trigger": trigger_text,
            "observed_at": observed_iso,
            "universe_sha256": universe_sha256,
            "existing_membership_snapshot_id": prior.get("membership_snapshot_id"),
            "files_written": [],
            "ledger_event_appended": False,
            "historical_retrojection_permitted": False,
        }

    source_path = str(contract["source_path"])
    universe_snapshot_id = f"git:{commit_sha}:{source_path}"
    try:
        snapshot = build_membership_snapshot(
            rows,
            universe_snapshot_id=universe_snapshot_id,
            universe_observed_at=observed_iso,
            universe_sha256=universe_sha256,
        )
    except ProspectiveMembershipError as exc:
        raise DurableMembershipCaptureError(str(exc)) from exc

    membership_id = str(snapshot["membership_snapshot_id"])
    raw_rel = Path(str(contract["archive"]["raw_subdirectory"])) / f"{membership_id}_universe_master.csv"
    snapshot_rel = Path(str(contract["archive"]["snapshot_subdirectory"])) / f"{membership_id}.json"
    raw_target = archive_root / raw_rel
    snapshot_target = archive_root / snapshot_rel

    # Exact-byte preservation. Existing orphan files are accepted only if identical.
    if raw_target.exists() and raw_target.read_bytes() != raw_bytes:
        raise DurableMembershipCaptureError(f"raw_archive_collision:{raw_target}")
    if not raw_target.exists():
        _atomic_write_bytes(raw_target, raw_bytes)
    if snapshot_target.exists():
        try:
            existing_snapshot = json.loads(snapshot_target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DurableMembershipCaptureError(f"snapshot_archive_unreadable:{snapshot_target}") from exc
        if existing_snapshot != snapshot:
            raise DurableMembershipCaptureError(f"snapshot_archive_collision:{snapshot_target}")
    else:
        _atomic_write_json(snapshot_target, snapshot)

    try:
        ledger_state = append_membership_snapshot(ledger, snapshot)
    except (ProspectiveMembershipError, OSError) as exc:
        raise DurableMembershipCaptureError(str(exc)) from exc

    latest = {
        "schema_version": "qm_b_durable_membership_latest_v1",
        "capture_status": "ARCHIVED",
        "source_commit_sha": commit_sha,
        "trigger": trigger_text,
        "observed_at": observed_iso,
        "universe_snapshot_id": universe_snapshot_id,
        "universe_sha256": universe_sha256,
        "membership_snapshot_id": membership_id,
        "membership_snapshot_sha256": snapshot["membership_snapshot_sha256"],
        "source_row_count": snapshot["source_row_count"],
        "stable_instrument_claim_count": snapshot["stable_instrument_claim_count"],
        "unresolved_row_count": snapshot["unresolved_row_count"],
        "raw_archive_path": str((Path(str(contract["archive_root"])) / raw_rel).as_posix()),
        "normalized_snapshot_path": str((Path(str(contract["archive_root"])) / snapshot_rel).as_posix()),
        "ledger_path": str((Path(str(contract["archive_root"])) / str(contract["ledger_filename"])).as_posix()),
        "ledger_event_count": ledger_state["event_count"],
        "ledger_head_hash": ledger_state["head_hash"],
        "historical_retrojection_permitted": False,
        "listing_status_promotion_performed": False,
        "tradability_promotion_performed": False,
        "project_investability_promotion_performed": False,
    }
    latest_path = archive_root / str(contract["latest_manifest_filename"])
    _atomic_write_json(latest_path, latest)

    return {
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "capture_status": "ARCHIVED",
        "reason_code": "NEW_UNIVERSE_SHA256",
        "source_commit_sha": commit_sha,
        "trigger": trigger_text,
        "observed_at": observed_iso,
        "universe_sha256": universe_sha256,
        "membership_snapshot_id": membership_id,
        "source_row_count": snapshot["source_row_count"],
        "stable_instrument_claim_count": snapshot["stable_instrument_claim_count"],
        "unresolved_row_count": snapshot["unresolved_row_count"],
        "files_written": [
            latest["raw_archive_path"],
            latest["normalized_snapshot_path"],
            latest["ledger_path"],
            str((Path(str(contract["archive_root"])) / str(contract["latest_manifest_filename"])).as_posix()),
        ],
        "ledger_event_appended": True,
        "ledger_event_count": ledger_state["event_count"],
        "ledger_head_hash": ledger_state["head_hash"],
        "historical_retrojection_permitted": False,
    }
