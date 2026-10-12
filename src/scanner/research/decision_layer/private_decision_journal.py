"""Append-only private ARGUS decision journal, separated from retrospective diagnoses.

Capture frozen evidence *before* forward outcomes become known. This module
does not infer the user's motivation, recommend orders, or score performance.
"""
from __future__ import annotations
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import os

MODES = {"prospective", "retrospective"}
KINDS = {"argus_assessment", "user_decision", "outcome"}
SCHEMA = "argus_private_decision_journal_v1"


def _instant(raw):
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("timestamp_required")
    value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("timestamp_timezone_required")
    return value.astimezone(timezone.utc)


def validate(entry):
    if not isinstance(entry, dict) or entry.get("schema_version") != SCHEMA:
        raise ValueError("invalid_journal_schema")
    mode, kind = entry.get("mode"), entry.get("kind")
    if mode not in MODES or kind not in KINDS:
        raise ValueError("invalid_mode_or_kind")
    recorded = _instant(entry.get("recorded_at"))
    effective = _instant(entry.get("effective_at"))
    if effective > recorded:
        raise ValueError("effective_time_after_record")
    if not entry.get("symbol") or not entry.get("subject_id"):
        raise ValueError("subject_and_symbol_required")
    if mode == "retrospective" and kind == "argus_assessment":
        if entry.get("historically_available") is not False or entry.get("validation_eligible") is not False:
            raise ValueError("retrospective_assessment_must_not_claim_historical_forecast")
    if kind == "argus_assessment":
        if not entry.get("snapshot_id") or not entry.get("argus_version") or not entry.get("assessment"):
            raise ValueError("assessment_requires_snapshot_version_state")
        observed = _instant(entry.get("evidence_available_at"))
        if mode == "prospective":
            if observed > recorded or entry.get("validation_eligible") is not True:
                raise ValueError("prospective_requires_available_frozen_evidence")
            if effective != recorded:
                raise ValueError("prospective_assessment_must_be_contemporaneous")
    if kind == "outcome":
        if not entry.get("parent_event_id"):
            raise ValueError("outcome_must_reference_prior_event")
        if entry.get("validation_eligible") is True:
            raise ValueError("outcome_cannot_set_validation_eligibility")
    return entry


def canonical(entry):
    return json.dumps(entry, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def event_id(entry):
    return sha256(canonical(entry).encode("utf-8")).hexdigest()


def append(path: Path, entry: dict, *, repo_root: Path):
    path = path.resolve()
    if path.is_relative_to(repo_root.resolve()):
        raise ValueError("private_journal_must_remain_outside_repository")
    validate(entry)
    eid = event_id(entry)
    existing = []
    if path.exists():
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    obj = json.loads(line)
                    if obj["event_id"] == eid:
                        if obj["entry"] != entry:
                            raise ValueError("journal_integrity_failure")
                        return eid
                    existing.append(obj)
    if entry["kind"] == "outcome":
        parent = next((obj for obj in existing if obj["event_id"] == entry["parent_event_id"]), None)
        if parent is None or parent["entry"]["kind"] == "outcome":
            raise ValueError("outcome_parent_not_found")
        if _instant(entry["recorded_at"]) <= _instant(parent["entry"]["recorded_at"]):
            raise ValueError("outcome_must_be_later_than_assessment_or_decision")
    previous = existing[-1]["chain_hash"] if existing else "genesis"
    payload = {"event_id": eid, "entry": entry, "previous_hash": previous}
    payload["chain_hash"] = sha256(canonical(payload).encode("utf-8")).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        handle.write(canonical(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return eid


def verify(path: Path):
    prior = "genesis"
    ids = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            obj = json.loads(line)
            if obj["event_id"] != event_id(obj["entry"]) or obj["previous_hash"] != prior:
                raise ValueError("journal_chain_integrity_failure")
            payload = {key: obj[key] for key in ("event_id", "entry", "previous_hash")}
            if obj["chain_hash"] != sha256(canonical(payload).encode("utf-8")).hexdigest():
                raise ValueError("journal_chain_integrity_failure")
            if obj["event_id"] in ids:
                raise ValueError("duplicate_journal_event")
            ids.add(obj["event_id"])
            prior = obj["chain_hash"]
    return len(ids)
