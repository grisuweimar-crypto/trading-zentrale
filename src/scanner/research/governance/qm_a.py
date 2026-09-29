"""QM-A research governance and evidence-consumption controls.

Research-only. This module does not compute scanner scores, Decision-Layer
outputs, portfolio actions or orders. It provides an append-only, hash-chained
governance ledger for future research work.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4


EVENT_SCHEMA_VERSION = "qm_a_ledger_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_a_research_governance_v1.json"


class GovernanceLedgerError(ValueError):
    """Raised when QM-A governance invariants are violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash_mapping(value: Mapping[str, Any]) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def load_qm_a_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        value = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GovernanceLedgerError(f"qm_a_contract_unreadable:{contract_path}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != "qm_a_research_governance_v1":
        raise GovernanceLedgerError("qm_a_contract_schema_invalid")
    return value


def classify_change(*, equivalence_demonstrated: bool, outcome_driven: bool) -> str:
    """Classify a research change conservatively.

    Outcome-driven changes always dominate. If mechanical equivalence is not
    demonstrated, the safe default is a preventive new version.
    """
    if outcome_driven:
        return "OUTCOME_DRIVEN_RESEARCH_CHANGE"
    if equivalence_demonstrated:
        return "MECHANICALLY_EQUIVALENT_REPAIR"
    return "PREVENTIVE_QA_NEW_VERSION"


class GovernanceLedger:
    """Append-only event-sourced QM-A ledger with fail-closed replay."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_a_contract(contract_path)

        sm = self.contract["evidence_state_machine"]
        self.states = set(sm["states"])
        self.allowed_transitions = {key: set(values) for key, values in sm["allowed_transitions"].items()}
        self.freeze_state = str(sm["identity_freeze_state"])
        self.spent_states = set(sm["spent_states"])

        identity = self.contract["immutable_analysis_identity"]
        self.identity_required_fields = tuple(identity["required_fields"])

        ec = self.contract["evidence_consumption"]
        self.change_classes = set(ec["change_classes"])
        self.access_modes = set(ec["access_modes"])
        self.visibility_levels = set(ec["outcome_visibility_levels"])
        self.performance_revealing_visibility = set(ec["performance_revealing_visibility"])
        self.evidence_effects = set(ec["evidence_effects"])
        self.successor_required_for = set(ec["successor_version_required_for"])
        self.required_inspection_fields = tuple(self.contract["required_inspection_fields"])

    @staticmethod
    def _key(analysis_id: str, version_id: str) -> str:
        return f"{analysis_id}::{version_id}"

    def _validate_identity(self, identity: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(identity, Mapping):
            raise GovernanceLedgerError("analysis_identity_must_be_object")
        normalized = dict(identity)
        missing = [field for field in self.identity_required_fields if not str(normalized.get(field) or "").strip()]
        if missing:
            raise GovernanceLedgerError("analysis_identity_missing:" + ",".join(missing))
        return normalized

    def _read_raw_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise GovernanceLedgerError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise GovernanceLedgerError(f"ledger_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: list[dict[str, Any]]) -> None:
        previous_hash: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise GovernanceLedgerError(f"ledger_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise GovernanceLedgerError(f"ledger_sequence_invalid:{expected_sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise GovernanceLedgerError(f"ledger_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise GovernanceLedgerError(f"ledger_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash_mapping(body):
                raise GovernanceLedgerError(f"ledger_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _validate_inspection_payload(self, payload: Mapping[str, Any]) -> None:
        missing = [field for field in self.required_inspection_fields if field not in payload]
        if missing:
            raise GovernanceLedgerError("inspection_fields_missing:" + ",".join(missing))

        if str(payload.get("access_mode")) not in self.access_modes:
            raise GovernanceLedgerError("inspection_access_mode_invalid")
        visibility = str(payload.get("outcome_visibility_level"))
        if visibility not in self.visibility_levels:
            raise GovernanceLedgerError("inspection_visibility_invalid")
        change_class = str(payload.get("change_class"))
        if change_class not in self.change_classes:
            raise GovernanceLedgerError("inspection_change_class_invalid")
        effect = str(payload.get("evidence_effect"))
        if effect not in self.evidence_effects:
            raise GovernanceLedgerError("inspection_evidence_effect_invalid")
        if not isinstance(payload.get("affected_hypothesis_ids"), list):
            raise GovernanceLedgerError("affected_hypothesis_ids_must_be_list")

        successor_version_id = str(payload.get("successor_version_id") or "").strip()
        if change_class in self.successor_required_for and not successor_version_id:
            raise GovernanceLedgerError("successor_version_required_for_change_class")

        spent_for_design = bool(payload.get("spent_for_design"))
        if change_class == "OUTCOME_DRIVEN_RESEARCH_CHANGE":
            if visibility not in self.performance_revealing_visibility:
                raise GovernanceLedgerError("outcome_driven_change_requires_visible_outcomes")
            if not spent_for_design or effect != "SPENT_FOR_DESIGN":
                raise GovernanceLedgerError("outcome_driven_change_must_spend_evidence")

        if change_class == "MECHANICALLY_EQUIVALENT_REPAIR":
            if not str(payload.get("review_or_approval_reference") or "").strip():
                raise GovernanceLedgerError("mechanical_repair_requires_equivalence_reference")
            if spent_for_design:
                raise GovernanceLedgerError("mechanical_repair_cannot_mark_spent_for_design")

        actor_id = str(payload.get("actor_id") or "").strip()
        reviewer_actor_id = str(payload.get("reviewer_actor_id") or "").strip()
        independent_review = bool(payload.get("independent_review", False))
        if independent_review and reviewer_actor_id and reviewer_actor_id == actor_id:
            raise GovernanceLedgerError("same_actor_review_cannot_be_marked_independent")

    def _replay(self, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        analyses: dict[str, dict[str, Any]] = {}

        for event in events:
            event_type = str(event.get("event_type") or "")
            analysis_id = str(event.get("analysis_id") or "")
            version_id = str(event.get("version_id") or "")
            if not analysis_id or not version_id:
                raise GovernanceLedgerError("ledger_analysis_identity_missing")

            key = self._key(analysis_id, version_id)
            payload = event.get("payload")
            if not isinstance(payload, dict):
                raise GovernanceLedgerError("ledger_payload_must_be_object")

            if event_type == "ANALYSIS_REGISTERED":
                if key in analyses:
                    raise GovernanceLedgerError(f"analysis_version_already_registered:{key}")
                supersedes = payload.get("supersedes_version_id")
                if supersedes:
                    predecessor = self._key(analysis_id, str(supersedes))
                    if predecessor not in analyses:
                        raise GovernanceLedgerError(f"superseded_version_not_registered:{predecessor}")
                    if str(supersedes) == version_id:
                        raise GovernanceLedgerError("analysis_version_cannot_supersede_itself")
                analyses[key] = {
                    "analysis_id": analysis_id,
                    "version_id": version_id,
                    "state": "DRAFT",
                    "identity_hash": None,
                    "identity": None,
                    "registered_at": event["recorded_at"],
                    "supersedes_version_id": supersedes,
                    "spent_for_design": False,
                    "outcome_evidence_inspected": False,
                    "confirmatory_evidence_consumed": False,
                    "inspection_count": 0,
                    "last_event_hash": event["entry_hash"],
                }
                continue

            if key not in analyses:
                raise GovernanceLedgerError(f"analysis_version_not_registered:{key}")
            current = analyses[key]

            if event_type == "STATE_TRANSITION":
                from_state = str(payload.get("from_state") or "")
                to_state = str(payload.get("to_state") or "")
                if from_state != current["state"]:
                    raise GovernanceLedgerError(f"transition_from_state_mismatch:{key}:{from_state}")
                if to_state not in self.states:
                    raise GovernanceLedgerError(f"transition_target_unknown:{to_state}")
                if to_state not in self.allowed_transitions.get(from_state, set()):
                    raise GovernanceLedgerError(f"transition_forbidden:{from_state}->{to_state}")

                supplied_identity = payload.get("analysis_identity")
                supplied_identity_hash = payload.get("analysis_identity_hash")

                if to_state == self.freeze_state and current["identity_hash"] is None:
                    identity = self._validate_identity(supplied_identity if isinstance(supplied_identity, Mapping) else {})
                    identity_hash = _hash_mapping(identity)
                    if supplied_identity_hash and supplied_identity_hash != identity_hash:
                        raise GovernanceLedgerError("analysis_identity_hash_mismatch")
                    current["identity"] = identity
                    current["identity_hash"] = identity_hash
                elif current["identity_hash"] is not None:
                    if supplied_identity is not None:
                        identity = self._validate_identity(supplied_identity if isinstance(supplied_identity, Mapping) else {})
                        if _hash_mapping(identity) != current["identity_hash"]:
                            raise GovernanceLedgerError("analysis_identity_changed_after_freeze")
                    if supplied_identity_hash and supplied_identity_hash != current["identity_hash"]:
                        raise GovernanceLedgerError("analysis_identity_hash_changed_after_freeze")
                elif to_state not in {"EXPLORATORY", "REJECTED", "RETIRED"}:
                    raise GovernanceLedgerError("frozen_analysis_identity_required")

                current["state"] = to_state
                if to_state in self.spent_states:
                    current["confirmatory_evidence_consumed"] = True
                current["last_event_hash"] = event["entry_hash"]
                continue

            if event_type == "EVIDENCE_INSPECTION":
                self._validate_inspection_payload(payload)
                current["inspection_count"] += 1
                if str(payload["outcome_visibility_level"]) in self.performance_revealing_visibility:
                    current["outcome_evidence_inspected"] = True
                if bool(payload["spent_for_design"]):
                    current["spent_for_design"] = True
                current["last_event_hash"] = event["entry_hash"]
                continue

            raise GovernanceLedgerError(f"ledger_event_type_unknown:{event_type}")

        return analyses

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def verify_integrity(self) -> dict[str, Any]:
        events, analyses = self._load_and_validate()
        return {
            "schema_version": "qm_a_ledger_verification_v1",
            "valid": True,
            "event_count": len(events),
            "analysis_version_count": len(analyses),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {key: value["state"] for key, value in sorted(analyses.items())},
            "spent_for_design": sorted(key for key, value in analyses.items() if value["spent_for_design"]),
        }

    def get_analysis(self, analysis_id: str, version_id: str) -> dict[str, Any]:
        _, analyses = self._load_and_validate()
        key = self._key(analysis_id, version_id)
        if key not in analyses:
            raise GovernanceLedgerError(f"analysis_version_not_registered:{key}")
        return dict(analyses[key])

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise GovernanceLedgerError(f"ledger_lock_exists:{self.lock_path}") from exc

    def _release_lock(self, fd: int) -> None:
        try:
            os.close(fd)
        finally:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def _append_event(
        self,
        *,
        event_type: str,
        analysis_id: str,
        version_id: str,
        actor_id: str,
        actor_role: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if event_type not in set(self.contract["append_only_ledger"]["event_types"]):
            raise GovernanceLedgerError(f"unsupported_event_type:{event_type}")
        if not str(actor_id).strip() or not str(actor_role).strip():
            raise GovernanceLedgerError("actor_identity_required")
        if not str(analysis_id).strip() or not str(version_id).strip():
            raise GovernanceLedgerError("analysis_id_and_version_required")

        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": event_type,
                "recorded_at": _utc_now(),
                "analysis_id": str(analysis_id),
                "version_id": str(version_id),
                "actor_id": str(actor_id),
                "actor_role": str(actor_role),
                "payload": dict(payload),
                "previous_event_hash": events[-1]["entry_hash"] if events else None,
            }
            event["entry_hash"] = _hash_mapping(event)

            # Critical concurrency guard: replay the candidate while the writer
            # lock is held. A stale from_state, duplicate registration or other
            # semantic conflict therefore fails before any bytes are appended.
            candidate = [*events, event]
            self._verify_hash_chain(candidate)
            self._replay(candidate)

            line = (_canonical_json(event) + "\n").encode("utf-8")
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            try:
                os.write(fd, line)
                os.fsync(fd)
            finally:
                os.close(fd)

            self._load_and_validate()
            return event
        finally:
            self._release_lock(lock_fd)

    def register_analysis(
        self,
        *,
        analysis_id: str,
        version_id: str,
        actor_id: str,
        actor_role: str,
        supersedes_version_id: str | None = None,
        description: str = "",
    ) -> dict[str, Any]:
        _, analyses = self._load_and_validate()
        key = self._key(analysis_id, version_id)
        if key in analyses:
            raise GovernanceLedgerError(f"analysis_version_already_registered:{key}")
        if supersedes_version_id:
            predecessor = self._key(analysis_id, supersedes_version_id)
            if predecessor not in analyses:
                raise GovernanceLedgerError(f"superseded_version_not_registered:{predecessor}")
            if supersedes_version_id == version_id:
                raise GovernanceLedgerError("analysis_version_cannot_supersede_itself")

        return self._append_event(
            event_type="ANALYSIS_REGISTERED",
            analysis_id=analysis_id,
            version_id=version_id,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "initial_state": "DRAFT",
                "supersedes_version_id": supersedes_version_id,
                "description": description,
            },
        )

    def transition(
        self,
        *,
        analysis_id: str,
        version_id: str,
        to_state: str,
        actor_id: str,
        actor_role: str,
        reason: str,
        analysis_identity: Mapping[str, Any] | None = None,
        review_or_approval_reference: str | None = None,
    ) -> dict[str, Any]:
        current = self.get_analysis(analysis_id, version_id)
        from_state = str(current["state"])
        if to_state not in self.states:
            raise GovernanceLedgerError(f"transition_target_unknown:{to_state}")
        if to_state not in self.allowed_transitions.get(from_state, set()):
            raise GovernanceLedgerError(f"transition_forbidden:{from_state}->{to_state}")
        if not str(reason).strip():
            raise GovernanceLedgerError("transition_reason_required")

        identity_hash = current.get("identity_hash")
        normalized_identity: dict[str, Any] | None = None
        if to_state == self.freeze_state and identity_hash is None:
            normalized_identity = self._validate_identity(analysis_identity or {})
            identity_hash = _hash_mapping(normalized_identity)
        elif identity_hash is not None and analysis_identity is not None:
            normalized_identity = self._validate_identity(analysis_identity)
            if _hash_mapping(normalized_identity) != identity_hash:
                raise GovernanceLedgerError("analysis_identity_changed_after_freeze")
        elif identity_hash is None and to_state not in {"EXPLORATORY", "REJECTED", "RETIRED"}:
            raise GovernanceLedgerError("frozen_analysis_identity_required")

        return self._append_event(
            event_type="STATE_TRANSITION",
            analysis_id=analysis_id,
            version_id=version_id,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "from_state": from_state,
                "to_state": to_state,
                "reason": reason,
                "analysis_identity": normalized_identity,
                "analysis_identity_hash": identity_hash,
                "review_or_approval_reference": review_or_approval_reference,
            },
        )

    def log_inspection(
        self,
        *,
        analysis_id: str,
        version_id: str,
        actor_id: str,
        actor_role: str,
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.get_analysis(analysis_id, version_id)
        payload = dict(record)
        payload.setdefault("inspection_id", str(uuid4()))
        payload.setdefault("inspected_at", _utc_now())
        payload["actor_id"] = actor_id
        payload["actor_role"] = actor_role
        self._validate_inspection_payload(payload)
        return self._append_event(
            event_type="EVIDENCE_INSPECTION",
            analysis_id=analysis_id,
            version_id=version_id,
            actor_id=actor_id,
            actor_role=actor_role,
            payload=payload,
        )
