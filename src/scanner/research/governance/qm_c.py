"""QM-C1 versioned hypothesis registry and governance bindings.

Research-only. This module provides immutable hypothesis-version identities,
append-only state transitions, QM-A compatibility checks and fail-closed QM-B
strict-universe checks. It does not implement analysis plans, multiplicity,
sequential monitoring, scanner semantics, portfolio actions or orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from scanner.research.governance.qm_a import GovernanceLedger


EVENT_SCHEMA_VERSION = "qm_c_hypothesis_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c_hypothesis_registry_v1.json"


class HypothesisRegistryError(ValueError):
    """Raised when a QM-C1 registry invariant is violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise HypothesisRegistryError(f"value_required:{field}")
    return normalized


def load_qm_c_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HypothesisRegistryError(f"qm_c_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c_hypothesis_registry_v1":
        raise HypothesisRegistryError("qm_c_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise HypothesisRegistryError("qm_c_contract_scope_invalid")
    return payload


def hypothesis_version_hash(record: Mapping[str, Any]) -> str:
    """Return the immutable semantic hash of one registered hypothesis version."""
    semantic_fields = (
        "hypothesis_id",
        "hypothesis_version",
        "research_question",
        "hypothesis_statement",
        "hypothesis_family_id",
        "research_mode",
        "qm_a_analysis_id",
        "universe_requirement",
    )
    semantic = {field: record[field] for field in semantic_fields}
    return _hash(semantic)


class HypothesisRegistry:
    """Append-only, hash-chained QM-C1 hypothesis registry."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c_contract(contract_path)
        spec = self.contract["hypothesis"]
        self.required_fields = tuple(spec["required_fields"])
        self.research_modes = set(spec["research_modes"])
        self.universe_requirements = set(spec["universe_requirements"])
        self.states = set(spec["states"])
        self.allowed_transitions = {key: set(values) for key, values in spec["allowed_transitions"].items()}
        self.mode_state_rules = {
            key: set(value["allowed_nonterminal_states"])
            for key, value in spec["mode_state_rules"].items()
        }
        self.event_types = set(self.contract["registry"]["event_types"])

    @staticmethod
    def _key(hypothesis_id: str, hypothesis_version: str) -> str:
        return f"{hypothesis_id}::{hypothesis_version}"

    def _normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise HypothesisRegistryError("hypothesis_record_must_be_object")
        missing = [field for field in self.required_fields if field not in record]
        if missing:
            raise HypothesisRegistryError("hypothesis_fields_missing:" + ",".join(missing))

        normalized = {
            "hypothesis_id": _nonblank(record.get("hypothesis_id"), field="hypothesis_id"),
            "hypothesis_version": _nonblank(record.get("hypothesis_version"), field="hypothesis_version"),
            "research_question": _nonblank(record.get("research_question"), field="research_question"),
            "hypothesis_statement": _nonblank(record.get("hypothesis_statement"), field="hypothesis_statement"),
            "hypothesis_family_id": _nonblank(record.get("hypothesis_family_id"), field="hypothesis_family_id"),
            "research_mode": _nonblank(record.get("research_mode"), field="research_mode").upper(),
            "qm_a_analysis_id": _nonblank(record.get("qm_a_analysis_id"), field="qm_a_analysis_id"),
            "universe_requirement": _nonblank(record.get("universe_requirement"), field="universe_requirement").upper(),
        }
        if normalized["research_mode"] not in self.research_modes:
            raise HypothesisRegistryError("research_mode_invalid")
        if normalized["universe_requirement"] not in self.universe_requirements:
            raise HypothesisRegistryError("universe_requirement_invalid")
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
                raise HypothesisRegistryError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise HypothesisRegistryError(f"registry_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: list[dict[str, Any]]) -> None:
        previous_hash: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise HypothesisRegistryError(f"registry_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise HypothesisRegistryError(f"registry_sequence_invalid:{expected_sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise HypothesisRegistryError(f"registry_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise HypothesisRegistryError(f"registry_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash(body):
                raise HypothesisRegistryError(f"registry_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _state_allowed_for_mode(self, *, research_mode: str, state: str) -> bool:
        if state in {"REJECTED", "RETIRED"}:
            return True
        return state in self.mode_state_rules[research_mode]

    def _replay(self, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        versions: dict[str, dict[str, Any]] = {}
        latest_version_by_id: dict[str, str] = {}

        for event in events:
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise HypothesisRegistryError(f"registry_event_type_unknown:{event_type}")
            hypothesis_id = _nonblank(event.get("hypothesis_id"), field="event.hypothesis_id")
            hypothesis_version = _nonblank(event.get("hypothesis_version"), field="event.hypothesis_version")
            key = self._key(hypothesis_id, hypothesis_version)
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise HypothesisRegistryError("registry_payload_must_be_object")

            if event_type == "HYPOTHESIS_REGISTERED":
                if key in versions:
                    raise HypothesisRegistryError(f"hypothesis_version_already_registered:{key}")
                record = self._normalize_record(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["hypothesis_id"] != hypothesis_id or record["hypothesis_version"] != hypothesis_version:
                    raise HypothesisRegistryError("event_record_identity_mismatch")
                stored_hash = _nonblank(payload.get("hypothesis_version_hash"), field="hypothesis_version_hash")
                if stored_hash != hypothesis_version_hash(record):
                    raise HypothesisRegistryError("hypothesis_version_hash_mismatch")

                supersedes = payload.get("supersedes_hypothesis_version")
                if supersedes is not None:
                    supersedes = _nonblank(supersedes, field="supersedes_hypothesis_version")
                    predecessor_key = self._key(hypothesis_id, supersedes)
                    if predecessor_key not in versions:
                        raise HypothesisRegistryError(f"superseded_hypothesis_version_not_registered:{predecessor_key}")
                    if supersedes == hypothesis_version:
                        raise HypothesisRegistryError("hypothesis_version_cannot_supersede_itself")
                    if latest_version_by_id.get(hypothesis_id) != supersedes:
                        raise HypothesisRegistryError("successor_must_supersede_latest_registered_version")
                elif hypothesis_id in latest_version_by_id:
                    raise HypothesisRegistryError("successor_version_requires_supersedes_reference")

                versions[key] = {
                    **record,
                    "state": "DRAFT",
                    "hypothesis_version_hash": stored_hash,
                    "supersedes_hypothesis_version": supersedes,
                    "registered_at": event["recorded_at"],
                    "registered_by": event["actor_id"],
                    "last_event_hash": event["entry_hash"],
                }
                latest_version_by_id[hypothesis_id] = hypothesis_version
                continue

            if key not in versions:
                raise HypothesisRegistryError(f"hypothesis_version_not_registered:{key}")
            current = versions[key]
            from_state = str(payload.get("from_state") or "")
            to_state = str(payload.get("to_state") or "")
            if from_state != current["state"]:
                raise HypothesisRegistryError("hypothesis_transition_from_state_mismatch")
            if to_state not in self.states:
                raise HypothesisRegistryError("hypothesis_transition_target_unknown")
            if to_state not in self.allowed_transitions.get(from_state, set()):
                raise HypothesisRegistryError(f"hypothesis_transition_forbidden:{from_state}->{to_state}")
            if not self._state_allowed_for_mode(research_mode=current["research_mode"], state=to_state):
                raise HypothesisRegistryError(
                    f"hypothesis_state_incompatible_with_mode:{current['research_mode']}:{to_state}"
                )
            _nonblank(payload.get("reason"), field="transition.reason")
            current["state"] = to_state
            current["last_event_hash"] = event["entry_hash"]

        return versions

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise HypothesisRegistryError(f"registry_lock_exists:{self.lock_path}") from exc

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
        hypothesis_id: str,
        hypothesis_version: str,
        actor_id: str,
        actor_role: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if event_type not in self.event_types:
            raise HypothesisRegistryError(f"unsupported_event_type:{event_type}")
        actor_id = _nonblank(actor_id, field="actor_id")
        actor_role = _nonblank(actor_role, field="actor_role")
        hypothesis_id = _nonblank(hypothesis_id, field="hypothesis_id")
        hypothesis_version = _nonblank(hypothesis_version, field="hypothesis_version")

        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": event_type,
                "recorded_at": _utc_now(),
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": hypothesis_version,
                "actor_id": actor_id,
                "actor_role": actor_role,
                "payload": dict(payload),
                "previous_event_hash": events[-1]["entry_hash"] if events else None,
            }
            event["entry_hash"] = _hash(event)
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

    def register_hypothesis(
        self,
        *,
        record: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        supersedes_hypothesis_version: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_record(record)
        _, versions = self._load_and_validate()
        key = self._key(normalized["hypothesis_id"], normalized["hypothesis_version"])
        if key in versions:
            raise HypothesisRegistryError(f"hypothesis_version_already_registered:{key}")
        return self._append_event(
            event_type="HYPOTHESIS_REGISTERED",
            hypothesis_id=normalized["hypothesis_id"],
            hypothesis_version=normalized["hypothesis_version"],
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "record": normalized,
                "hypothesis_version_hash": hypothesis_version_hash(normalized),
                "supersedes_hypothesis_version": supersedes_hypothesis_version,
            },
        )

    def transition(
        self,
        *,
        hypothesis_id: str,
        hypothesis_version: str,
        to_state: str,
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        current = self.get_hypothesis(hypothesis_id, hypothesis_version)
        to_state = _nonblank(to_state, field="to_state").upper()
        reason = _nonblank(reason, field="reason")
        if to_state not in self.states:
            raise HypothesisRegistryError("hypothesis_transition_target_unknown")
        if to_state not in self.allowed_transitions.get(current["state"], set()):
            raise HypothesisRegistryError(f"hypothesis_transition_forbidden:{current['state']}->{to_state}")
        if not self._state_allowed_for_mode(research_mode=current["research_mode"], state=to_state):
            raise HypothesisRegistryError(
                f"hypothesis_state_incompatible_with_mode:{current['research_mode']}:{to_state}"
            )
        return self._append_event(
            event_type="HYPOTHESIS_STATE_TRANSITION",
            hypothesis_id=hypothesis_id,
            hypothesis_version=hypothesis_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={"from_state": current["state"], "to_state": to_state, "reason": reason},
        )

    def get_hypothesis(self, hypothesis_id: str, hypothesis_version: str) -> dict[str, Any]:
        _, versions = self._load_and_validate()
        key = self._key(hypothesis_id, hypothesis_version)
        if key not in versions:
            raise HypothesisRegistryError(f"hypothesis_version_not_registered:{key}")
        return dict(versions[key])

    def verify_integrity(self) -> dict[str, Any]:
        events, versions = self._load_and_validate()
        return {
            "schema_version": "qm_c_hypothesis_registry_verification_v1",
            "valid": True,
            "event_count": len(events),
            "hypothesis_version_count": len(versions),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {key: value["state"] for key, value in sorted(versions.items())},
        }

    def validate_qm_a_binding(
        self,
        *,
        hypothesis_id: str,
        hypothesis_version: str,
        qm_a_ledger: GovernanceLedger,
        qm_a_version_id: str,
    ) -> dict[str, Any]:
        """Validate that a hypothesis version and its QM-A analysis are the same governed object."""
        hypothesis = self.get_hypothesis(hypothesis_id, hypothesis_version)
        analysis = qm_a_ledger.get_analysis(hypothesis["qm_a_analysis_id"], qm_a_version_id)

        if analysis["analysis_id"] != hypothesis["qm_a_analysis_id"]:
            raise HypothesisRegistryError("qm_a_analysis_id_mismatch")

        frozen_identity = analysis.get("identity")
        if frozen_identity is not None:
            actual = str(frozen_identity.get(self.contract["qm_a_binding"]["identity_field"]) or "")
            if actual != hypothesis["hypothesis_version_hash"]:
                raise HypothesisRegistryError("qm_a_hypothesis_version_hash_mismatch")

        if hypothesis["state"] == "FROZEN_FOR_CONFIRMATION" and analysis["state"] != "FROZEN_FOR_CONFIRMATION":
            raise HypothesisRegistryError("qm_c_frozen_requires_qm_a_frozen")

        return {
            "valid": True,
            "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
            "qm_a_analysis_id": analysis["analysis_id"],
            "qm_a_version_id": analysis["version_id"],
            "qm_a_state": analysis["state"],
        }

    def validate_qm_b_binding(
        self,
        *,
        hypothesis_id: str,
        hypothesis_version: str,
        qm_b_closure: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Fail closed when confirmation requests a strict QM-B universe that is not promoted."""
        hypothesis = self.get_hypothesis(hypothesis_id, hypothesis_version)
        binding = self.contract["qm_b_binding"]
        if not isinstance(qm_b_closure, Mapping) or qm_b_closure.get("schema_version") != binding["required_closure_schema"]:
            raise HypothesisRegistryError("qm_b_closure_schema_invalid")

        promotion_field = str(binding["strict_promotion_boolean_field"])
        promoted = qm_b_closure.get(promotion_field)
        if not isinstance(promoted, bool):
            raise HypothesisRegistryError("qm_b_strict_promotion_flag_invalid")

        strict_required = hypothesis["universe_requirement"] == binding["strict_universe_requirement"]
        blocked = strict_required and hypothesis["research_mode"] == "CONFIRMATION" and not promoted
        if blocked:
            status_field = str(binding["blocked_status_field"])
            status = str(qm_b_closure.get(status_field) or "")
            raise HypothesisRegistryError(f"qm_b_strict_universe_not_promoted:{status or 'UNKNOWN'}")

        return {
            "valid": True,
            "strict_universe_required": strict_required,
            "strict_universe_promoted": promoted,
            "qm_b_status": qm_b_closure.get(binding["blocked_status_field"]),
        }
