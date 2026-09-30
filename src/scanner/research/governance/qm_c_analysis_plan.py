"""QM-C2 versioned analysis plans and confirmatory freeze governance.

Research-only. A confirmatory analysis is not ready merely because its QM-C1
hypothesis is frozen. QM-C2 requires a matching immutable analysis-plan version,
a predeclared freeze context, the exact QM-A immutable identity, and continued
respect for QM-B strict-universe promotion constraints.
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
from scanner.research.governance.qm_c import HypothesisRegistry


EVENT_SCHEMA_VERSION = "qm_c_analysis_plan_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c_analysis_plan_v1.json"


class AnalysisPlanError(ValueError):
    """Raised when a QM-C2 analysis-plan invariant is violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise AnalysisPlanError(f"value_required:{field}")
    return normalized


def _string_list(value: Any, *, field: str, nonempty: bool) -> list[str]:
    if not isinstance(value, list):
        raise AnalysisPlanError(f"list_required:{field}")
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_nonblank(item, field=f"{field}[{index}]"))
    if nonempty and not normalized:
        raise AnalysisPlanError(f"nonempty_list_required:{field}")
    if len(set(normalized)) != len(normalized):
        raise AnalysisPlanError(f"duplicate_list_value:{field}")
    return normalized


def load_qm_c_analysis_plan_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AnalysisPlanError(f"qm_c_analysis_plan_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c_analysis_plan_v1":
        raise AnalysisPlanError("qm_c_analysis_plan_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise AnalysisPlanError("qm_c_analysis_plan_contract_scope_invalid")
    return payload


def analysis_plan_hash(record: Mapping[str, Any]) -> str:
    """Hash the full declared semantic analysis plan, excluding runtime freeze context."""
    return _hash(dict(record))


class AnalysisPlanRegistry:
    """Append-only, hash-chained QM-C2 analysis-plan registry."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c_analysis_plan_contract(contract_path)
        plan = self.contract["analysis_plan"]
        self.required_fields = tuple(plan["required_fields"])
        self.research_modes = set(plan["research_modes"])
        self.states = set(plan["states"])
        self.allowed_transitions = {key: set(values) for key, values in plan["allowed_transitions"].items()}
        self.mode_state_rules = {
            key: set(value["allowed_nonterminal_states"])
            for key, value in plan["mode_state_rules"].items()
        }
        self.list_fields = tuple(plan["list_fields"])
        self.nonempty_list_fields = set(plan["nonempty_list_fields"])
        self.freeze_context_fields = tuple(self.contract["freeze_context"]["required_fields"])
        self.event_types = set(self.contract["registry"]["event_types"])

    @staticmethod
    def _key(plan_id: str, plan_version: str) -> str:
        return f"{plan_id}::{plan_version}"

    def _normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise AnalysisPlanError("analysis_plan_record_must_be_object")
        missing = [field for field in self.required_fields if field not in record]
        if missing:
            raise AnalysisPlanError("analysis_plan_fields_missing:" + ",".join(missing))

        normalized: dict[str, Any] = {
            "analysis_plan_id": _nonblank(record.get("analysis_plan_id"), field="analysis_plan_id"),
            "analysis_plan_version": _nonblank(record.get("analysis_plan_version"), field="analysis_plan_version"),
            "hypothesis_id": _nonblank(record.get("hypothesis_id"), field="hypothesis_id"),
            "hypothesis_version": _nonblank(record.get("hypothesis_version"), field="hypothesis_version"),
            "hypothesis_version_hash": _nonblank(record.get("hypothesis_version_hash"), field="hypothesis_version_hash"),
            "research_mode": _nonblank(record.get("research_mode"), field="research_mode").upper(),
            "primary_estimand": _nonblank(record.get("primary_estimand"), field="primary_estimand"),
            "population_definition": _nonblank(record.get("population_definition"), field="population_definition"),
            "universe_requirement": _nonblank(record.get("universe_requirement"), field="universe_requirement").upper(),
        }
        if normalized["research_mode"] not in self.research_modes:
            raise AnalysisPlanError("analysis_plan_research_mode_invalid")
        for field in self.list_fields:
            normalized[field] = _string_list(
                record.get(field), field=field, nonempty=field in self.nonempty_list_fields
            )
        return normalized

    def _normalize_freeze_context(self, context: Mapping[str, Any]) -> dict[str, str]:
        if not isinstance(context, Mapping):
            raise AnalysisPlanError("freeze_context_must_be_object")
        missing = [field for field in self.freeze_context_fields if field not in context]
        if missing:
            raise AnalysisPlanError("freeze_context_fields_missing:" + ",".join(missing))
        return {field: _nonblank(context.get(field), field=f"freeze_context.{field}") for field in self.freeze_context_fields}

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
                raise AnalysisPlanError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise AnalysisPlanError(f"analysis_plan_registry_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: list[dict[str, Any]]) -> None:
        previous_hash: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise AnalysisPlanError(f"analysis_plan_registry_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise AnalysisPlanError(f"analysis_plan_registry_sequence_invalid:{expected_sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise AnalysisPlanError(f"analysis_plan_registry_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise AnalysisPlanError(f"analysis_plan_registry_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash(body):
                raise AnalysisPlanError(f"analysis_plan_registry_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _state_allowed_for_mode(self, *, research_mode: str, state: str) -> bool:
        if state in {"INVALIDATED", "RETIRED"}:
            return True
        return state in self.mode_state_rules[research_mode]

    def _replay(self, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        plans: dict[str, dict[str, Any]] = {}
        latest_version_by_id: dict[str, str] = {}

        for event in events:
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise AnalysisPlanError(f"analysis_plan_registry_event_type_unknown:{event_type}")
            plan_id = _nonblank(event.get("analysis_plan_id"), field="event.analysis_plan_id")
            plan_version = _nonblank(event.get("analysis_plan_version"), field="event.analysis_plan_version")
            key = self._key(plan_id, plan_version)
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise AnalysisPlanError("analysis_plan_registry_payload_must_be_object")

            if event_type == "ANALYSIS_PLAN_REGISTERED":
                if key in plans:
                    raise AnalysisPlanError(f"analysis_plan_version_already_registered:{key}")
                record = self._normalize_record(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["analysis_plan_id"] != plan_id or record["analysis_plan_version"] != plan_version:
                    raise AnalysisPlanError("analysis_plan_event_record_identity_mismatch")
                stored_hash = _nonblank(payload.get("analysis_plan_hash"), field="analysis_plan_hash")
                if stored_hash != analysis_plan_hash(record):
                    raise AnalysisPlanError("analysis_plan_hash_mismatch")
                supersedes = payload.get("supersedes_analysis_plan_version")
                if supersedes is not None:
                    supersedes = _nonblank(supersedes, field="supersedes_analysis_plan_version")
                    predecessor_key = self._key(plan_id, supersedes)
                    if predecessor_key not in plans:
                        raise AnalysisPlanError(f"superseded_analysis_plan_version_not_registered:{predecessor_key}")
                    if supersedes == plan_version:
                        raise AnalysisPlanError("analysis_plan_version_cannot_supersede_itself")
                    if latest_version_by_id.get(plan_id) != supersedes:
                        raise AnalysisPlanError("analysis_plan_successor_must_supersede_latest_registered_version")
                elif plan_id in latest_version_by_id:
                    raise AnalysisPlanError("analysis_plan_successor_requires_supersedes_reference")
                plans[key] = {
                    **record,
                    "state": "DRAFT",
                    "analysis_plan_hash": stored_hash,
                    "supersedes_analysis_plan_version": supersedes,
                    "freeze_context": None,
                    "freeze_context_hash": None,
                    "registered_at": event["recorded_at"],
                    "registered_by": event["actor_id"],
                    "last_event_hash": event["entry_hash"],
                }
                latest_version_by_id[plan_id] = plan_version
                continue

            if key not in plans:
                raise AnalysisPlanError(f"analysis_plan_version_not_registered:{key}")
            current = plans[key]

            if event_type == "ANALYSIS_PLAN_FROZEN":
                if current["state"] != "DRAFT":
                    raise AnalysisPlanError("analysis_plan_freeze_requires_draft")
                if current["research_mode"] != "CONFIRMATION":
                    raise AnalysisPlanError("only_confirmation_plan_can_freeze")
                if payload.get("analysis_plan_hash") != current["analysis_plan_hash"]:
                    raise AnalysisPlanError("analysis_plan_freeze_hash_mismatch")
                context = self._normalize_freeze_context(
                    payload.get("freeze_context") if isinstance(payload.get("freeze_context"), Mapping) else {}
                )
                if payload.get("freeze_context_hash") != _hash(context):
                    raise AnalysisPlanError("analysis_plan_freeze_context_hash_mismatch")
                current["state"] = "FROZEN_FOR_CONFIRMATION"
                current["freeze_context"] = context
                current["freeze_context_hash"] = payload["freeze_context_hash"]
                current["last_event_hash"] = event["entry_hash"]
                continue

            from_state = str(payload.get("from_state") or "")
            to_state = str(payload.get("to_state") or "")
            if from_state != current["state"]:
                raise AnalysisPlanError("analysis_plan_transition_from_state_mismatch")
            if to_state not in self.states:
                raise AnalysisPlanError("analysis_plan_transition_target_unknown")
            if to_state == "FROZEN_FOR_CONFIRMATION":
                raise AnalysisPlanError("analysis_plan_freeze_requires_freeze_event")
            if to_state not in self.allowed_transitions.get(from_state, set()):
                raise AnalysisPlanError(f"analysis_plan_transition_forbidden:{from_state}->{to_state}")
            if not self._state_allowed_for_mode(research_mode=current["research_mode"], state=to_state):
                raise AnalysisPlanError(f"analysis_plan_state_incompatible_with_mode:{current['research_mode']}:{to_state}")
            _nonblank(payload.get("reason"), field="transition.reason")
            current["state"] = to_state
            current["last_event_hash"] = event["entry_hash"]

        return plans

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise AnalysisPlanError(f"analysis_plan_registry_lock_exists:{self.lock_path}") from exc

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
        analysis_plan_id: str,
        analysis_plan_version: str,
        actor_id: str,
        actor_role: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if event_type not in self.event_types:
            raise AnalysisPlanError(f"unsupported_analysis_plan_event_type:{event_type}")
        analysis_plan_id = _nonblank(analysis_plan_id, field="analysis_plan_id")
        analysis_plan_version = _nonblank(analysis_plan_version, field="analysis_plan_version")
        actor_id = _nonblank(actor_id, field="actor_id")
        actor_role = _nonblank(actor_role, field="actor_role")

        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": event_type,
                "recorded_at": _utc_now(),
                "analysis_plan_id": analysis_plan_id,
                "analysis_plan_version": analysis_plan_version,
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

    @staticmethod
    def _validate_hypothesis_binding(record: Mapping[str, Any], hypothesis: Mapping[str, Any]) -> None:
        for field in ("hypothesis_id", "hypothesis_version", "hypothesis_version_hash", "research_mode", "universe_requirement"):
            if record[field] != hypothesis[field]:
                raise AnalysisPlanError(f"analysis_plan_hypothesis_binding_mismatch:{field}")
        if hypothesis["state"] in {"REJECTED", "RETIRED"}:
            raise AnalysisPlanError("analysis_plan_cannot_bind_terminal_hypothesis")

    def register_plan(
        self,
        *,
        record: Mapping[str, Any],
        hypothesis_registry: HypothesisRegistry,
        actor_id: str,
        actor_role: str,
        supersedes_analysis_plan_version: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_record(record)
        hypothesis = hypothesis_registry.get_hypothesis(normalized["hypothesis_id"], normalized["hypothesis_version"])
        self._validate_hypothesis_binding(normalized, hypothesis)
        _, plans = self._load_and_validate()
        key = self._key(normalized["analysis_plan_id"], normalized["analysis_plan_version"])
        if key in plans:
            raise AnalysisPlanError(f"analysis_plan_version_already_registered:{key}")
        return self._append_event(
            event_type="ANALYSIS_PLAN_REGISTERED",
            analysis_plan_id=normalized["analysis_plan_id"],
            analysis_plan_version=normalized["analysis_plan_version"],
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "record": normalized,
                "analysis_plan_hash": analysis_plan_hash(normalized),
                "supersedes_analysis_plan_version": supersedes_analysis_plan_version,
            },
        )

    def get_plan(self, analysis_plan_id: str, analysis_plan_version: str) -> dict[str, Any]:
        _, plans = self._load_and_validate()
        key = self._key(analysis_plan_id, analysis_plan_version)
        if key not in plans:
            raise AnalysisPlanError(f"analysis_plan_version_not_registered:{key}")
        return dict(plans[key])

    def transition(
        self,
        *,
        analysis_plan_id: str,
        analysis_plan_version: str,
        to_state: str,
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        current = self.get_plan(analysis_plan_id, analysis_plan_version)
        to_state = _nonblank(to_state, field="to_state").upper()
        if to_state == "FROZEN_FOR_CONFIRMATION":
            raise AnalysisPlanError("analysis_plan_freeze_requires_freeze_plan")
        if to_state not in self.states:
            raise AnalysisPlanError("analysis_plan_transition_target_unknown")
        if to_state not in self.allowed_transitions.get(current["state"], set()):
            raise AnalysisPlanError(f"analysis_plan_transition_forbidden:{current['state']}->{to_state}")
        if not self._state_allowed_for_mode(research_mode=current["research_mode"], state=to_state):
            raise AnalysisPlanError(f"analysis_plan_state_incompatible_with_mode:{current['research_mode']}:{to_state}")
        return self._append_event(
            event_type="ANALYSIS_PLAN_STATE_TRANSITION",
            analysis_plan_id=analysis_plan_id,
            analysis_plan_version=analysis_plan_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={"from_state": current["state"], "to_state": to_state, "reason": _nonblank(reason, field="reason")},
        )

    def freeze_plan(
        self,
        *,
        analysis_plan_id: str,
        analysis_plan_version: str,
        freeze_context: Mapping[str, Any],
        hypothesis_registry: HypothesisRegistry,
        qm_b_closure: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        current = self.get_plan(analysis_plan_id, analysis_plan_version)
        if current["state"] != "DRAFT":
            raise AnalysisPlanError("analysis_plan_freeze_requires_draft")
        if current["research_mode"] != "CONFIRMATION":
            raise AnalysisPlanError("only_confirmation_plan_can_freeze")
        hypothesis = hypothesis_registry.get_hypothesis(current["hypothesis_id"], current["hypothesis_version"])
        self._validate_hypothesis_binding(current, hypothesis)
        if hypothesis["state"] != "DRAFT":
            raise AnalysisPlanError("analysis_plan_must_freeze_before_hypothesis_confirmation_freeze")
        # Reuse the canonical QM-C1 -> QM-B fail-closed check rather than copying
        # or weakening historical-universe semantics in QM-C2.
        hypothesis_registry.validate_qm_b_binding(
            hypothesis_id=current["hypothesis_id"],
            hypothesis_version=current["hypothesis_version"],
            qm_b_closure=qm_b_closure,
        )
        context = self._normalize_freeze_context(freeze_context)
        return self._append_event(
            event_type="ANALYSIS_PLAN_FROZEN",
            analysis_plan_id=analysis_plan_id,
            analysis_plan_version=analysis_plan_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "analysis_plan_hash": current["analysis_plan_hash"],
                "freeze_context": context,
                "freeze_context_hash": _hash(context),
                "reason": _nonblank(reason, field="reason"),
            },
        )

    def build_qm_a_identity(self, analysis_plan_id: str, analysis_plan_version: str) -> dict[str, str]:
        plan = self.get_plan(analysis_plan_id, analysis_plan_version)
        if plan["state"] != "FROZEN_FOR_CONFIRMATION" or not isinstance(plan.get("freeze_context"), Mapping):
            raise AnalysisPlanError("frozen_analysis_plan_required_for_qm_a_identity")
        identity = {
            "hypothesis_version_hash": plan["hypothesis_version_hash"],
            "analysis_plan_hash": plan["analysis_plan_hash"],
            **dict(plan["freeze_context"]),
        }
        required = tuple(self.contract["qm_a_binding"]["required_identity_fields"])
        if set(identity) != set(required):
            raise AnalysisPlanError("qm_a_identity_field_set_mismatch")
        return {field: _nonblank(identity.get(field), field=f"qm_a_identity.{field}") for field in required}

    def validate_confirmation_ready(
        self,
        *,
        analysis_plan_id: str,
        analysis_plan_version: str,
        hypothesis_registry: HypothesisRegistry,
        qm_a_ledger: GovernanceLedger,
        qm_a_version_id: str,
        qm_b_closure: Mapping[str, Any],
    ) -> dict[str, Any]:
        plan = self.get_plan(analysis_plan_id, analysis_plan_version)
        if plan["research_mode"] != "CONFIRMATION":
            raise AnalysisPlanError("confirmation_readiness_requires_confirmation_plan")
        if plan["state"] != self.contract["confirmation_readiness"]["plan_state_required"]:
            raise AnalysisPlanError("confirmation_readiness_requires_frozen_plan")

        hypothesis = hypothesis_registry.get_hypothesis(plan["hypothesis_id"], plan["hypothesis_version"])
        self._validate_hypothesis_binding(plan, hypothesis)
        if hypothesis["state"] != self.contract["confirmation_readiness"]["hypothesis_state_required"]:
            raise AnalysisPlanError("confirmation_readiness_requires_frozen_hypothesis")

        hypothesis_registry.validate_qm_b_binding(
            hypothesis_id=plan["hypothesis_id"],
            hypothesis_version=plan["hypothesis_version"],
            qm_b_closure=qm_b_closure,
        )
        hypothesis_registry.validate_qm_a_binding(
            hypothesis_id=plan["hypothesis_id"],
            hypothesis_version=plan["hypothesis_version"],
            qm_a_ledger=qm_a_ledger,
            qm_a_version_id=qm_a_version_id,
        )
        analysis = qm_a_ledger.get_analysis(hypothesis["qm_a_analysis_id"], qm_a_version_id)
        if analysis["state"] != self.contract["confirmation_readiness"]["qm_a_state_required"]:
            raise AnalysisPlanError("confirmation_readiness_requires_frozen_qm_a_analysis")
        actual_identity = analysis.get("identity")
        if not isinstance(actual_identity, Mapping):
            raise AnalysisPlanError("confirmation_readiness_requires_qm_a_identity")
        expected_identity = self.build_qm_a_identity(analysis_plan_id, analysis_plan_version)
        for field, expected in expected_identity.items():
            if actual_identity.get(field) != expected:
                raise AnalysisPlanError(f"confirmation_qm_a_identity_mismatch:{field}")

        return {
            "valid": True,
            "analysis_plan_id": plan["analysis_plan_id"],
            "analysis_plan_version": plan["analysis_plan_version"],
            "analysis_plan_hash": plan["analysis_plan_hash"],
            "hypothesis_id": plan["hypothesis_id"],
            "hypothesis_version": plan["hypothesis_version"],
            "hypothesis_version_hash": plan["hypothesis_version_hash"],
            "qm_a_analysis_id": analysis["analysis_id"],
            "qm_a_version_id": analysis["version_id"],
            "states": {
                "analysis_plan": plan["state"],
                "hypothesis": hypothesis["state"],
                "qm_a": analysis["state"],
            },
        }

    def verify_integrity(self) -> dict[str, Any]:
        events, plans = self._load_and_validate()
        return {
            "schema_version": "qm_c_analysis_plan_registry_verification_v1",
            "valid": True,
            "event_count": len(events),
            "analysis_plan_version_count": len(plans),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {key: value["state"] for key, value in sorted(plans.items())},
        }
