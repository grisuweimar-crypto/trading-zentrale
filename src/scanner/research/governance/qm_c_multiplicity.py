"""QM-C3 confirmatory multiplicity and sequential-monitoring governance.

Research-only. This module does not calculate adjusted p-values or effects. It
freezes exact QM-C1/QM-C2 members into a confirmatory family, predeclares the
multiplicity strategy and permitted outcome-inspection schedule, and fails
closed on unplanned or out-of-order looks.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import uuid4

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry


EVENT_SCHEMA_VERSION = "qm_c_multiplicity_monitoring_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c_multiplicity_monitoring_v1.json"


class MultiplicityMonitoringError(ValueError):
    """Raised when a QM-C3 multiplicity or monitoring invariant is violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise MultiplicityMonitoringError(f"value_required:{field}")
    return normalized


def _bool(value: Any, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise MultiplicityMonitoringError(f"boolean_required:{field}")
    return value


def _probability(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiplicityMonitoringError(f"probability_required:{field}")
    result = float(value)
    if not math.isfinite(result) or not 0.0 < result < 1.0:
        raise MultiplicityMonitoringError(f"probability_out_of_range:{field}")
    return result


def load_qm_c_multiplicity_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MultiplicityMonitoringError(f"qm_c_multiplicity_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c_multiplicity_monitoring_v1":
        raise MultiplicityMonitoringError("qm_c_multiplicity_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise MultiplicityMonitoringError("qm_c_multiplicity_contract_scope_invalid")
    return payload


def control_plan_hash(record: Mapping[str, Any]) -> str:
    """Return the immutable semantic hash of a normalized QM-C3 control plan."""
    return _hash(dict(record))


class MultiplicityMonitoringRegistry:
    """Append-only, hash-chained registry of confirmatory control plans and looks."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c_multiplicity_contract(contract_path)
        self.event_types = set(self.contract["registry"]["event_types"])
        spec = self.contract["control_plan"]
        self.required_fields = tuple(spec["required_fields"])
        self.states = set(spec["states"])
        self.terminal_states = set(spec["terminal_states"])
        self.allowed_transitions = {key: set(values) for key, values in spec["allowed_transitions"].items()}
        self.member_required_fields = tuple(self.contract["family_member"]["required_fields"])
        self.multiplicity_strategies = set(self.contract["multiplicity"]["strategies"])
        self.monitoring_modes = set(self.contract["sequential_monitoring"]["modes"])
        self.look_decisions = set(self.contract["sequential_monitoring"]["look_decisions"])

    @staticmethod
    def _key(control_plan_id: str, control_plan_version: str) -> str:
        return f"{control_plan_id}::{control_plan_version}"

    def _normalize_member(self, value: Any, *, index: int) -> dict[str, str]:
        if not isinstance(value, Mapping):
            raise MultiplicityMonitoringError(f"family_member_must_be_object:{index}")
        missing = [field for field in self.member_required_fields if field not in value]
        if missing:
            raise MultiplicityMonitoringError(
                f"family_member_fields_missing:{index}:" + ",".join(missing)
            )
        return {
            field: _nonblank(value.get(field), field=f"family_members[{index}].{field}")
            for field in self.member_required_fields
        }

    def _normalize_multiplicity(
        self, strategy_value: Any, parameters_value: Any, *, member_count: int
    ) -> tuple[str, dict[str, Any]]:
        strategy = _nonblank(strategy_value, field="multiplicity_strategy").upper()
        if strategy not in self.multiplicity_strategies:
            raise MultiplicityMonitoringError("multiplicity_strategy_invalid")
        if not isinstance(parameters_value, Mapping):
            raise MultiplicityMonitoringError("multiplicity_parameters_must_be_object")
        parameters = dict(parameters_value)
        spec = self.contract["multiplicity"]

        if strategy == "PREDECLARED_SINGLE_PRIMARY":
            if member_count != 1:
                raise MultiplicityMonitoringError("single_primary_requires_exactly_one_family_member")
            if parameters:
                raise MultiplicityMonitoringError("single_primary_parameters_must_be_empty")
            return strategy, {}

        if strategy in set(spec["fwer_strategies"]):
            field = str(spec["fwer_parameter"])
            if set(parameters) != {field}:
                raise MultiplicityMonitoringError(f"fwer_parameters_must_equal:{field}")
            return strategy, {field: _probability(parameters[field], field=f"multiplicity_parameters.{field}")}

        if strategy in set(spec["fdr_strategies"]):
            field = str(spec["fdr_parameter"])
            if set(parameters) != {field}:
                raise MultiplicityMonitoringError(f"fdr_parameters_must_equal:{field}")
            return strategy, {field: _probability(parameters[field], field=f"multiplicity_parameters.{field}")}

        required = tuple(spec["custom_required_fields"])
        if set(parameters) != set(required):
            raise MultiplicityMonitoringError("custom_multiplicity_parameters_invalid")
        return strategy, {field: _nonblank(parameters.get(field), field=f"multiplicity_parameters.{field}") for field in required}

    def _normalize_monitoring(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise MultiplicityMonitoringError("sequential_monitoring_must_be_object")
        spec = self.contract["sequential_monitoring"]
        required = tuple(spec["required_fields"])
        missing = [field for field in required if field not in value]
        if missing:
            raise MultiplicityMonitoringError("sequential_monitoring_fields_missing:" + ",".join(missing))

        mode = _nonblank(value.get("mode"), field="sequential_monitoring.mode").upper()
        if mode not in self.monitoring_modes:
            raise MultiplicityMonitoringError("sequential_monitoring_mode_invalid")
        early_stop = _bool(value.get("early_stop_allowed"), field="sequential_monitoring.early_stop_allowed")
        stopping_rule = _nonblank(value.get("stopping_rule"), field="sequential_monitoring.stopping_rule")
        raw_looks = value.get("planned_looks")
        if not isinstance(raw_looks, list) or not raw_looks:
            raise MultiplicityMonitoringError("planned_looks_nonempty_list_required")

        look_required = tuple(spec["look_required_fields"])
        planned_looks: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        previous_fraction = 0.0
        for index, raw in enumerate(raw_looks):
            if not isinstance(raw, Mapping):
                raise MultiplicityMonitoringError(f"planned_look_must_be_object:{index}")
            missing_look = [field for field in look_required if field not in raw]
            if missing_look:
                raise MultiplicityMonitoringError(
                    f"planned_look_fields_missing:{index}:" + ",".join(missing_look)
                )
            look_id = _nonblank(raw.get("look_id"), field=f"planned_looks[{index}].look_id")
            if look_id in seen_ids:
                raise MultiplicityMonitoringError(f"duplicate_planned_look_id:{look_id}")
            seen_ids.add(look_id)
            fraction_value = raw.get("information_fraction")
            if isinstance(fraction_value, bool) or not isinstance(fraction_value, (int, float)):
                raise MultiplicityMonitoringError(f"information_fraction_required:{look_id}")
            fraction = float(fraction_value)
            if not math.isfinite(fraction) or not 0.0 < fraction <= 1.0:
                raise MultiplicityMonitoringError(f"information_fraction_out_of_range:{look_id}")
            if fraction <= previous_fraction:
                raise MultiplicityMonitoringError("information_fractions_must_be_strictly_increasing")
            previous_fraction = fraction
            planned_looks.append({"look_id": look_id, "information_fraction": fraction})

        if not math.isclose(planned_looks[-1]["information_fraction"], float(spec["final_information_fraction"]), rel_tol=0.0, abs_tol=1e-12):
            raise MultiplicityMonitoringError("final_planned_look_must_have_information_fraction_one")

        if mode == "FIXED_HORIZON_NO_INTERIM":
            if len(planned_looks) != 1 or not math.isclose(planned_looks[0]["information_fraction"], 1.0):
                raise MultiplicityMonitoringError("fixed_horizon_requires_single_final_look")
            if early_stop:
                raise MultiplicityMonitoringError("fixed_horizon_cannot_allow_early_stop")
        elif mode == "PREDECLARED_LOOKS":
            if len(planned_looks) < 2:
                raise MultiplicityMonitoringError("predeclared_looks_requires_at_least_two_looks")
        else:
            custom_required = tuple(spec["custom_required_fields"])
            extra = value.get("custom_rule")
            if not isinstance(extra, Mapping) or set(extra) != set(custom_required):
                raise MultiplicityMonitoringError("custom_monitoring_rule_invalid")
            custom_rule = {
                field: _nonblank(extra.get(field), field=f"sequential_monitoring.custom_rule.{field}")
                for field in custom_required
            }
            return {
                "mode": mode,
                "planned_looks": planned_looks,
                "early_stop_allowed": early_stop,
                "stopping_rule": stopping_rule,
                "custom_rule": custom_rule,
            }

        if "custom_rule" in value:
            raise MultiplicityMonitoringError("custom_rule_only_allowed_for_custom_monitoring")
        return {
            "mode": mode,
            "planned_looks": planned_looks,
            "early_stop_allowed": early_stop,
            "stopping_rule": stopping_rule,
        }

    def _normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise MultiplicityMonitoringError("control_plan_record_must_be_object")
        missing = [field for field in self.required_fields if field not in record]
        if missing:
            raise MultiplicityMonitoringError("control_plan_fields_missing:" + ",".join(missing))

        research_mode = _nonblank(record.get("research_mode"), field="research_mode").upper()
        if research_mode != self.contract["control_plan"]["research_mode_required"]:
            raise MultiplicityMonitoringError("control_plan_requires_confirmation_mode")
        raw_members = record.get("family_members")
        if not isinstance(raw_members, list) or not raw_members:
            raise MultiplicityMonitoringError("family_members_nonempty_list_required")
        members = [self._normalize_member(value, index=index) for index, value in enumerate(raw_members)]
        hypothesis_keys = [(m["hypothesis_id"], m["hypothesis_version"]) for m in members]
        plan_keys = [(m["analysis_plan_id"], m["analysis_plan_version"]) for m in members]
        if len(set(hypothesis_keys)) != len(hypothesis_keys):
            raise MultiplicityMonitoringError("duplicate_hypothesis_family_member")
        if len(set(plan_keys)) != len(plan_keys):
            raise MultiplicityMonitoringError("duplicate_analysis_plan_family_member")

        strategy, parameters = self._normalize_multiplicity(
            record.get("multiplicity_strategy"), record.get("multiplicity_parameters"), member_count=len(members)
        )
        monitoring = self._normalize_monitoring(record.get("sequential_monitoring"))
        return {
            "control_plan_id": _nonblank(record.get("control_plan_id"), field="control_plan_id"),
            "control_plan_version": _nonblank(record.get("control_plan_version"), field="control_plan_version"),
            "hypothesis_family_id": _nonblank(record.get("hypothesis_family_id"), field="hypothesis_family_id"),
            "research_mode": research_mode,
            "family_members": members,
            "multiplicity_strategy": strategy,
            "multiplicity_parameters": parameters,
            "sequential_monitoring": monitoring,
        }

    def _read_raw_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        result: list[dict[str, Any]] = []
        for line_number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise MultiplicityMonitoringError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise MultiplicityMonitoringError(f"control_registry_line_not_object:{line_number}")
            result.append(value)
        return result

    def _verify_hash_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous_hash: str | None = None
        for expected_sequence, raw_event in enumerate(events, start=1):
            event = dict(raw_event)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise MultiplicityMonitoringError(f"control_registry_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise MultiplicityMonitoringError(f"control_registry_sequence_invalid:{expected_sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise MultiplicityMonitoringError(f"control_registry_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise MultiplicityMonitoringError(f"control_registry_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash(body):
                raise MultiplicityMonitoringError(f"control_registry_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        controls: dict[str, dict[str, Any]] = {}
        latest_version_by_id: dict[str, str] = {}
        for raw_event in events:
            event = dict(raw_event)
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise MultiplicityMonitoringError(f"control_registry_event_type_unknown:{event_type}")
            control_id = _nonblank(event.get("control_plan_id"), field="event.control_plan_id")
            control_version = _nonblank(event.get("control_plan_version"), field="event.control_plan_version")
            key = self._key(control_id, control_version)
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise MultiplicityMonitoringError("control_registry_payload_must_be_object")

            if event_type == "CONTROL_PLAN_REGISTERED":
                if key in controls:
                    raise MultiplicityMonitoringError(f"control_plan_version_already_registered:{key}")
                record = self._normalize_record(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["control_plan_id"] != control_id or record["control_plan_version"] != control_version:
                    raise MultiplicityMonitoringError("control_plan_event_record_identity_mismatch")
                stored_hash = _nonblank(payload.get("control_plan_hash"), field="control_plan_hash")
                if stored_hash != control_plan_hash(record):
                    raise MultiplicityMonitoringError("control_plan_hash_mismatch")
                supersedes = payload.get("supersedes_control_plan_version")
                if supersedes is not None:
                    supersedes = _nonblank(supersedes, field="supersedes_control_plan_version")
                    predecessor_key = self._key(control_id, supersedes)
                    if predecessor_key not in controls:
                        raise MultiplicityMonitoringError(f"superseded_control_plan_version_not_registered:{predecessor_key}")
                    if latest_version_by_id.get(control_id) != supersedes:
                        raise MultiplicityMonitoringError("control_plan_successor_must_supersede_latest_registered_version")
                    if supersedes == control_version:
                        raise MultiplicityMonitoringError("control_plan_version_cannot_supersede_itself")
                elif control_id in latest_version_by_id:
                    raise MultiplicityMonitoringError("control_plan_successor_requires_supersedes_reference")
                controls[key] = {
                    **record,
                    "state": "DRAFT",
                    "control_plan_hash": stored_hash,
                    "supersedes_control_plan_version": supersedes,
                    "freeze_binding_hash": None,
                    "recorded_looks": [],
                    "registered_at": event["recorded_at"],
                    "last_event_hash": event["entry_hash"],
                }
                latest_version_by_id[control_id] = control_version
                continue

            if key not in controls:
                raise MultiplicityMonitoringError(f"control_plan_version_not_registered:{key}")
            current = controls[key]

            if event_type == "CONTROL_PLAN_FROZEN":
                if current["state"] != "DRAFT":
                    raise MultiplicityMonitoringError("control_plan_freeze_requires_draft")
                if payload.get("control_plan_hash") != current["control_plan_hash"]:
                    raise MultiplicityMonitoringError("control_plan_freeze_hash_mismatch")
                binding_hash = _nonblank(payload.get("freeze_binding_hash"), field="freeze_binding_hash")
                binding_snapshot = payload.get("binding_snapshot")
                if not isinstance(binding_snapshot, list) or not binding_snapshot:
                    raise MultiplicityMonitoringError("binding_snapshot_nonempty_list_required")
                if binding_hash != _hash(binding_snapshot):
                    raise MultiplicityMonitoringError("freeze_binding_hash_mismatch")
                current["state"] = "FROZEN_FOR_CONFIRMATION"
                current["freeze_binding_hash"] = binding_hash
                current["last_event_hash"] = event["entry_hash"]
                continue

            if event_type == "MONITORING_LOOK_RECORDED":
                if current["state"] not in {"FROZEN_FOR_CONFIRMATION", "MONITORING"}:
                    raise MultiplicityMonitoringError(f"monitoring_look_not_allowed_in_state:{current['state']}")
                planned = current["sequential_monitoring"]["planned_looks"]
                look_index = len(current["recorded_looks"])
                if look_index >= len(planned):
                    raise MultiplicityMonitoringError("monitoring_look_exceeds_predeclared_schedule")
                expected = planned[look_index]
                if payload.get("look_id") != expected["look_id"]:
                    raise MultiplicityMonitoringError("monitoring_look_out_of_order")
                if payload.get("information_fraction") != expected["information_fraction"]:
                    raise MultiplicityMonitoringError("monitoring_information_fraction_mismatch")
                decision = str(payload.get("decision") or "")
                if decision not in self.look_decisions:
                    raise MultiplicityMonitoringError("monitoring_look_decision_invalid")
                final = look_index == len(planned) - 1
                if final and decision != "FINAL_COMPLETE":
                    raise MultiplicityMonitoringError("final_look_requires_final_complete_decision")
                if not final and decision == "FINAL_COMPLETE":
                    raise MultiplicityMonitoringError("interim_look_cannot_be_final_complete")
                if decision in {"STOP_EFFICACY", "STOP_FUTILITY"} and not current["sequential_monitoring"]["early_stop_allowed"]:
                    raise MultiplicityMonitoringError("early_stop_decision_not_predeclared")
                if decision == "CONTINUE" and final:
                    raise MultiplicityMonitoringError("final_look_cannot_continue")
                artifact_hash = _nonblank(payload.get("artifact_hash"), field="monitoring_look.artifact_hash")
                recorded_at = _nonblank(payload.get("observed_at"), field="monitoring_look.observed_at")
                qm_a_states = payload.get("qm_a_states")
                if not isinstance(qm_a_states, Mapping) or not qm_a_states:
                    raise MultiplicityMonitoringError("monitoring_look_qm_a_states_required")
                current["recorded_looks"].append({
                    "look_id": expected["look_id"],
                    "information_fraction": expected["information_fraction"],
                    "decision": decision,
                    "artifact_hash": artifact_hash,
                    "observed_at": recorded_at,
                    "qm_a_states": dict(qm_a_states),
                })
                if decision in {"STOP_EFFICACY", "STOP_FUTILITY"}:
                    current["state"] = "STOPPED"
                elif final:
                    current["state"] = "COMPLETE"
                else:
                    current["state"] = "MONITORING"
                current["last_event_hash"] = event["entry_hash"]
                continue

            from_state = str(payload.get("from_state") or "")
            to_state = str(payload.get("to_state") or "")
            if from_state != current["state"]:
                raise MultiplicityMonitoringError("control_plan_transition_from_state_mismatch")
            if to_state not in self.states:
                raise MultiplicityMonitoringError("control_plan_transition_target_unknown")
            if to_state not in self.allowed_transitions.get(from_state, set()):
                raise MultiplicityMonitoringError(f"control_plan_transition_forbidden:{from_state}->{to_state}")
            _nonblank(payload.get("reason"), field="transition.reason")
            current["state"] = to_state
            current["last_event_hash"] = event["entry_hash"]
        return controls

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise MultiplicityMonitoringError(f"control_registry_lock_exists:{self.lock_path}") from exc

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
        control_plan_id: str,
        control_plan_version: str,
        actor_id: str,
        actor_role: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if event_type not in self.event_types:
            raise MultiplicityMonitoringError(f"unsupported_control_event_type:{event_type}")
        control_plan_id = _nonblank(control_plan_id, field="control_plan_id")
        control_plan_version = _nonblank(control_plan_version, field="control_plan_version")
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
                "control_plan_id": control_plan_id,
                "control_plan_version": control_plan_version,
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
    def _validate_member_static_binding(
        member: Mapping[str, str],
        *,
        family_id: str,
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        hypothesis = hypothesis_registry.get_hypothesis(member["hypothesis_id"], member["hypothesis_version"])
        if hypothesis["hypothesis_family_id"] != family_id:
            raise MultiplicityMonitoringError("family_member_hypothesis_family_mismatch")
        if hypothesis["research_mode"] != "CONFIRMATION":
            raise MultiplicityMonitoringError("family_member_must_be_confirmation_hypothesis")
        if hypothesis["hypothesis_version_hash"] != member["hypothesis_version_hash"]:
            raise MultiplicityMonitoringError("family_member_hypothesis_hash_mismatch")
        if hypothesis["qm_a_analysis_id"] != member["qm_a_analysis_id"]:
            raise MultiplicityMonitoringError("family_member_qm_a_analysis_id_mismatch")

        plan = analysis_plan_registry.get_plan(member["analysis_plan_id"], member["analysis_plan_version"])
        expected = {
            "hypothesis_id": member["hypothesis_id"],
            "hypothesis_version": member["hypothesis_version"],
            "hypothesis_version_hash": member["hypothesis_version_hash"],
            "analysis_plan_hash": member["analysis_plan_hash"],
        }
        for field, expected_value in expected.items():
            if plan.get(field) != expected_value:
                raise MultiplicityMonitoringError(f"family_member_analysis_plan_binding_mismatch:{field}")
        if plan["research_mode"] != "CONFIRMATION":
            raise MultiplicityMonitoringError("family_member_analysis_plan_must_be_confirmation")
        return hypothesis, plan

    def register_control_plan(
        self,
        *,
        record: Mapping[str, Any],
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
        actor_id: str,
        actor_role: str,
        supersedes_control_plan_version: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_record(record)
        for member in normalized["family_members"]:
            self._validate_member_static_binding(
                member,
                family_id=normalized["hypothesis_family_id"],
                hypothesis_registry=hypothesis_registry,
                analysis_plan_registry=analysis_plan_registry,
            )
        _, controls = self._load_and_validate()
        key = self._key(normalized["control_plan_id"], normalized["control_plan_version"])
        if key in controls:
            raise MultiplicityMonitoringError(f"control_plan_version_already_registered:{key}")
        return self._append_event(
            event_type="CONTROL_PLAN_REGISTERED",
            control_plan_id=normalized["control_plan_id"],
            control_plan_version=normalized["control_plan_version"],
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "record": normalized,
                "control_plan_hash": control_plan_hash(normalized),
                "supersedes_control_plan_version": supersedes_control_plan_version,
            },
        )

    def get_control_plan(self, control_plan_id: str, control_plan_version: str) -> dict[str, Any]:
        _, controls = self._load_and_validate()
        key = self._key(control_plan_id, control_plan_version)
        if key not in controls:
            raise MultiplicityMonitoringError(f"control_plan_version_not_registered:{key}")
        return dict(controls[key])

    def freeze_control_plan(
        self,
        *,
        control_plan_id: str,
        control_plan_version: str,
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
        qm_a_ledger: GovernanceLedger,
        qm_b_closure: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        if current["state"] != "DRAFT":
            raise MultiplicityMonitoringError("control_plan_freeze_requires_draft")
        binding_snapshot: list[dict[str, Any]] = []
        for member in current["family_members"]:
            hypothesis, plan = self._validate_member_static_binding(
                member,
                family_id=current["hypothesis_family_id"],
                hypothesis_registry=hypothesis_registry,
                analysis_plan_registry=analysis_plan_registry,
            )
            readiness = analysis_plan_registry.validate_confirmation_ready(
                analysis_plan_id=member["analysis_plan_id"],
                analysis_plan_version=member["analysis_plan_version"],
                hypothesis_registry=hypothesis_registry,
                qm_a_ledger=qm_a_ledger,
                qm_a_version_id=member["qm_a_version_id"],
                qm_b_closure=qm_b_closure,
            )
            if readiness["analysis_plan_hash"] != member["analysis_plan_hash"]:
                raise MultiplicityMonitoringError("family_member_readiness_plan_hash_mismatch")
            binding_snapshot.append({
                "hypothesis_id": hypothesis["hypothesis_id"],
                "hypothesis_version": hypothesis["hypothesis_version"],
                "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
                "analysis_plan_id": plan["analysis_plan_id"],
                "analysis_plan_version": plan["analysis_plan_version"],
                "analysis_plan_hash": plan["analysis_plan_hash"],
                "qm_a_analysis_id": member["qm_a_analysis_id"],
                "qm_a_version_id": member["qm_a_version_id"],
                "qm_a_state": readiness["states"]["qm_a"],
            })
        return self._append_event(
            event_type="CONTROL_PLAN_FROZEN",
            control_plan_id=control_plan_id,
            control_plan_version=control_plan_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "control_plan_hash": current["control_plan_hash"],
                "binding_snapshot": binding_snapshot,
                "freeze_binding_hash": _hash(binding_snapshot),
                "reason": _nonblank(reason, field="reason"),
            },
        )

    def validate_evaluation_ready(
        self,
        *,
        control_plan_id: str,
        control_plan_version: str,
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
        qm_a_ledger: GovernanceLedger,
        qm_b_closure: Mapping[str, Any],
    ) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        if current["state"] != "FROZEN_FOR_CONFIRMATION":
            raise MultiplicityMonitoringError("evaluation_requires_frozen_control_plan")
        members: list[dict[str, Any]] = []
        for member in current["family_members"]:
            self._validate_member_static_binding(
                member,
                family_id=current["hypothesis_family_id"],
                hypothesis_registry=hypothesis_registry,
                analysis_plan_registry=analysis_plan_registry,
            )
            readiness = analysis_plan_registry.validate_confirmation_ready(
                analysis_plan_id=member["analysis_plan_id"],
                analysis_plan_version=member["analysis_plan_version"],
                hypothesis_registry=hypothesis_registry,
                qm_a_ledger=qm_a_ledger,
                qm_a_version_id=member["qm_a_version_id"],
                qm_b_closure=qm_b_closure,
            )
            members.append({
                "hypothesis_id": member["hypothesis_id"],
                "analysis_plan_id": member["analysis_plan_id"],
                "qm_a_analysis_id": member["qm_a_analysis_id"],
                "ready": readiness["valid"],
            })
        return {
            "valid": True,
            "control_plan_id": current["control_plan_id"],
            "control_plan_version": current["control_plan_version"],
            "control_plan_hash": current["control_plan_hash"],
            "hypothesis_family_id": current["hypothesis_family_id"],
            "multiplicity_strategy": current["multiplicity_strategy"],
            "sequential_mode": current["sequential_monitoring"]["mode"],
            "next_look": current["sequential_monitoring"]["planned_looks"][0],
            "members": members,
        }

    def record_monitoring_look(
        self,
        *,
        control_plan_id: str,
        control_plan_version: str,
        look_id: str,
        artifact_hash: str,
        decision: str,
        qm_a_ledger: GovernanceLedger,
        actor_id: str,
        actor_role: str,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        if current["state"] not in {"FROZEN_FOR_CONFIRMATION", "MONITORING"}:
            raise MultiplicityMonitoringError(f"monitoring_look_not_allowed_in_state:{current['state']}")
        planned = current["sequential_monitoring"]["planned_looks"]
        index = len(current["recorded_looks"])
        if index >= len(planned):
            raise MultiplicityMonitoringError("monitoring_look_exceeds_predeclared_schedule")
        expected = planned[index]
        if _nonblank(look_id, field="look_id") != expected["look_id"]:
            raise MultiplicityMonitoringError(f"monitoring_look_out_of_order:expected={expected['look_id']}")
        decision = _nonblank(decision, field="decision").upper()
        if decision not in self.look_decisions:
            raise MultiplicityMonitoringError("monitoring_look_decision_invalid")
        final = index == len(planned) - 1
        if final and decision != "FINAL_COMPLETE":
            raise MultiplicityMonitoringError("final_look_requires_final_complete_decision")
        if not final and decision == "FINAL_COMPLETE":
            raise MultiplicityMonitoringError("interim_look_cannot_be_final_complete")
        if decision in {"STOP_EFFICACY", "STOP_FUTILITY"} and not current["sequential_monitoring"]["early_stop_allowed"]:
            raise MultiplicityMonitoringError("early_stop_decision_not_predeclared")

        qm_a_states: dict[str, str] = {}
        for member in current["family_members"]:
            analysis = qm_a_ledger.get_analysis(member["qm_a_analysis_id"], member["qm_a_version_id"])
            state = str(analysis["state"])
            if index == 0:
                if state != "FROZEN_FOR_CONFIRMATION":
                    raise MultiplicityMonitoringError(
                        f"first_monitoring_look_requires_frozen_qm_a:{member['qm_a_analysis_id']}:{state}"
                    )
            elif state not in qm_a_ledger.spent_states:
                raise MultiplicityMonitoringError(
                    f"subsequent_monitoring_look_requires_consumed_qm_a_evidence:{member['qm_a_analysis_id']}:{state}"
                )
            qm_a_states[f"{member['qm_a_analysis_id']}::{member['qm_a_version_id']}"] = state

        return self._append_event(
            event_type="MONITORING_LOOK_RECORDED",
            control_plan_id=control_plan_id,
            control_plan_version=control_plan_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "look_id": expected["look_id"],
                "information_fraction": expected["information_fraction"],
                "artifact_hash": _nonblank(artifact_hash, field="artifact_hash"),
                "decision": decision,
                "observed_at": observed_at or _utc_now(),
                "qm_a_states": qm_a_states,
            },
        )

    def transition(
        self,
        *,
        control_plan_id: str,
        control_plan_version: str,
        to_state: str,
        actor_id: str,
        actor_role: str,
        reason: str,
    ) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        to_state = _nonblank(to_state, field="to_state").upper()
        if to_state not in self.states:
            raise MultiplicityMonitoringError("control_plan_transition_target_unknown")
        if to_state not in self.allowed_transitions.get(current["state"], set()):
            raise MultiplicityMonitoringError(f"control_plan_transition_forbidden:{current['state']}->{to_state}")
        return self._append_event(
            event_type="CONTROL_PLAN_STATE_TRANSITION",
            control_plan_id=control_plan_id,
            control_plan_version=control_plan_version,
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "from_state": current["state"],
                "to_state": to_state,
                "reason": _nonblank(reason, field="reason"),
            },
        )

    def verify_integrity(self) -> dict[str, Any]:
        events, controls = self._load_and_validate()
        return {
            "schema_version": "qm_c_multiplicity_monitoring_verification_v1",
            "valid": True,
            "event_count": len(events),
            "control_plan_version_count": len(controls),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {key: value["state"] for key, value in sorted(controls.items())},
            "recorded_look_counts": {key: len(value["recorded_looks"]) for key, value in sorted(controls.items())},
        }
