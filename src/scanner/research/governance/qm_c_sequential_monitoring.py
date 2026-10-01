"""QM-C4 sequential-monitoring governance bound to a frozen QM-C3 plan."""
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
from scanner.research.governance.qm_c_families_multiplicity import FamilyMultiplicityRegistry

EVENT_SCHEMA_VERSION = "qm_c4_sequential_monitoring_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c4_sequential_monitoring_v1.json"


class SequentialMonitoringError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise SequentialMonitoringError(f"value_required:{field}")
    return result


def load_qm_c4_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SequentialMonitoringError(f"qm_c4_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c4_sequential_monitoring_v1":
        raise SequentialMonitoringError("qm_c4_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise SequentialMonitoringError("qm_c4_contract_scope_invalid")
    return payload


def monitoring_plan_hash(record: Mapping[str, Any]) -> str:
    return _hash(dict(record))


class SequentialMonitoringRegistry:
    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c4_contract(contract_path)
        self.event_types = set(self.contract["registry"]["event_types"])
        self.required = tuple(self.contract["monitoring_plan"]["required_fields"])
        self.states = set(self.contract["monitoring_plan"]["states"])
        self.transitions = {k: set(v) for k, v in self.contract["monitoring_plan"]["allowed_manual_transitions"].items()}
        self.modes = set(self.contract["schedule"]["modes"])
        self.decisions = set(self.contract["schedule"]["look_decisions"])

    @staticmethod
    def _key(plan_id: str, version: str) -> str:
        return f"{plan_id}::{version}"

    def _schedule(self, raw: Mapping[str, Any]) -> tuple[str, list[dict[str, Any]], bool, str, dict[str, str] | None]:
        mode = _text(raw.get("mode"), "mode").upper()
        if mode not in self.modes:
            raise SequentialMonitoringError("monitoring_mode_invalid")
        early_stop = raw.get("early_stop_allowed")
        if not isinstance(early_stop, bool):
            raise SequentialMonitoringError("early_stop_allowed_boolean_required")
        stopping_rule = _text(raw.get("stopping_rule"), "stopping_rule")
        values = raw.get("planned_looks")
        if not isinstance(values, list) or not values:
            raise SequentialMonitoringError("planned_looks_nonempty_list_required")
        required = tuple(self.contract["schedule"]["look_required_fields"])
        looks = []
        seen = set(); previous = 0.0
        for i, item in enumerate(values):
            if not isinstance(item, Mapping):
                raise SequentialMonitoringError(f"planned_look_must_be_object:{i}")
            missing = [f for f in required if f not in item]
            if missing:
                raise SequentialMonitoringError(f"planned_look_fields_missing:{i}:" + ",".join(missing))
            look_id = _text(item.get("look_id"), f"planned_looks[{i}].look_id")
            if look_id in seen:
                raise SequentialMonitoringError(f"duplicate_planned_look_id:{look_id}")
            seen.add(look_id)
            fraction = item.get("information_fraction")
            if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
                raise SequentialMonitoringError(f"information_fraction_required:{look_id}")
            fraction = float(fraction)
            if not math.isfinite(fraction) or not 0.0 < fraction <= 1.0 or fraction <= previous:
                raise SequentialMonitoringError("information_fractions_must_be_strictly_increasing")
            previous = fraction; looks.append({"look_id": look_id, "information_fraction": fraction})
        if not math.isclose(looks[-1]["information_fraction"], 1.0, rel_tol=0.0, abs_tol=1e-12):
            raise SequentialMonitoringError("final_planned_look_must_have_information_fraction_one")
        custom = None
        if mode == "FIXED_HORIZON_NO_INTERIM":
            if len(looks) != 1 or early_stop:
                raise SequentialMonitoringError("fixed_horizon_requires_single_final_look_without_early_stop")
        elif mode == "PREDECLARED_LOOKS":
            if len(looks) < 2:
                raise SequentialMonitoringError("predeclared_looks_requires_at_least_two_looks")
        else:
            value = raw.get("custom_rule")
            fields = tuple(self.contract["schedule"]["custom_required_fields"])
            if not isinstance(value, Mapping) or set(value) != set(fields):
                raise SequentialMonitoringError("custom_monitoring_rule_invalid")
            custom = {f: _text(value.get(f), f"custom_rule.{f}") for f in fields}
        return mode, looks, early_stop, stopping_rule, custom

    def _normalize(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping): raise SequentialMonitoringError("monitoring_plan_record_must_be_object")
        missing = [f for f in self.required if f not in raw]
        if missing: raise SequentialMonitoringError("monitoring_plan_fields_missing:" + ",".join(missing))
        mode, looks, early_stop, stopping_rule, custom = self._schedule(raw)
        result = {
            "monitoring_plan_id": _text(raw.get("monitoring_plan_id"), "monitoring_plan_id"),
            "monitoring_plan_version": _text(raw.get("monitoring_plan_version"), "monitoring_plan_version"),
            "control_plan_id": _text(raw.get("control_plan_id"), "control_plan_id"),
            "control_plan_version": _text(raw.get("control_plan_version"), "control_plan_version"),
            "control_plan_hash": _text(raw.get("control_plan_hash"), "control_plan_hash"),
            "mode": mode, "planned_looks": looks, "early_stop_allowed": early_stop, "stopping_rule": stopping_rule,
        }
        if custom is not None: result["custom_rule"] = custom
        return result

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists(): return []
        result = []
        for n, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip(): continue
            try: value = json.loads(line)
            except json.JSONDecodeError as exc: raise SequentialMonitoringError(f"invalid_jsonl_line:{n}") from exc
            if not isinstance(value, dict): raise SequentialMonitoringError(f"registry_line_not_object:{n}")
            result.append(value)
        return result

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous = None
        for seq, raw in enumerate(events, 1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION or event.get("sequence") != seq or event.get("previous_event_hash") != previous:
                raise SequentialMonitoringError(f"registry_chain_invalid:{seq}")
            stored = str(event.get("entry_hash") or ""); body = dict(event); body.pop("entry_hash", None)
            if not stored or stored != _hash(body): raise SequentialMonitoringError(f"registry_entry_hash_invalid:{seq}")
            previous = stored

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        plans = {}; latest = {}
        for raw in events:
            event = dict(raw); et = str(event.get("event_type") or "")
            if et not in self.event_types: raise SequentialMonitoringError(f"registry_event_type_unknown:{et}")
            pid = _text(event.get("monitoring_plan_id"), "event.monitoring_plan_id"); ver = _text(event.get("monitoring_plan_version"), "event.monitoring_plan_version"); key = self._key(pid, ver)
            payload = event.get("payload")
            if not isinstance(payload, Mapping): raise SequentialMonitoringError("registry_payload_must_be_object")
            if et == "MONITORING_PLAN_REGISTERED":
                if key in plans: raise SequentialMonitoringError(f"monitoring_plan_version_already_registered:{key}")
                record = self._normalize(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                stored = _text(payload.get("monitoring_plan_hash"), "monitoring_plan_hash")
                if record["monitoring_plan_id"] != pid or record["monitoring_plan_version"] != ver or stored != monitoring_plan_hash(record): raise SequentialMonitoringError("monitoring_plan_registration_identity_or_hash_mismatch")
                supersedes = payload.get("supersedes_monitoring_plan_version")
                if supersedes is not None:
                    supersedes = _text(supersedes, "supersedes_monitoring_plan_version")
                    if self._key(pid, supersedes) not in plans or latest.get(pid) != supersedes: raise SequentialMonitoringError("monitoring_plan_successor_must_supersede_latest_registered_version")
                elif pid in latest: raise SequentialMonitoringError("monitoring_plan_successor_requires_supersedes_reference")
                plans[key] = {**record, "monitoring_plan_hash": stored, "state": "DRAFT", "recorded_looks": [], "supersedes_monitoring_plan_version": supersedes, "last_event_hash": event["entry_hash"]}; latest[pid] = ver; continue
            if key not in plans: raise SequentialMonitoringError(f"monitoring_plan_version_not_registered:{key}")
            current = plans[key]
            if et == "MONITORING_PLAN_FROZEN":
                if current["state"] != "DRAFT" or payload.get("monitoring_plan_hash") != current["monitoring_plan_hash"]: raise SequentialMonitoringError("monitoring_plan_freeze_invalid")
                current["state"] = "FROZEN_FOR_CONFIRMATION"
            elif et == "MONITORING_LOOK_RECORDED":
                if current["state"] not in {"FROZEN_FOR_CONFIRMATION", "MONITORING"}: raise SequentialMonitoringError(f"monitoring_look_not_allowed_in_state:{current['state']}")
                index = len(current["recorded_looks"]); planned = current["planned_looks"]
                if index >= len(planned): raise SequentialMonitoringError("monitoring_look_exceeds_predeclared_schedule")
                expected = planned[index]
                if payload.get("look_id") != expected["look_id"] or payload.get("information_fraction") != expected["information_fraction"]: raise SequentialMonitoringError("monitoring_look_out_of_order_or_fraction_mismatch")
                decision = str(payload.get("decision") or ""); final = index == len(planned) - 1
                if decision not in self.decisions: raise SequentialMonitoringError("monitoring_look_decision_invalid")
                if final and decision != "FINAL_COMPLETE": raise SequentialMonitoringError("final_look_requires_final_complete_decision")
                if not final and decision == "FINAL_COMPLETE": raise SequentialMonitoringError("interim_look_cannot_be_final_complete")
                if decision in {"STOP_EFFICACY", "STOP_FUTILITY"} and not current["early_stop_allowed"]: raise SequentialMonitoringError("early_stop_decision_not_predeclared")
                states = payload.get("qm_a_states")
                if not isinstance(states, Mapping) or not states: raise SequentialMonitoringError("monitoring_look_qm_a_states_required")
                current["recorded_looks"].append({"look_id": expected["look_id"], "information_fraction": expected["information_fraction"], "decision": decision, "artifact_hash": _text(payload.get("artifact_hash"), "artifact_hash"), "observed_at": _text(payload.get("observed_at"), "observed_at"), "qm_a_states": dict(states)})
                current["state"] = "STOPPED" if decision in {"STOP_EFFICACY", "STOP_FUTILITY"} else ("COMPLETE" if final else "MONITORING")
            else:
                source = str(payload.get("from_state") or ""); target = str(payload.get("to_state") or "")
                if source != current["state"] or target not in self.transitions.get(source, set()): raise SequentialMonitoringError(f"monitoring_plan_transition_forbidden:{source}->{target}")
                _text(payload.get("reason"), "reason"); current["state"] = target
            current["last_event_hash"] = event["entry_hash"]
        return plans

    def _load(self):
        events = self._read(); self._verify_chain(events); return events, self._replay(events)

    def _append(self, event_type: str, pid: str, ver: str, actor_id: str, actor_role: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try: lock = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc: raise SequentialMonitoringError(f"registry_lock_exists:{self.lock_path}") from exc
        try:
            events, _ = self._load()
            event = {"schema_version": EVENT_SCHEMA_VERSION, "sequence": len(events)+1, "event_id": str(uuid4()), "event_type": event_type, "recorded_at": _now(), "monitoring_plan_id": _text(pid,"monitoring_plan_id"), "monitoring_plan_version": _text(ver,"monitoring_plan_version"), "actor_id": _text(actor_id,"actor_id"), "actor_role": _text(actor_role,"actor_role"), "payload": dict(payload), "previous_event_hash": events[-1]["entry_hash"] if events else None}
            event["entry_hash"] = _hash(event); candidate = [*events, event]; self._verify_chain(candidate); self._replay(candidate)
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            try: os.write(fd, (_json(event)+"\n").encode("utf-8")); os.fsync(fd)
            finally: os.close(fd)
            return event
        finally:
            os.close(lock)
            try: self.lock_path.unlink()
            except FileNotFoundError: pass

    def register_monitoring_plan(self, *, record: Mapping[str, Any], control_registry: FamilyMultiplicityRegistry, actor_id: str, actor_role: str, supersedes_monitoring_plan_version: str | None = None) -> dict[str, Any]:
        normalized = self._normalize(record); control = control_registry.get_control_plan(normalized["control_plan_id"], normalized["control_plan_version"])
        if control["state"] != "FROZEN_FOR_CONFIRMATION": raise SequentialMonitoringError("monitoring_plan_requires_frozen_qm_c3_control_plan")
        if control["control_plan_hash"] != normalized["control_plan_hash"]: raise SequentialMonitoringError("monitoring_plan_control_hash_mismatch")
        return self._append("MONITORING_PLAN_REGISTERED", normalized["monitoring_plan_id"], normalized["monitoring_plan_version"], actor_id, actor_role, {"record": normalized, "monitoring_plan_hash": monitoring_plan_hash(normalized), "supersedes_monitoring_plan_version": supersedes_monitoring_plan_version})

    def get_monitoring_plan(self, monitoring_plan_id: str, monitoring_plan_version: str) -> dict[str, Any]:
        _, plans = self._load(); key = self._key(monitoring_plan_id, monitoring_plan_version)
        if key not in plans: raise SequentialMonitoringError(f"monitoring_plan_version_not_registered:{key}")
        return dict(plans[key])

    def freeze_monitoring_plan(self, *, monitoring_plan_id: str, monitoring_plan_version: str, control_registry: FamilyMultiplicityRegistry, actor_id: str, actor_role: str, reason: str) -> dict[str, Any]:
        current = self.get_monitoring_plan(monitoring_plan_id, monitoring_plan_version); control = control_registry.get_control_plan(current["control_plan_id"], current["control_plan_version"])
        if current["state"] != "DRAFT": raise SequentialMonitoringError("monitoring_plan_freeze_requires_draft")
        if control["state"] != "FROZEN_FOR_CONFIRMATION" or control["control_plan_hash"] != current["control_plan_hash"]: raise SequentialMonitoringError("monitoring_plan_freeze_requires_same_frozen_qm_c3_plan")
        return self._append("MONITORING_PLAN_FROZEN", monitoring_plan_id, monitoring_plan_version, actor_id, actor_role, {"monitoring_plan_hash": current["monitoring_plan_hash"], "control_plan_hash": current["control_plan_hash"], "reason": _text(reason,"reason")})

    def validate_evaluation_ready(self, *, monitoring_plan_id: str, monitoring_plan_version: str, control_registry: FamilyMultiplicityRegistry) -> dict[str, Any]:
        current = self.get_monitoring_plan(monitoring_plan_id, monitoring_plan_version); control = control_registry.get_control_plan(current["control_plan_id"], current["control_plan_version"])
        if current["state"] != "FROZEN_FOR_CONFIRMATION": raise SequentialMonitoringError("evaluation_requires_frozen_monitoring_plan")
        if control["state"] != "FROZEN_FOR_CONFIRMATION" or control["control_plan_hash"] != current["control_plan_hash"]: raise SequentialMonitoringError("evaluation_requires_matching_frozen_qm_c3_plan")
        return {"valid": True, "monitoring_plan_id": current["monitoring_plan_id"], "monitoring_plan_version": current["monitoring_plan_version"], "monitoring_plan_hash": current["monitoring_plan_hash"], "control_plan_id": current["control_plan_id"], "control_plan_hash": current["control_plan_hash"], "next_look": current["planned_looks"][0]}

    def record_monitoring_look(self, *, monitoring_plan_id: str, monitoring_plan_version: str, look_id: str, artifact_hash: str, decision: str, control_registry: FamilyMultiplicityRegistry, qm_a_ledger: GovernanceLedger, actor_id: str, actor_role: str, observed_at: str | None = None) -> dict[str, Any]:
        current = self.get_monitoring_plan(monitoring_plan_id, monitoring_plan_version); control = control_registry.get_control_plan(current["control_plan_id"], current["control_plan_version"])
        if control["state"] != "FROZEN_FOR_CONFIRMATION" or control["control_plan_hash"] != current["control_plan_hash"]: raise SequentialMonitoringError("monitoring_look_requires_matching_frozen_qm_c3_plan")
        if current["state"] not in {"FROZEN_FOR_CONFIRMATION", "MONITORING"}: raise SequentialMonitoringError(f"monitoring_look_not_allowed_in_state:{current['state']}")
        index = len(current["recorded_looks"]); planned = current["planned_looks"]
        if index >= len(planned): raise SequentialMonitoringError("monitoring_look_exceeds_predeclared_schedule")
        expected = planned[index]
        if _text(look_id,"look_id") != expected["look_id"]: raise SequentialMonitoringError(f"monitoring_look_out_of_order:expected={expected['look_id']}")
        decision = _text(decision,"decision").upper(); final = index == len(planned)-1
        if decision not in self.decisions: raise SequentialMonitoringError("monitoring_look_decision_invalid")
        if final and decision != "FINAL_COMPLETE": raise SequentialMonitoringError("final_look_requires_final_complete_decision")
        if not final and decision == "FINAL_COMPLETE": raise SequentialMonitoringError("interim_look_cannot_be_final_complete")
        if decision in {"STOP_EFFICACY","STOP_FUTILITY"} and not current["early_stop_allowed"]: raise SequentialMonitoringError("early_stop_decision_not_predeclared")
        states = {}
        for member in control["family_members"]:
            analysis = qm_a_ledger.get_analysis(member["qm_a_analysis_id"], member["qm_a_version_id"]); state = str(analysis["state"])
            if index == 0 and state != "FROZEN_FOR_CONFIRMATION": raise SequentialMonitoringError(f"first_monitoring_look_requires_frozen_qm_a:{state}")
            if index > 0 and state not in qm_a_ledger.spent_states: raise SequentialMonitoringError(f"subsequent_monitoring_look_requires_consumed_qm_a_evidence:{state}")
            states[f"{member['qm_a_analysis_id']}::{member['qm_a_version_id']}"] = state
        return self._append("MONITORING_LOOK_RECORDED", monitoring_plan_id, monitoring_plan_version, actor_id, actor_role, {"look_id": expected["look_id"], "information_fraction": expected["information_fraction"], "artifact_hash": _text(artifact_hash,"artifact_hash"), "decision": decision, "observed_at": observed_at or _now(), "qm_a_states": states})

    def transition(self, *, monitoring_plan_id: str, monitoring_plan_version: str, to_state: str, actor_id: str, actor_role: str, reason: str) -> dict[str, Any]:
        current = self.get_monitoring_plan(monitoring_plan_id, monitoring_plan_version); target = _text(to_state,"to_state").upper()
        if target not in self.transitions.get(current["state"], set()): raise SequentialMonitoringError(f"monitoring_plan_transition_forbidden:{current['state']}->{target}")
        return self._append("MONITORING_PLAN_STATE_TRANSITION", monitoring_plan_id, monitoring_plan_version, actor_id, actor_role, {"from_state": current["state"], "to_state": target, "reason": _text(reason,"reason")})

    def verify_integrity(self) -> dict[str, Any]:
        events, plans = self._load()
        return {"schema_version": "qm_c4_sequential_monitoring_verification_v1", "valid": True, "event_count": len(events), "monitoring_plan_version_count": len(plans), "head_hash": events[-1]["entry_hash"] if events else None, "states": {k:v["state"] for k,v in sorted(plans.items())}, "recorded_look_counts": {k:len(v["recorded_looks"]) for k,v in sorted(plans.items())}}
