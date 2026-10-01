"""QM-C3 hypothesis-family and multiplicity governance.

QM-C3 freezes exact confirmatory family membership and multiplicity treatment.
Sequential outcome inspection belongs to QM-C4 and is deliberately absent here.
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

EVENT_SCHEMA_VERSION = "qm_c3_family_multiplicity_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c3_families_multiplicity_v1.json"


class FamilyMultiplicityError(ValueError):
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
        raise FamilyMultiplicityError(f"value_required:{field}")
    return result


def _probability(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FamilyMultiplicityError(f"probability_required:{field}")
    result = float(value)
    if not math.isfinite(result) or not 0.0 < result < 1.0:
        raise FamilyMultiplicityError(f"probability_out_of_range:{field}")
    return result


def load_qm_c3_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FamilyMultiplicityError(f"qm_c3_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c3_families_multiplicity_v1":
        raise FamilyMultiplicityError("qm_c3_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise FamilyMultiplicityError("qm_c3_contract_scope_invalid")
    return payload


def control_plan_hash(record: Mapping[str, Any]) -> str:
    return _hash(dict(record))


class FamilyMultiplicityRegistry:
    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c3_contract(contract_path)
        self.event_types = set(self.contract["registry"]["event_types"])
        self.required_fields = tuple(self.contract["control_plan"]["required_fields"])
        self.member_fields = tuple(self.contract["family_member"]["required_fields"])
        self.states = set(self.contract["control_plan"]["states"])
        self.transitions = {k: set(v) for k, v in self.contract["control_plan"]["allowed_transitions"].items()}
        self.strategies = set(self.contract["multiplicity"]["strategies"])

    @staticmethod
    def _key(plan_id: str, version: str) -> str:
        return f"{plan_id}::{version}"

    def _member(self, raw: Any, index: int) -> dict[str, str]:
        if not isinstance(raw, Mapping):
            raise FamilyMultiplicityError(f"family_member_must_be_object:{index}")
        missing = [f for f in self.member_fields if f not in raw]
        if missing:
            raise FamilyMultiplicityError(f"family_member_fields_missing:{index}:" + ",".join(missing))
        return {f: _text(raw.get(f), f"family_members[{index}].{f}") for f in self.member_fields}

    def _multiplicity(self, strategy_value: Any, parameters_value: Any, member_count: int) -> tuple[str, dict[str, Any]]:
        strategy = _text(strategy_value, "multiplicity_strategy").upper()
        if strategy not in self.strategies:
            raise FamilyMultiplicityError("multiplicity_strategy_invalid")
        if not isinstance(parameters_value, Mapping):
            raise FamilyMultiplicityError("multiplicity_parameters_must_be_object")
        params = dict(parameters_value)
        spec = self.contract["multiplicity"]
        if strategy == "PREDECLARED_SINGLE_PRIMARY":
            if member_count != 1:
                raise FamilyMultiplicityError("single_primary_requires_exactly_one_family_member")
            if params:
                raise FamilyMultiplicityError("single_primary_parameters_must_be_empty")
            return strategy, {}
        if strategy in set(spec["fwer_strategies"]):
            field = str(spec["fwer_parameter"])
            if set(params) != {field}:
                raise FamilyMultiplicityError(f"fwer_parameters_must_equal:{field}")
            return strategy, {field: _probability(params[field], f"multiplicity_parameters.{field}")}
        if strategy in set(spec["fdr_strategies"]):
            field = str(spec["fdr_parameter"])
            if set(params) != {field}:
                raise FamilyMultiplicityError(f"fdr_parameters_must_equal:{field}")
            return strategy, {field: _probability(params[field], f"multiplicity_parameters.{field}")}
        required = tuple(spec["custom_required_fields"])
        if set(params) != set(required):
            raise FamilyMultiplicityError("custom_multiplicity_parameters_invalid")
        return strategy, {f: _text(params.get(f), f"multiplicity_parameters.{f}") for f in required}

    def _normalize(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise FamilyMultiplicityError("control_plan_record_must_be_object")
        missing = [f for f in self.required_fields if f not in raw]
        if missing:
            raise FamilyMultiplicityError("control_plan_fields_missing:" + ",".join(missing))
        mode = _text(raw.get("research_mode"), "research_mode").upper()
        if mode != "CONFIRMATION":
            raise FamilyMultiplicityError("control_plan_requires_confirmation_mode")
        values = raw.get("family_members")
        if not isinstance(values, list) or not values:
            raise FamilyMultiplicityError("family_members_nonempty_list_required")
        members = [self._member(item, i) for i, item in enumerate(values)]
        hkeys = [(m["hypothesis_id"], m["hypothesis_version"]) for m in members]
        pkeys = [(m["analysis_plan_id"], m["analysis_plan_version"]) for m in members]
        if len(set(hkeys)) != len(hkeys):
            raise FamilyMultiplicityError("duplicate_hypothesis_family_member")
        if len(set(pkeys)) != len(pkeys):
            raise FamilyMultiplicityError("duplicate_analysis_plan_family_member")
        strategy, params = self._multiplicity(raw.get("multiplicity_strategy"), raw.get("multiplicity_parameters"), len(members))
        return {
            "control_plan_id": _text(raw.get("control_plan_id"), "control_plan_id"),
            "control_plan_version": _text(raw.get("control_plan_version"), "control_plan_version"),
            "hypothesis_family_id": _text(raw.get("hypothesis_family_id"), "hypothesis_family_id"),
            "research_mode": mode,
            "family_members": members,
            "multiplicity_strategy": strategy,
            "multiplicity_parameters": params,
        }

    def _events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        result = []
        for n, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise FamilyMultiplicityError(f"invalid_jsonl_line:{n}") from exc
            if not isinstance(value, dict):
                raise FamilyMultiplicityError(f"registry_line_not_object:{n}")
            result.append(value)
        return result

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous = None
        for seq, raw in enumerate(events, 1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION or event.get("sequence") != seq:
                raise FamilyMultiplicityError(f"registry_event_invalid:{seq}")
            if event.get("previous_event_hash") != previous:
                raise FamilyMultiplicityError(f"registry_previous_hash_invalid:{seq}")
            stored = str(event.get("entry_hash") or "")
            body = dict(event); body.pop("entry_hash", None)
            if not stored or stored != _hash(body):
                raise FamilyMultiplicityError(f"registry_entry_hash_invalid:{seq}")
            previous = stored

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        plans: dict[str, dict[str, Any]] = {}
        latest: dict[str, str] = {}
        for raw in events:
            event = dict(raw)
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise FamilyMultiplicityError(f"registry_event_type_unknown:{event_type}")
            plan_id = _text(event.get("control_plan_id"), "event.control_plan_id")
            version = _text(event.get("control_plan_version"), "event.control_plan_version")
            key = self._key(plan_id, version)
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise FamilyMultiplicityError("registry_payload_must_be_object")
            if event_type == "CONTROL_PLAN_REGISTERED":
                if key in plans:
                    raise FamilyMultiplicityError(f"control_plan_version_already_registered:{key}")
                record = self._normalize(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["control_plan_id"] != plan_id or record["control_plan_version"] != version:
                    raise FamilyMultiplicityError("control_plan_event_identity_mismatch")
                stored_hash = _text(payload.get("control_plan_hash"), "control_plan_hash")
                if stored_hash != control_plan_hash(record):
                    raise FamilyMultiplicityError("control_plan_hash_mismatch")
                supersedes = payload.get("supersedes_control_plan_version")
                if supersedes is not None:
                    supersedes = _text(supersedes, "supersedes_control_plan_version")
                    if self._key(plan_id, supersedes) not in plans or latest.get(plan_id) != supersedes:
                        raise FamilyMultiplicityError("control_plan_successor_must_supersede_latest_registered_version")
                elif plan_id in latest:
                    raise FamilyMultiplicityError("control_plan_successor_requires_supersedes_reference")
                plans[key] = {**record, "state": "DRAFT", "control_plan_hash": stored_hash, "freeze_binding_hash": None, "supersedes_control_plan_version": supersedes, "last_event_hash": event["entry_hash"]}
                latest[plan_id] = version
                continue
            if key not in plans:
                raise FamilyMultiplicityError(f"control_plan_version_not_registered:{key}")
            current = plans[key]
            if event_type == "CONTROL_PLAN_FROZEN":
                if current["state"] != "DRAFT":
                    raise FamilyMultiplicityError("control_plan_freeze_requires_draft")
                if payload.get("control_plan_hash") != current["control_plan_hash"]:
                    raise FamilyMultiplicityError("control_plan_freeze_hash_mismatch")
                snapshot = payload.get("binding_snapshot")
                if not isinstance(snapshot, list) or not snapshot:
                    raise FamilyMultiplicityError("binding_snapshot_nonempty_list_required")
                binding_hash = _text(payload.get("freeze_binding_hash"), "freeze_binding_hash")
                if binding_hash != _hash(snapshot):
                    raise FamilyMultiplicityError("freeze_binding_hash_mismatch")
                current["state"] = "FROZEN_FOR_CONFIRMATION"
                current["freeze_binding_hash"] = binding_hash
            else:
                from_state = str(payload.get("from_state") or "")
                to_state = str(payload.get("to_state") or "")
                if from_state != current["state"] or to_state not in self.states or to_state not in self.transitions.get(from_state, set()):
                    raise FamilyMultiplicityError(f"control_plan_transition_forbidden:{from_state}->{to_state}")
                _text(payload.get("reason"), "transition.reason")
                current["state"] = to_state
            current["last_event_hash"] = event["entry_hash"]
        return plans

    def _load(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._events(); self._verify_chain(events)
        return events, self._replay(events)

    def _lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise FamilyMultiplicityError(f"registry_lock_exists:{self.lock_path}") from exc

    def _append(self, event_type: str, plan_id: str, version: str, actor_id: str, actor_role: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        fd_lock = self._lock()
        try:
            events, _ = self._load()
            event = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": event_type,
                "recorded_at": _now(),
                "control_plan_id": _text(plan_id, "control_plan_id"),
                "control_plan_version": _text(version, "control_plan_version"),
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "payload": dict(payload),
                "previous_event_hash": events[-1]["entry_hash"] if events else None,
            }
            event["entry_hash"] = _hash(event)
            self._verify_chain([*events, event]); self._replay([*events, event])
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            try:
                os.write(fd, (_json(event) + "\n").encode("utf-8")); os.fsync(fd)
            finally:
                os.close(fd)
            return event
        finally:
            os.close(fd_lock)
            try: self.lock_path.unlink()
            except FileNotFoundError: pass

    @staticmethod
    def _static_member(member: Mapping[str, str], family_id: str, hypotheses: HypothesisRegistry, plans: AnalysisPlanRegistry) -> tuple[dict[str, Any], dict[str, Any]]:
        hypothesis = hypotheses.get_hypothesis(member["hypothesis_id"], member["hypothesis_version"])
        if hypothesis["hypothesis_family_id"] != family_id:
            raise FamilyMultiplicityError("family_member_hypothesis_family_mismatch")
        if hypothesis["research_mode"] != "CONFIRMATION" or hypothesis["hypothesis_version_hash"] != member["hypothesis_version_hash"]:
            raise FamilyMultiplicityError("family_member_hypothesis_binding_mismatch")
        if hypothesis["qm_a_analysis_id"] != member["qm_a_analysis_id"]:
            raise FamilyMultiplicityError("family_member_qm_a_analysis_id_mismatch")
        plan = plans.get_plan(member["analysis_plan_id"], member["analysis_plan_version"])
        checks = {
            "hypothesis_id": member["hypothesis_id"], "hypothesis_version": member["hypothesis_version"],
            "hypothesis_version_hash": member["hypothesis_version_hash"], "analysis_plan_hash": member["analysis_plan_hash"]
        }
        for field, expected in checks.items():
            if plan.get(field) != expected:
                raise FamilyMultiplicityError(f"family_member_analysis_plan_binding_mismatch:{field}")
        if plan["research_mode"] != "CONFIRMATION":
            raise FamilyMultiplicityError("family_member_analysis_plan_must_be_confirmation")
        return hypothesis, plan

    def register_control_plan(self, *, record: Mapping[str, Any], hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, actor_id: str, actor_role: str, supersedes_control_plan_version: str | None = None) -> dict[str, Any]:
        normalized = self._normalize(record)
        for member in normalized["family_members"]:
            self._static_member(member, normalized["hypothesis_family_id"], hypothesis_registry, analysis_plan_registry)
        _, plans = self._load()
        key = self._key(normalized["control_plan_id"], normalized["control_plan_version"])
        if key in plans:
            raise FamilyMultiplicityError(f"control_plan_version_already_registered:{key}")
        return self._append("CONTROL_PLAN_REGISTERED", normalized["control_plan_id"], normalized["control_plan_version"], actor_id, actor_role, {"record": normalized, "control_plan_hash": control_plan_hash(normalized), "supersedes_control_plan_version": supersedes_control_plan_version})

    def get_control_plan(self, control_plan_id: str, control_plan_version: str) -> dict[str, Any]:
        _, plans = self._load(); key = self._key(control_plan_id, control_plan_version)
        if key not in plans: raise FamilyMultiplicityError(f"control_plan_version_not_registered:{key}")
        return dict(plans[key])

    def freeze_control_plan(self, *, control_plan_id: str, control_plan_version: str, hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, qm_a_ledger: GovernanceLedger, qm_b_closure: Mapping[str, Any], actor_id: str, actor_role: str, reason: str) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        if current["state"] != "DRAFT": raise FamilyMultiplicityError("control_plan_freeze_requires_draft")
        snapshot = []
        for member in current["family_members"]:
            hypothesis, plan = self._static_member(member, current["hypothesis_family_id"], hypothesis_registry, analysis_plan_registry)
            ready = analysis_plan_registry.validate_confirmation_ready(analysis_plan_id=member["analysis_plan_id"], analysis_plan_version=member["analysis_plan_version"], hypothesis_registry=hypothesis_registry, qm_a_ledger=qm_a_ledger, qm_a_version_id=member["qm_a_version_id"], qm_b_closure=qm_b_closure)
            if ready["analysis_plan_hash"] != member["analysis_plan_hash"]: raise FamilyMultiplicityError("family_member_readiness_plan_hash_mismatch")
            snapshot.append({"hypothesis_id": hypothesis["hypothesis_id"], "hypothesis_version": hypothesis["hypothesis_version"], "hypothesis_version_hash": hypothesis["hypothesis_version_hash"], "analysis_plan_id": plan["analysis_plan_id"], "analysis_plan_version": plan["analysis_plan_version"], "analysis_plan_hash": plan["analysis_plan_hash"], "qm_a_analysis_id": member["qm_a_analysis_id"], "qm_a_version_id": member["qm_a_version_id"], "qm_a_state": ready["states"]["qm_a"]})
        return self._append("CONTROL_PLAN_FROZEN", control_plan_id, control_plan_version, actor_id, actor_role, {"control_plan_hash": current["control_plan_hash"], "binding_snapshot": snapshot, "freeze_binding_hash": _hash(snapshot), "reason": _text(reason, "reason")})

    def validate_multiplicity_ready(self, *, control_plan_id: str, control_plan_version: str) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version)
        if current["state"] != "FROZEN_FOR_CONFIRMATION":
            raise FamilyMultiplicityError("multiplicity_evaluation_requires_frozen_control_plan")
        return {"valid": True, "control_plan_id": current["control_plan_id"], "control_plan_version": current["control_plan_version"], "control_plan_hash": current["control_plan_hash"], "hypothesis_family_id": current["hypothesis_family_id"], "multiplicity_strategy": current["multiplicity_strategy"], "family_members": current["family_members"]}

    def transition(self, *, control_plan_id: str, control_plan_version: str, to_state: str, actor_id: str, actor_role: str, reason: str) -> dict[str, Any]:
        current = self.get_control_plan(control_plan_id, control_plan_version); target = _text(to_state, "to_state").upper()
        if target not in self.transitions.get(current["state"], set()): raise FamilyMultiplicityError(f"control_plan_transition_forbidden:{current['state']}->{target}")
        return self._append("CONTROL_PLAN_STATE_TRANSITION", control_plan_id, control_plan_version, actor_id, actor_role, {"from_state": current["state"], "to_state": target, "reason": _text(reason, "reason")})

    def verify_integrity(self) -> dict[str, Any]:
        events, plans = self._load()
        return {"schema_version": "qm_c3_family_multiplicity_verification_v1", "valid": True, "event_count": len(events), "control_plan_version_count": len(plans), "head_hash": events[-1]["entry_hash"] if events else None, "states": {k: v["state"] for k, v in sorted(plans.items())}}
