"""QM-C5 immutable result retention bound to C1/C2/C3/C4/QM-A."""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import uuid4

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import FamilyMultiplicityRegistry
from scanner.research.governance.qm_c_sequential_monitoring import SequentialMonitoringRegistry

EVENT_SCHEMA_VERSION = "qm_c5_result_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c5_negative_results_v1.json"


class NegativeResultRegistryError(ValueError):
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
        raise NegativeResultRegistryError(f"value_required:{field}")
    return result


def _optional(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field)


def load_qm_c5_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NegativeResultRegistryError(f"qm_c5_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c5_negative_results_v1":
        raise NegativeResultRegistryError("qm_c5_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise NegativeResultRegistryError("qm_c5_contract_scope_invalid")
    return payload


def result_hash(record: Mapping[str, Any]) -> str:
    return _hash(dict(record))


class NegativeResultRegistry:
    OPTIONAL_BINDINGS = (
        "evidence_artifact_hash", "analysis_plan_id", "analysis_plan_version", "analysis_plan_hash",
        "control_plan_id", "control_plan_version", "control_plan_hash",
        "monitoring_plan_id", "monitoring_plan_version", "monitoring_plan_hash",
        "qm_a_analysis_id", "qm_a_version_id",
    )

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path); self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c5_contract(contract_path); spec = self.contract["result_record"]
        self.required = tuple(spec["required_fields"]); self.scopes = set(spec["evidence_scopes"])
        self.classes = set(spec["outcome_classifications"]); self.evaluated = set(spec["evaluated_classifications"]); self.no_outcome = set(spec["no_outcome_classifications"])

    @staticmethod
    def _key(result_id: str, version: str) -> str:
        return f"{result_id}::{version}"

    def _normalize(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping): raise NegativeResultRegistryError("result_record_must_be_object")
        missing = [f for f in self.required if f not in raw]
        if missing: raise NegativeResultRegistryError("result_fields_missing:" + ",".join(missing))
        result = {
            "result_id": _text(raw.get("result_id"), "result_id"), "result_version": _text(raw.get("result_version"), "result_version"),
            "hypothesis_id": _text(raw.get("hypothesis_id"), "hypothesis_id"), "hypothesis_version": _text(raw.get("hypothesis_version"), "hypothesis_version"),
            "hypothesis_version_hash": _text(raw.get("hypothesis_version_hash"), "hypothesis_version_hash"),
            "evidence_scope": _text(raw.get("evidence_scope"), "evidence_scope").upper(),
            "outcome_classification": _text(raw.get("outcome_classification"), "outcome_classification").upper(),
            "conclusion": _text(raw.get("conclusion"), "conclusion"),
        }
        if result["evidence_scope"] not in self.scopes: raise NegativeResultRegistryError("result_evidence_scope_invalid")
        if result["outcome_classification"] not in self.classes: raise NegativeResultRegistryError("result_outcome_classification_invalid")
        for field in self.OPTIONAL_BINDINGS: result[field] = _optional(raw.get(field), field)
        scope = result["evidence_scope"]; classification = result["outcome_classification"]
        if scope == "NO_OUTCOME_EVIDENCE":
            if classification not in self.no_outcome: raise NegativeResultRegistryError("no_outcome_scope_requires_no_outcome_classification")
            forbidden = [f for f in self.OPTIONAL_BINDINGS if result[f] is not None]
            if forbidden: raise NegativeResultRegistryError("no_outcome_scope_forbids_bindings:" + ",".join(forbidden))
        else:
            if classification not in self.evaluated: raise NegativeResultRegistryError("evaluated_scope_requires_evaluated_classification")
            if result["evidence_artifact_hash"] is None: raise NegativeResultRegistryError("evaluated_result_requires_evidence_artifact_hash")
        if scope == "CONFIRMATORY":
            required = ["analysis_plan_id","analysis_plan_version","analysis_plan_hash","control_plan_id","control_plan_version","control_plan_hash","monitoring_plan_id","monitoring_plan_version","monitoring_plan_hash","qm_a_analysis_id","qm_a_version_id"]
            missing_bindings = [f for f in required if result[f] is None]
            if missing_bindings: raise NegativeResultRegistryError("confirmatory_result_bindings_missing:" + ",".join(missing_bindings))
        elif scope == "EXPLORATORY":
            for prefix in ("control_plan", "monitoring_plan"):
                if any(result[f] is not None for f in (f"{prefix}_id",f"{prefix}_version",f"{prefix}_hash")): raise NegativeResultRegistryError("exploratory_result_forbids_confirmatory_control_binding")
        return result

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists(): return []
        result=[]
        for n,line in enumerate(self.path.read_text(encoding="utf-8").splitlines(),1):
            if not line.strip(): continue
            try: value=json.loads(line)
            except json.JSONDecodeError as exc: raise NegativeResultRegistryError(f"invalid_jsonl_line:{n}") from exc
            if not isinstance(value,dict): raise NegativeResultRegistryError(f"registry_line_not_object:{n}")
            result.append(value)
        return result

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous=None
        for seq,raw in enumerate(events,1):
            event=dict(raw)
            if event.get("schema_version")!=EVENT_SCHEMA_VERSION or event.get("sequence")!=seq or event.get("previous_event_hash")!=previous: raise NegativeResultRegistryError(f"registry_chain_invalid:{seq}")
            stored=str(event.get("entry_hash") or ""); body=dict(event); body.pop("entry_hash",None)
            if not stored or stored!=_hash(body): raise NegativeResultRegistryError(f"registry_entry_hash_invalid:{seq}")
            previous=stored

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str,dict[str,Any]]:
        results={}; latest={}
        for raw in events:
            event=dict(raw)
            if event.get("event_type")!="RESULT_REGISTERED": raise NegativeResultRegistryError("result_registry_event_type_unknown")
            rid=_text(event.get("result_id"),"event.result_id"); ver=_text(event.get("result_version"),"event.result_version"); key=self._key(rid,ver)
            if key in results: raise NegativeResultRegistryError(f"result_version_already_registered:{key}")
            payload=event.get("payload")
            if not isinstance(payload,Mapping): raise NegativeResultRegistryError("registry_payload_must_be_object")
            record=self._normalize(payload.get("record") if isinstance(payload.get("record"),Mapping) else {})
            stored=_text(payload.get("result_hash"),"result_hash")
            if record["result_id"]!=rid or record["result_version"]!=ver or stored!=result_hash(record): raise NegativeResultRegistryError("result_registration_identity_or_hash_mismatch")
            supersedes=payload.get("supersedes_result_version")
            if supersedes is not None:
                supersedes=_text(supersedes,"supersedes_result_version")
                if self._key(rid,supersedes) not in results or latest.get(rid)!=supersedes: raise NegativeResultRegistryError("result_successor_must_supersede_latest_registered_version")
            elif rid in latest: raise NegativeResultRegistryError("result_successor_requires_supersedes_reference")
            results[key]={**record,"result_hash":stored,"supersedes_result_version":supersedes,"registered_at":event["recorded_at"],"last_event_hash":event["entry_hash"]}; latest[rid]=ver
        return results

    def _load(self):
        events=self._read(); self._verify_chain(events); return events,self._replay(events)

    def _append(self, record: Mapping[str,Any], actor_id: str, actor_role: str, supersedes: str|None) -> dict[str,Any]:
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try: lock=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
        except FileExistsError as exc: raise NegativeResultRegistryError(f"registry_lock_exists:{self.lock_path}") from exc
        try:
            events,_=self._load(); event={"schema_version":EVENT_SCHEMA_VERSION,"sequence":len(events)+1,"event_id":str(uuid4()),"event_type":"RESULT_REGISTERED","recorded_at":_now(),"result_id":record["result_id"],"result_version":record["result_version"],"actor_id":_text(actor_id,"actor_id"),"actor_role":_text(actor_role,"actor_role"),"payload":{"record":dict(record),"result_hash":result_hash(record),"supersedes_result_version":supersedes},"previous_event_hash":events[-1]["entry_hash"] if events else None}
            event["entry_hash"]=_hash(event); candidate=[*events,event]; self._verify_chain(candidate); self._replay(candidate)
            fd=os.open(self.path,os.O_CREAT|os.O_APPEND|os.O_WRONLY,0o644)
            try: os.write(fd,(_json(event)+"\n").encode("utf-8")); os.fsync(fd)
            finally: os.close(fd)
            return event
        finally:
            os.close(lock)
            try:self.lock_path.unlink()
            except FileNotFoundError:pass

    def _validate_confirmatory(self, record: Mapping[str,Any], hypothesis: Mapping[str,Any], *, analysis_plans: AnalysisPlanRegistry, controls: FamilyMultiplicityRegistry, monitoring: SequentialMonitoringRegistry, qm_a: GovernanceLedger) -> None:
        if hypothesis["research_mode"]!="CONFIRMATION": raise NegativeResultRegistryError("confirmatory_result_requires_confirmation_hypothesis")
        plan=analysis_plans.get_plan(record["analysis_plan_id"],record["analysis_plan_version"])
        checks={"hypothesis_id":record["hypothesis_id"],"hypothesis_version":record["hypothesis_version"],"hypothesis_version_hash":record["hypothesis_version_hash"],"analysis_plan_hash":record["analysis_plan_hash"]}
        for field,expected in checks.items():
            if plan.get(field)!=expected: raise NegativeResultRegistryError(f"confirmatory_result_plan_binding_mismatch:{field}")
        control=controls.get_control_plan(record["control_plan_id"],record["control_plan_version"])
        if control["control_plan_hash"]!=record["control_plan_hash"] or control["state"]!="FROZEN_FOR_CONFIRMATION": raise NegativeResultRegistryError("confirmatory_result_control_plan_binding_invalid")
        matches=[m for m in control["family_members"] if m["hypothesis_id"]==record["hypothesis_id"] and m["hypothesis_version"]==record["hypothesis_version"] and m["hypothesis_version_hash"]==record["hypothesis_version_hash"] and m["analysis_plan_id"]==record["analysis_plan_id"] and m["analysis_plan_version"]==record["analysis_plan_version"] and m["analysis_plan_hash"]==record["analysis_plan_hash"] and m["qm_a_analysis_id"]==record["qm_a_analysis_id"] and m["qm_a_version_id"]==record["qm_a_version_id"]]
        if len(matches)!=1: raise NegativeResultRegistryError("confirmatory_result_member_not_exactly_present_in_control_plan")
        monitor=monitoring.get_monitoring_plan(record["monitoring_plan_id"],record["monitoring_plan_version"])
        if monitor["monitoring_plan_hash"]!=record["monitoring_plan_hash"] or monitor["control_plan_id"]!=record["control_plan_id"] or monitor["control_plan_version"]!=record["control_plan_version"] or monitor["control_plan_hash"]!=record["control_plan_hash"]: raise NegativeResultRegistryError("confirmatory_result_monitoring_binding_invalid")
        if monitor["state"] not in {"COMPLETE","STOPPED"} or not monitor["recorded_looks"]: raise NegativeResultRegistryError("confirmatory_result_requires_completed_or_stopped_monitoring_plan")
        analysis=qm_a.get_analysis(record["qm_a_analysis_id"],record["qm_a_version_id"])
        if analysis["state"] not in qm_a.spent_states: raise NegativeResultRegistryError(f"confirmatory_result_requires_consumed_qm_a_evidence:{analysis['state']}")

    def register_result(self, *, record: Mapping[str,Any], hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, control_registry: FamilyMultiplicityRegistry, monitoring_registry: SequentialMonitoringRegistry, qm_a_ledger: GovernanceLedger, actor_id: str, actor_role: str, supersedes_result_version: str|None=None) -> dict[str,Any]:
        normalized=self._normalize(record); hypothesis=hypothesis_registry.get_hypothesis(normalized["hypothesis_id"],normalized["hypothesis_version"])
        if hypothesis["hypothesis_version_hash"]!=normalized["hypothesis_version_hash"]: raise NegativeResultRegistryError("result_hypothesis_hash_mismatch")
        if normalized["evidence_scope"]=="CONFIRMATORY": self._validate_confirmatory(normalized,hypothesis,analysis_plans=analysis_plan_registry,controls=control_registry,monitoring=monitoring_registry,qm_a=qm_a_ledger)
        elif normalized["evidence_scope"]=="EXPLORATORY":
            if hypothesis["research_mode"]!="DISCOVERY": raise NegativeResultRegistryError("exploratory_result_requires_discovery_hypothesis")
            if normalized["analysis_plan_id"] is not None:
                plan=analysis_plan_registry.get_plan(normalized["analysis_plan_id"],normalized["analysis_plan_version"])
                if plan["analysis_plan_hash"]!=normalized["analysis_plan_hash"] or plan["hypothesis_version_hash"]!=normalized["hypothesis_version_hash"]: raise NegativeResultRegistryError("exploratory_result_analysis_plan_binding_invalid")
        else:
            cls=normalized["outcome_classification"]
            if cls=="REJECTED_PRE_EVALUATION" and hypothesis["state"] not in {"REJECTED","RETIRED"}: raise NegativeResultRegistryError("rejected_pre_evaluation_requires_rejected_hypothesis")
            if cls=="RETIRED_WITHOUT_EVALUATION" and hypothesis["state"]!="RETIRED": raise NegativeResultRegistryError("retired_without_evaluation_requires_retired_hypothesis")
        _,results=self._load(); key=self._key(normalized["result_id"],normalized["result_version"])
        if key in results: raise NegativeResultRegistryError(f"result_version_already_registered:{key}")
        return self._append(normalized,actor_id,actor_role,supersedes_result_version)

    def get_result(self,result_id:str,result_version:str)->dict[str,Any]:
        _,results=self._load(); key=self._key(result_id,result_version)
        if key not in results: raise NegativeResultRegistryError(f"result_version_not_registered:{key}")
        return dict(results[key])

    def verify_integrity(self)->dict[str,Any]:
        events,results=self._load(); counts={}
        for row in results.values(): counts[row["outcome_classification"]]=counts.get(row["outcome_classification"],0)+1
        return {"schema_version":"qm_c5_result_registry_verification_v1","valid":True,"event_count":len(events),"result_version_count":len(results),"head_hash":events[-1]["entry_hash"] if events else None,"classification_counts":dict(sorted(counts.items()))}
