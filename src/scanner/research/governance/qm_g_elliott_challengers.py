"""QM-G / BA-QM6 Elliott challenger registry and evaluation gates.

Research-only. The frozen Elliott-vNext implementation remains the core. New
Elliott ideas are registered as versioned sidecar challengers and cannot alter
counts, W6 review transport, Universal Stance, W8 action semantics, portfolio
actions, execution, or orders. Confirmatory evaluation is allowed only after
QM-C identity/freeze/multiplicity bindings, QM-A evidence state, PIT/count
freeze and QM-I lineage review have all been validated.
"""
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
from scanner.research.governance.qm_i_lineage import LineageRegistry


SCHEMA_VERSION = "qm_g_elliott_challenger_registry_v1"
EVENT_SCHEMA_VERSION = "qm_g_elliott_challenger_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_g_elliott_challenger_registry_v1.json"


class ElliottChallengerError(ValueError):
    """Raised when a QM-G challenger violates a frozen research boundary."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ElliottChallengerError(f"value_required:{field}")
    return result


def _sha256(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(ch not in "0123456789abcdef" for ch in result):
        raise ElliottChallengerError(f"sha256_required:{field}")
    return result


def _git_sha(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 40 or any(ch not in "0123456789abcdef" for ch in result):
        raise ElliottChallengerError(f"git_sha_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ElliottChallengerError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ElliottChallengerError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ElliottChallengerError(f"object_required:{field}")
    return json.loads(_json(dict(value)))


def _required_mapping(value: Any, field: str, required: Sequence[str]) -> dict[str, Any]:
    result = _mapping(value, field)
    missing = [name for name in required if name not in result]
    if missing:
        raise ElliottChallengerError(f"fields_missing:{field}:" + ",".join(missing))
    return result


def load_qm_g_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ElliottChallengerError(f"qm_g_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ElliottChallengerError("qm_g_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ElliottChallengerError("qm_g_contract_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise ElliottChallengerError("qm_g_execution_scope_invalid")
    return payload


def challenger_version_hash(record: Mapping[str, Any]) -> str:
    """Immutable hash of the normalized challenger definition and its bindings."""
    return _hash(dict(record))


class ElliottChallengerRegistry:
    """Append-only immutable registry for Elliott sidecar challengers."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_g_contract(contract_path)
        self.required_fields = tuple(self.contract["challenger"]["required_fields"])
        self.event_types = set(self.contract["registry"]["event_types"])

    @staticmethod
    def _key(challenger_id: str, challenger_version: str) -> str:
        return f"{challenger_id}::{challenger_version}"

    def _normalize(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise ElliottChallengerError("challenger_record_must_be_object")
        missing = [field for field in self.required_fields if field not in raw]
        if missing:
            raise ElliottChallengerError("challenger_fields_missing:" + ",".join(missing))

        c = self.contract
        normalized: dict[str, Any] = {
            "challenger_id": _text(raw.get("challenger_id"), "challenger_id"),
            "challenger_version": _text(raw.get("challenger_version"), "challenger_version"),
            "hypothesis_id": _text(raw.get("hypothesis_id"), "hypothesis_id"),
            "hypothesis_version": _text(raw.get("hypothesis_version"), "hypothesis_version"),
            "hypothesis_version_hash": _sha256(raw.get("hypothesis_version_hash"), "hypothesis_version_hash"),
            "hypothesis_family_id": _text(raw.get("hypothesis_family_id"), "hypothesis_family_id"),
            "analysis_plan_id": _text(raw.get("analysis_plan_id"), "analysis_plan_id"),
            "analysis_plan_version": _text(raw.get("analysis_plan_version"), "analysis_plan_version"),
            "analysis_plan_hash": _sha256(raw.get("analysis_plan_hash"), "analysis_plan_hash"),
            "control_plan_id": _text(raw.get("control_plan_id"), "control_plan_id"),
            "control_plan_version": _text(raw.get("control_plan_version"), "control_plan_version"),
            "control_plan_hash": _sha256(raw.get("control_plan_hash"), "control_plan_hash"),
            "qm_a_analysis_id": _text(raw.get("qm_a_analysis_id"), "qm_a_analysis_id"),
            "qm_a_version_id": _text(raw.get("qm_a_version_id"), "qm_a_version_id"),
            "evidence_state": _text(raw.get("evidence_state"), "evidence_state").upper(),
        }
        if normalized["evidence_state"] not in set(c["evidence_state"]["allowed_registration_states"]):
            raise ElliottChallengerError("challenger_evidence_state_invalid")

        feature = _required_mapping(raw.get("feature_definition"), "feature_definition", c["feature_definition"]["required_fields"])
        feature["feature_id"] = _text(feature.get("feature_id"), "feature_definition.feature_id")
        feature["definition"] = _text(feature.get("definition"), "feature_definition.definition")
        if not isinstance(feature.get("inputs"), list) or not feature["inputs"]:
            raise ElliottChallengerError("feature_inputs_nonempty_list_required")
        feature["inputs"] = [_text(item, f"feature_definition.inputs[{i}]") for i, item in enumerate(feature["inputs"])]
        feature["missing_policy"] = _text(feature.get("missing_policy"), "feature_definition.missing_policy")
        normalized["feature_definition"] = feature

        pit = _required_mapping(raw.get("pit_contract"), "pit_contract", c["pit_contract"]["required_fields"])
        for field in ("as_of_field", "available_from_field", "missing_policy"):
            pit[field] = _text(pit.get(field), f"pit_contract.{field}")
        for field in ("future_data_forbidden", "outcome_data_excluded_from_feature_build", "retroactive_reclassification_forbidden"):
            if pit.get(field) is not True:
                raise ElliottChallengerError(f"pit_guard_required:{field}")
        normalized["pit_contract"] = pit

        core = _required_mapping(raw.get("core_binding"), "core_binding", c["core_binding"]["required_fields"])
        core["source_commit"] = _git_sha(core.get("source_commit"), "core_binding.source_commit")
        for field in ("module", "schema_version", "integration_contract"):
            core[field] = _text(core.get(field), f"core_binding.{field}")
        core["core_content_hash"] = _sha256(core.get("core_content_hash"), "core_binding.core_content_hash")
        expected = {
            "module": c["core_binding"]["expected_module"],
            "schema_version": c["core_binding"]["expected_schema_version"],
            "integration_contract": c["core_binding"]["expected_integration_contract"],
        }
        for field, expected_value in expected.items():
            if core[field] != expected_value:
                raise ElliottChallengerError(f"elliott_core_binding_mismatch:{field}")
        if core.get("core_frozen") is not True:
            raise ElliottChallengerError("elliott_core_must_be_frozen")
        if core.get("hard_rules_overridden") is not False:
            raise ElliottChallengerError("elliott_hard_rule_override_forbidden")
        normalized["core_binding"] = core

        freeze = _required_mapping(raw.get("count_scenario_freeze"), "count_scenario_freeze", c["count_scenario_freeze"]["required_fields"])
        for field in ("freeze_id", "count_or_scenario_id"):
            freeze[field] = _text(freeze.get(field), f"count_scenario_freeze.{field}")
        freeze["frozen_output_hash"] = _sha256(freeze.get("frozen_output_hash"), "count_scenario_freeze.frozen_output_hash")
        freeze["frozen_at"] = _timestamp(freeze.get("frozen_at"), "count_scenario_freeze.frozen_at").isoformat()
        if str(freeze.get("outcome_visibility_at_freeze") or "").upper() != "NONE":
            raise ElliottChallengerError("count_scenario_freeze_must_precede_outcome_visibility")
        freeze["outcome_visibility_at_freeze"] = "NONE"
        if freeze.get("retrofit_after_outcome_forbidden") is not True:
            raise ElliottChallengerError("retroactive_count_fit_must_be_forbidden")
        normalized["count_scenario_freeze"] = freeze

        decision = _required_mapping(raw.get("decision_layer_evaluation"), "decision_layer_evaluation", c["decision_layer_evaluation"]["required_fields"])
        if decision.get("comparison") != "B5_VS_B6":
            raise ElliottChallengerError("qm_g_decision_comparison_must_be_b5_vs_b6")
        for field in ("stateful_policy_required", "same_starting_state_required", "same_observation_grid_required", "same_eligibility_required", "same_tradeability_required", "same_action_availability_required", "same_cost_model_required", "qm_f_incremental_ablation_required"):
            if decision.get(field) is not True:
                raise ElliottChallengerError(f"qm_f_guard_required:{field}")
        normalized["decision_layer_evaluation"] = decision

        lineage = _required_mapping(raw.get("lineage_binding"), "lineage_binding", c["lineage_binding"]["required_fields"])
        for side in ("core", "challenger"):
            ref = _required_mapping(lineage.get(side), f"lineage_binding.{side}", ("node_id", "version_id", "content_hash"))
            for field in ("node_id", "version_id"):
                ref[field] = _text(ref.get(field), f"lineage_binding.{side}.{field}")
            ref["content_hash"] = _sha256(ref.get("content_hash"), f"lineage_binding.{side}.content_hash")
            lineage[side] = ref
        if lineage.get("independent_confirmation_claimed") is not False:
            raise ElliottChallengerError("challenger_may_not_claim_independent_confirmation")
        normalized["lineage_binding"] = lineage

        boundaries = _required_mapping(raw.get("boundaries"), "boundaries", c["boundaries"]["required_record_fields"])
        for field in c["boundaries"]["required_false_fields"]:
            if boundaries.get(field) is not False:
                raise ElliottChallengerError(f"challenger_boundary_must_be_false:{field}")
        if boundaries.get("sidecar_evidence_only") is not True:
            raise ElliottChallengerError("challenger_must_be_sidecar_evidence")
        normalized["boundaries"] = boundaries
        return normalized

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ElliottChallengerError(f"invalid_jsonl_line:{number}") from exc
            if not isinstance(value, dict):
                raise ElliottChallengerError(f"registry_line_not_object:{number}")
            rows.append(value)
        return rows

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous = None
        for sequence, raw in enumerate(events, start=1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION or event.get("sequence") != sequence:
                raise ElliottChallengerError(f"registry_event_invalid:{sequence}")
            if event.get("previous_event_hash") != previous:
                raise ElliottChallengerError(f"registry_previous_hash_invalid:{sequence}")
            stored = str(event.get("entry_hash") or "")
            body = dict(event); body.pop("entry_hash", None)
            if not stored or stored != _hash(body):
                raise ElliottChallengerError(f"registry_entry_hash_invalid:{sequence}")
            previous = stored

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        latest: dict[str, str] = {}
        for raw in events:
            event = dict(raw)
            if event.get("event_type") not in self.event_types:
                raise ElliottChallengerError(f"registry_event_type_unknown:{event.get('event_type')}")
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise ElliottChallengerError("registry_payload_must_be_object")
            record = self._normalize(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
            challenger_id = record["challenger_id"]
            version = record["challenger_version"]
            key = self._key(challenger_id, version)
            if key in records:
                raise ElliottChallengerError(f"challenger_version_already_registered:{key}")
            stored_hash = _text(payload.get("challenger_version_hash"), "challenger_version_hash")
            if stored_hash != challenger_version_hash(record):
                raise ElliottChallengerError("challenger_version_hash_mismatch")
            supersedes = payload.get("supersedes_challenger_version")
            if supersedes is not None:
                supersedes = _text(supersedes, "supersedes_challenger_version")
                if self._key(challenger_id, supersedes) not in records or latest.get(challenger_id) != supersedes:
                    raise ElliottChallengerError("challenger_successor_must_supersede_latest_version")
            elif challenger_id in latest:
                raise ElliottChallengerError("challenger_successor_requires_supersedes_reference")
            records[key] = {**record, "challenger_version_hash": stored_hash, "supersedes_challenger_version": supersedes, "registered_at": event["recorded_at"], "registered_by": event["actor_id"]}
            latest[challenger_id] = version
        return records

    def _load(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read(); self._verify_chain(events)
        return events, self._replay(events)

    def register_challenger(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str, supersedes_challenger_version: str | None = None) -> dict[str, Any]:
        normalized = self._normalize(record)
        events, records = self._load()
        key = self._key(normalized["challenger_id"], normalized["challenger_version"])
        if key in records:
            raise ElliottChallengerError(f"challenger_version_already_registered:{key}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            lock_fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ElliottChallengerError(f"registry_lock_exists:{self.lock_path}") from exc
        try:
            events, _ = self._load()
            event = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": "CHALLENGER_REGISTERED",
                "recorded_at": _now(),
                "challenger_id": normalized["challenger_id"],
                "challenger_version": normalized["challenger_version"],
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "payload": {"record": normalized, "challenger_version_hash": challenger_version_hash(normalized), "supersedes_challenger_version": supersedes_challenger_version},
                "previous_event_hash": events[-1]["entry_hash"] if events else None,
            }
            event["entry_hash"] = _hash(event)
            candidate = [*events, event]
            self._verify_chain(candidate); self._replay(candidate)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(_json(event) + "\n"); handle.flush(); os.fsync(handle.fileno())
            return event
        finally:
            os.close(lock_fd)
            try: self.lock_path.unlink()
            except FileNotFoundError: pass

    def get_challenger(self, challenger_id: str, challenger_version: str) -> dict[str, Any]:
        _, records = self._load()
        key = self._key(challenger_id, challenger_version)
        if key not in records:
            raise ElliottChallengerError(f"challenger_version_not_registered:{key}")
        return dict(records[key])

    def validate_evaluation_ready(self, *, challenger_id: str, challenger_version: str, hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, multiplicity_registry: FamilyMultiplicityRegistry, qm_a_ledger: GovernanceLedger, qm_b_closure: Mapping[str, Any], lineage_registry: LineageRegistry, evaluation_started_at: str) -> dict[str, Any]:
        """Fail closed unless the registered challenger is pre-outcome evaluation-ready."""
        record = self.get_challenger(challenger_id, challenger_version)
        hypothesis = hypothesis_registry.get_hypothesis(record["hypothesis_id"], record["hypothesis_version"])
        for field in ("hypothesis_version_hash", "hypothesis_family_id", "qm_a_analysis_id"):
            if hypothesis.get(field) != record[field]:
                raise ElliottChallengerError(f"qm_c_hypothesis_binding_mismatch:{field}")
        if hypothesis.get("research_mode") != "CONFIRMATION" or hypothesis.get("state") != "FROZEN_FOR_CONFIRMATION":
            raise ElliottChallengerError("qm_c_hypothesis_must_be_frozen_confirmation")

        plan = analysis_plan_registry.get_plan(record["analysis_plan_id"], record["analysis_plan_version"])
        checks = {"hypothesis_id": record["hypothesis_id"], "hypothesis_version": record["hypothesis_version"], "hypothesis_version_hash": record["hypothesis_version_hash"], "analysis_plan_hash": record["analysis_plan_hash"]}
        for field, expected in checks.items():
            if plan.get(field) != expected:
                raise ElliottChallengerError(f"qm_c_analysis_plan_binding_mismatch:{field}")
        if plan.get("research_mode") != "CONFIRMATION" or plan.get("state") != "FROZEN_FOR_CONFIRMATION":
            raise ElliottChallengerError("qm_c_analysis_plan_must_be_frozen_confirmation")

        try:
            confirmation_ready = analysis_plan_registry.validate_confirmation_ready(
                analysis_plan_id=record["analysis_plan_id"], analysis_plan_version=record["analysis_plan_version"], hypothesis_registry=hypothesis_registry, qm_a_ledger=qm_a_ledger, qm_a_version_id=record["qm_a_version_id"], qm_b_closure=qm_b_closure,
            )
        except Exception as exc:
            raise ElliottChallengerError("qm_c_confirmation_readiness_blocked") from exc
        if confirmation_ready.get("valid") is not True:
            raise ElliottChallengerError("qm_c_confirmation_readiness_blocked")

        multiplicity = multiplicity_registry.validate_multiplicity_ready(control_plan_id=record["control_plan_id"], control_plan_version=record["control_plan_version"])
        if multiplicity.get("control_plan_hash") != record["control_plan_hash"]:
            raise ElliottChallengerError("qm_c_multiplicity_control_hash_mismatch")
        if multiplicity.get("hypothesis_family_id") != record["hypothesis_family_id"]:
            raise ElliottChallengerError("qm_c_multiplicity_family_mismatch")
        member_match = any(member.get("hypothesis_id") == record["hypothesis_id"] and member.get("hypothesis_version") == record["hypothesis_version"] and member.get("analysis_plan_id") == record["analysis_plan_id"] and member.get("analysis_plan_version") == record["analysis_plan_version"] for member in multiplicity.get("family_members", []))
        if not member_match:
            raise ElliottChallengerError("qm_c_multiplicity_member_missing")

        analysis = qm_a_ledger.get_analysis(record["qm_a_analysis_id"], record["qm_a_version_id"])
        if analysis.get("state") != record["evidence_state"]:
            raise ElliottChallengerError("qm_a_evidence_state_mismatch")
        if analysis.get("state") != "FROZEN_FOR_CONFIRMATION":
            raise ElliottChallengerError("qm_g_evaluation_requires_unspent_frozen_evidence")

        freeze_time = _timestamp(record["count_scenario_freeze"]["frozen_at"], "count_scenario_freeze.frozen_at")
        evaluation_time = _timestamp(evaluation_started_at, "evaluation_started_at")
        if freeze_time > evaluation_time:
            raise ElliottChallengerError("count_scenario_freeze_after_evaluation_start")

        refs = []
        for side in ("core", "challenger"):
            ref = record["lineage_binding"][side]
            node = lineage_registry.get_node(ref["node_id"], ref["version_id"])
            if node.get("content_hash") != ref["content_hash"]:
                raise ElliottChallengerError(f"qm_i_lineage_hash_mismatch:{side}")
            if node.get("lineage_complete") is not True:
                raise ElliottChallengerError(f"qm_i_lineage_incomplete:{side}")
            refs.append({"node_id": ref["node_id"], "version_id": ref["version_id"]})
        lineage_review = lineage_registry.double_counting_review(combination_id=f"qm-g:{challenger_id}:{challenger_version}", evidence_nodes=refs, purports_independent=False)
        if lineage_review.get("status") == "REVIEW_REQUIRED":
            allowed = {"COMMON_ANCESTRY_REVIEW_REQUIRED", "DIRECT_DEPENDENCY_REVIEW_REQUIRED"}
            triggers = {item.get("trigger") for item in lineage_review.get("triggers", [])}
            if not triggers or not triggers.issubset(allowed):
                raise ElliottChallengerError("qm_i_lineage_review_blocking")

        return {"schema_version": "qm_g_elliott_challenger_readiness_v1", "valid": True, "challenger_id": record["challenger_id"], "challenger_version": record["challenger_version"], "challenger_version_hash": record["challenger_version_hash"], "core_binding": dict(record["core_binding"]), "count_scenario_freeze": dict(record["count_scenario_freeze"]), "qm_c": {"hypothesis_id": record["hypothesis_id"], "hypothesis_version": record["hypothesis_version"], "analysis_plan_id": record["analysis_plan_id"], "analysis_plan_version": record["analysis_plan_version"], "control_plan_id": record["control_plan_id"], "control_plan_version": record["control_plan_version"]}, "qm_c_confirmation_ready": True, "qm_a_evidence_state": analysis["state"], "qm_i_double_counting_review": lineage_review, "decision_layer_evaluation": dict(record["decision_layer_evaluation"]), "promotion_performed": False, "productive_integration_enabled": False, "execution_allowed": False, "universal_stance_changed": False, "w6_interface_changed": False, "w8_action_matrix_changed": False}

    def verify_integrity(self) -> dict[str, Any]:
        events, records = self._load()
        return {"schema_version": "qm_g_elliott_challenger_registry_verification_v1", "valid": True, "event_count": len(events), "challenger_version_count": len(records), "head_hash": events[-1]["entry_hash"] if events else None}
