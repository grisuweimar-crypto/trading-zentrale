"""Phase L5 Candidate Registry & Hard Freeze.

L5 turns L4-eligible, L1-budgeted Discovery candidates into immutable PAT
research objects. It writes only inside the Pattern Discovery Lab artifact
namespace and emits an exact QM-C1/QM-C2 handoff package. It deliberately does
not write the central QM registries because L0 confines runtime writes to the
lab namespace.

No prospective outcome, rating, promotion, decision or execution semantics are
activated here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_c import (
    HypothesisRegistry,
    hypothesis_version_hash,
    load_qm_c_contract,
)
from scanner.research.governance.qm_c_analysis_plan import (
    AnalysisPlanRegistry,
    analysis_plan_hash,
    load_qm_c_analysis_plan_contract,
)

from .boundary import PatternDiscoveryBoundary
from .run_contract import verify_run_manifest
from .search_engine import verify_search_result
from .statistical_guard import verify_statistical_evidence


SCHEMA_VERSION = "pattern_discovery_l5_candidate_registry_v1"
EVENT_SCHEMA_VERSION = "pattern_discovery_l5_registry_event_v1"
SNAPSHOT_SCHEMA_VERSION = "pattern_discovery_l5_freeze_snapshot_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l5_candidate_registry_v1.json"
)


class CandidateFreezeError(ValueError):
    """Raised when an L5 candidate-freeze invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise CandidateFreezeError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CandidateFreezeError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise CandidateFreezeError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_candidate_registry_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateFreezeError(
            f"candidate_registry_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise CandidateFreezeError("candidate_registry_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise CandidateFreezeError("candidate_registry_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise CandidateFreezeError(
            "candidate_registry_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise CandidateFreezeError("candidate_registry_execution_forbidden")
    return payload


def candidate_registry_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_candidate_registry_contract()
    )
    return _hash(value)


def _condition_root(condition: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature_id": condition["feature_id"],
        "transformation_id": condition["transformation_id"],
        "parameters": dict(condition["parameters"]),
        "state": condition["state"],
    }


def _pattern_root_identity(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pattern_type": candidate["pattern_type"],
        "target_id": candidate["target_id"],
        "expected_direction": candidate["expected_direction"],
        "horizon_sessions": candidate["horizon_sessions"],
        "baseline": candidate["baseline"],
        "conditions": sorted(
            (_condition_root(condition) for condition in candidate["conditions"]),
            key=_canonical_json,
        ),
    }


def pattern_id_for_candidate(
    candidate: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_candidate_registry_contract()
    )
    digest = _hash(_pattern_root_identity(candidate))
    identity = spec["identity"]
    return (
        f"{identity['pattern_id_prefix']}-"
        f"{digest[:int(identity['pattern_id_digest_chars'])].upper()}"
    )


def _description(candidate: Mapping[str, Any]) -> str:
    parts = []
    for condition in candidate["conditions"]:
        params = condition.get("parameters") or {}
        param_text = (
            f" {json.dumps(params, sort_keys=True, separators=(',', ':'))}"
            if params
            else ""
        )
        parts.append(
            f"{condition['feature_id']} "
            f"{condition['transformation_id']}{param_text} "
            f"is {condition['state']}"
        )
    joined = " AND ".join(parts)
    direction = str(candidate["expected_direction"]).lower()
    return (
        f"When {joined}, test whether {candidate['target_id']} has the "
        f"predeclared {direction} direction over "
        f"{candidate['horizon_sessions']} sessions versus baseline "
        f"{candidate['baseline']}."
    )


def _pattern_spec(
    *,
    pattern_id: str,
    pattern_version: str,
    candidate: Mapping[str, Any],
    manifest: Mapping[str, Any],
    l3_result: Mapping[str, Any],
    l4_evidence: Mapping[str, Any],
    freeze_timestamp: str,
    description: str,
) -> dict[str, Any]:
    prereg = manifest["preregistration"]
    feature_versions = sorted(
        {
            (
                str(condition["feature_id"]),
                str(condition["feature_version"]),
                str(condition["feature_version_hash"]),
            )
            for condition in candidate["conditions"]
        }
    )
    transformations = [
        {
            "feature_id": condition["feature_id"],
            "feature_version": condition["feature_version"],
            "transformation_id": condition["transformation_id"],
            "transformation_version": condition["transformation_version"],
            "parameters": dict(condition["parameters"]),
            "state": condition["state"],
        }
        for condition in candidate["conditions"]
    ]
    transformations.sort(key=_canonical_json)

    return {
        "identity": {
            "pattern_id": pattern_id,
            "pattern_version": pattern_version,
            "discovery_run_id": manifest["run_id"],
            "candidate_id": candidate["candidate_id"],
            "candidate_spec_hash": candidate["spec_hash"],
        },
        "semantics": {
            "pattern_type": candidate["pattern_type"],
            "natural_language_description": description,
            "conditions": candidate["conditions"],
            "feature_versions": [
                {
                    "feature_id": feature_id,
                    "feature_version": feature_version,
                    "feature_version_hash": feature_version_hash,
                }
                for feature_id, feature_version, feature_version_hash
                in feature_versions
            ],
            "transformation_rules": transformations,
        },
        "forecast": {
            "target_id": candidate["target_id"],
            "expected_direction": candidate["expected_direction"],
            "horizon_sessions": candidate["horizon_sessions"],
            "baseline": candidate["baseline"],
            "reference_definition": candidate["baseline"],
        },
        "data": {
            "universe_version": prereg["universe_version"],
            "feature_library_version": l4_evidence["feature_library_version"],
            "discovery_period": {
                "start": l3_result["observations"]["as_of_min"],
                "end": l3_result["observations"]["as_of_max"],
                "data_cutoff": prereg["data_cutoff"],
            },
            "pit_rules": list(prereg["pit_rules"]),
            "minimum_coverage_rule": "L4_MATURE_TARGET_COVERAGE_GATE",
        },
        "statistics": {
            "primary_method": prereg["statistical_primary_method"],
            "multiple_testing": dict(prereg["multiple_testing"]),
            "minimum_criteria": dict(prereg["minimum_criteria"]),
            "robustness_checks": list(prereg["robustness_checks"]),
            "discovery_evidence_hash": l4_evidence["evidence_hash"],
        },
        "freeze": {
            "freeze_timestamp": freeze_timestamp,
            "code_version": prereg["code_version"],
            "l1_manifest_hash": manifest["manifest_hash"],
            "l3_result_hash": l3_result["result_hash"],
            "l4_evidence_hash": l4_evidence["evidence_hash"],
        },
    }


def _qm_external_context(
    raw: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        raise CandidateFreezeError("qm_external_context_must_be_object")
    required = tuple(
        contract["qm_handoff"]["required_external_context_fields"]
    )
    missing = [field for field in required if field not in raw]
    if missing:
        raise CandidateFreezeError(
            "qm_external_context_fields_missing:" + ",".join(missing)
        )
    extra = sorted(set(raw) - set(required))
    if extra:
        raise CandidateFreezeError(
            "qm_external_context_unknown_fields:" + ",".join(extra)
        )
    return {
        field: _text(raw[field], f"qm_external_context.{field}")
        for field in required
    }


def _validate_qm_record_fields(
    record: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    section: str,
) -> None:
    required = tuple(contract[section]["required_fields"])
    missing = [field for field in required if field not in record]
    if missing:
        raise CandidateFreezeError(
            f"qm_record_fields_missing:{section}:" + ",".join(missing)
        )
    extra = sorted(set(record) - set(required))
    if extra:
        raise CandidateFreezeError(
            f"qm_record_unknown_fields:{section}:" + ",".join(extra)
        )


def _qm_handoff(
    *,
    pattern_id: str,
    pattern_version: str,
    pattern_spec: Mapping[str, Any],
    pattern_spec_hash: str,
    candidate: Mapping[str, Any],
    manifest: Mapping[str, Any],
    freeze_timestamp: str,
    external_context: Mapping[str, str],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    qm_c1 = load_qm_c_contract()
    qm_c2 = load_qm_c_analysis_plan_contract()

    hypothesis_id = f"H-{pattern_id}"
    hypothesis_version = str(contract["qm_handoff"]["hypothesis_version"])
    plan_id = f"AP-{pattern_id}"
    plan_version = str(contract["qm_handoff"]["analysis_plan_version"])
    qm_a_analysis_id = f"A-{pattern_id}"

    description = pattern_spec["semantics"]["natural_language_description"]
    hypothesis_record = {
        "hypothesis_id": hypothesis_id,
        "hypothesis_version": hypothesis_version,
        "research_question": (
            f"Does frozen {pattern_id} retain its predeclared effect on "
            f"strictly post-freeze observations?"
        ),
        "hypothesis_statement": (
            f"On future PIT observations matching {pattern_id} "
            f"{pattern_version}, {candidate['target_id']} shows the "
            f"predeclared {str(candidate['expected_direction']).lower()} "
            f"effect versus {candidate['baseline']} over "
            f"{candidate['horizon_sessions']} sessions."
        ),
        "hypothesis_family_id": f"HF-{candidate['family_id']}",
        "research_mode": str(contract["qm_handoff"]["research_mode"]),
        "qm_a_analysis_id": qm_a_analysis_id,
        "universe_requirement": str(
            contract["qm_handoff"]["universe_requirement"]
        ),
    }
    _validate_qm_record_fields(
        hypothesis_record,
        contract=qm_c1,
        section="hypothesis",
    )
    if hypothesis_record["research_mode"] not in set(
        qm_c1["hypothesis"]["research_modes"]
    ):
        raise CandidateFreezeError("qm_c1_research_mode_invalid")
    if hypothesis_record["universe_requirement"] not in set(
        qm_c1["hypothesis"]["universe_requirements"]
    ):
        raise CandidateFreezeError("qm_c1_universe_requirement_invalid")
    hypothesis_hash = hypothesis_version_hash(hypothesis_record)

    plan_record = {
        "analysis_plan_id": plan_id,
        "analysis_plan_version": plan_version,
        "hypothesis_id": hypothesis_id,
        "hypothesis_version": hypothesis_version,
        "hypothesis_version_hash": hypothesis_hash,
        "research_mode": hypothesis_record["research_mode"],
        "primary_estimand": (
            f"Prospective probability advantage and aligned outcome of "
            f"{pattern_id} {pattern_version} versus the frozen baseline."
        ),
        "primary_metrics": [
            "direction_probability",
            "probability_advantage_lift",
            "mean_aligned_outcome",
        ],
        "population_definition": (
            f"Strictly post-freeze PIT observations matching the immutable "
            f"{pattern_id} {pattern_version} conditions under universe "
            f"{manifest['preregistration']['universe_version']}."
        ),
        "universe_requirement": hypothesis_record["universe_requirement"],
        "evaluation_windows": [
            f"{int(candidate['horizon_sessions'])}_sessions"
        ],
        "exclusion_rules": sorted(
            set(str(value) for value in manifest["preregistration"]["exclusion_rules"])
            | {
                "pre_freeze_observation",
                "missing_or_immature_outcome",
                "pattern_version_mismatch",
            }
        ),
        "outcome_definitions": [
            (
                f"{candidate['target_id']} with expected_direction="
                f"{candidate['expected_direction']} and baseline="
                f"{candidate['baseline']}"
            )
        ],
        "planned_sensitivity_analyses": sorted(
            str(value)
            for value in manifest["preregistration"]["robustness_checks"]
        ),
    }
    _validate_qm_record_fields(
        plan_record,
        contract=qm_c2,
        section="analysis_plan",
    )
    plan_hash = analysis_plan_hash(plan_record)

    label_hash = _hash(
        {
            "target_id": candidate["target_id"],
            "expected_direction": candidate["expected_direction"],
            "horizon_sessions": candidate["horizon_sessions"],
        }
    )
    benchmark_hash = _hash(
        {
            "pattern_type": candidate["pattern_type"],
            "baseline": candidate["baseline"],
        }
    )
    freeze_context = {
        "code_or_commit_hash": _text(
            manifest["preregistration"]["code_version"], "code_version"
        ),
        "config_hash": _text(manifest["config_hash"], "config_hash"),
        "dataset_snapshot_hash": _text(
            manifest["input_fingerprint_hash"], "input_fingerprint_hash"
        ),
        "universe_ledger_version": external_context[
            "universe_ledger_version"
        ],
        "instrument_master_version": external_context[
            "instrument_master_version"
        ],
        "label_definition_hash": label_hash,
        "benchmark_definition_hash": benchmark_hash,
        "environment_or_dependency_fingerprint": external_context[
            "environment_or_dependency_fingerprint"
        ],
        "evaluation_cohort_id": (
            f"COHORT-{pattern_id}-{pattern_version}-POSTFREEZE"
        ),
        "temporal_boundary": freeze_timestamp,
    }
    required_freeze = tuple(qm_c2["freeze_context"]["required_fields"])
    if set(freeze_context) != set(required_freeze):
        raise CandidateFreezeError("qm_c2_freeze_context_field_set_mismatch")
    for field in required_freeze:
        _text(freeze_context[field], f"freeze_context.{field}")

    qm_a_identity = {
        "hypothesis_version_hash": hypothesis_hash,
        "analysis_plan_hash": plan_hash,
        **freeze_context,
    }
    required_identity = tuple(
        qm_c2["qm_a_binding"]["required_identity_fields"]
    )
    if set(qm_a_identity) != set(required_identity):
        raise CandidateFreezeError("qm_a_identity_field_set_mismatch")

    handoff = {
        "application_status": contract["qm_handoff"][
            "handoff_application_status"
        ],
        "direct_registry_write_performed": False,
        "l0_write_boundary_preserved": True,
        "application_order": [
            "QM_C1_REGISTER_HYPOTHESIS",
            "QM_C2_REGISTER_ANALYSIS_PLAN",
            "QM_C2_FREEZE_ANALYSIS_PLAN",
            "QM_C1_TRANSITION_HYPOTHESIS_TO_FROZEN_FOR_CONFIRMATION",
            "QM_A_BIND_EXACT_IDENTITY_IN_LATER_GOVERNANCE_STEP",
        ],
        "hypothesis_record": hypothesis_record,
        "hypothesis_version_hash": hypothesis_hash,
        "requested_hypothesis_target_state": contract["qm_handoff"][
            "requested_hypothesis_target_state"
        ],
        "analysis_plan_record": plan_record,
        "analysis_plan_hash": plan_hash,
        "freeze_context": freeze_context,
        "freeze_context_hash": _hash(freeze_context),
        "requested_analysis_plan_target_state": contract["qm_handoff"][
            "requested_analysis_plan_target_state"
        ],
        "qm_a_analysis_id": qm_a_analysis_id,
        "qm_a_identity": qm_a_identity,
        "qm_a_state_after_l5": contract["qm_handoff"][
            "qm_a_state_after_l5"
        ],
        "pattern_spec_hash": pattern_spec_hash,
        "pattern_description": description,
    }
    handoff["handoff_hash"] = _hash(handoff)
    return handoff


def validate_applied_qm_handoff(
    frozen_pattern: Mapping[str, Any],
    *,
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
) -> dict[str, Any]:
    """Verify that external QM-C application exactly matches the frozen handoff.

    This function is read-only. It does not register, freeze or transition
    anything and therefore preserves the L0 write boundary.
    """
    if not isinstance(frozen_pattern, Mapping):
        raise CandidateFreezeError("frozen_pattern_must_be_object")
    handoff = frozen_pattern.get("qm_c_handoff")
    if not isinstance(handoff, Mapping):
        raise CandidateFreezeError("qm_c_handoff_missing")

    hypothesis_record = handoff["hypothesis_record"]
    plan_record = handoff["analysis_plan_record"]
    hypothesis = hypothesis_registry.get_hypothesis(
        str(hypothesis_record["hypothesis_id"]),
        str(hypothesis_record["hypothesis_version"]),
    )
    plan = analysis_plan_registry.get_plan(
        str(plan_record["analysis_plan_id"]),
        str(plan_record["analysis_plan_version"]),
    )

    if hypothesis.get("hypothesis_version_hash") != handoff.get(
        "hypothesis_version_hash"
    ):
        raise CandidateFreezeError("qm_c1_applied_hypothesis_hash_mismatch")
    if hypothesis.get("state") != handoff.get(
        "requested_hypothesis_target_state"
    ):
        raise CandidateFreezeError("qm_c1_applied_hypothesis_state_mismatch")
    if plan.get("analysis_plan_hash") != handoff.get("analysis_plan_hash"):
        raise CandidateFreezeError("qm_c2_applied_plan_hash_mismatch")
    if plan.get("state") != handoff.get("requested_analysis_plan_target_state"):
        raise CandidateFreezeError("qm_c2_applied_plan_state_mismatch")
    if plan.get("freeze_context_hash") != handoff.get("freeze_context_hash"):
        raise CandidateFreezeError("qm_c2_applied_freeze_context_hash_mismatch")
    if plan.get("freeze_context") != handoff.get("freeze_context"):
        raise CandidateFreezeError("qm_c2_applied_freeze_context_mismatch")

    actual_qm_a_identity = analysis_plan_registry.build_qm_a_identity(
        str(plan_record["analysis_plan_id"]),
        str(plan_record["analysis_plan_version"]),
    )
    if actual_qm_a_identity != handoff.get("qm_a_identity"):
        raise CandidateFreezeError("qm_c2_qm_a_identity_mismatch")

    return {
        "valid": True,
        "pattern_id": frozen_pattern.get("pattern_id"),
        "pattern_version": frozen_pattern.get("pattern_version"),
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "analysis_plan_id": plan["analysis_plan_id"],
        "analysis_plan_version": plan["analysis_plan_version"],
        "analysis_plan_hash": plan["analysis_plan_hash"],
        "states": {
            "hypothesis": hypothesis["state"],
            "analysis_plan": plan["state"],
            "qm_a": "NOT_VALIDATED_BY_L5",
        },
    }


def _version_number(version: str) -> int:
    text = _text(version, "pattern_version")
    if not text.startswith("v") or not text[1:].isdigit():
        raise CandidateFreezeError(f"pattern_version_invalid:{text}")
    number = int(text[1:])
    if number <= 0:
        raise CandidateFreezeError(f"pattern_version_invalid:{text}")
    return number


class PatternCandidateRegistry:
    """Append-only hash-chained registry of immutable frozen PAT versions."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = (
            dict(contract)
            if contract is not None
            else load_candidate_registry_contract()
        )

    @staticmethod
    def _key(pattern_id: str, pattern_version: str) -> str:
        return f"{pattern_id}::{pattern_version}"

    def _read_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CandidateFreezeError(
                    f"pattern_registry_invalid_json:{line_number}"
                ) from exc
            if not isinstance(event, dict):
                raise CandidateFreezeError(
                    f"pattern_registry_event_not_object:{line_number}"
                )
            events.append(event)
        return events

    def _verify_hash_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise CandidateFreezeError(
                    f"pattern_registry_schema_invalid:{expected_sequence}"
                )
            if event.get("sequence") != expected_sequence:
                raise CandidateFreezeError(
                    f"pattern_registry_sequence_invalid:{expected_sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise CandidateFreezeError(
                    f"pattern_registry_previous_hash_invalid:{expected_sequence}"
                )
            stored = str(event.get("entry_hash") or "")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored != _hash(body):
                raise CandidateFreezeError(
                    f"pattern_registry_entry_hash_invalid:{expected_sequence}"
                )
            previous = stored

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        versions: dict[str, dict[str, Any]] = {}
        latest_by_id: dict[str, str] = {}
        for event in events:
            if event.get("event_type") != "PATTERN_FROZEN":
                raise CandidateFreezeError(
                    f"pattern_registry_event_type_unknown:{event.get('event_type')}"
                )
            record = event.get("record")
            if not isinstance(record, Mapping):
                raise CandidateFreezeError("pattern_registry_record_missing")
            pattern_id = _text(record.get("pattern_id"), "pattern_id")
            version = _text(record.get("pattern_version"), "pattern_version")
            _version_number(version)
            key = self._key(pattern_id, version)
            if key in versions:
                raise CandidateFreezeError(
                    f"pattern_version_already_frozen:{key}"
                )
            supersedes = event.get("supersedes_pattern_version")
            if supersedes is None:
                if pattern_id in latest_by_id:
                    raise CandidateFreezeError(
                        "pattern_successor_requires_supersedes_reference"
                    )
            else:
                supersedes = _text(
                    supersedes, "supersedes_pattern_version"
                )
                predecessor_key = self._key(pattern_id, supersedes)
                if predecessor_key not in versions:
                    raise CandidateFreezeError(
                        f"superseded_pattern_version_not_frozen:{predecessor_key}"
                    )
                if latest_by_id.get(pattern_id) != supersedes:
                    raise CandidateFreezeError(
                        "pattern_successor_must_supersede_latest_version"
                    )
                if _version_number(version) <= _version_number(supersedes):
                    raise CandidateFreezeError(
                        "pattern_successor_version_must_increase"
                    )
            record_hash = _text(
                record.get("frozen_record_hash"), "frozen_record_hash"
            )
            body = dict(record)
            body.pop("frozen_record_hash", None)
            if record_hash != _hash(body):
                raise CandidateFreezeError(
                    f"pattern_frozen_record_hash_invalid:{key}"
                )
            versions[key] = dict(record)
            latest_by_id[pattern_id] = version
        return versions

    def _load(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def get_pattern(
        self, pattern_id: str, pattern_version: str
    ) -> dict[str, Any]:
        _, versions = self._load()
        key = self._key(pattern_id, pattern_version)
        if key not in versions:
            raise CandidateFreezeError(f"pattern_version_not_frozen:{key}")
        return dict(versions[key])

    def register_frozen_pattern(
        self,
        *,
        record: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        supersedes_pattern_version: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise CandidateFreezeError("frozen_pattern_record_must_be_object")
        pattern_id = _text(record.get("pattern_id"), "pattern_id")
        version = _text(record.get("pattern_version"), "pattern_version")
        actor_id = _text(actor_id, "actor_id")
        actor_role = _text(actor_role, "actor_role")
        _version_number(version)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError as exc:
                raise CandidateFreezeError(
                    "pattern_registry_lock_already_held"
                ) from exc

            events, versions = self._load()
            key = self._key(pattern_id, version)
            if key in versions:
                existing = versions[key]
                if existing == dict(record):
                    return {
                        "idempotent": True,
                        "pattern_id": pattern_id,
                        "pattern_version": version,
                        "frozen_record_hash": existing["frozen_record_hash"],
                    }
                raise CandidateFreezeError(
                    f"pattern_version_already_frozen:{key}"
                )

            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": (
                    f"PFE-{_hash({'pattern_id': pattern_id, 'pattern_version': version, 'frozen_record_hash': record['frozen_record_hash']})[:24].upper()}"
                ),
                "event_type": "PATTERN_FROZEN",
                "recorded_at": record["freeze_timestamp"],
                "pattern_id": pattern_id,
                "pattern_version": version,
                "actor_id": actor_id,
                "actor_role": actor_role,
                "supersedes_pattern_version": supersedes_pattern_version,
                "record": dict(record),
                "previous_event_hash": (
                    events[-1]["entry_hash"] if events else None
                ),
            }
            event["entry_hash"] = _hash(event)
            candidate_events = [*events, event]
            self._verify_hash_chain(candidate_events)
            self._replay(candidate_events)

            line = (_canonical_json(event) + "\n").encode("utf-8")
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, line)
                os.fsync(fd)
            finally:
                os.close(fd)
            self._load()
            return event
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def verify_integrity(self) -> dict[str, Any]:
        events, versions = self._load()
        return {
            "valid": True,
            "event_count": len(events),
            "pattern_version_count": len(versions),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "patterns": {
                key: {
                    "pattern_spec_hash": value["pattern_spec_hash"],
                    "freeze_timestamp": value["freeze_timestamp"],
                }
                for key, value in sorted(versions.items())
            },
        }


def _validate_source_bindings(
    manifest: Mapping[str, Any],
    l3_result: Mapping[str, Any],
    l4_evidence: Mapping[str, Any],
) -> None:
    verify_run_manifest(manifest)
    verify_search_result(l3_result)
    verify_statistical_evidence(l4_evidence)

    if l3_result.get("run_id") != manifest.get("run_id"):
        raise CandidateFreezeError("l5_l3_run_id_mismatch")
    if l4_evidence.get("run_id") != manifest.get("run_id"):
        raise CandidateFreezeError("l5_l4_run_id_mismatch")
    if l3_result.get("l1_manifest_hash") != manifest.get("manifest_hash"):
        raise CandidateFreezeError("l5_l3_manifest_hash_mismatch")
    if l4_evidence.get("l1_manifest_hash") != manifest.get("manifest_hash"):
        raise CandidateFreezeError("l5_l4_manifest_hash_mismatch")
    if l4_evidence.get("l3_result_hash") != l3_result.get("result_hash"):
        raise CandidateFreezeError("l5_l4_l3_hash_mismatch")
    if l4_evidence.get("prospective_confirmation_used") is not False:
        raise CandidateFreezeError("l5_confirmation_data_forbidden")


def _eligible_records(
    l4_evidence: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    gate = str(contract["bindings"]["l4_gate_status_required"])
    shortlist = str(contract["bindings"]["l3_shortlist_status_required"])
    eligible = [
        dict(record)
        for record in l4_evidence["candidate_evidence"]
        if record.get("l4_gate_status") == gate
        and record.get("l3_shortlist_status") == shortlist
    ]
    eligible.sort(
        key=lambda record: (
            int(record["horizon_sessions"]),
            str(record["family_id"]),
            str(record["candidate_id"]),
        )
    )

    budget = manifest["preregistration"]["candidate_budget"]
    total_limit = int(budget["max_frozen_candidates_total"])
    horizon_limit = int(budget["max_frozen_candidates_per_horizon"])
    if len(eligible) > total_limit:
        raise CandidateFreezeError("l5_total_candidate_budget_exceeded")
    by_horizon: dict[int, int] = {}
    for record in eligible:
        horizon = int(record["horizon_sessions"])
        by_horizon[horizon] = by_horizon.get(horizon, 0) + 1
        if by_horizon[horizon] > horizon_limit:
            raise CandidateFreezeError(
                f"l5_horizon_candidate_budget_exceeded:{horizon}"
            )
    return eligible


def build_frozen_pattern_record(
    *,
    candidate: Mapping[str, Any],
    manifest: Mapping[str, Any],
    l3_result: Mapping[str, Any],
    l4_evidence: Mapping[str, Any],
    freeze_timestamp: str,
    qm_external_context: Mapping[str, str],
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_candidate_registry_contract()
    )
    if candidate.get("l4_gate_status") != spec["bindings"][
        "l4_gate_status_required"
    ]:
        raise CandidateFreezeError("candidate_not_l4_eligible")
    if candidate.get("l3_shortlist_status") != spec["bindings"][
        "l3_shortlist_status_required"
    ]:
        raise CandidateFreezeError("candidate_not_in_discovery_shortlist")

    freeze_at = _timestamp(freeze_timestamp, "freeze_timestamp")
    cutoff = _timestamp(
        manifest["preregistration"]["data_cutoff"], "data_cutoff"
    )
    declared_start = _timestamp(
        manifest["preregistration"]["declared_start_at"],
        "declared_start_at",
    )
    if freeze_at < cutoff:
        raise CandidateFreezeError("freeze_timestamp_before_data_cutoff")
    if freeze_at < declared_start:
        raise CandidateFreezeError("freeze_timestamp_before_declared_start")

    external = _qm_external_context(qm_external_context, spec)
    pattern_id = pattern_id_for_candidate(candidate, contract=spec)
    pattern_version = str(spec["identity"]["initial_pattern_version"])
    description = _description(candidate)
    pattern_spec = _pattern_spec(
        pattern_id=pattern_id,
        pattern_version=pattern_version,
        candidate=candidate,
        manifest=manifest,
        l3_result=l3_result,
        l4_evidence=l4_evidence,
        freeze_timestamp=freeze_at,
        description=description,
    )
    pattern_spec_hash = _hash(pattern_spec)
    handoff = _qm_handoff(
        pattern_id=pattern_id,
        pattern_version=pattern_version,
        pattern_spec=pattern_spec,
        pattern_spec_hash=pattern_spec_hash,
        candidate=candidate,
        manifest=manifest,
        freeze_timestamp=freeze_at,
        external_context=external,
        contract=spec,
    )

    record: dict[str, Any] = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": pattern_id,
        "pattern_version": pattern_version,
        "pattern_spec_hash": pattern_spec_hash,
        "natural_language_description": description,
        "pattern_spec": pattern_spec,
        "freeze_timestamp": freeze_at,
        "data_cutoff": manifest["preregistration"]["data_cutoff"],
        "code_version": manifest["preregistration"]["code_version"],
        "universe_version": manifest["preregistration"]["universe_version"],
        "feature_library_version": l4_evidence["feature_library_version"],
        "candidate_id": candidate["candidate_id"],
        "discovery_run_id": manifest["run_id"],
        "l4_evidence_hash": l4_evidence["evidence_hash"],
        "discovery_evidence": candidate["discovery_evidence"],
        "qm_c_handoff": handoff,
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    PatternDiscoveryBoundary().assert_research_payload(record)
    record["frozen_record_hash"] = _hash(record)
    return record


def freeze_candidates(
    manifest: Mapping[str, Any],
    l3_result: Mapping[str, Any],
    l4_evidence: Mapping[str, Any],
    *,
    repo_root: str | Path,
    freeze_timestamp: str,
    qm_external_context: Mapping[str, Any],
    actor_id: str,
    actor_role: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Freeze every L4-eligible shortlisted candidate as immutable PAT v1."""
    spec = (
        dict(contract)
        if contract is not None
        else load_candidate_registry_contract()
    )
    _validate_source_bindings(manifest, l3_result, l4_evidence)
    eligible = _eligible_records(
        l4_evidence,
        contract=spec,
        manifest=manifest,
    )
    freeze_at = _timestamp(freeze_timestamp, "freeze_timestamp")
    external = _qm_external_context(qm_external_context, spec)

    root = Path(repo_root).resolve()
    registry_repo_path = str(
        spec["identity"]["registry_path_template"]
    )
    PatternDiscoveryBoundary().assert_write_path_allowed(registry_repo_path)
    registry_path = (root / registry_repo_path).resolve()
    try:
        registry_path.relative_to(root)
    except ValueError as exc:
        raise CandidateFreezeError(
            "pattern_registry_path_outside_repo"
        ) from exc
    registry = PatternCandidateRegistry(registry_path, contract=spec)

    frozen: list[dict[str, Any]] = []
    for candidate in eligible:
        record = build_frozen_pattern_record(
            candidate=candidate,
            manifest=manifest,
            l3_result=l3_result,
            l4_evidence=l4_evidence,
            freeze_timestamp=freeze_at,
            qm_external_context=external,
            contract=spec,
        )
        registry.register_frozen_pattern(
            record=record,
            actor_id=actor_id,
            actor_role=actor_role,
        )
        frozen.append(record)

    registry_status = registry.verify_integrity()
    snapshot: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L5",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "run_id": manifest["run_id"],
        "freeze_timestamp": freeze_at,
        "l1_manifest_hash": manifest["manifest_hash"],
        "l3_result_hash": l3_result["result_hash"],
        "l4_evidence_hash": l4_evidence["evidence_hash"],
        "l5_contract_hash": candidate_registry_contract_hash(spec),
        "frozen_pattern_count": len(frozen),
        "frozen_patterns": frozen,
        "pattern_registry": registry_status,
        "qm_c_handoff": {
            "package_count": len(frozen),
            "all_packages_ready_not_applied_by_l5": all(
                record["qm_c_handoff"]["application_status"]
                == "READY_NOT_APPLIED_BY_L5"
                for record in frozen
            ),
            "direct_qm_registry_write_performed": False,
        },
        "boundaries": {
            "dependency_graph_performed": False,
            "prospective_capture_started": False,
            "confirmation_evaluation_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(snapshot)
    snapshot["snapshot_hash"] = _hash(snapshot)
    return snapshot


def verify_freeze_snapshot(
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise CandidateFreezeError("l5_snapshot_must_be_object")
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise CandidateFreezeError("l5_snapshot_schema_invalid")
    if snapshot.get("research_only") is not True:
        raise CandidateFreezeError("l5_snapshot_research_only_guard_missing")
    if snapshot.get("productive_integration_enabled") is not False:
        raise CandidateFreezeError(
            "l5_snapshot_productive_integration_forbidden"
        )
    if snapshot.get("execution_allowed") is not False:
        raise CandidateFreezeError("l5_snapshot_execution_forbidden")
    if snapshot.get("qm_c_handoff", {}).get(
        "direct_qm_registry_write_performed"
    ) is not False:
        raise CandidateFreezeError("l5_direct_qm_registry_write_forbidden")

    stored = _text(snapshot.get("snapshot_hash"), "snapshot_hash")
    body = dict(snapshot)
    body.pop("snapshot_hash", None)
    if _hash(body) != stored:
        raise CandidateFreezeError("l5_snapshot_hash_mismatch")
    for record in snapshot.get("frozen_patterns", []):
        if not isinstance(record, Mapping):
            raise CandidateFreezeError("l5_frozen_pattern_must_be_object")
        frozen_hash = _text(
            record.get("frozen_record_hash"), "frozen_record_hash"
        )
        record_body = dict(record)
        record_body.pop("frozen_record_hash", None)
        if _hash(record_body) != frozen_hash:
            raise CandidateFreezeError("l5_frozen_pattern_hash_mismatch")
        if record.get("confirmation_data_used") is not False:
            raise CandidateFreezeError("l5_confirmation_data_forbidden")
        if record.get("prospective_capture_started") is not False:
            raise CandidateFreezeError("l5_prospective_capture_forbidden")
    PatternDiscoveryBoundary().assert_research_payload(snapshot)
    return {
        "valid": True,
        "run_id": snapshot.get("run_id"),
        "snapshot_hash": stored,
        "frozen_pattern_count": snapshot.get("frozen_pattern_count"),
    }


def freeze_snapshot_repo_path(
    run_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_candidate_registry_contract()
    )
    return str(spec["identity"]["snapshot_path_template"]).format(
        run_id=_text(run_id, "run_id")
    )


def write_freeze_snapshot(
    repo_root: str | Path,
    snapshot: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
) -> Path:
    verify_freeze_snapshot(snapshot)
    guard = boundary or PatternDiscoveryBoundary()
    repo_path = freeze_snapshot_repo_path(str(snapshot["run_id"]))
    guard.assert_write_path_allowed(repo_path)

    root = Path(repo_root).resolve()
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise CandidateFreezeError(
            "l5_snapshot_path_outside_repo"
        ) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(snapshot),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
    except FileExistsError as exc:
        raise CandidateFreezeError(
            f"l5_snapshot_already_exists:{repo_path}"
        ) from exc
    return target
