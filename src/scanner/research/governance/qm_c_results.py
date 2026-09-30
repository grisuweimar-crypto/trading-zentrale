"""QM-C4 result retention, deterministic duplicate audit and full QM-C audit.

Research-only. The result registry is symmetric: positive, negative and
inconclusive evaluated outcomes are retained under the same immutable rules,
while rejected/retired hypotheses remain represented without invented outcome
evidence. Confirmatory results must bind exactly to QM-C1, QM-C2, QM-C3 and
QM-A identities.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import unicodedata
from typing import Any, Mapping, Sequence
from uuid import uuid4

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_multiplicity import MultiplicityMonitoringRegistry


EVENT_SCHEMA_VERSION = "qm_c_result_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c_results_audit_v1.json"


class ResultAuditError(ValueError):
    """Raised when a QM-C4 result or audit invariant is violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ResultAuditError(f"value_required:{field}")
    return normalized


def _optional_nonblank(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    return _nonblank(value, field=field)


def load_qm_c_results_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResultAuditError(f"qm_c_results_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_c_results_audit_v1":
        raise ResultAuditError("qm_c_results_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ResultAuditError("qm_c_results_contract_scope_invalid")
    return payload


def result_hash(record: Mapping[str, Any]) -> str:
    """Return the immutable semantic hash of one normalized result version."""
    return _hash(dict(record))


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"\s+", " ", normalized).strip()


def hypothesis_duplicate_fingerprint(hypothesis: Mapping[str, Any]) -> str:
    """Deterministic exact-semantic fingerprint, deliberately not fuzzy matching."""
    semantic = {
        "research_question": _normalize_text(str(hypothesis["research_question"])),
        "hypothesis_statement": _normalize_text(str(hypothesis["hypothesis_statement"])),
        "research_mode": str(hypothesis["research_mode"]).upper(),
        "universe_requirement": str(hypothesis["universe_requirement"]).upper(),
    }
    return _hash(semantic)


class ResultRegistry:
    """Append-only, hash-chained result-version registry."""

    _OPTIONAL_BINDING_FIELDS = (
        "analysis_plan_id",
        "analysis_plan_version",
        "analysis_plan_hash",
        "control_plan_id",
        "control_plan_version",
        "control_plan_hash",
        "qm_a_analysis_id",
        "qm_a_version_id",
        "evidence_artifact_hash",
    )

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_c_results_contract(contract_path)
        spec = self.contract["result_record"]
        self.required_fields = tuple(spec["required_fields"])
        self.evidence_scopes = set(spec["evidence_scopes"])
        self.outcome_classifications = set(spec["outcome_classifications"])
        self.evaluated_classifications = set(spec["evaluated_classifications"])
        self.no_outcome_classifications = set(spec["no_outcome_classifications"])
        self.confirmatory_binding_fields = tuple(spec["confirmatory_required_binding_fields"])
        self.no_outcome_null_fields = tuple(spec["no_outcome_binding_fields_must_be_null"])
        self.event_types = set(self.contract["result_registry"]["event_types"])

    @staticmethod
    def _key(result_id: str, result_version: str) -> str:
        return f"{result_id}::{result_version}"

    def _normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise ResultAuditError("result_record_must_be_object")
        missing = [field for field in self.required_fields if field not in record]
        if missing:
            raise ResultAuditError("result_fields_missing:" + ",".join(missing))
        normalized: dict[str, Any] = {
            "result_id": _nonblank(record.get("result_id"), field="result_id"),
            "result_version": _nonblank(record.get("result_version"), field="result_version"),
            "hypothesis_id": _nonblank(record.get("hypothesis_id"), field="hypothesis_id"),
            "hypothesis_version": _nonblank(record.get("hypothesis_version"), field="hypothesis_version"),
            "hypothesis_version_hash": _nonblank(record.get("hypothesis_version_hash"), field="hypothesis_version_hash"),
            "evidence_scope": _nonblank(record.get("evidence_scope"), field="evidence_scope").upper(),
            "outcome_classification": _nonblank(record.get("outcome_classification"), field="outcome_classification").upper(),
            "conclusion": _nonblank(record.get("conclusion"), field="conclusion"),
        }
        if normalized["evidence_scope"] not in self.evidence_scopes:
            raise ResultAuditError("result_evidence_scope_invalid")
        if normalized["outcome_classification"] not in self.outcome_classifications:
            raise ResultAuditError("result_outcome_classification_invalid")
        for field in self._OPTIONAL_BINDING_FIELDS:
            normalized[field] = _optional_nonblank(record.get(field), field=field)

        scope = normalized["evidence_scope"]
        classification = normalized["outcome_classification"]
        if scope == "NO_OUTCOME_EVIDENCE":
            if classification not in self.no_outcome_classifications:
                raise ResultAuditError("no_outcome_scope_requires_no_outcome_classification")
            violations = [field for field in self.no_outcome_null_fields if normalized.get(field) is not None]
            if violations:
                raise ResultAuditError("no_outcome_scope_forbids_bindings:" + ",".join(violations))
        else:
            if classification not in self.evaluated_classifications:
                raise ResultAuditError("evaluated_scope_requires_evaluated_classification")
            if normalized["evidence_artifact_hash"] is None:
                raise ResultAuditError("evaluated_result_requires_evidence_artifact_hash")

        if scope == "CONFIRMATORY":
            missing_bindings = [field for field in self.confirmatory_binding_fields if normalized.get(field) is None]
            if missing_bindings:
                raise ResultAuditError("confirmatory_result_bindings_missing:" + ",".join(missing_bindings))
        elif scope == "EXPLORATORY":
            # A discovery result may have a QM-C2 exploratory plan and/or QM-A
            # research identity, but never a QM-C3 confirmatory control plan.
            forbidden = [field for field in ("control_plan_id", "control_plan_version", "control_plan_hash") if normalized.get(field) is not None]
            if forbidden:
                raise ResultAuditError("exploratory_result_forbids_control_plan_binding")
            plan_values = [normalized.get(field) for field in ("analysis_plan_id", "analysis_plan_version", "analysis_plan_hash")]
            if any(value is not None for value in plan_values) and not all(value is not None for value in plan_values):
                raise ResultAuditError("exploratory_analysis_plan_binding_must_be_complete_or_null")
            qa_values = [normalized.get(field) for field in ("qm_a_analysis_id", "qm_a_version_id")]
            if any(value is not None for value in qa_values) and not all(value is not None for value in qa_values):
                raise ResultAuditError("exploratory_qm_a_binding_must_be_complete_or_null")
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
                raise ResultAuditError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise ResultAuditError(f"result_registry_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous_hash: str | None = None
        for expected_sequence, raw_event in enumerate(events, start=1):
            event = dict(raw_event)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise ResultAuditError(f"result_registry_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise ResultAuditError(f"result_registry_sequence_invalid:{expected_sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise ResultAuditError(f"result_registry_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise ResultAuditError(f"result_registry_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash(body):
                raise ResultAuditError(f"result_registry_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        latest_version_by_id: dict[str, str] = {}
        for raw_event in events:
            event = dict(raw_event)
            if event.get("event_type") not in self.event_types:
                raise ResultAuditError(f"result_registry_event_type_unknown:{event.get('event_type')}")
            result_id = _nonblank(event.get("result_id"), field="event.result_id")
            result_version = _nonblank(event.get("result_version"), field="event.result_version")
            key = self._key(result_id, result_version)
            if key in results:
                raise ResultAuditError(f"result_version_already_registered:{key}")
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise ResultAuditError("result_registry_payload_must_be_object")
            record = self._normalize_record(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
            if record["result_id"] != result_id or record["result_version"] != result_version:
                raise ResultAuditError("result_event_record_identity_mismatch")
            stored_hash = _nonblank(payload.get("result_hash"), field="result_hash")
            if stored_hash != result_hash(record):
                raise ResultAuditError("result_hash_mismatch")
            supersedes = payload.get("supersedes_result_version")
            if supersedes is not None:
                supersedes = _nonblank(supersedes, field="supersedes_result_version")
                predecessor_key = self._key(result_id, supersedes)
                if predecessor_key not in results:
                    raise ResultAuditError(f"superseded_result_version_not_registered:{predecessor_key}")
                if latest_version_by_id.get(result_id) != supersedes:
                    raise ResultAuditError("result_successor_must_supersede_latest_registered_version")
                if supersedes == result_version:
                    raise ResultAuditError("result_version_cannot_supersede_itself")
            elif result_id in latest_version_by_id:
                raise ResultAuditError("result_successor_requires_supersedes_reference")
            results[key] = {
                **record,
                "result_hash": stored_hash,
                "supersedes_result_version": supersedes,
                "registered_at": event["recorded_at"],
                "registered_by": event["actor_id"],
                "last_event_hash": event["entry_hash"],
            }
            latest_version_by_id[result_id] = result_version
        return results

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise ResultAuditError(f"result_registry_lock_exists:{self.lock_path}") from exc

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
        result_id: str,
        result_version: str,
        actor_id: str,
        actor_role: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        result_id = _nonblank(result_id, field="result_id")
        result_version = _nonblank(result_version, field="result_version")
        actor_id = _nonblank(actor_id, field="actor_id")
        actor_role = _nonblank(actor_role, field="actor_role")
        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": "RESULT_REGISTERED",
                "recorded_at": _utc_now(),
                "result_id": result_id,
                "result_version": result_version,
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
    def _validate_c1_binding(record: Mapping[str, Any], hypothesis_registry: HypothesisRegistry) -> dict[str, Any]:
        hypothesis = hypothesis_registry.get_hypothesis(record["hypothesis_id"], record["hypothesis_version"])
        if hypothesis["hypothesis_version_hash"] != record["hypothesis_version_hash"]:
            raise ResultAuditError("result_hypothesis_hash_mismatch")
        return hypothesis

    @staticmethod
    def _validate_confirmatory_binding(
        record: Mapping[str, Any],
        *,
        hypothesis: Mapping[str, Any],
        analysis_plan_registry: AnalysisPlanRegistry,
        control_registry: MultiplicityMonitoringRegistry,
        qm_a_ledger: GovernanceLedger,
    ) -> None:
        if hypothesis["research_mode"] != "CONFIRMATION":
            raise ResultAuditError("confirmatory_result_requires_confirmation_hypothesis")
        plan = analysis_plan_registry.get_plan(record["analysis_plan_id"], record["analysis_plan_version"])
        expected_plan = {
            "hypothesis_id": record["hypothesis_id"],
            "hypothesis_version": record["hypothesis_version"],
            "hypothesis_version_hash": record["hypothesis_version_hash"],
            "analysis_plan_hash": record["analysis_plan_hash"],
        }
        for field, expected in expected_plan.items():
            if plan.get(field) != expected:
                raise ResultAuditError(f"confirmatory_result_plan_binding_mismatch:{field}")
        control = control_registry.get_control_plan(record["control_plan_id"], record["control_plan_version"])
        if control["control_plan_hash"] != record["control_plan_hash"]:
            raise ResultAuditError("confirmatory_result_control_plan_hash_mismatch")
        if control["state"] not in {"COMPLETE", "STOPPED"}:
            raise ResultAuditError("confirmatory_result_requires_completed_or_stopped_control_plan")
        if not control["recorded_looks"]:
            raise ResultAuditError("confirmatory_result_requires_recorded_monitoring_look")
        matching_members = [
            member for member in control["family_members"]
            if member["hypothesis_id"] == record["hypothesis_id"]
            and member["hypothesis_version"] == record["hypothesis_version"]
            and member["hypothesis_version_hash"] == record["hypothesis_version_hash"]
            and member["analysis_plan_id"] == record["analysis_plan_id"]
            and member["analysis_plan_version"] == record["analysis_plan_version"]
            and member["analysis_plan_hash"] == record["analysis_plan_hash"]
            and member["qm_a_analysis_id"] == record["qm_a_analysis_id"]
            and member["qm_a_version_id"] == record["qm_a_version_id"]
        ]
        if len(matching_members) != 1:
            raise ResultAuditError("confirmatory_result_member_not_exactly_present_in_control_plan")
        analysis = qm_a_ledger.get_analysis(record["qm_a_analysis_id"], record["qm_a_version_id"])
        if analysis["state"] not in qm_a_ledger.spent_states:
            raise ResultAuditError(f"confirmatory_result_requires_consumed_qm_a_evidence:{analysis['state']}")

    @staticmethod
    def _validate_exploratory_binding(
        record: Mapping[str, Any],
        *,
        hypothesis: Mapping[str, Any],
        analysis_plan_registry: AnalysisPlanRegistry,
        qm_a_ledger: GovernanceLedger,
    ) -> None:
        if hypothesis["research_mode"] != "DISCOVERY":
            raise ResultAuditError("exploratory_result_requires_discovery_hypothesis")
        if record.get("analysis_plan_id") is not None:
            plan = analysis_plan_registry.get_plan(record["analysis_plan_id"], record["analysis_plan_version"])
            if plan["analysis_plan_hash"] != record["analysis_plan_hash"]:
                raise ResultAuditError("exploratory_result_analysis_plan_hash_mismatch")
            if plan["hypothesis_version_hash"] != record["hypothesis_version_hash"]:
                raise ResultAuditError("exploratory_result_hypothesis_plan_hash_mismatch")
            if plan["research_mode"] != "DISCOVERY":
                raise ResultAuditError("exploratory_result_requires_discovery_analysis_plan")
        if record.get("qm_a_analysis_id") is not None:
            if record["qm_a_analysis_id"] != hypothesis["qm_a_analysis_id"]:
                raise ResultAuditError("exploratory_result_qm_a_analysis_id_mismatch")
            qm_a_ledger.get_analysis(record["qm_a_analysis_id"], record["qm_a_version_id"])

    def register_result(
        self,
        *,
        record: Mapping[str, Any],
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
        control_registry: MultiplicityMonitoringRegistry,
        qm_a_ledger: GovernanceLedger,
        actor_id: str,
        actor_role: str,
        supersedes_result_version: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_record(record)
        hypothesis = self._validate_c1_binding(normalized, hypothesis_registry)
        scope = normalized["evidence_scope"]
        if scope == "CONFIRMATORY":
            self._validate_confirmatory_binding(
                normalized,
                hypothesis=hypothesis,
                analysis_plan_registry=analysis_plan_registry,
                control_registry=control_registry,
                qm_a_ledger=qm_a_ledger,
            )
        elif scope == "EXPLORATORY":
            self._validate_exploratory_binding(
                normalized,
                hypothesis=hypothesis,
                analysis_plan_registry=analysis_plan_registry,
                qm_a_ledger=qm_a_ledger,
            )
        else:
            classification = normalized["outcome_classification"]
            if classification == "REJECTED_PRE_EVALUATION" and hypothesis["state"] not in {"REJECTED", "RETIRED"}:
                raise ResultAuditError("rejected_pre_evaluation_result_requires_rejected_hypothesis")
            if classification == "RETIRED_WITHOUT_EVALUATION" and hypothesis["state"] != "RETIRED":
                raise ResultAuditError("retired_without_evaluation_requires_retired_hypothesis")

        _, results = self._load_and_validate()
        key = self._key(normalized["result_id"], normalized["result_version"])
        if key in results:
            raise ResultAuditError(f"result_version_already_registered:{key}")
        return self._append_event(
            result_id=normalized["result_id"],
            result_version=normalized["result_version"],
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "record": normalized,
                "result_hash": result_hash(normalized),
                "supersedes_result_version": supersedes_result_version,
            },
        )

    def get_result(self, result_id: str, result_version: str) -> dict[str, Any]:
        _, results = self._load_and_validate()
        key = self._key(result_id, result_version)
        if key not in results:
            raise ResultAuditError(f"result_version_not_registered:{key}")
        return dict(results[key])

    def verify_integrity(self) -> dict[str, Any]:
        events, results = self._load_and_validate()
        counts: dict[str, int] = {}
        for result in results.values():
            classification = result["outcome_classification"]
            counts[classification] = counts.get(classification, 0) + 1
        return {
            "schema_version": "qm_c_result_registry_verification_v1",
            "valid": True,
            "event_count": len(events),
            "result_version_count": len(results),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "classification_counts": dict(sorted(counts.items())),
        }


def detect_duplicate_hypotheses(hypothesis_registry: HypothesisRegistry) -> list[dict[str, Any]]:
    """Report cross-ID deterministic semantic duplicates without merging them."""
    hypothesis_registry.verify_integrity()
    _, versions = hypothesis_registry._load_and_validate()  # same governance package; validated replay
    grouped: dict[str, list[dict[str, Any]]] = {}
    for hypothesis in versions.values():
        fingerprint = hypothesis_duplicate_fingerprint(hypothesis)
        grouped.setdefault(fingerprint, []).append(hypothesis)

    findings: list[dict[str, Any]] = []
    for fingerprint, members in sorted(grouped.items()):
        distinct_ids = sorted({str(member["hypothesis_id"]) for member in members})
        if len(distinct_ids) < 2:
            continue
        finding_id = "QM-C-DUP-" + fingerprint[:16].upper()
        findings.append({
            "finding_id": finding_id,
            "classification": "POTENTIAL_EXACT_SEMANTIC_DUPLICATE",
            "fingerprint": fingerprint,
            "hypothesis_ids": distinct_ids,
            "members": [
                {
                    "hypothesis_id": member["hypothesis_id"],
                    "hypothesis_version": member["hypothesis_version"],
                    "hypothesis_version_hash": member["hypothesis_version_hash"],
                    "hypothesis_family_id": member["hypothesis_family_id"],
                }
                for member in sorted(members, key=lambda item: (item["hypothesis_id"], item["hypothesis_version"]))
            ],
            "automatic_merge_performed": False,
            "requires_review": True,
        })
    return findings


def audit_qm_c_system(
    *,
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
    control_registry: MultiplicityMonitoringRegistry,
    result_registry: ResultRegistry,
) -> dict[str, Any]:
    """Audit integrity and static cross-references across QM-C1 through QM-C4."""
    c1 = hypothesis_registry.verify_integrity()
    c2 = analysis_plan_registry.verify_integrity()
    c3 = control_registry.verify_integrity()
    c4 = result_registry.verify_integrity()
    _, hypotheses = hypothesis_registry._load_and_validate()
    _, plans = analysis_plan_registry._load_and_validate()
    _, controls = control_registry._load_and_validate()
    _, results = result_registry._load_and_validate()

    cross_reference_errors: list[str] = []
    for key, plan in plans.items():
        hypothesis_key = f"{plan['hypothesis_id']}::{plan['hypothesis_version']}"
        hypothesis = hypotheses.get(hypothesis_key)
        if hypothesis is None:
            cross_reference_errors.append(f"plan_missing_hypothesis:{key}:{hypothesis_key}")
            continue
        if hypothesis["hypothesis_version_hash"] != plan["hypothesis_version_hash"]:
            cross_reference_errors.append(f"plan_hypothesis_hash_mismatch:{key}")

    for key, control in controls.items():
        for member in control["family_members"]:
            hkey = f"{member['hypothesis_id']}::{member['hypothesis_version']}"
            pkey = f"{member['analysis_plan_id']}::{member['analysis_plan_version']}"
            hypothesis = hypotheses.get(hkey)
            plan = plans.get(pkey)
            if hypothesis is None:
                cross_reference_errors.append(f"control_missing_hypothesis:{key}:{hkey}")
            elif hypothesis["hypothesis_version_hash"] != member["hypothesis_version_hash"]:
                cross_reference_errors.append(f"control_hypothesis_hash_mismatch:{key}:{hkey}")
            if plan is None:
                cross_reference_errors.append(f"control_missing_plan:{key}:{pkey}")
            elif plan["analysis_plan_hash"] != member["analysis_plan_hash"]:
                cross_reference_errors.append(f"control_plan_hash_mismatch:{key}:{pkey}")

    for key, result in results.items():
        hkey = f"{result['hypothesis_id']}::{result['hypothesis_version']}"
        hypothesis = hypotheses.get(hkey)
        if hypothesis is None:
            cross_reference_errors.append(f"result_missing_hypothesis:{key}:{hkey}")
        elif hypothesis["hypothesis_version_hash"] != result["hypothesis_version_hash"]:
            cross_reference_errors.append(f"result_hypothesis_hash_mismatch:{key}:{hkey}")
        if result["analysis_plan_id"] is not None:
            pkey = f"{result['analysis_plan_id']}::{result['analysis_plan_version']}"
            plan = plans.get(pkey)
            if plan is None:
                cross_reference_errors.append(f"result_missing_plan:{key}:{pkey}")
            elif plan["analysis_plan_hash"] != result["analysis_plan_hash"]:
                cross_reference_errors.append(f"result_plan_hash_mismatch:{key}:{pkey}")
        if result["control_plan_id"] is not None:
            ckey = f"{result['control_plan_id']}::{result['control_plan_version']}"
            control = controls.get(ckey)
            if control is None:
                cross_reference_errors.append(f"result_missing_control:{key}:{ckey}")
            elif control["control_plan_hash"] != result["control_plan_hash"]:
                cross_reference_errors.append(f"result_control_hash_mismatch:{key}:{ckey}")

    duplicates = detect_duplicate_hypotheses(hypothesis_registry)
    return {
        "schema_version": "qm_c_full_system_audit_v1",
        "status": "PASS" if not cross_reference_errors else "FAIL",
        "integrity": {"QM-C1": c1, "QM-C2": c2, "QM-C3": c3, "QM-C4": c4},
        "counts": {
            "hypothesis_versions": len(hypotheses),
            "analysis_plan_versions": len(plans),
            "control_plan_versions": len(controls),
            "result_versions": len(results),
            "duplicate_findings": len(duplicates),
        },
        "cross_reference_errors": cross_reference_errors,
        "duplicate_findings": duplicates,
        "duplicate_findings_are_auto_merged": False,
    }
