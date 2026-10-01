"""QM-H defect, near-miss and CAPA continuous governance controls.

Research-only. QM-H records findings and their corrective/preventive lifecycle.
It intentionally does not mutate QM-A evidence-consumption state, QM-B universe
semantics, QM-C research identities, scanner scores, decisions or orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping
from uuid import uuid4

EVENT_SCHEMA_VERSION = "qm_h_ledger_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_h_capa_v1.json"


class CapaLedgerError(ValueError):
    """Raised when QM-H governance invariants are violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash_mapping(value: Mapping[str, Any]) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def load_qm_h_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        value = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CapaLedgerError(f"qm_h_contract_unreadable:{contract_path}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != "qm_h_capa_v1":
        raise CapaLedgerError("qm_h_contract_schema_invalid")
    return value


class CapaLedger:
    """Append-only, hash-chained event ledger for QM-H findings and CAPA."""

    _FINDING_ID = re.compile(r"^QM-H-[A-Z0-9][A-Z0-9._-]*$")
    _CAPA_ID = re.compile(r"^QM-H-CAPA-[A-Z0-9][A-Z0-9._-]*$")

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_h_contract(contract_path)
        sm = self.contract["state_machine"]
        self.initial_state = str(sm["initial_state"])
        self.states = set(sm["states"])
        self.terminal_states = set(sm["terminal_states"])
        self.allowed_transitions = {key: set(values) for key, values in sm["allowed_transitions"].items()}
        self.categories = set(self.contract["finding_categories"])
        self.severities = set(self.contract["severity_levels"])
        self.evidence_impacts = set(self.contract["evidence_impact_classes"])
        self.required_transition_fields = {key: tuple(values) for key, values in self.contract["required_transition_fields"].items()}
        self.identity_contract = {key: tuple(values) for key, values in self.contract["identity_reference_contract"].items()}
        self.known_external_blockers = {str(row["blocker_id"]): str(row["state"]) for row in self.contract["known_qm_b_external_blockers"]}

    def _validate_finding_id(self, finding_id: str) -> str:
        finding_id = str(finding_id).strip()
        if not self._FINDING_ID.fullmatch(finding_id):
            raise CapaLedgerError("finding_id_invalid")
        return finding_id

    def _validate_capa_id(self, capa_id: Any) -> str:
        capa_id = str(capa_id or "").strip()
        if not self._CAPA_ID.fullmatch(capa_id):
            raise CapaLedgerError("capa_id_invalid")
        return capa_id

    def _validate_identity_refs(self, refs: Any) -> list[dict[str, Any]]:
        if not isinstance(refs, list):
            raise CapaLedgerError("identity_refs_must_be_list")
        normalized: list[dict[str, Any]] = []
        for index, ref in enumerate(refs):
            if not isinstance(ref, Mapping):
                raise CapaLedgerError(f"identity_ref_must_be_object:{index}")
            kind = str(ref.get("kind") or "").strip()
            if kind not in self.identity_contract:
                raise CapaLedgerError(f"identity_ref_kind_unknown:{kind}")
            identity = ref.get("identity")
            if not isinstance(identity, Mapping):
                raise CapaLedgerError(f"identity_ref_identity_must_be_object:{index}")
            identity = dict(identity)
            missing = [field for field in self.identity_contract[kind] if not str(identity.get(field) or "").strip()]
            if missing:
                raise CapaLedgerError(f"identity_ref_missing:{kind}:" + ",".join(missing))
            if kind == "QM_B_EXTERNAL_BLOCKER":
                blocker_id = str(identity["blocker_id"])
                expected_state = self.known_external_blockers.get(blocker_id)
                if expected_state is None:
                    raise CapaLedgerError(f"qm_b_external_blocker_unknown:{blocker_id}")
                if str(identity["state"]) != expected_state:
                    raise CapaLedgerError(f"qm_b_external_blocker_state_mismatch:{blocker_id}")
            normalized.append({"kind": kind, "identity": identity, "relationship": str(ref.get("relationship") or "AFFECTED_BY_FINDING")})
        return normalized

    def _validate_evidence_impact(self, value: Any) -> str:
        impact = str(value or "").strip()
        if impact not in self.evidence_impacts:
            raise CapaLedgerError(f"evidence_impact_invalid:{impact}")
        return impact

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
                raise CapaLedgerError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise CapaLedgerError(f"ledger_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: list[dict[str, Any]]) -> None:
        previous_hash: str | None = None
        seen_event_ids: set[str] = set()
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise CapaLedgerError(f"ledger_schema_invalid:{expected_sequence}")
            if event.get("sequence") != expected_sequence:
                raise CapaLedgerError(f"ledger_sequence_invalid:{expected_sequence}")
            event_id = str(event.get("event_id") or "")
            if not event_id or event_id in seen_event_ids:
                raise CapaLedgerError(f"ledger_event_id_invalid:{expected_sequence}")
            seen_event_ids.add(event_id)
            if event.get("previous_event_hash") != previous_hash:
                raise CapaLedgerError(f"ledger_previous_hash_invalid:{expected_sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise CapaLedgerError(f"ledger_entry_hash_missing:{expected_sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash_mapping(body):
                raise CapaLedgerError(f"ledger_entry_hash_invalid:{expected_sequence}")
            previous_hash = stored_hash

    def _require_fields(self, to_status: str, details: Mapping[str, Any]) -> None:
        required = self.required_transition_fields.get(to_status, ())
        missing = [field for field in required if not str(details.get(field) or "").strip()]
        if missing:
            raise CapaLedgerError(f"transition_fields_missing:{to_status}:" + ",".join(missing))

    def _validate_transition_details(self, *, finding: Mapping[str, Any], to_status: str, details: Mapping[str, Any]) -> None:
        self._require_fields(to_status, details)
        if to_status == "TRIAGED" and str(details["severity"]) not in self.severities:
            raise CapaLedgerError("severity_invalid")
        if to_status in {"ACTION_PLANNED", "IMPLEMENTED", "EFFECTIVENESS_VERIFIED", "CLOSED"}:
            capa_id = self._validate_capa_id(details.get("capa_id"))
            existing = finding.get("capa_id")
            if existing and capa_id != existing:
                raise CapaLedgerError("capa_id_cannot_change")
        if to_status == "EFFECTIVENESS_VERIFIED" and str(details.get("effectiveness_result")) != "EFFECTIVE":
            raise CapaLedgerError("effectiveness_verified_requires_effective_result")
        if to_status == "CLOSED":
            if finding["evidence_impact"] != "NO_KNOWN_EVIDENCE_IMPACT" and not str(details.get("evidence_disposition_reference") or "").strip():
                raise CapaLedgerError("closure_requires_evidence_disposition_reference")
            if finding["category"] == "EXTERNAL_EVIDENCE_GAP" and not str(details.get("external_evidence_resolution_reference") or "").strip():
                raise CapaLedgerError("external_evidence_gap_requires_resolution_reference")

    def _replay(self, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        findings: dict[str, dict[str, Any]] = {}
        for event in events:
            event_type = str(event.get("event_type") or "")
            finding_id = str(event.get("finding_id") or "")
            payload = event.get("payload")
            if not isinstance(payload, dict):
                raise CapaLedgerError("ledger_payload_must_be_object")
            if event_type == "FINDING_REGISTERED":
                self._validate_finding_id(finding_id)
                if finding_id in findings:
                    raise CapaLedgerError(f"finding_already_registered:{finding_id}")
                category = str(payload.get("category") or "")
                if category not in self.categories:
                    raise CapaLedgerError(f"finding_category_invalid:{category}")
                title = str(payload.get("title") or "").strip()
                description = str(payload.get("description") or "").strip()
                source_reference = str(payload.get("source_reference") or "").strip()
                if not title or not description or not source_reference:
                    raise CapaLedgerError("finding_core_fields_required")
                findings[finding_id] = {
                    "finding_id": finding_id,
                    "category": category,
                    "title": title,
                    "description": description,
                    "source_reference": source_reference,
                    "status": self.initial_state,
                    "identity_refs": self._validate_identity_refs(payload.get("identity_refs", [])),
                    "evidence_impact": self._validate_evidence_impact(payload.get("evidence_impact")),
                    "capa_id": None,
                    "registered_at": event["recorded_at"],
                    "last_event_hash": event["entry_hash"],
                    "history_length": 1,
                }
                continue
            if event_type != "FINDING_TRANSITION":
                raise CapaLedgerError(f"ledger_event_type_unknown:{event_type}")
            if finding_id not in findings:
                raise CapaLedgerError(f"finding_not_registered:{finding_id}")
            current = findings[finding_id]
            from_status = str(payload.get("from_status") or "")
            to_status = str(payload.get("to_status") or "")
            if from_status != current["status"]:
                raise CapaLedgerError(f"transition_from_status_mismatch:{finding_id}:{from_status}")
            if to_status not in self.states:
                raise CapaLedgerError(f"transition_target_unknown:{to_status}")
            if to_status not in self.allowed_transitions.get(from_status, set()):
                raise CapaLedgerError(f"transition_forbidden:{from_status}->{to_status}")
            details = payload.get("details")
            if not isinstance(details, Mapping):
                raise CapaLedgerError("transition_details_must_be_object")
            self._validate_transition_details(finding=current, to_status=to_status, details=details)
            if "evidence_impact" in details:
                current["evidence_impact"] = self._validate_evidence_impact(details["evidence_impact"])
            if to_status in {"ACTION_PLANNED", "IMPLEMENTED", "EFFECTIVENESS_VERIFIED", "CLOSED"}:
                current["capa_id"] = self._validate_capa_id(details.get("capa_id"))
            current["status"] = to_status
            current["history_length"] += 1
            current["last_event_hash"] = event["entry_hash"]
        return findings

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def verify_integrity(self) -> dict[str, Any]:
        events, findings = self._load_and_validate()
        return {"schema_version": "qm_h_ledger_verification_v1", "valid": True, "event_count": len(events), "finding_count": len(findings), "head_hash": events[-1]["entry_hash"] if events else None, "states": {key: value["status"] for key, value in sorted(findings.items())}, "open_findings": sorted(key for key, value in findings.items() if value["status"] not in self.terminal_states)}

    def get_finding(self, finding_id: str) -> dict[str, Any]:
        _, findings = self._load_and_validate()
        finding_id = self._validate_finding_id(finding_id)
        if finding_id not in findings:
            raise CapaLedgerError(f"finding_not_registered:{finding_id}")
        return dict(findings[finding_id])

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise CapaLedgerError(f"ledger_lock_exists:{self.lock_path}") from exc

    def _release_lock(self, fd: int) -> None:
        try:
            os.close(fd)
        finally:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def _append_event(self, *, event_type: str, finding_id: str, actor_id: str, actor_role: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if event_type not in {"FINDING_REGISTERED", "FINDING_TRANSITION"}:
            raise CapaLedgerError(f"unsupported_event_type:{event_type}")
        finding_id = self._validate_finding_id(finding_id)
        if not str(actor_id).strip() or not str(actor_role).strip():
            raise CapaLedgerError("actor_identity_required")
        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {"schema_version": EVENT_SCHEMA_VERSION, "sequence": len(events) + 1, "event_id": str(uuid4()), "event_type": event_type, "recorded_at": _utc_now(), "finding_id": finding_id, "actor_id": str(actor_id), "actor_role": str(actor_role), "payload": dict(payload), "previous_event_hash": events[-1]["entry_hash"] if events else None}
            event["entry_hash"] = _hash_mapping(event)
            candidate = [*events, event]
            self._verify_hash_chain(candidate)
            self._replay(candidate)
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(_canonical_json(event) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return dict(event)
        finally:
            self._release_lock(lock_fd)

    def register_finding(self, *, finding_id: str, category: str, title: str, description: str, source_reference: str, evidence_impact: str, identity_refs: list[Mapping[str, Any]] | None, actor_id: str, actor_role: str) -> dict[str, Any]:
        return self._append_event(event_type="FINDING_REGISTERED", finding_id=finding_id, actor_id=actor_id, actor_role=actor_role, payload={"category": str(category), "title": str(title), "description": str(description), "source_reference": str(source_reference), "evidence_impact": str(evidence_impact), "identity_refs": list(identity_refs or [])})

    def transition(self, *, finding_id: str, to_status: str, actor_id: str, actor_role: str, details: Mapping[str, Any]) -> dict[str, Any]:
        current = self.get_finding(finding_id)
        return self._append_event(event_type="FINDING_TRANSITION", finding_id=finding_id, actor_id=actor_id, actor_role=actor_role, payload={"from_status": current["status"], "to_status": str(to_status), "details": dict(details)})
