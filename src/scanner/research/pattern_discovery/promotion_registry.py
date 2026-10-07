"""Append-only L12 promotion decision / reversal registry."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .promotion_gate import (
    PromotionGateError,
    _hash,
    _json,
    _sha,
    _text,
    _time,
    load_promotion_contract,
    verify_promotion_decision,
)

EVENT_SCHEMA_VERSION = "pattern_discovery_l12_promotion_registry_event_v1"


class PromotionRegistry:
    """Hash-chained audit log for admission, rejection, defer and reversal."""

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
            else load_promotion_contract()
        )

    @staticmethod
    def _key(identity: Mapping[str, Any]) -> str:
        return "::".join(
            (
                _text(identity.get("pattern_id"), "pattern_id"),
                _text(identity.get("pattern_version"), "pattern_version"),
                _text(identity.get("pattern_spec_hash"), "pattern_spec_hash"),
            )
        )

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PromotionGateError(
                    f"promotion_registry_invalid_json:{number}"
                ) from exc
            if not isinstance(value, dict):
                raise PromotionGateError(
                    f"promotion_registry_event_not_object:{number}"
                )
            rows.append(value)
        return rows

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        previous: str | None = None
        states: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()
        allowed_reversal_reasons = set(
            self.contract["reversibility"]["reason_codes"]
        )
        allowed_actions = set(
            self.contract["reversibility"]["allowed_terminal_actions"]
        )
        for sequence, raw in enumerate(events, 1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise PromotionGateError(
                    f"promotion_registry_schema_invalid:{sequence}"
                )
            if event.get("sequence") != sequence:
                raise PromotionGateError(
                    f"promotion_registry_sequence_invalid:{sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise PromotionGateError(
                    f"promotion_registry_previous_hash_invalid:{sequence}"
                )
            stored = _sha(
                event.get("entry_hash"),
                f"promotion_registry.entry_hash.{sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise PromotionGateError(
                    f"promotion_registry_entry_hash_invalid:{sequence}"
                )
            event_id = _text(event.get("event_id"), "event_id")
            if event_id in seen:
                raise PromotionGateError(
                    "promotion_registry_duplicate_event_id"
                )
            seen.add(event_id)
            identity = event.get("pattern_identity")
            if not isinstance(identity, Mapping):
                raise PromotionGateError(
                    "promotion_registry_pattern_identity_missing"
                )
            key = self._key(identity)
            current = states.get(key)
            event_type = _text(event.get("event_type"), "event_type")

            if event_type == "PROMOTION_DECISION_RECORDED":
                decision = event.get("decision")
                if not isinstance(decision, Mapping):
                    raise PromotionGateError(
                        "promotion_registry_decision_missing"
                    )
                verify_promotion_decision(
                    decision, contract=self.contract
                )
                if self._key(decision["pattern_identity"]) != key:
                    raise PromotionGateError(
                        "promotion_registry_decision_identity_mismatch"
                    )
                if (
                    current is not None
                    and current["status"] == "ADMITTED"
                    and current.get("decision_id") != decision["decision_id"]
                ):
                    raise PromotionGateError(
                        "promotion_registry_requires_reversal_before_new_decision"
                    )
                states[key] = {
                    "status": decision["promotion_status"],
                    "decision_id": decision["decision_id"],
                    "decision_hash": decision["decision_hash"],
                    "review_id": decision["review_id"],
                    "last_event_hash": stored,
                }
            elif event_type == "PROMOTION_STATE_REVERSED":
                if current is None or current["status"] != "ADMITTED":
                    raise PromotionGateError(
                        "promotion_reversal_requires_admitted_state"
                    )
                reversal = event.get("reversal")
                if not isinstance(reversal, Mapping):
                    raise PromotionGateError(
                        "promotion_reversal_payload_missing"
                    )
                action = _text(reversal.get("action"), "reversal.action")
                if action not in allowed_actions:
                    raise PromotionGateError(
                        f"promotion_reversal_action_invalid:{action}"
                    )
                reasons = reversal.get("reason_codes")
                if not isinstance(reasons, list) or not reasons:
                    raise PromotionGateError(
                        "promotion_reversal_reason_codes_required"
                    )
                if any(str(x) not in allowed_reversal_reasons for x in reasons):
                    raise PromotionGateError(
                        "promotion_reversal_reason_code_invalid"
                    )
                _sha(reversal.get("evidence_hash"), "reversal.evidence_hash")
                states[key] = {
                    **current,
                    "status": (
                        "DEMOTED" if action == "DEMOTE" else "ROLLED_BACK"
                    ),
                    "last_event_hash": stored,
                }
            else:
                raise PromotionGateError(
                    f"promotion_registry_event_type_unknown:{event_type}"
                )
            previous = stored
        return states

    def _append(
        self,
        *,
        event_type: str,
        identity: Mapping[str, Any],
        payload_key: str,
        payload: Mapping[str, Any],
        recorded_at: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
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
                raise PromotionGateError(
                    "promotion_registry_lock_already_held"
                ) from exc
            events = self._read()
            self._replay(events)
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_type": event_type,
                "recorded_at": _time(recorded_at, "recorded_at"),
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "pattern_identity": dict(identity),
                payload_key: dict(payload),
                "previous_event_hash": (
                    events[-1]["entry_hash"] if events else None
                ),
            }
            event["event_id"] = "PRE-" + _hash(event)[:24].upper()
            event["entry_hash"] = _hash(event)
            self._replay([*events, event])
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, (_json(event) + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            return event
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def record_decision(
        self,
        decision: Mapping[str, Any],
    ) -> dict[str, Any]:
        verify_promotion_decision(decision, contract=self.contract)
        events = self._read()
        self._replay(events)
        for event in events:
            existing = event.get("decision")
            if (
                isinstance(existing, Mapping)
                and existing.get("decision_id") == decision.get("decision_id")
            ):
                if dict(existing) != dict(decision):
                    raise PromotionGateError(
                        "promotion_decision_identity_collision"
                    )
                return {
                    "valid": True,
                    "idempotent": True,
                    "event_id": event["event_id"],
                    "entry_hash": event["entry_hash"],
                }
        event = self._append(
            event_type="PROMOTION_DECISION_RECORDED",
            identity=decision["pattern_identity"],
            payload_key="decision",
            payload=decision,
            recorded_at=decision["reviewed_at"],
            actor_id=decision["reviewer_id"],
            actor_role=decision["reviewer_role"],
        )
        return {
            "valid": True,
            "idempotent": False,
            "event_id": event["event_id"],
            "entry_hash": event["entry_hash"],
        }

    def reverse(
        self,
        *,
        pattern_identity: Mapping[str, Any],
        action: str,
        reason_codes: Sequence[str],
        actor_id: str,
        actor_role: str,
        observed_at: str,
        evidence_hash: str,
    ) -> dict[str, Any]:
        normalized = _text(action, "action").upper()
        allowed_actions = set(
            self.contract["reversibility"]["allowed_terminal_actions"]
        )
        if normalized not in allowed_actions:
            raise PromotionGateError(
                f"promotion_reversal_action_not_allowed:{normalized}"
            )
        allowed_reasons = set(
            self.contract["reversibility"]["reason_codes"]
        )
        reasons = sorted({_text(x, "reason_code") for x in reason_codes})
        if not reasons:
            raise PromotionGateError(
                "promotion_reversal_reason_codes_required"
            )
        if any(x not in allowed_reasons for x in reasons):
            raise PromotionGateError(
                "promotion_reversal_reason_code_not_allowed"
            )
        event = self._append(
            event_type="PROMOTION_STATE_REVERSED",
            identity=pattern_identity,
            payload_key="reversal",
            payload={
                "action": normalized,
                "reason_codes": reasons,
                "evidence_hash": _sha(evidence_hash, "evidence_hash"),
            },
            recorded_at=observed_at,
            actor_id=actor_id,
            actor_role=actor_role,
        )
        return {
            "valid": True,
            "status": (
                "DEMOTED" if normalized == "DEMOTE" else "ROLLED_BACK"
            ),
            "event_id": event["event_id"],
            "entry_hash": event["entry_hash"],
        }

    def current_status(
        self,
        pattern_identity: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        value = self._replay(self._read()).get(self._key(pattern_identity))
        return dict(value) if value is not None else None

    def verify_integrity(self) -> dict[str, Any]:
        events = self._read()
        states = self._replay(events)
        return {
            "valid": True,
            "event_count": len(events),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {
                key: value["status"]
                for key, value in sorted(states.items())
            },
        }
