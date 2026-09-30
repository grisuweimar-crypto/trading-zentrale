"""QM-B execution-channel evidence.

Research-only.  This layer evaluates whether at least one registered execution
channel has explicit PIT instrument-level availability evidence.  It stores no
credentials or personal account identifiers and cannot place orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "qm_b_execution_channel_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_execution_channel_v1.json"


class ExecutionChannelError(ValueError):
    """Raised when execution-channel evidence is malformed or unsafe."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ExecutionChannelError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExecutionChannelError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ExecutionChannelError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_execution_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExecutionChannelError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_execution_channel_v1":
        raise ExecutionChannelError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ExecutionChannelError("contract_scope_invalid")
    if payload.get("order_execution_enabled") is not False:
        raise ExecutionChannelError("order_execution_must_be_disabled")
    if payload.get("pit_rules", {}).get("historical_retrojection_permitted") is not False:
        raise ExecutionChannelError("retrojection_guard_missing")
    return payload


def _validate_record(record: Mapping[str, Any], contract: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    forbidden = [field for field in contract["forbidden_evidence_fields"] if field in record]
    if forbidden:
        raise ExecutionChannelError(f"forbidden_sensitive_fields:{index}:" + ",".join(sorted(forbidden)))
    missing = [field for field in contract["required_evidence_fields"] if field not in record]
    if missing:
        raise ExecutionChannelError(f"evidence_fields_missing:{index}:" + ",".join(sorted(missing)))
    instrument_id = _clean(record.get("instrument_id"))
    channel_id = _clean(record.get("channel_id"))
    source_id = _clean(record.get("source_id"))
    status = _clean(record.get("status"))
    if not instrument_id or not channel_id or not source_id:
        raise ExecutionChannelError(f"evidence_identity_missing:{index}")
    if status not in set(contract["status_values"]):
        raise ExecutionChannelError(f"evidence_status_invalid:{index}:{status}")
    if not isinstance(record.get("pit_verified"), bool):
        raise ExecutionChannelError(f"pit_verified_boolean_required:{index}")
    start = _utc(record.get("valid_from"), field=f"evidence[{index}].valid_from")
    end_raw = record.get("valid_to")
    end = _utc(end_raw, field=f"evidence[{index}].valid_to") if _clean(end_raw) else None
    if end is not None and end <= start:
        raise ExecutionChannelError(f"evidence_interval_invalid:{index}")
    return {
        "instrument_id": instrument_id,
        "channel_id": channel_id,
        "source_id": source_id,
        "status": status,
        "pit_verified": record["pit_verified"],
        "valid_from": start,
        "valid_to": end,
        "reason_codes": list(record.get("reason_codes") or []),
    }


def execution_status_as_of(
    *,
    instrument_id: str,
    as_of: str,
    evidence_records: Sequence[Mapping[str, Any]],
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_execution_contract(contract_path)
    iid = _clean(instrument_id)
    if not iid:
        raise ExecutionChannelError("instrument_id_required")
    when = _utc(as_of, field="as_of")
    rows = [_validate_record(row, contract, index=i) for i, row in enumerate(evidence_records)]
    eligible = [
        row for row in rows
        if row["instrument_id"] == iid
        and row["pit_verified"] is True
        and row["valid_from"] <= when
        and (row["valid_to"] is None or when < row["valid_to"])
    ]
    if not eligible:
        return {
            "instrument_id": iid,
            "as_of": when.isoformat(),
            "execution_channel_status": "UNKNOWN",
            "available_channel_ids": [],
            "observed_channel_ids": [],
            "reason_codes": ["NO_PIT_EXECUTION_CHANNEL_EVIDENCE"],
            "evidence_valid_from": None,
            "historical_retrojection_permitted": False,
        }

    latest_by_channel: dict[str, list[dict[str, Any]]] = {}
    for row in eligible:
        latest_by_channel.setdefault(row["channel_id"], []).append(row)
    resolved: dict[str, str] = {}
    channel_times: dict[str, datetime] = {}
    conflicts: list[str] = []
    for channel_id, channel_rows in latest_by_channel.items():
        latest = max(row["valid_from"] for row in channel_rows)
        latest_rows = [row for row in channel_rows if row["valid_from"] == latest]
        statuses = sorted({row["status"] for row in latest_rows})
        channel_times[channel_id] = latest
        if len(statuses) != 1:
            resolved[channel_id] = "UNKNOWN"
            conflicts.append(channel_id)
        else:
            resolved[channel_id] = statuses[0]

    available = sorted(channel for channel, status in resolved.items() if status == "AVAILABLE")
    restricted = sorted(channel for channel, status in resolved.items() if status == "RESTRICTED")
    unavailable = sorted(channel for channel, status in resolved.items() if status == "UNAVAILABLE")
    if available:
        overall = "AVAILABLE"
        reasons = ["AT_LEAST_ONE_PIT_CHANNEL_AVAILABLE"]
    elif restricted:
        overall = "RESTRICTED"
        reasons = ["NO_AVAILABLE_CHANNEL_AND_AT_LEAST_ONE_RESTRICTED"]
    elif unavailable and len(unavailable) == len(resolved):
        overall = "UNAVAILABLE"
        reasons = ["ALL_OBSERVED_CHANNELS_UNAVAILABLE"]
    else:
        overall = "UNKNOWN"
        reasons = ["CHANNEL_EVIDENCE_INCOMPLETE_OR_CONFLICTING"]
    if conflicts:
        reasons.append("LATEST_SAME_CHANNEL_CONFLICT:" + ",".join(sorted(conflicts)))

    usable_times = [channel_times[channel] for channel in resolved]
    return {
        "instrument_id": iid,
        "as_of": when.isoformat(),
        "execution_channel_status": overall,
        "available_channel_ids": available,
        "observed_channel_ids": sorted(resolved),
        "resolved_channel_statuses": dict(sorted(resolved.items())),
        "reason_codes": reasons,
        "evidence_valid_from": max(usable_times).isoformat() if usable_times else None,
        "historical_retrojection_permitted": False,
    }


def audit_membership_execution_gap(
    membership_snapshot: Mapping[str, Any],
    *,
    as_of: str,
    evidence_records: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    if membership_snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise ExecutionChannelError("membership_snapshot_schema_invalid")
    claims = membership_snapshot.get("claims")
    if not isinstance(claims, list):
        raise ExecutionChannelError("membership_claims_must_be_list")
    results = [
        execution_status_as_of(
            instrument_id=_clean(claim.get("instrument_id")),
            as_of=as_of,
            evidence_records=evidence_records,
        )
        for claim in claims
    ]
    counts: dict[str, int] = {}
    for row in results:
        status = row["execution_channel_status"]
        counts[status] = counts.get(status, 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": _clean(membership_snapshot.get("membership_snapshot_id")),
        "as_of": _utc(as_of, field="as_of").isoformat(),
        "instrument_count": len(results),
        "status_counts": dict(sorted(counts.items())),
        "registered_channel_count": len(load_execution_contract().get("channel_registry") or []),
        "current_gap_status": "BLOCKED_NO_EXECUTION_CHANNEL_EVIDENCE" if counts.get("UNKNOWN", 0) == len(results) else "PARTIAL_OR_RESOLVED",
        "historical_retrojection_permitted": False,
        "results": results,
    }
