"""QM-B market tradability evidence evaluation.

Research-only. Tradability is venue-specific and must not be inferred from listing,
price history, scanner observations, provider quote success or symbol suffixes.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "qm_b_market_tradability_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_market_tradability_v1.json"


class MarketTradabilityError(ValueError):
    """Raised when market-tradability evidence is malformed or unsafe."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise MarketTradabilityError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise MarketTradabilityError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise MarketTradabilityError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_tradability_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MarketTradabilityError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_market_tradability_v1":
        raise MarketTradabilityError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise MarketTradabilityError("contract_scope_invalid")
    pit = payload.get("pit_rules")
    if not isinstance(pit, Mapping):
        raise MarketTradabilityError("pit_rules_missing")
    if pit.get("historical_retrojection_permitted") is not False:
        raise MarketTradabilityError("retrojection_must_be_forbidden")
    if pit.get("absence_of_event_is_positive_evidence") is not False:
        raise MarketTradabilityError("absence_positive_inference_must_be_forbidden")
    aggregation = payload.get("aggregation")
    if not isinstance(aggregation, Mapping):
        raise MarketTradabilityError("aggregation_missing")
    return payload


def _registered_source_ids(contract: Mapping[str, Any]) -> set[str]:
    values = contract.get("registered_sources") or []
    ids: set[str] = set()
    for value in values:
        if isinstance(value, str):
            source_id = _clean(value)
        elif isinstance(value, Mapping):
            source_id = _clean(value.get("source_id"))
        else:
            raise MarketTradabilityError("registered_source_invalid")
        if not source_id:
            raise MarketTradabilityError("registered_source_id_required")
        ids.add(source_id)
    return ids


def _validate_record(record: Mapping[str, Any], contract: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    required = contract.get("required_evidence_fields") or []
    missing = [field for field in required if field not in record]
    if missing:
        raise MarketTradabilityError(f"evidence_fields_missing:{index}:" + ",".join(missing))
    instrument_id = _clean(record.get("instrument_id"))
    venue_id = _clean(record.get("venue_id"))
    source_id = _clean(record.get("source_id"))
    status = _clean(record.get("status"))
    if not instrument_id or not venue_id or not source_id:
        raise MarketTradabilityError(f"evidence_identity_missing:{index}")
    if status not in set(contract.get("status_values") or []):
        raise MarketTradabilityError(f"evidence_status_invalid:{index}:{status}")
    if source_id not in _registered_source_ids(contract):
        raise MarketTradabilityError(f"evidence_source_not_registered:{index}:{source_id}")
    if not isinstance(record.get("pit_verified"), bool):
        raise MarketTradabilityError(f"evidence_pit_boolean_required:{index}")
    start = _utc(record.get("valid_from"), field=f"evidence[{index}].valid_from")
    end_raw = _clean(record.get("valid_to"))
    end = _utc(end_raw, field=f"evidence[{index}].valid_to") if end_raw else None
    if end is not None and end <= start:
        raise MarketTradabilityError(f"evidence_interval_invalid:{index}")
    return {
        "instrument_id": instrument_id,
        "venue_id": venue_id,
        "source_id": source_id,
        "status": status,
        "valid_from": start,
        "valid_to": end,
        "pit_verified": bool(record["pit_verified"]),
        "reason_codes": list(record.get("reason_codes") or []),
    }


def evaluate_market_tradability(
    *,
    instrument_id: str,
    as_of: str,
    evidence_records: Sequence[Mapping[str, Any]],
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_tradability_contract(contract_path)
    iid = _clean(instrument_id)
    if not iid:
        raise MarketTradabilityError("instrument_id_required")
    when = _utc(as_of, field="as_of")
    validated = [_validate_record(row, contract, index=i) for i, row in enumerate(evidence_records)]
    relevant = [
        row for row in validated
        if row["instrument_id"] == iid
        and row["pit_verified"] is True
        and row["valid_from"] <= when
        and (row["valid_to"] is None or when < row["valid_to"])
    ]

    venues: dict[str, dict[str, Any]] = {}
    for venue_id in sorted({row["venue_id"] for row in relevant}):
        venue_rows = [row for row in relevant if row["venue_id"] == venue_id]
        latest_time = max(row["valid_from"] for row in venue_rows)
        latest = [row for row in venue_rows if row["valid_from"] == latest_time]
        statuses = sorted({row["status"] for row in latest})
        sources = sorted({row["source_id"] for row in latest})
        reasons = sorted({str(code) for row in latest for code in row["reason_codes"]})
        if len(statuses) != 1:
            venues[venue_id] = {
                "resolved_status": "UNKNOWN",
                "evidence_valid_from": latest_time.isoformat(),
                "source_ids": sources,
                "reason_codes": ["LATEST_SAME_VENUE_CONFLICT"] + reasons,
            }
        else:
            venues[venue_id] = {
                "resolved_status": statuses[0],
                "evidence_valid_from": latest_time.isoformat(),
                "source_ids": sources,
                "reason_codes": reasons,
            }

    if not venues:
        overall = "UNKNOWN"
        reason_codes = ["NO_PIT_MARKET_TRADABILITY_EVIDENCE_AS_OF"]
    else:
        states = [row["resolved_status"] for row in venues.values()]
        if "TRADABLE" in states:
            overall = "TRADABLE"
            reason_codes = ["AT_LEAST_ONE_VENUE_PIT_VERIFIED_TRADABLE"]
        elif "SUSPENDED" in states:
            overall = "SUSPENDED"
            reason_codes = ["NO_TRADABLE_VENUE_AND_AT_LEAST_ONE_SUSPENDED"]
        elif all(state == "NOT_TRADABLE" for state in states):
            overall = "NOT_TRADABLE"
            reason_codes = ["ALL_OBSERVED_VENUES_NOT_TRADABLE"]
        else:
            overall = "UNKNOWN"
            reason_codes = ["VENUE_STATE_INCOMPLETE_OR_CONFLICTING"]

    valid_from_values = [
        _utc(row["evidence_valid_from"], field=f"venue.{venue}.evidence_valid_from")
        for venue, row in venues.items()
        if row.get("evidence_valid_from") and row["resolved_status"] == "TRADABLE"
    ]
    tradability_valid_from = max(valid_from_values).isoformat() if overall == "TRADABLE" and valid_from_values else None
    return {
        "schema_version": "qm_b_market_tradability_result_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "instrument_id": iid,
        "as_of": when.isoformat(),
        "market_tradability_status": overall,
        "tradability_valid_from": tradability_valid_from,
        "strict_dimension_positive": overall == "TRADABLE",
        "historical_retrojection_permitted": False,
        "reason_codes": reason_codes,
        "venues": venues,
    }


def audit_membership_snapshot(
    membership_snapshot: Mapping[str, Any],
    *,
    evidence_records: Sequence[Mapping[str, Any]] = (),
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_tradability_contract(contract_path)
    if membership_snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise MarketTradabilityError("membership_snapshot_schema_invalid")
    claims = membership_snapshot.get("claims")
    if not isinstance(claims, list):
        raise MarketTradabilityError("membership_claims_must_be_list")
    as_of = _utc(membership_snapshot.get("membership_valid_from"), field="membership_valid_from").isoformat()
    results: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for claim in claims:
        if not isinstance(claim, Mapping):
            raise MarketTradabilityError("membership_claim_not_object")
        instrument_id = _clean(claim.get("instrument_id"))
        if not instrument_id:
            raise MarketTradabilityError("membership_claim_instrument_id_required")
        subset = [row for row in evidence_records if _clean(row.get("instrument_id")) == instrument_id]
        result = evaluate_market_tradability(
            instrument_id=instrument_id,
            as_of=as_of,
            evidence_records=subset,
            contract_path=contract_path,
        )
        status = result["market_tradability_status"]
        counts[status] = counts.get(status, 0) + 1
        results.append(result)
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": membership_snapshot.get("membership_snapshot_id"),
        "as_of": as_of,
        "instrument_count": len(results),
        "registered_source_count": len(_registered_source_ids(contract)),
        "status_counts": dict(sorted(counts.items())),
        "positive_tradability_count": sum(1 for row in results if row["strict_dimension_positive"]),
        "current_gap_status": contract["current_repository_assessment"]["current_gap_status"],
        "historical_retrojection_permitted": False,
        "results": results,
    }
