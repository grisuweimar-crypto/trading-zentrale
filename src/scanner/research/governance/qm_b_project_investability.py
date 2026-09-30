"""QM-B project-investability evidence evaluation.

Research-only.  Project membership, listing, market tradability and project
investability are deliberately separate.  Missing evidence fails closed to UNKNOWN;
no symbol, price, country, currency or scanner presence heuristic is accepted as a
substitute for an explicit required evidence dimension.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_b_identity_reconciliation import is_valid_isin


SCHEMA_VERSION = "qm_b_project_investability_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_project_investability_v1.json"


class ProjectInvestabilityError(ValueError):
    """Raised when project-investability evidence is malformed or unsafe."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProjectInvestabilityError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProjectInvestabilityError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProjectInvestabilityError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_investability_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectInvestabilityError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_project_investability_v1":
        raise ProjectInvestabilityError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProjectInvestabilityError("contract_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise ProjectInvestabilityError("execution_must_be_disabled")
    required = payload.get("required_dimensions")
    positive = payload.get("positive_requirements")
    if not isinstance(required, list) or not isinstance(positive, Mapping):
        raise ProjectInvestabilityError("contract_dimensions_invalid")
    if set(required) != set(positive):
        raise ProjectInvestabilityError("positive_requirements_dimension_mismatch")
    pit = payload.get("pit_rules")
    if not isinstance(pit, Mapping):
        raise ProjectInvestabilityError("pit_rules_missing")
    if pit.get("historical_retrojection_permitted") is not False or pit.get("absence_is_negative_evidence") is not False:
        raise ProjectInvestabilityError("unsafe_pit_rule")
    return payload


def _validate_evidence_record(record: Mapping[str, Any], contract: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    required = ["instrument_id", "dimension", "status", "valid_from", "pit_verified", "source_id"]
    missing = [field for field in required if field not in record]
    if missing:
        raise ProjectInvestabilityError(f"evidence_fields_missing:{index}:" + ",".join(missing))
    instrument_id = _clean(record.get("instrument_id"))
    dimension = _clean(record.get("dimension"))
    status = _clean(record.get("status"))
    source_id = _clean(record.get("source_id"))
    if not instrument_id or not source_id:
        raise ProjectInvestabilityError(f"evidence_identity_missing:{index}")
    if dimension not in contract["required_dimensions"]:
        raise ProjectInvestabilityError(f"evidence_dimension_invalid:{index}:{dimension}")
    allowed = set(contract["dimension_status_values"][dimension])
    if status not in allowed:
        raise ProjectInvestabilityError(f"evidence_status_invalid:{index}:{dimension}:{status}")
    if not isinstance(record.get("pit_verified"), bool):
        raise ProjectInvestabilityError(f"evidence_pit_boolean_required:{index}")
    start = _utc(record.get("valid_from"), field=f"evidence[{index}].valid_from")
    end_raw = record.get("valid_to")
    end = _utc(end_raw, field=f"evidence[{index}].valid_to") if _clean(end_raw) else None
    if end is not None and end <= start:
        raise ProjectInvestabilityError(f"evidence_interval_invalid:{index}")
    return {
        "instrument_id": instrument_id,
        "dimension": dimension,
        "status": status,
        "valid_from": start,
        "valid_to": end,
        "pit_verified": bool(record["pit_verified"]),
        "source_id": source_id,
        "reason_codes": list(record.get("reason_codes") or []),
    }


def resolve_dimensions(
    *,
    instrument_id: str,
    as_of: str,
    evidence_records: Sequence[Mapping[str, Any]],
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_investability_contract(contract_path)
    when = _utc(as_of, field="as_of")
    iid = _clean(instrument_id)
    if not iid:
        raise ProjectInvestabilityError("instrument_id_required")

    validated = [_validate_evidence_record(record, contract, index=i) for i, record in enumerate(evidence_records)]
    dimensions: dict[str, dict[str, Any]] = {}
    for dimension in contract["required_dimensions"]:
        eligible = [
            row for row in validated
            if row["instrument_id"] == iid
            and row["dimension"] == dimension
            and row["pit_verified"] is True
            and row["valid_from"] <= when
            and (row["valid_to"] is None or when < row["valid_to"])
        ]
        if not eligible:
            dimensions[dimension] = {
                "resolved_status": "UNKNOWN",
                "evidence_valid_from": None,
                "source_ids": [],
                "reason_codes": ["NO_PIT_EVIDENCE_AS_OF"],
            }
            continue
        latest = max(row["valid_from"] for row in eligible)
        latest_rows = [row for row in eligible if row["valid_from"] == latest]
        statuses = sorted({row["status"] for row in latest_rows})
        sources = sorted({row["source_id"] for row in latest_rows})
        reasons = sorted({str(code) for row in latest_rows for code in row["reason_codes"]})
        if len(statuses) != 1:
            dimensions[dimension] = {
                "resolved_status": "CONFLICTING_EVIDENCE",
                "evidence_valid_from": latest.isoformat(),
                "source_ids": sources,
                "reason_codes": ["LATEST_EVIDENCE_CONFLICT"] + reasons,
            }
        else:
            dimensions[dimension] = {
                "resolved_status": statuses[0],
                "evidence_valid_from": latest.isoformat(),
                "source_ids": sources,
                "reason_codes": reasons,
            }
    return {"instrument_id": iid, "as_of": when.isoformat(), "dimensions": dimensions}


def evaluate_project_investability(
    *,
    instrument_id: str,
    as_of: str,
    evidence_records: Sequence[Mapping[str, Any]],
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_investability_contract(contract_path)
    resolved = resolve_dimensions(
        instrument_id=instrument_id,
        as_of=as_of,
        evidence_records=evidence_records,
        contract_path=contract_path,
    )
    dimensions = resolved["dimensions"]
    status_by_dimension = {name: row["resolved_status"] for name, row in dimensions.items()}

    restricted_hits: list[str] = []
    hard_negative_hits: list[str] = []
    missing_or_conflicting: list[str] = []
    for dimension in contract["required_dimensions"]:
        status = status_by_dimension[dimension]
        if status in set(contract.get("restricted_values", {}).get(dimension, [])):
            restricted_hits.append(dimension)
        if status in set(contract.get("hard_negative_values", {}).get(dimension, [])):
            hard_negative_hits.append(dimension)
        if status in {"UNKNOWN", "PARTIAL", "CONFLICTING", "CONFLICTING_EVIDENCE"}:
            missing_or_conflicting.append(dimension)

    # Explicit restriction is preserved distinctly. Explicit hard negatives then win
    # over missing dimensions. Otherwise incomplete evidence remains UNKNOWN.
    if restricted_hits:
        overall = "RESTRICTED"
        reasons = [f"RESTRICTED:{name}" for name in restricted_hits]
    elif hard_negative_hits:
        overall = "NOT_INVESTABLE"
        reasons = [f"HARD_NEGATIVE:{name}" for name in hard_negative_hits]
    elif missing_or_conflicting:
        overall = "UNKNOWN"
        reasons = [f"MISSING_OR_CONFLICTING:{name}" for name in missing_or_conflicting]
    else:
        mismatches = [
            name for name, expected in contract["positive_requirements"].items()
            if status_by_dimension[name] != expected
        ]
        if mismatches:
            overall = "UNKNOWN"
            reasons = [f"POSITIVE_REQUIREMENT_NOT_MET:{name}" for name in mismatches]
        else:
            overall = "INVESTABLE"
            reasons = ["ALL_REQUIRED_DIMENSIONS_PIT_VERIFIED_POSITIVE"]

    valid_from_values = [
        _utc(row["evidence_valid_from"], field=f"resolved.{name}.evidence_valid_from")
        for name, row in dimensions.items()
        if row.get("evidence_valid_from")
    ]
    investability_valid_from = max(valid_from_values).isoformat() if overall == "INVESTABLE" and len(valid_from_values) == len(contract["required_dimensions"]) else None

    return {
        "schema_version": "qm_b_project_investability_result_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "instrument_id": resolved["instrument_id"],
        "as_of": resolved["as_of"],
        "project_investability_status": overall,
        "investability_valid_from": investability_valid_from,
        "strict_universe_promotion_ready": overall == "INVESTABLE",
        "historical_retrojection_permitted": False,
        "reason_codes": reasons,
        "dimensions": dimensions,
    }


def _membership_identity_evidence(claim: Mapping[str, Any], *, snapshot_time: str, source_id: str) -> list[dict[str, Any]]:
    instrument_id = _clean(claim.get("instrument_id"))
    isin = _clean(claim.get("canonical_isin")).upper()
    membership_status = _clean(claim.get("membership_observation_status"))
    if not instrument_id:
        return []
    rows: list[dict[str, Any]] = []
    expected_id = f"urn:scanner:isin:{isin}" if isin else ""
    identity_status = "VERIFIED" if is_valid_isin(isin) and instrument_id == expected_id else "UNKNOWN"
    rows.append({
        "instrument_id": instrument_id,
        "dimension": "stable_identity",
        "status": identity_status,
        "valid_from": snapshot_time,
        "pit_verified": True,
        "source_id": source_id,
        "reason_codes": ["STABLE_ISIN_ID_IN_MEMBERSHIP_SNAPSHOT"] if identity_status == "VERIFIED" else ["MEMBERSHIP_IDENTITY_NOT_STABLE_ISIN"],
    })
    mapped_membership = "OBSERVED_IN_PROJECT_UNIVERSE" if membership_status == "OBSERVED_IN_PROJECT_UNIVERSE" else "UNKNOWN"
    rows.append({
        "instrument_id": instrument_id,
        "dimension": "project_membership",
        "status": mapped_membership,
        "valid_from": snapshot_time,
        "pit_verified": True,
        "source_id": source_id,
        "reason_codes": ["PROSPECTIVE_MEMBERSHIP_SNAPSHOT"],
    })
    return rows


def audit_membership_snapshot(
    membership_snapshot: Mapping[str, Any],
    *,
    additional_evidence: Sequence[Mapping[str, Any]] = (),
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_investability_contract(contract_path)
    if membership_snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise ProjectInvestabilityError("membership_snapshot_schema_invalid")
    if membership_snapshot.get("historical_retrojection_permitted") is not False:
        raise ProjectInvestabilityError("membership_snapshot_retrojection_guard_missing")
    claims = membership_snapshot.get("claims")
    if not isinstance(claims, list):
        raise ProjectInvestabilityError("membership_claims_must_be_list")
    snapshot_time = _utc(membership_snapshot.get("membership_valid_from"), field="membership_valid_from").isoformat()
    source_id = _clean(membership_snapshot.get("membership_snapshot_id"))
    if not source_id:
        raise ProjectInvestabilityError("membership_snapshot_id_required")

    results: list[dict[str, Any]] = []
    gap_counts = {dimension: 0 for dimension in contract["required_dimensions"]}
    status_counts: dict[str, int] = {}
    for claim in claims:
        if not isinstance(claim, Mapping):
            raise ProjectInvestabilityError("membership_claim_not_object")
        instrument_id = _clean(claim.get("instrument_id"))
        if not instrument_id:
            raise ProjectInvestabilityError("membership_claim_instrument_id_required")
        evidence = _membership_identity_evidence(claim, snapshot_time=snapshot_time, source_id=source_id)
        evidence.extend(dict(row) for row in additional_evidence if _clean(row.get("instrument_id")) == instrument_id)
        result = evaluate_project_investability(
            instrument_id=instrument_id,
            as_of=snapshot_time,
            evidence_records=evidence,
            contract_path=contract_path,
        )
        status = result["project_investability_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        for dimension, row in result["dimensions"].items():
            if row["resolved_status"] in {"UNKNOWN", "PARTIAL", "CONFLICTING", "CONFLICTING_EVIDENCE"}:
                gap_counts[dimension] += 1
        results.append(result)

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": source_id,
        "as_of": snapshot_time,
        "instrument_count": len(results),
        "status_counts": dict(sorted(status_counts.items())),
        "evidence_gap_counts": gap_counts,
        "strict_universe_promotion_ready_count": sum(1 for row in results if row["strict_universe_promotion_ready"]),
        "historical_retrojection_permitted": False,
        "results": results,
    }
