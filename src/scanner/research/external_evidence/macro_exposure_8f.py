from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Mapping


MACRO_SCHEMA_VERSION = "external_evidence_8f_macro_observation_v1"
CONTEXT_SCHEMA_VERSION = "external_evidence_8f_macro_context_v1"
CONTRACT_SCHEMA_VERSION = "external_evidence_8f_macro_exposure_v1"
EXPOSURE_MAP_SCHEMA_VERSION = "external_evidence_8f_exposure_map_v1"


class MacroExposure8FError(ValueError):
    pass


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise MacroExposure8FError(f"missing {field}")
    return value


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise MacroExposure8FError(f"missing {field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise MacroExposure8FError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MacroExposure8FError(f"{field} must be timezone-aware: {value!r}")
    return parsed


def _date(value: Any, *, field: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise MacroExposure8FError(f"missing {field}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise MacroExposure8FError(f"invalid {field}: {value!r}") from exc


def _sha256(value: Any, *, field: str) -> str:
    text = _required_text({field: value}, field).lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise MacroExposure8FError(f"{field} must be a 64-character hex SHA-256 digest")
    return text


def _forbid_semantic_shortcuts(row: Mapping[str, Any]) -> None:
    forbidden = {
        "direction",
        "market_direction",
        "bullish",
        "bearish",
        "weight",
        "score_weight",
        "threshold",
        "outcome",
        "forward_return",
        "phase7_stance",
        "portfolio_action",
    }
    present = sorted(key for key in forbidden if key in row and row.get(key) not in (None, ""))
    if present:
        raise MacroExposure8FError(
            "Phase 8F foundation forbids direction/outcome/weight/threshold fields: " + ", ".join(present)
        )


def _historical_vintage_safe_floor(realtime_start: date) -> datetime:
    return datetime.combine(realtime_start + timedelta(days=1), time.min, tzinfo=timezone.utc)


def validate_macro_observation(
    row: Mapping[str, Any],
    *,
    allowed_series_ids: set[str] | None = None,
) -> dict[str, Any]:
    _forbid_semantic_shortcuts(row)

    series_id = _required_text(row, "series_id")
    if allowed_series_ids is not None and series_id not in allowed_series_ids:
        raise MacroExposure8FError(f"series_id is not enabled by the 8F contract: {series_id}")

    observation_date = _date(row.get("observation_date"), field="observation_date")
    realtime_start = _date(row.get("realtime_start"), field="realtime_start")
    realtime_end = _date(row.get("realtime_end"), field="realtime_end")
    if realtime_end < realtime_start:
        raise MacroExposure8FError("realtime_end cannot precede realtime_start")

    ingested_at = _aware_datetime(row.get("ingested_at"), field="ingested_at")
    valid_from = _aware_datetime(row.get("valid_from"), field="valid_from")
    historical_proof = row.get("historical_vintage_independently_proven") is True
    exact_publication_proof = row.get("historical_publication_time_independently_proven") is True
    published_at_raw = row.get("published_at")
    published_at = None if published_at_raw in (None, "") else _aware_datetime(published_at_raw, field="published_at")

    if exact_publication_proof:
        if not historical_proof:
            raise MacroExposure8FError(
                "exact historical publication-time proof requires historical vintage proof"
            )
        if published_at is None:
            raise MacroExposure8FError("exact historical publication-time proof requires published_at")
        if published_at.date() != realtime_start:
            raise MacroExposure8FError(
                "published_at local calendar date must match realtime_start for exact release proof"
            )
        if published_at > ingested_at:
            raise MacroExposure8FError("published_at cannot be later than ingested_at")
        if valid_from < published_at:
            raise MacroExposure8FError(
                "valid_from cannot precede independently proven exact publication time"
            )
    elif published_at is not None:
        raise MacroExposure8FError(
            "published_at may only be supplied when exact historical publication-time proof is enabled"
        )
    elif historical_proof:
        safe_floor = _historical_vintage_safe_floor(realtime_start)
        if valid_from < safe_floor:
            raise MacroExposure8FError(
                "historical day-level vintage cannot become usable before the next UTC day"
            )
    elif valid_from < ingested_at:
        raise MacroExposure8FError(
            "valid_from cannot precede actual ingestion without independent historical vintage proof"
        )

    status = _required_text(row, "status").upper()
    if status not in {"KNOWN", "UNKNOWN", "STALE", "LICENSED_OUT", "LOW_COVERAGE"}:
        raise MacroExposure8FError(f"unsupported macro observation status: {status}")

    raw_value = row.get("value")
    value: float | None
    if status == "KNOWN":
        if raw_value in (None, ""):
            raise MacroExposure8FError("KNOWN macro observation requires a numeric value")
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise MacroExposure8FError("macro observation value must be numeric") from exc
        if not math.isfinite(value):
            raise MacroExposure8FError("macro observation value must be finite")
    else:
        if raw_value not in (None, ""):
            raise MacroExposure8FError("non-KNOWN macro observation must not carry a numeric value")
        value = None

    availability_proof_type = str(row.get("availability_proof_type") or "").strip().upper() or None
    if exact_publication_proof and availability_proof_type != "ARCHIVED_RELEASE_EXACT_TIMESTAMP":
        raise MacroExposure8FError(
            "exact historical publication-time proof requires ARCHIVED_RELEASE_EXACT_TIMESTAMP"
        )

    return {
        "schema_version": MACRO_SCHEMA_VERSION,
        "series_id": series_id,
        "factor_id": _required_text(row, "factor_id"),
        "observation_date": observation_date,
        "value": value,
        "units": _required_text(row, "units"),
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "revision_id": _required_text(row, "revision_id"),
        "source_id": _required_text(row, "source_id"),
        "source_record_sha256": _sha256(row.get("source_record_sha256"), field="source_record_sha256"),
        "license_status": _required_text(row, "license_status").upper(),
        "status": status,
        "ingested_at": ingested_at,
        "valid_from": valid_from,
        "historical_vintage_independently_proven": historical_proof,
        "historical_publication_time_independently_proven": exact_publication_proof,
        "published_at": published_at,
        "availability_proof_type": availability_proof_type,
    }


def validate_exposure_mapping(
    row: Mapping[str, Any],
    *,
    allowed_factor_ids: set[str],
) -> dict[str, Any]:
    _forbid_semantic_shortcuts(row)

    factor_id = _required_text(row, "factor_id")
    if factor_id not in allowed_factor_ids:
        raise MacroExposure8FError(f"unsupported factor_id: {factor_id}")

    human_reviewed = row.get("human_reviewed") is True
    if not human_reviewed:
        raise MacroExposure8FError("exposure mapping must be explicitly human-reviewed")

    reviewed_at = _aware_datetime(row.get("reviewed_at"), field="reviewed_at")
    evidence_valid_from = _aware_datetime(row.get("evidence_valid_from"), field="evidence_valid_from")
    valid_from = _aware_datetime(row.get("valid_from"), field="valid_from")
    if valid_from < reviewed_at:
        raise MacroExposure8FError("exposure mapping may not be retrojected before human review")
    if valid_from < evidence_valid_from:
        raise MacroExposure8FError("exposure mapping may not predate its documentary evidence")

    valid_to_raw = row.get("valid_to")
    valid_to = None if valid_to_raw in (None, "") else _aware_datetime(valid_to_raw, field="valid_to")
    if valid_to is not None and valid_to <= valid_from:
        raise MacroExposure8FError("valid_to must be later than valid_from")

    relationship_class = _required_text(row, "relationship_class").upper()
    allowed_relationships = {
        "REVENUE_LINK",
        "INPUT_COST_LINK",
        "FINANCING_SENSITIVITY",
        "CURRENCY_TRANSLATION",
        "BALANCE_SHEET_LINK",
        "OTHER_DOCUMENTED",
    }
    if relationship_class not in allowed_relationships:
        raise MacroExposure8FError(f"unsupported relationship_class: {relationship_class}")

    review_status = _required_text(row, "review_status").upper()
    if review_status not in {"ACTIVE", "RETIRED"}:
        raise MacroExposure8FError(f"unsupported review_status: {review_status}")

    return {
        "mapping_id": _required_text(row, "mapping_id"),
        "map_version": _required_text(row, "map_version"),
        "subject_id": _required_text(row, "subject_id"),
        "factor_id": factor_id,
        "relationship_class": relationship_class,
        "evidence_type": _required_text(row, "evidence_type").upper(),
        "evidence_reference": _required_text(row, "evidence_reference"),
        "evidence_sha256": _sha256(row.get("evidence_sha256"), field="evidence_sha256"),
        "evidence_valid_from": evidence_valid_from,
        "human_reviewed": True,
        "reviewed_at": reviewed_at,
        "review_status": review_status,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value


def _latest_available_observations(
    rows: Iterable[dict[str, Any]],
    *,
    factor_id: str,
    as_of: datetime,
) -> list[dict[str, Any]]:
    available = [
        row
        for row in rows
        if row["factor_id"] == factor_id
        and row["status"] == "KNOWN"
        and row["valid_from"] <= as_of
        and row["observation_date"] <= as_of.date()
    ]
    if not available:
        return []

    by_series: dict[str, list[dict[str, Any]]] = {}
    for row in available:
        by_series.setdefault(row["series_id"], []).append(row)

    selected: list[dict[str, Any]] = []
    for series_id in sorted(by_series):
        series_rows = by_series[series_id]
        latest_observation_date = max(row["observation_date"] for row in series_rows)
        same_observation = [
            row for row in series_rows if row["observation_date"] == latest_observation_date
        ]
        selected.append(
            max(
                same_observation,
                key=lambda row: (row["valid_from"], row["realtime_start"], row["revision_id"]),
            )
        )
    return selected


def build_macro_context(
    *,
    observations: Iterable[Mapping[str, Any]],
    mappings: Iterable[Mapping[str, Any]],
    as_of: datetime,
    allowed_series_ids: set[str],
    allowed_factor_ids: set[str],
) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise MacroExposure8FError("as_of must be timezone-aware")

    normalized_observations = [
        validate_macro_observation(row, allowed_series_ids=allowed_series_ids) for row in observations
    ]
    normalized_mappings = [
        validate_exposure_mapping(row, allowed_factor_ids=allowed_factor_ids) for row in mappings
    ]

    active_mappings = [
        row
        for row in normalized_mappings
        if row["review_status"] == "ACTIVE"
        and row["valid_from"] <= as_of
        and (row["valid_to"] is None or as_of < row["valid_to"])
    ]

    contexts: list[dict[str, Any]] = []
    for mapping in sorted(active_mappings, key=lambda row: (row["subject_id"], row["factor_id"], row["mapping_id"])):
        factor_observations = _latest_available_observations(
            normalized_observations,
            factor_id=mapping["factor_id"],
            as_of=as_of,
        )
        contexts.append(
            {
                "subject_id": mapping["subject_id"],
                "factor_id": mapping["factor_id"],
                "relationship_class": mapping["relationship_class"],
                "mapping_id": mapping["mapping_id"],
                "mapping_valid_from": mapping["valid_from"],
                "status": "KNOWN" if factor_observations else "UNKNOWN",
                "series_count": len(factor_observations),
                "macro_observations": factor_observations,
                "macro_observation": factor_observations[0] if len(factor_observations) == 1 else None,
            }
        )

    return _serialize(
        {
            "schema_version": CONTEXT_SCHEMA_VERSION,
            "phase": "8F_macro_exposure_context",
            "status": "OUTCOME_BLIND_CONTEXT_ONLY",
            "as_of": as_of,
            "context_count": len(contexts),
            "contexts": contexts,
            "guards": {
                "market_outcomes_read": False,
                "market_direction_assigned": False,
                "exposure_sign_assigned": False,
                "weights_selected": False,
                "thresholds_selected": False,
                "interaction_research_enabled": False,
                "phase7_integration_enabled": False,
                "production_external_evidence_enabled": False,
                "single_series_silently_selected_from_multiseries_factor": False,
            },
        }
    )


def validate_phase8f_contract(
    macro_config: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
) -> dict[str, Any]:
    if macro_config.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        raise MacroExposure8FError("unexpected Phase 8F macro contract schema_version")
    if exposure_map.get("schema_version") != EXPOSURE_MAP_SCHEMA_VERSION:
        raise MacroExposure8FError("unexpected Phase 8F exposure map schema_version")

    principles = macro_config.get("principles") or {}
    required_false = {
        "market_outcomes_may_be_read",
        "market_direction_may_be_assigned",
        "exposure_sign_may_be_assigned",
        "threshold_selection_enabled",
        "interaction_research_enabled",
        "phase7_integration_enabled",
        "production_external_evidence_enabled",
    }
    wrong = sorted(key for key in required_false if principles.get(key) is not False)
    if wrong:
        raise MacroExposure8FError("Phase 8F hard guards must remain false: " + ", ".join(wrong))

    vintage_contract = macro_config.get("macro_vintage_contract", {})
    if vintage_contract.get("day_level_historical_vintage_valid_from") != "NEXT_UTC_DAY":
        raise MacroExposure8FError("day-level historical macro vintage must fail closed to NEXT_UTC_DAY")
    if vintage_contract.get("exact_timestamp_historical_vintage_valid_from") != "PROVEN_PUBLISHED_AT":
        raise MacroExposure8FError(
            "exact-timestamp historical macro vintage must use independently proven published_at"
        )
    if vintage_contract.get("later_revision_overwrites_original") is not False:
        raise MacroExposure8FError("later macro revisions may not overwrite original vintages")

    factors = macro_config.get("factor_catalog") or []
    factor_ids = [str(item.get("factor_id") or "") for item in factors]
    if not factor_ids or any(not value for value in factor_ids) or len(factor_ids) != len(set(factor_ids)):
        raise MacroExposure8FError("factor_catalog must contain unique non-empty factor_id values")

    if exposure_map.get("map_version") in (None, ""):
        raise MacroExposure8FError("exposure map requires map_version")
    if exposure_map.get("retroactive_mapping_allowed") is not False:
        raise MacroExposure8FError("exposure map must forbid retroactive mapping")
    if exposure_map.get("undocumented_mapping_allowed") is not False:
        raise MacroExposure8FError("exposure map must forbid undocumented mapping")

    mappings = exposure_map.get("mappings") or []
    for mapping in mappings:
        validate_exposure_mapping(mapping, allowed_factor_ids=set(factor_ids))

    return {
        "schema_version": "external_evidence_8f_contract_gate_v1",
        "phase": "8F_macro_exposure_context",
        "status": "PASS_FOUNDATION_CONTRACT",
        "factor_count": len(factor_ids),
        "mapping_count": len(mappings),
        "outcomes_read": False,
        "direction_assigned": False,
        "phase7_integration_enabled": False,
        "production_external_evidence_enabled": False,
    }
