from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from scanner.research.external_evidence.macro_exposure_8f import validate_macro_observation


SCHEMA_VERSION = "external_evidence_8f_macro_ledger_v1"


class MacroLedger8FError(ValueError):
    pass


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


def observation_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row["source_id"]),
        str(row["series_id"]),
        str(row["observation_date"]),
        str(row["revision_id"]),
    )


def _semantic_fingerprint(row: Mapping[str, Any]) -> tuple[Any, ...]:
    fields = (
        "factor_id",
        "value",
        "units",
        "realtime_start",
        "realtime_end",
        "source_record_sha256",
        "license_status",
        "status",
        "ingested_at",
        "valid_from",
        "historical_vintage_independently_proven",
        "historical_publication_time_independently_proven",
        "published_at",
        "availability_proof_type",
    )
    return tuple(str(row.get(field)) for field in fields)


def merge_macro_observations_append_only(
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    new_rows: Iterable[Mapping[str, Any]],
    allowed_series_ids: set[str],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for raw in [*existing_rows, *new_rows]:
        row = validate_macro_observation(raw, allowed_series_ids=allowed_series_ids)
        identity = observation_identity(row)
        previous = merged.get(identity)
        if previous is not None:
            if _semantic_fingerprint(previous) != _semantic_fingerprint(row):
                raise MacroLedger8FError(
                    "append-only macro ledger identity collision with changed content: "
                    + "|".join(identity)
                )
            continue
        merged[identity] = row

    return sorted(
        merged.values(),
        key=lambda row: (
            row["valid_from"],
            row["source_id"],
            row["series_id"],
            row["observation_date"],
            row["revision_id"],
        ),
    )


def build_macro_coverage_audit(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    normalized = list(rows)
    factor_counts = Counter(str(row["factor_id"]) for row in normalized)
    source_counts = Counter(str(row["source_id"]) for row in normalized)
    status_counts = Counter(str(row["status"]) for row in normalized)
    proof_counts = Counter(str(row.get("availability_proof_type") or "UNSPECIFIED") for row in normalized)

    by_series: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in normalized:
        by_series[str(row["series_id"])].append(row)

    series_coverage: list[dict[str, Any]] = []
    for series_id, series_rows in sorted(by_series.items()):
        observation_dates = [row["observation_date"] for row in series_rows]
        valid_froms = [row["valid_from"] for row in series_rows]
        known_count = sum(1 for row in series_rows if str(row["status"]) == "KNOWN")
        exact_historical_count = sum(
            1
            for row in series_rows
            if row.get("historical_publication_time_independently_proven") is True
        )
        prospective_count = sum(
            1
            for row in series_rows
            if str(row.get("availability_proof_type") or "") == "ACTUAL_PROSPECTIVE_INGESTION"
        )
        series_coverage.append(
            {
                "series_id": series_id,
                "factor_id": str(series_rows[0]["factor_id"]),
                "source_id": str(series_rows[0]["source_id"]),
                "row_count": len(series_rows),
                "known_count": known_count,
                "missing_or_nonknown_count": len(series_rows) - known_count,
                "first_observation_date": min(observation_dates).isoformat(),
                "last_observation_date": max(observation_dates).isoformat(),
                "first_valid_from": min(valid_froms).isoformat(),
                "last_valid_from": max(valid_froms).isoformat(),
                "exact_historical_release_rows": exact_historical_count,
                "prospective_ingestion_rows": prospective_count,
            }
        )

    return {
        "row_count": len(normalized),
        "series_count": len(by_series),
        "factor_counts": dict(sorted(factor_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "availability_proof_counts": dict(sorted(proof_counts.items())),
        "series_coverage": series_coverage,
    }


def build_macro_ledger(
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    new_rows: Iterable[Mapping[str, Any]],
    allowed_series_ids: set[str],
    as_of: datetime,
) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise MacroLedger8FError("as_of must be timezone-aware")

    merged = merge_macro_observations_append_only(
        existing_rows=existing_rows,
        new_rows=new_rows,
        allowed_series_ids=allowed_series_ids,
    )
    knowable = [row for row in merged if row["valid_from"] <= as_of]
    future = [row for row in merged if row["valid_from"] > as_of]

    return _serialize(
        {
            "schema_version": SCHEMA_VERSION,
            "phase": "8F_macro_exposure_context",
            "status": "OUTCOME_BLIND_APPEND_ONLY_MACRO_LEDGER",
            "as_of": as_of,
            "row_count": len(merged),
            "knowable_row_count": len(knowable),
            "future_row_count": len(future),
            "observations": merged,
            "coverage": build_macro_coverage_audit(knowable),
            "guards": {
                "append_only_identity_enforced": True,
                "later_revision_overwrites_original": False,
                "future_valid_from_excluded_from_asof_coverage": True,
                "market_outcomes_read": False,
                "market_direction_assigned": False,
                "threshold_selection_run": False,
                "phase7_integration_enabled": False,
                "production_external_evidence_enabled": False,
            },
        }
    )
