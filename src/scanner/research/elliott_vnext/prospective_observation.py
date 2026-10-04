from __future__ import annotations

"""Stage-4 prospective observation for Elliott vNext.

The activation itself ends after Stage 3.  This module implements the next
research phase requested by the project: derive a compact, research-only
observation report from already archived prospective Elliott 6H captures.

It does not alter Elliott counts, choose a degree, use market outcomes, create
review contexts, change Decision-Layer state, or perform promotion.
"""

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from scanner.research.decision_layer.phase6_elliott import (
    Elliott6HAdapterError,
    validate_elliott_6h_output,
)
from scanner.research.governance.qm_g_scenario_stability import (
    ScenarioStabilityError,
    build_scenario_stability_feature,
)


SCHEMA_VERSION = "elliott_vnext_prospective_observation_v1"
CAPTURE_SCHEMA_VERSION = "elliott_vnext_prospective_capture_v1"
MODULE = "elliott_vnext_stage4_prospective_observation"


class ProspectiveObservationError(ValueError):
    """Raised when Stage-4 observation cannot be derived without inference."""


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ProspectiveObservationError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProspectiveObservationError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProspectiveObservationError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def load_capture_archive(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        raise ProspectiveObservationError(f"capture_archive_missing:{target}")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(target.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProspectiveObservationError(
                f"capture_archive_invalid_json:{line_number}"
            ) from exc
        if not isinstance(row, dict):
            raise ProspectiveObservationError(
                f"capture_archive_record_not_object:{line_number}"
            )
        rows.append(row)
    return rows


def _normalize_capture(raw: Mapping[str, Any], index: int) -> dict[str, Any]:
    if raw.get("schema_version") != CAPTURE_SCHEMA_VERSION:
        raise ProspectiveObservationError(f"capture_schema_invalid:{index}")
    capture_id = str(raw.get("capture_id") or "").strip()
    snapshot_id = str(raw.get("snapshot_id") or "").strip()
    source_commit = str(raw.get("source_publication_commit") or "").strip().lower()
    if not capture_id or not snapshot_id:
        raise ProspectiveObservationError(f"capture_identity_missing:{index}")
    if len(source_commit) != 40 or any(ch not in "0123456789abcdef" for ch in source_commit):
        raise ProspectiveObservationError(f"capture_source_commit_invalid:{index}")
    captured_at = _timestamp(raw.get("captured_at"), f"capture[{index}].captured_at")
    if raw.get("validation_partition") != "prospective_unspent":
        raise ProspectiveObservationError(
            f"capture_not_prospective_unspent:{capture_id}"
        )
    guards = raw.get("guards")
    if not isinstance(guards, Mapping):
        raise ProspectiveObservationError(f"capture_guards_missing:{capture_id}")
    required_true = (
        "research_only",
        "missing_evidence_not_imputed",
        "multi_degree_outputs_retained_without_reducer",
    )
    required_false = (
        "productive_integration_enabled",
        "changes_universal_stance",
        "changes_portfolio_action",
        "direct_ordering_allowed",
        "future_rows_used",
        "frozen_elliott_core_modified",
    )
    for key in required_true:
        if guards.get(key) is not True:
            raise ProspectiveObservationError(
                f"capture_guard_must_be_true:{capture_id}:{key}"
            )
    for key in required_false:
        if guards.get(key) is not False:
            raise ProspectiveObservationError(
                f"capture_guard_must_be_false:{capture_id}:{key}"
            )

    outputs = raw.get("outputs")
    if not isinstance(outputs, list):
        raise ProspectiveObservationError(f"capture_outputs_missing:{capture_id}")
    normalized_outputs: list[dict[str, Any]] = []
    keys: set[tuple[str, str, str]] = set()
    for output_index, output_raw in enumerate(outputs):
        if not isinstance(output_raw, Mapping):
            raise ProspectiveObservationError(
                f"capture_output_not_object:{capture_id}:{output_index}"
            )
        try:
            output = validate_elliott_6h_output(output_raw)
        except Elliott6HAdapterError as exc:
            raise ProspectiveObservationError(
                f"capture_output_invalid:{capture_id}:{output_index}:{exc}"
            ) from exc
        key = (
            str(output.get("symbol") or ""),
            str(output.get("timeframe") or ""),
            str(output.get("degree") or ""),
        )
        if not all(key):
            raise ProspectiveObservationError(
                f"capture_output_identity_incomplete:{capture_id}:{output_index}"
            )
        if key in keys:
            raise ProspectiveObservationError(
                "capture_duplicate_dimension:" + capture_id + ":" + ":".join(key)
            )
        keys.add(key)
        normalized_outputs.append(dict(output))

    repair = raw.get("repair")
    supersedes: list[str] = []
    if isinstance(repair, Mapping):
        values = repair.get("supersedes_capture_ids")
        if isinstance(values, list):
            supersedes = sorted(
                {
                    str(value).strip()
                    for value in values
                    if str(value).strip()
                }
            )

    return {
        "capture_id": capture_id,
        "snapshot_id": snapshot_id,
        "source_commit": source_commit,
        "captured_at": captured_at,
        "outputs": normalized_outputs,
        "supersedes_capture_ids": supersedes,
    }


def _effective_captures(captures: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [_normalize_capture(raw, index) for index, raw in enumerate(captures)]
    seen_ids: set[str] = set()
    for row in normalized:
        capture_id = row["capture_id"]
        if capture_id in seen_ids:
            raise ProspectiveObservationError(f"duplicate_capture_id:{capture_id}")
        seen_ids.add(capture_id)

    superseded = {
        capture_id
        for row in normalized
        for capture_id in row["supersedes_capture_ids"]
    }
    effective = [row for row in normalized if row["capture_id"] not in superseded]
    effective.sort(key=lambda row: (row["captured_at"], row["capture_id"]))
    previous: datetime | None = None
    for row in effective:
        if previous is not None and row["captured_at"] <= previous:
            raise ProspectiveObservationError(
                "effective_capture_availability_must_be_strictly_increasing"
            )
        previous = row["captured_at"]
    return effective


def build_prospective_observation(
    captures: Iterable[Mapping[str, Any]],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Derive a compact Scenario-Stability report from prospective captures."""
    effective = _effective_captures(captures)
    generated = _timestamp(
        generated_at or datetime.now(timezone.utc).isoformat(),
        "generated_at",
    )

    dimensions: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    capture_by_output: dict[str, str] = {}
    for capture in effective:
        for output in capture["outputs"]:
            key = (
                str(output["symbol"]),
                str(output["timeframe"]),
                str(output["degree"]),
            )
            output_id = str(output.get("output_id") or "").strip()
            if not output_id:
                raise ProspectiveObservationError(
                    "output_id_required:" + ":".join(key)
                )
            if output_id in capture_by_output:
                raise ProspectiveObservationError(
                    f"output_id_reused_across_captures:{output_id}"
                )
            capture_by_output[output_id] = capture["capture_id"]
            dimensions.setdefault(key, []).append(
                {
                    "source_commit": capture["source_commit"],
                    "available_from": capture["captured_at"].isoformat(),
                    "output": output,
                }
            )

    cumulative_counts = {
        "STABLE": 0,
        "CHANGED": 0,
        "INSUFFICIENT_EVIDENCE": 0,
    }
    latest_counts = dict.fromkeys(cumulative_counts, 0)
    dimension_summaries: list[dict[str, Any]] = []
    current_capture_id = effective[-1]["capture_id"] if effective else None
    current_snapshot_id = effective[-1]["snapshot_id"] if effective else None
    comparable_dimensions = 0
    latest_comparisons = 0

    for key in sorted(dimensions):
        snapshots = dimensions[key]
        if len(snapshots) < 2:
            continue
        try:
            feature = build_scenario_stability_feature(snapshots)
        except ScenarioStabilityError as exc:
            raise ProspectiveObservationError(
                "scenario_stability_failed:" + ":".join(key) + f":{exc}"
            ) from exc
        comparable_dimensions += 1
        counts = feature.get("counts")
        observations = feature.get("observations")
        if not isinstance(counts, Mapping) or not isinstance(observations, list):
            raise ProspectiveObservationError(
                "scenario_stability_feature_shape_invalid:" + ":".join(key)
            )
        for status in cumulative_counts:
            cumulative_counts[status] += int(counts.get(status) or 0)
        latest = observations[-1] if observations else None
        if not isinstance(latest, Mapping):
            raise ProspectiveObservationError(
                "scenario_stability_latest_observation_missing:" + ":".join(key)
            )
        latest_status = str(latest.get("feature_status") or "")
        if latest_status not in cumulative_counts:
            raise ProspectiveObservationError(
                "scenario_stability_status_invalid:" + latest_status
            )
        current_output_id = str(latest.get("current_output_id") or "")
        current_output_capture = capture_by_output.get(current_output_id)
        reaches_current_capture = (
            current_capture_id is not None
            and current_output_capture == current_capture_id
        )
        if reaches_current_capture:
            latest_counts[latest_status] += 1
            latest_comparisons += 1
        dimension_summaries.append(
            {
                "symbol": key[0],
                "timeframe": key[1],
                "degree": key[2],
                "observation_count": int(feature.get("observation_count") or 0),
                "counts": {
                    status: int(counts.get(status) or 0)
                    for status in cumulative_counts
                },
                "latest_status": latest_status,
                "prior_output_id": latest.get("prior_output_id"),
                "current_output_id": current_output_id,
                "available_from": latest.get("available_from"),
                "reaches_current_capture": reaches_current_capture,
            }
        )

    if len(effective) < 2:
        status = "INSUFFICIENT_PROSPECTIVE_HISTORY"
    elif comparable_dimensions == 0:
        status = "INSUFFICIENT_COMPARABLE_OUTPUTS"
    else:
        status = "OBSERVATION_ACTIVE"

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "module": MODULE,
        "phase": "STAGE_4_PROSPECTIVE_OBSERVATION",
        "status": status,
        "generated_at": generated.isoformat(),
        "effective_capture_count": len(effective),
        "superseded_capture_count": max(0, len(list(captures)) - len(effective))
        if isinstance(captures, list)
        else None,
        "current_capture_id": current_capture_id,
        "current_snapshot_id": current_snapshot_id,
        "dimension_count": len(dimensions),
        "comparable_dimension_count": comparable_dimensions,
        "latest_capture_comparison_count": latest_comparisons,
        "scenario_stability": {
            "definition_source": "QM-G Scenario Stability",
            "comparison": "STRICT_PRIMARY_SCENARIO_ID_EQUALITY_BETWEEN_CONSECUTIVE_AVAILABLE_OUTPUTS",
            "cumulative_counts": cumulative_counts,
            "latest_capture_counts": latest_counts,
            "dimensions": dimension_summaries,
        },
        "guards": {
            "research_only": True,
            "prospective_inputs_only": True,
            "market_outcomes_used": False,
            "retroactive_reclassification_performed": False,
            "elliott_core_modified": False,
            "degree_reducer_used": False,
            "scenario_selected_by_performance": False,
            "review_context_created": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "productive_promotion_performed": False,
            "empirical_conclusion_allowed": False,
        },
    }
    result["report_hash"] = _canonical_hash(result)
    return result
