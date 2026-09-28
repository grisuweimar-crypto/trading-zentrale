from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any, Mapping

from scanner.research.external_evidence.research_8g import (
    ACTIVE_FACTORS,
    HORIZONS,
    ExternalEvidence8GError,
    assert_outcome_blind_payload,
    factor_snapshot_eligibility,
    slot_state,
    validate_split_manifest,
)


def _ts(value: object) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8GError(f"invalid_timestamp:{value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8GError(f"timezone_required:{value}")
    return parsed.astimezone(timezone.utc)


def _day(value: object) -> date:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8GError(f"invalid_as_of_date:{value}") from exc


def _snapshot_as_of(snapshot_metadata: Mapping[str, Any]) -> str:
    raw = snapshot_metadata.get("as_of")
    if raw is None:
        raise ExternalEvidence8GError("snapshot_as_of_required_for_sampling_identity")
    as_of = _day(raw)
    generated_raw = snapshot_metadata.get("generated_at") or snapshot_metadata.get("views_generated_at")
    if not generated_raw:
        raise ExternalEvidence8GError("snapshot_generated_at_required_for_sampling_identity")
    generated = _ts(generated_raw)
    if as_of > generated.date():
        raise ExternalEvidence8GError("snapshot_as_of_after_generated_at")
    return as_of.isoformat()


def _source_filtered_ledger(
    macro_ledger: Mapping[str, Any], specs: Mapping[str, Any]
) -> dict[str, Any]:
    rows = macro_ledger.get("observations")
    if not isinstance(rows, list):
        raise ExternalEvidence8GError("macro_ledger_observations_missing")

    filtered: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        factor_id = str(row.get("factor_id") or "")
        if factor_id not in ACTIVE_FACTORS:
            continue
        factor_spec = specs["factor_specs"][factor_id]
        expected_source = str(factor_spec["source_id"])
        expected_series = {str(x) for x in factor_spec["series_ids"]}
        series_id = str(row.get("series_id") or "")
        source_id = str(row.get("source_id") or "")

        if series_id not in expected_series:
            continue
        if source_id != expected_source:
            raise ExternalEvidence8GError(
                f"frozen_source_mismatch:{factor_id}:{series_id}:{source_id}"
            )
        filtered.append(row)

    out = dict(macro_ledger)
    out["observations"] = filtered
    return out


def validate_guarded_manifest(
    manifest: Mapping[str, Any], specs: Mapping[str, Any], plan: Mapping[str, Any]
) -> None:
    validate_split_manifest(manifest, specs, plan)
    streams = manifest["streams"]
    for key, stream in streams.items():
        assignments = stream["assignments"]
        seen_as_of: set[str] = set()
        previous_as_of: date | None = None
        previous_generated_at: datetime | None = None
        for assignment in assignments:
            if "as_of" not in assignment:
                raise ExternalEvidence8GError(f"guarded_manifest_as_of_missing:{key}")
            as_of_text = str(assignment["as_of"])
            as_of = _day(as_of_text)
            generated_at = _ts(assignment["generated_at"])
            if as_of_text in seen_as_of:
                raise ExternalEvidence8GError(f"guarded_manifest_duplicate_as_of:{key}:{as_of_text}")
            seen_as_of.add(as_of_text)
            if previous_as_of is not None and as_of <= previous_as_of:
                raise ExternalEvidence8GError(f"guarded_manifest_as_of_not_strictly_increasing:{key}")
            if previous_generated_at is not None and generated_at <= previous_generated_at:
                raise ExternalEvidence8GError(f"guarded_manifest_generated_at_not_strictly_increasing:{key}")
            if as_of > generated_at.date():
                raise ExternalEvidence8GError(f"guarded_manifest_as_of_after_generated_at:{key}")
            previous_as_of = as_of
            previous_generated_at = generated_at


def bind_snapshot_to_manifest_guarded(
    *,
    manifest: Mapping[str, Any],
    snapshot_metadata: Mapping[str, Any],
    macro_ledger: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_split_manifest(manifest, specs, plan)
    assert_outcome_blind_payload(snapshot_metadata)
    as_of = _snapshot_as_of(snapshot_metadata)
    generated_raw = snapshot_metadata.get("generated_at") or snapshot_metadata.get("views_generated_at")
    generated_at = _ts(generated_raw)
    filtered_ledger = _source_filtered_ledger(macro_ledger, specs)

    # Non-empty manifests created by this guarded binder must already carry the
    # sampling-day identity. This prevents silently mixing the earlier, weaker
    # snapshot-id-only binding semantics with the corrected 8G-B semantics.
    for key, stream in manifest["streams"].items():
        assignments = stream["assignments"]
        if assignments and any("as_of" not in item for item in assignments):
            raise ExternalEvidence8GError(f"legacy_binding_without_as_of_requires_rebuild:{key}")

    updated = deepcopy(dict(manifest))
    audit: dict[str, Any] = {
        "schema_version": "external_evidence_8g_split_binding_audit_v2",
        "snapshot_id": str(snapshot_metadata.get("snapshot_id") or ""),
        "as_of": as_of,
        "generated_at": generated_at.isoformat(),
        "bound": [],
        "skipped": [],
        "outcomes_read": False,
        "sampling_identity": "factor_id+horizon_sessions+as_of",
    }

    for factor_id in ACTIVE_FACTORS:
        eligibility = factor_snapshot_eligibility(
            factor_id=factor_id,
            snapshot_metadata=snapshot_metadata,
            macro_ledger=filtered_ledger,
            exposure_map=exposure_map,
            specs=specs,
        )
        if not eligibility["eligible"]:
            for horizon in HORIZONS:
                audit["skipped"].append({
                    "stream": f"{factor_id}_x_{horizon}t",
                    "reason": eligibility["reason"],
                })
            continue

        for horizon in HORIZONS:
            key = f"{factor_id}_x_{horizon}t"
            stream = updated["streams"][key]
            assignments = stream["assignments"]

            existing_snapshot = next(
                (item for item in assignments if item["snapshot_id"] == eligibility["snapshot_id"]),
                None,
            )
            if existing_snapshot is not None:
                audit["skipped"].append({"stream": key, "reason": "IDEMPOTENT_ALREADY_BOUND"})
                continue

            existing_day = next(
                (item for item in assignments if str(item.get("as_of")) == as_of),
                None,
            )
            if existing_day is not None:
                audit["skipped"].append({"stream": key, "reason": "AS_OF_DATE_ALREADY_BOUND"})
                continue

            if len(assignments) >= 16 * horizon:
                stream["status"] = "COHORT_FULL"
                audit["skipped"].append({"stream": key, "reason": "COHORT_FULL"})
                continue

            if assignments:
                last = assignments[-1]
                last_as_of = _day(last["as_of"])
                last_generated_at = _ts(last["generated_at"])
                if _day(as_of) < last_as_of:
                    raise ExternalEvidence8GError(f"out_of_order_as_of_binding:{key}")
                if generated_at <= last_generated_at:
                    raise ExternalEvidence8GError(f"out_of_order_generated_at_binding:{key}")

            ordinal = len(assignments) + 1
            state = slot_state(horizon, ordinal)
            assignment = {
                "ordinal": ordinal,
                "snapshot_id": eligibility["snapshot_id"],
                "as_of": as_of,
                "generated_at": eligibility["generated_at"],
                "split": state["split"],
                "usable": state["usable"],
                "purge_reason": state["purge_reason"],
                "factor_state_sha256": eligibility["factor_state_sha256"],
                "active_mapping_count": eligibility["active_mapping_count"],
                "active_mapping_ids_sha256": eligibility["active_mapping_ids_sha256"],
            }
            assignments.append(assignment)
            if len(assignments) >= 16 * horizon:
                stream["status"] = "COHORT_FULL"
            audit["bound"].append({"stream": key, **assignment})

    validate_guarded_manifest(updated, specs, plan)
    return updated, audit


def exact_split_boundary_purge_required(
    *,
    label_available_from: str,
    next_split_first_as_of: str,
) -> bool:
    """Return whether an earlier-split row crosses into the next split.

    This uses only the structural label-availability date, never the label value.
    It is intended for the later outcome-join stage after the split binding was
    already frozen.
    """
    return _day(label_available_from) >= _day(next_split_first_as_of)
