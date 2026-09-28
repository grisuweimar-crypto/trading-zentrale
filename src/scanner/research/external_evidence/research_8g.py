from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Mapping


CHALLENGER_SCHEMA = "external_evidence_8g_challenger_specs_v1"
SPLIT_PLAN_SCHEMA = "external_evidence_8g_split_plan_v1"
MANIFEST_SCHEMA = "external_evidence_8g_split_manifest_v1"
ACTIVE_FACTORS = ("rates_policy", "yield_curve", "fx")
HORIZONS = (5, 20, 40, 60)
FORBIDDEN_OUTCOME_KEY_PARTS = (
    "peer_excess",
    "adverse_excursion",
    "path_max_drawdown",
    "label_",
    "future_return",
    "return_5t",
    "return_20t",
    "return_40t",
    "return_60t",
)


class ExternalEvidence8GError(ValueError):
    """Raised when the Phase-8G outcome-blind research boundary is violated."""


def _ts(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
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


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _walk_keys(value: object) -> list[str]:
    keys: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            keys.append(str(key))
            keys.extend(_walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.extend(_walk_keys(item))
    return keys


def assert_outcome_blind_payload(payload: Mapping[str, Any]) -> None:
    for key in _walk_keys(payload):
        lowered = key.lower()
        if any(part in lowered for part in FORBIDDEN_OUTCOME_KEY_PARTS):
            raise ExternalEvidence8GError(f"outcome_key_forbidden_in_split_binding:{key}")


def validate_challenger_specs(specs: Mapping[str, Any]) -> None:
    if specs.get("schema_version") != CHALLENGER_SCHEMA:
        raise ExternalEvidence8GError("unsupported_challenger_schema")
    if specs.get("phase") != "8G-B":
        raise ExternalEvidence8GError("challenger_phase_mismatch")
    if specs.get("research_only") is not True:
        raise ExternalEvidence8GError("challengers_must_be_research_only")
    if specs.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GError("productive_integration_must_remain_disabled")
    if specs.get("outcome_values_read_while_defining_specs") is not False:
        raise ExternalEvidence8GError("challenger_specs_not_outcome_blind")
    if tuple(specs.get("active_confirmatory_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GError("unexpected_active_factor_family")

    construction = specs.get("challenger_construction")
    if not isinstance(construction, Mapping):
        raise ExternalEvidence8GError("challenger_construction_missing")
    required_false = (
        "cross_factor_interactions_allowed",
        "feature_sign_flip_allowed",
        "outcome_derived_thresholds_allowed",
        "standalone_relationship_class_term_allowed",
        "multiple_confirmatory_variants_per_factor",
    )
    for key in required_false:
        if construction.get(key) is not False:
            raise ExternalEvidence8GError(f"challenger_guard_must_be_false:{key}")
    if construction.get("exactly_one_external_factor_family") is not True:
        raise ExternalEvidence8GError("single_factor_challenger_required")
    if construction.get("semantic_sign_assignment") is not None:
        raise ExternalEvidence8GError("semantic_factor_sign_must_remain_unassigned")

    estimator = specs.get("estimator")
    if not isinstance(estimator, Mapping):
        raise ExternalEvidence8GError("estimator_missing")
    if estimator.get("family") != "ridge_linear_regression" or float(estimator.get("alpha")) != 1.0:
        raise ExternalEvidence8GError("unexpected_estimator")
    if estimator.get("hyperparameter_tuning_allowed") is not False:
        raise ExternalEvidence8GError("hyperparameter_tuning_forbidden")
    if estimator.get("feature_selection_allowed") is not False:
        raise ExternalEvidence8GError("feature_selection_forbidden")

    factor_specs = specs.get("factor_specs")
    if not isinstance(factor_specs, Mapping) or set(factor_specs) != set(ACTIVE_FACTORS):
        raise ExternalEvidence8GError("active_factor_specs_mismatch")
    for factor_id in ACTIVE_FACTORS:
        factor = factor_specs[factor_id]
        if factor.get("historical_retrojection_allowed") is not False:
            raise ExternalEvidence8GError(f"historical_retrojection_forbidden:{factor_id}")
        if factor.get("natural_sign_preserved") is not True:
            raise ExternalEvidence8GError(f"natural_sign_required:{factor_id}")
        if not factor.get("series_ids") or not factor.get("feature_fields"):
            raise ExternalEvidence8GError(f"factor_spec_incomplete:{factor_id}")

    guards = specs.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GError("challenger_guards_must_remain_false")


def validate_split_plan(plan: Mapping[str, Any], specs: Mapping[str, Any]) -> None:
    validate_challenger_specs(specs)
    if plan.get("schema_version") != SPLIT_PLAN_SCHEMA:
        raise ExternalEvidence8GError("unsupported_split_plan_schema")
    if plan.get("phase") != "8G-B":
        raise ExternalEvidence8GError("split_plan_phase_mismatch")
    if plan.get("research_only") is not True:
        raise ExternalEvidence8GError("split_plan_must_be_research_only")
    if plan.get("outcome_values_allowed_in_assignment") is not False:
        raise ExternalEvidence8GError("split_assignment_must_be_outcome_blind")
    if tuple(plan.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GError("split_plan_factor_mismatch")
    if tuple(int(x) for x in plan.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GError("split_plan_horizon_mismatch")

    ranges = plan.get("slot_ranges_by_horizon")
    if not isinstance(ranges, Mapping):
        raise ExternalEvidence8GError("slot_ranges_missing")
    for horizon in HORIZONS:
        block = ranges.get(str(horizon))
        if not isinstance(block, Mapping):
            raise ExternalEvidence8GError(f"slot_range_missing:{horizon}")
        expected = {
            "total": [1, 16 * horizon],
            "discovery_raw": [1, 8 * horizon],
            "discovery_usable": [1, 7 * horizon],
            "discovery_purge": [7 * horizon + 1, 8 * horizon],
            "validation_raw": [8 * horizon + 1, 12 * horizon],
            "validation_usable": [8 * horizon + 1, 11 * horizon],
            "validation_purge": [11 * horizon + 1, 12 * horizon],
            "holdout_usable": [12 * horizon + 1, 16 * horizon],
        }
        for key, expected_range in expected.items():
            if list(block.get(key) or ()) != expected_range:
                raise ExternalEvidence8GError(f"invalid_slot_range:{horizon}:{key}")
        if int(block.get("block_length_sessions")) != 2 * horizon:
            raise ExternalEvidence8GError(f"invalid_block_length:{horizon}")

    hypotheses = set(str(x) for x in plan.get("hypothesis_family") or ())
    expected_hypotheses = {f"{factor}_x_{h}t" for factor in ACTIVE_FACTORS for h in HORIZONS}
    if hypotheses != expected_hypotheses:
        raise ExternalEvidence8GError("hypothesis_family_mismatch")
    if plan.get("snapshot_reassignment_allowed") is not False:
        raise ExternalEvidence8GError("snapshot_reassignment_forbidden")
    if plan.get("slot_reassignment_allowed") is not False:
        raise ExternalEvidence8GError("slot_reassignment_forbidden")
    if plan.get("assignment_after_label_inspection_allowed") is not False:
        raise ExternalEvidence8GError("assignment_after_label_inspection_forbidden")


def slot_state(horizon: int, ordinal: int) -> dict[str, Any]:
    if horizon not in HORIZONS:
        raise ExternalEvidence8GError(f"unsupported_horizon:{horizon}")
    if ordinal < 1 or ordinal > 16 * horizon:
        raise ExternalEvidence8GError(f"slot_out_of_range:{horizon}:{ordinal}")
    if ordinal <= 7 * horizon:
        return {"split": "DISCOVERY", "usable": True, "purge_reason": None}
    if ordinal <= 8 * horizon:
        return {"split": "DISCOVERY", "usable": False, "purge_reason": "FORWARD_WINDOW_BOUNDARY_PURGE"}
    if ordinal <= 11 * horizon:
        return {"split": "VALIDATION", "usable": True, "purge_reason": None}
    if ordinal <= 12 * horizon:
        return {"split": "VALIDATION", "usable": False, "purge_reason": "FORWARD_WINDOW_BOUNDARY_PURGE"}
    return {"split": "HOLDOUT", "usable": True, "purge_reason": None}


def _latest_rows_by_observation_date(rows: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        day = str(row.get("observation_date") or "")
        previous = result.get(day)
        if previous is None or _ts(row["valid_from"]) > _ts(previous["valid_from"]):
            result[day] = row
    return result


def _knowable_factor_rows(
    ledger: Mapping[str, Any], factor_id: str, snapshot_at: datetime
) -> list[Mapping[str, Any]]:
    rows = ledger.get("observations")
    if not isinstance(rows, list):
        raise ExternalEvidence8GError("macro_ledger_observations_missing")
    knowable: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or str(row.get("factor_id")) != factor_id:
            continue
        if str(row.get("status")) != "KNOWN":
            continue
        if _ts(row.get("valid_from")) <= snapshot_at:
            knowable.append(row)
    return knowable


def _freshness_ok(latest_observation_date: str, snapshot_at: datetime, max_age_days: int) -> bool:
    try:
        obs_day = datetime.fromisoformat(latest_observation_date).date()
    except ValueError as exc:
        raise ExternalEvidence8GError("invalid_observation_date") from exc
    age = (snapshot_at.date() - obs_day).days
    return 0 <= age <= max_age_days


def _derive_factor_state(
    ledger: Mapping[str, Any],
    factor_id: str,
    snapshot_at: datetime,
    specs: Mapping[str, Any],
) -> tuple[dict[str, float] | None, list[Mapping[str, Any]], str | None]:
    factor = specs["factor_specs"][factor_id]
    series_ids = [str(x) for x in factor["series_ids"]]
    rows = _knowable_factor_rows(ledger, factor_id, snapshot_at)
    rows = [row for row in rows if str(row.get("series_id")) in series_ids]
    max_age = int(specs["common_pit_rules"]["max_observation_age_calendar_days"])

    if factor_id in {"rates_policy", "fx"}:
        series_id = series_ids[0]
        series_rows = [row for row in rows if str(row.get("series_id")) == series_id]
        by_day = _latest_rows_by_observation_date(series_rows)
        ordered_days = sorted(by_day)
        if len(ordered_days) < 2:
            return None, [], "PIT_INELIGIBLE_MISSING"
        latest_day, previous_day = ordered_days[-1], ordered_days[-2]
        if not _freshness_ok(latest_day, snapshot_at, max_age):
            return None, [], "PIT_INELIGIBLE_STALE"
        latest = by_day[latest_day]
        previous = by_day[previous_day]
        latest_value = float(latest["value"])
        previous_value = float(previous["value"])
        if factor_id == "rates_policy":
            features = {
                "rates_policy_level_pct": latest_value,
                "rates_policy_delta_pp": latest_value - previous_value,
            }
        else:
            if latest_value <= 0 or previous_value <= 0:
                return None, [], "PIT_INELIGIBLE_NONPOSITIVE_FX"
            import math
            latest_log = math.log(latest_value)
            previous_log = math.log(previous_value)
            features = {
                "fx_log_usd_per_eur": latest_log,
                "fx_log_change": latest_log - previous_log,
            }
        return features, [previous, latest], None

    if factor_id == "yield_curve":
        by_series: dict[str, dict[str, Mapping[str, Any]]] = {}
        for series_id in series_ids:
            by_series[series_id] = _latest_rows_by_observation_date(
                [row for row in rows if str(row.get("series_id")) == series_id]
            )
        common_days = sorted(set(by_series[series_ids[0]]).intersection(by_series[series_ids[1]]))
        if len(common_days) < 2:
            return None, [], "PIT_INELIGIBLE_MISSING"
        latest_day, previous_day = common_days[-1], common_days[-2]
        if not _freshness_ok(latest_day, snapshot_at, max_age):
            return None, [], "PIT_INELIGIBLE_STALE"
        two = by_series["RIFLGFCY02_N.B"]
        ten = by_series["RIFLGFCY10_N.B"]
        latest_spread = float(ten[latest_day]["value"]) - float(two[latest_day]["value"])
        previous_spread = float(ten[previous_day]["value"]) - float(two[previous_day]["value"])
        features = {
            "yield_curve_10y_minus_2y_pp": latest_spread,
            "yield_curve_delta_pp": latest_spread - previous_spread,
        }
        used_rows = [
            two[previous_day], ten[previous_day], two[latest_day], ten[latest_day]
        ]
        return features, used_rows, None

    raise ExternalEvidence8GError(f"unsupported_active_factor:{factor_id}")


def active_factor_mappings(
    exposure_map: Mapping[str, Any], factor_id: str, snapshot_at: datetime
) -> list[Mapping[str, Any]]:
    mappings = exposure_map.get("mappings")
    if not isinstance(mappings, list):
        raise ExternalEvidence8GError("exposure_mappings_missing")
    active: list[Mapping[str, Any]] = []
    for mapping in mappings:
        if not isinstance(mapping, Mapping) or str(mapping.get("factor_id")) != factor_id:
            continue
        if mapping.get("human_reviewed") is not True or str(mapping.get("review_status")) != "ACTIVE":
            continue
        if _ts(mapping.get("valid_from")) > snapshot_at:
            continue
        valid_to = mapping.get("valid_to")
        if valid_to is not None and _ts(valid_to) <= snapshot_at:
            continue
        active.append(mapping)
    return sorted(active, key=lambda row: str(row.get("mapping_id")))


def factor_snapshot_eligibility(
    *,
    factor_id: str,
    snapshot_metadata: Mapping[str, Any],
    macro_ledger: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
    specs: Mapping[str, Any],
) -> dict[str, Any]:
    validate_challenger_specs(specs)
    assert_outcome_blind_payload(snapshot_metadata)
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GError(f"factor_not_confirmatory:{factor_id}")

    snapshot_id = str(snapshot_metadata.get("snapshot_id") or "").strip()
    if not snapshot_id:
        return {"eligible": False, "reason": "SNAPSHOT_ID_MISSING"}
    generated_at_raw = snapshot_metadata.get("generated_at") or snapshot_metadata.get("views_generated_at")
    if not generated_at_raw:
        return {"eligible": False, "reason": "SNAPSHOT_GENERATED_AT_MISSING"}
    snapshot_at = _ts(generated_at_raw)
    if snapshot_metadata.get("latest_run_complete") is not True:
        return {"eligible": False, "reason": "SCANNER_SNAPSHOT_INCOMPLETE"}
    if snapshot_at < _ts(specs["prospective_cohort_not_before"]):
        return {"eligible": False, "reason": "BEFORE_PROSPECTIVE_COHORT_START"}

    features, used_rows, failure = _derive_factor_state(macro_ledger, factor_id, snapshot_at, specs)
    if failure:
        return {"eligible": False, "reason": failure}
    assert features is not None
    mappings = active_factor_mappings(exposure_map, factor_id, snapshot_at)
    if not mappings:
        return {"eligible": False, "reason": "NO_ACTIVE_HUMAN_REVIEWED_MAPPING"}

    observation_identity = sorted(
        (
            str(row.get("source_id")),
            str(row.get("series_id")),
            str(row.get("observation_date")),
            str(row.get("revision_id")),
            str(row.get("valid_from")),
            str(row.get("source_record_sha256")),
        )
        for row in used_rows
    )
    mapping_identity = sorted(
        (
            str(row.get("mapping_id")),
            str(row.get("subject_id")),
            str(row.get("relationship_class")),
            str(row.get("valid_from")),
            str(row.get("valid_to")),
        )
        for row in mappings
    )
    return {
        "eligible": True,
        "reason": "ELIGIBLE",
        "snapshot_id": snapshot_id,
        "generated_at": snapshot_at.isoformat(),
        "factor_id": factor_id,
        "feature_values": features,
        "factor_state_sha256": _digest({"observations": observation_identity, "features": features}),
        "active_mapping_count": len(mappings),
        "active_mapping_ids_sha256": _digest(mapping_identity),
    }


def empty_split_manifest(specs: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    validate_split_plan(plan, specs)
    streams = {
        f"{factor}_x_{horizon}t": {
            "factor_id": factor,
            "horizon_sessions": horizon,
            "target_slots": 16 * horizon,
            "assignments": [],
            "status": "COLLECTING",
        }
        for factor in ACTIVE_FACTORS
        for horizon in HORIZONS
    }
    return {
        "schema_version": MANIFEST_SCHEMA,
        "phase": "8G-B",
        "manifest_id": plan["manifest_id"],
        "status": "OUTCOME_BLIND_APPEND_ONLY_BINDINGS",
        "research_only": True,
        "challenger_specs_commit": plan["challenger_specs_commit"],
        "prospective_cohort_not_before": plan["prospective_cohort_not_before"],
        "outcomes_read": False,
        "bindings_are_append_only": True,
        "streams": streams,
    }


def validate_split_manifest(
    manifest: Mapping[str, Any], specs: Mapping[str, Any], plan: Mapping[str, Any]
) -> None:
    validate_split_plan(plan, specs)
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ExternalEvidence8GError("unsupported_split_manifest_schema")
    if manifest.get("manifest_id") != plan.get("manifest_id"):
        raise ExternalEvidence8GError("manifest_id_mismatch")
    if manifest.get("research_only") is not True or manifest.get("outcomes_read") is not False:
        raise ExternalEvidence8GError("manifest_must_remain_outcome_blind")
    if manifest.get("bindings_are_append_only") is not True:
        raise ExternalEvidence8GError("manifest_must_be_append_only")
    streams = manifest.get("streams")
    if not isinstance(streams, Mapping):
        raise ExternalEvidence8GError("manifest_streams_missing")

    expected_streams = {f"{factor}_x_{h}t" for factor in ACTIVE_FACTORS for h in HORIZONS}
    if set(streams) != expected_streams:
        raise ExternalEvidence8GError("manifest_stream_set_mismatch")
    for key, stream in streams.items():
        factor = str(stream.get("factor_id"))
        horizon = int(stream.get("horizon_sessions"))
        if key != f"{factor}_x_{horizon}t":
            raise ExternalEvidence8GError(f"manifest_stream_identity_mismatch:{key}")
        assignments = stream.get("assignments")
        if not isinstance(assignments, list):
            raise ExternalEvidence8GError(f"manifest_assignments_missing:{key}")
        if len(assignments) > 16 * horizon:
            raise ExternalEvidence8GError(f"manifest_stream_overfilled:{key}")
        seen_snapshots: set[str] = set()
        previous_time: datetime | None = None
        for index, assignment in enumerate(assignments, start=1):
            if int(assignment.get("ordinal")) != index:
                raise ExternalEvidence8GError(f"manifest_nonsequential_ordinal:{key}")
            snapshot_id = str(assignment.get("snapshot_id") or "")
            if not snapshot_id or snapshot_id in seen_snapshots:
                raise ExternalEvidence8GError(f"manifest_duplicate_snapshot:{key}")
            seen_snapshots.add(snapshot_id)
            at = _ts(assignment.get("generated_at"))
            if previous_time is not None and at < previous_time:
                raise ExternalEvidence8GError(f"manifest_out_of_order:{key}")
            previous_time = at
            expected = slot_state(horizon, index)
            if assignment.get("split") != expected["split"]:
                raise ExternalEvidence8GError(f"manifest_split_mismatch:{key}:{index}")
            if assignment.get("usable") is not expected["usable"]:
                raise ExternalEvidence8GError(f"manifest_usable_mismatch:{key}:{index}")
            if assignment.get("purge_reason") != expected["purge_reason"]:
                raise ExternalEvidence8GError(f"manifest_purge_mismatch:{key}:{index}")


def bind_snapshot_to_manifest(
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
    updated = deepcopy(dict(manifest))
    audit: dict[str, Any] = {
        "schema_version": "external_evidence_8g_split_binding_audit_v1",
        "snapshot_id": str(snapshot_metadata.get("snapshot_id") or ""),
        "bound": [],
        "skipped": [],
        "outcomes_read": False,
    }

    for factor_id in ACTIVE_FACTORS:
        eligibility = factor_snapshot_eligibility(
            factor_id=factor_id,
            snapshot_metadata=snapshot_metadata,
            macro_ledger=macro_ledger,
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
            existing = next(
                (item for item in assignments if item["snapshot_id"] == eligibility["snapshot_id"]),
                None,
            )
            if existing is not None:
                audit["skipped"].append({"stream": key, "reason": "IDEMPOTENT_ALREADY_BOUND"})
                continue
            if len(assignments) >= 16 * horizon:
                stream["status"] = "COHORT_FULL"
                audit["skipped"].append({"stream": key, "reason": "COHORT_FULL"})
                continue
            if assignments and _ts(eligibility["generated_at"]) < _ts(assignments[-1]["generated_at"]):
                raise ExternalEvidence8GError(f"out_of_order_snapshot_binding:{key}")

            ordinal = len(assignments) + 1
            state = slot_state(horizon, ordinal)
            assignment = {
                "ordinal": ordinal,
                "snapshot_id": eligibility["snapshot_id"],
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

    validate_split_manifest(updated, specs, plan)
    return updated, audit
