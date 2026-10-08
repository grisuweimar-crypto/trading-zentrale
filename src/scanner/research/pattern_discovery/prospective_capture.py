"""Phase L7 prospective Pattern Discovery capture.

L7 matches immutable, QM-C-authorized PAT versions against genuinely later
point-in-time scanner snapshots and appends immutable prospective claims.

It deliberately stops before outcome maturation. No future price, return,
benchmark outcome, rating, confirmation result, decision, portfolio action or
execution instruction is created here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry

from .boundary import PatternDiscoveryBoundary
from .candidate_registry import (
    CandidateFreezeError,
    validate_applied_qm_handoff,
    verify_freeze_snapshot,
)
from .feature_library import FeatureLibrary, FeatureLibraryError


SCHEMA_VERSION = "pattern_discovery_l7_prospective_capture_v2"
LEGACY_SCHEMA_VERSION = "pattern_discovery_l7_prospective_capture_v1"
CLAIM_SCHEMA_VERSION = "pattern_discovery_l7_prospective_claim_v1"
EVENT_SCHEMA_VERSION = "pattern_discovery_l7_claim_event_v1"
CAPTURE_REPORT_SCHEMA_VERSION = "pattern_discovery_l7_capture_report_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l7_prospective_capture_v2.json"
)
LEGACY_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l7_prospective_capture_v1.json"
)


class ProspectiveCaptureError(ValueError):
    """Raised when an L7 prospective-capture invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ProspectiveCaptureError(f"value_required:{field}")
    return result


def _safe_token(value: Any, field: str) -> str:
    text = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", text):
        raise ProspectiveCaptureError(f"safe_token_required:{field}")
    return text


def _as_datetime(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProspectiveCaptureError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveCaptureError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: Any, field: str) -> str:
    return _as_datetime(value, field).isoformat().replace("+00:00", "Z")


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProspectiveCaptureError(f"finite_number_required:{field}")
    result = float(value)
    if not math.isfinite(result):
        raise ProspectiveCaptureError(f"finite_number_required:{field}")
    return result


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _state_text(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, ".12g")
    return str(value).strip()


def load_prospective_capture_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveCaptureError(
            f"prospective_capture_contract_unreadable:{target}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") not in {SCHEMA_VERSION, LEGACY_SCHEMA_VERSION}:
        raise ProspectiveCaptureError("prospective_capture_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise ProspectiveCaptureError("prospective_capture_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise ProspectiveCaptureError(
            "prospective_capture_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise ProspectiveCaptureError("prospective_capture_execution_forbidden")
    if payload.get("principles", {}).get(
        "outcome_information_is_forbidden_at_claim_creation"
    ) is not True:
        raise ProspectiveCaptureError("prospective_capture_outcome_guard_required")
    return payload


def prospective_capture_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    return _hash(value)


def _sha256_text(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise ProspectiveCaptureError(f"sha256_required:{field}")
    return text


def _normalize_snapshot_metadata(
    metadata: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        raise ProspectiveCaptureError("snapshot_metadata_must_be_object")
    required = tuple(contract["snapshot"]["required_metadata_fields"])
    missing = [field for field in required if field not in metadata]
    if missing:
        raise ProspectiveCaptureError(
            "snapshot_metadata_fields_missing:" + ",".join(missing)
        )
    if metadata.get("schema_version") != contract["snapshot"][
        "metadata_schema_version"
    ]:
        raise ProspectiveCaptureError("snapshot_metadata_schema_invalid")
    if metadata.get("latest_run_complete") is not True:
        raise ProspectiveCaptureError("snapshot_run_not_complete")
    source = metadata.get("source")
    if not isinstance(source, Mapping):
        raise ProspectiveCaptureError("snapshot_source_must_be_object")
    source_required = tuple(contract["snapshot"]["required_source_fields"])
    source_missing = [field for field in source_required if field not in source]
    if source_missing:
        raise ProspectiveCaptureError(
            "snapshot_source_fields_missing:" + ",".join(source_missing)
        )
    return {
        "schema_version": metadata["schema_version"],
        "snapshot_id": _safe_token(metadata["snapshot_id"], "snapshot_id"),
        "as_of": _timestamp(metadata["as_of"], "snapshot.as_of"),
        "generated_at": _timestamp(
            metadata["generated_at"], "snapshot.generated_at"
        ),
        "latest_run_complete": True,
        "source": {
            "file": _text(source["file"], "snapshot.source.file"),
            "sha256": _sha256_text(
                source["sha256"], "snapshot.source.sha256"
            ),
        },
    }


def _row_identity_projection(
    row: Mapping[str, Any],
    *,
    feature_library: FeatureLibrary,
) -> dict[str, Any]:
    fields = {
        feature["source_field"]
        for feature in feature_library.features.values()
    }
    fields.update(
        {
            "symbol",
            "as_of",
            "generated_at",
            "snapshot_id",
            "observation_type",
            "data_source",
        }
    )
    return {
        field: row.get(field)
        for field in sorted(fields)
        if field in row
    }


def _normalize_current_rows(
    observations: Sequence[Mapping[str, Any]],
    *,
    metadata: Mapping[str, Any],
    feature_library: FeatureLibrary,
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if isinstance(observations, (str, bytes, bytearray)) or not isinstance(
        observations, Sequence
    ):
        raise ProspectiveCaptureError("current_observations_sequence_required")
    required = tuple(contract["snapshot"]["required_row_fields"])
    expected_snapshot = str(metadata["snapshot_id"])
    expected_generated = str(metadata["generated_at"])
    expected_observation_type = str(
        contract["snapshot"]["required_observation_type"]
    )
    expected_data_source = str(contract["snapshot"]["required_data_source"])
    generated_dt = _as_datetime(expected_generated, "snapshot.generated_at")
    normalized: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()

    for index, raw in enumerate(observations):
        if not isinstance(raw, Mapping):
            raise ProspectiveCaptureError(
                f"current_observation_must_be_object:{index}"
            )
        missing = [field for field in required if field not in raw]
        if missing:
            raise ProspectiveCaptureError(
                f"current_observation_fields_missing:{index}:"
                + ",".join(missing)
            )
        symbol = _text(raw.get("symbol"), f"current[{index}].symbol")
        if symbol in seen_symbols:
            raise ProspectiveCaptureError(
                f"duplicate_current_snapshot_symbol:{symbol}"
            )
        seen_symbols.add(symbol)
        snapshot_id = _safe_token(
            raw.get("snapshot_id"), f"current[{index}].snapshot_id"
        )
        generated_at = _timestamp(
            raw.get("generated_at"), f"current[{index}].generated_at"
        )
        as_of = _timestamp(raw.get("as_of"), f"current[{index}].as_of")
        if snapshot_id != expected_snapshot:
            raise ProspectiveCaptureError(
                f"current_snapshot_id_mismatch:{symbol}"
            )
        if generated_at != expected_generated:
            raise ProspectiveCaptureError(
                f"current_generated_at_mismatch:{symbol}"
            )
        if _as_datetime(as_of, f"current[{index}].as_of") > generated_dt:
            raise ProspectiveCaptureError(
                f"current_observation_after_snapshot_generation:{symbol}"
            )
        if raw.get("observation_type") != expected_observation_type:
            raise ProspectiveCaptureError(
                f"current_observation_type_invalid:{symbol}"
            )
        if raw.get("data_source") != expected_data_source:
            raise ProspectiveCaptureError(
                f"current_data_source_invalid:{symbol}"
            )
        copy = dict(raw)
        copy["symbol"] = symbol
        copy["snapshot_id"] = snapshot_id
        copy["generated_at"] = generated_at
        copy["as_of"] = as_of
        copy["_l7_row_hash"] = _hash(
            _row_identity_projection(
                copy,
                feature_library=feature_library,
            )
        )
        normalized.append(copy)

    normalized.sort(key=lambda row: str(row["symbol"]))
    return normalized


def _normalize_history_rows(
    history: Sequence[Mapping[str, Any]],
    *,
    current_generated_at: str,
    feature_library: FeatureLibrary,
) -> list[dict[str, Any]]:
    if isinstance(history, (str, bytes, bytearray)) or not isinstance(
        history, Sequence
    ):
        raise ProspectiveCaptureError("history_sequence_required")
    cutoff = _as_datetime(current_generated_at, "snapshot.generated_at")
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for index, raw in enumerate(history):
        if not isinstance(raw, Mapping):
            raise ProspectiveCaptureError(
                f"history_observation_must_be_object:{index}"
            )
        symbol = _text(raw.get("symbol"), f"history[{index}].symbol")
        as_of = _timestamp(raw.get("as_of"), f"history[{index}].as_of")
        generated_at = _timestamp(
            raw.get("generated_at"), f"history[{index}].generated_at"
        )
        snapshot_id = _safe_token(
            raw.get("snapshot_id"), f"history[{index}].snapshot_id"
        )
        if _as_datetime(generated_at, f"history[{index}].generated_at") >= cutoff:
            raise ProspectiveCaptureError(
                f"history_not_strictly_before_current_snapshot:{symbol}:{snapshot_id}"
            )
        if _as_datetime(as_of, f"history[{index}].as_of") >= cutoff:
            raise ProspectiveCaptureError(
                f"history_as_of_not_before_current_snapshot:{symbol}:{snapshot_id}"
            )
        identity = (symbol, as_of, snapshot_id)
        if identity in seen:
            raise ProspectiveCaptureError(
                f"duplicate_history_observation:{symbol}:{as_of}:{snapshot_id}"
            )
        seen.add(identity)
        copy = dict(raw)
        copy["symbol"] = symbol
        copy["as_of"] = as_of
        copy["generated_at"] = generated_at
        copy["snapshot_id"] = snapshot_id
        copy["_l7_row_hash"] = _hash(
            _row_identity_projection(
                copy,
                feature_library=feature_library,
            )
        )
        normalized.append(copy)

    normalized.sort(
        key=lambda row: (
            str(row["symbol"]),
            str(row["as_of"]),
            str(row["generated_at"]),
            str(row["snapshot_id"]),
        )
    )
    return normalized


def _snapshot_binding(
    metadata: Mapping[str, Any],
    current_rows: Sequence[Mapping[str, Any]],
    *,
    snapshot_file_sha256: str,
    history_rows: Sequence[Mapping[str, Any]],
    history_file_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    current_hash = _sha256_text(
        snapshot_file_sha256, "snapshot_file_sha256"
    )
    history_hash = _sha256_text(
        history_file_sha256, "history_file_sha256"
    )
    snapshot_binding: dict[str, Any] = {
        "snapshot_id": metadata["snapshot_id"],
        "snapshot_as_of": metadata["as_of"],
        "snapshot_generated_at": metadata["generated_at"],
        "snapshot_metadata_schema_version": metadata["schema_version"],
        "snapshot_source_file": metadata["source"]["file"],
        "snapshot_source_sha256": metadata["source"]["sha256"],
        "snapshot_file_sha256": current_hash,
        "row_count": len(current_rows),
        "symbol_count": len({str(row["symbol"]) for row in current_rows}),
        "projected_rows_hash": _hash(
            [
                {
                    "symbol": row["symbol"],
                    "as_of": row["as_of"],
                    "snapshot_id": row["snapshot_id"],
                    "row_hash": row["_l7_row_hash"],
                }
                for row in current_rows
            ]
        ),
    }
    snapshot_binding["snapshot_binding_hash"] = _hash(snapshot_binding)

    history_binding: dict[str, Any] = {
        "history_file_sha256": history_hash,
        "row_count": len(history_rows),
        "symbol_count": len({str(row["symbol"]) for row in history_rows}),
        "projected_rows_hash": _hash(
            [
                {
                    "symbol": row["symbol"],
                    "as_of": row["as_of"],
                    "snapshot_id": row["snapshot_id"],
                    "row_hash": row["_l7_row_hash"],
                }
                for row in history_rows
            ]
        ),
        "max_generated_at": (
            max(str(row["generated_at"]) for row in history_rows)
            if history_rows
            else None
        ),
    }
    history_binding["history_binding_hash"] = _hash(history_binding)
    return snapshot_binding, history_binding


def _normalize_sessions(
    sessions: Sequence[Mapping[str, Any]],
    *,
    capture_at: str,
    contract: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], str]:
    if isinstance(sessions, (str, bytes, bytearray)) or not isinstance(
        sessions, Sequence
    ):
        raise ProspectiveCaptureError("market_sessions_sequence_required")
    required = tuple(contract["market_session"]["required_fields"])
    capture_dt = _as_datetime(capture_at, "capture_at")
    by_symbol: dict[str, dict[str, Any]] = {}

    for index, raw in enumerate(sessions):
        if not isinstance(raw, Mapping):
            raise ProspectiveCaptureError(
                f"market_session_must_be_object:{index}"
            )
        missing = [field for field in required if field not in raw]
        if missing:
            raise ProspectiveCaptureError(
                f"market_session_fields_missing:{index}:"
                + ",".join(missing)
            )
        symbol = _text(raw.get("symbol"), f"market_sessions[{index}].symbol")
        if symbol in by_symbol:
            raise ProspectiveCaptureError(
                f"duplicate_market_session_symbol:{symbol}"
            )
        start_at = _timestamp(
            raw.get("start_at"), f"market_sessions[{index}].start_at"
        )
        if _as_datetime(start_at, f"market_sessions[{index}].start_at") <= capture_dt:
            raise ProspectiveCaptureError(
                f"market_session_not_strictly_after_capture:{symbol}"
            )
        by_symbol[symbol] = {
            "symbol": symbol,
            "session_id": _safe_token(
                raw.get("session_id"),
                f"market_sessions[{index}].session_id",
            ),
            "calendar_id": _safe_token(
                raw.get("calendar_id"),
                f"market_sessions[{index}].calendar_id",
            ),
            "start_at": start_at,
            "source": _text(
                raw.get("source"),
                f"market_sessions[{index}].source",
            ),
        }
    normalized = [by_symbol[key] for key in sorted(by_symbol)]
    return by_symbol, _hash(normalized)


def _verify_frozen_pattern_record(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise ProspectiveCaptureError("frozen_pattern_must_be_object")
    pattern_id = _text(record.get("pattern_id"), "pattern_id")
    pattern_version = _text(record.get("pattern_version"), "pattern_version")
    pattern_spec_hash = _sha256_text(
        record.get("pattern_spec_hash"), "pattern_spec_hash"
    )
    frozen_record_hash = _sha256_text(
        record.get("frozen_record_hash"), "frozen_record_hash"
    )
    body = dict(record)
    body.pop("frozen_record_hash", None)
    if _hash(body) != frozen_record_hash:
        raise ProspectiveCaptureError(
            f"frozen_pattern_record_hash_mismatch:{pattern_id}:{pattern_version}"
        )
    pattern_spec = record.get("pattern_spec")
    if not isinstance(pattern_spec, Mapping):
        raise ProspectiveCaptureError("pattern_spec_missing")
    if _hash(pattern_spec) != pattern_spec_hash:
        raise ProspectiveCaptureError(
            f"pattern_spec_hash_mismatch:{pattern_id}:{pattern_version}"
        )
    identity = pattern_spec.get("identity")
    if not isinstance(identity, Mapping):
        raise ProspectiveCaptureError("pattern_spec_identity_missing")
    if identity.get("pattern_id") != pattern_id:
        raise ProspectiveCaptureError("pattern_spec_pattern_id_mismatch")
    if identity.get("pattern_version") != pattern_version:
        raise ProspectiveCaptureError("pattern_spec_pattern_version_mismatch")
    forecast = pattern_spec.get("forecast")
    semantics = pattern_spec.get("semantics")
    if not isinstance(forecast, Mapping) or not isinstance(semantics, Mapping):
        raise ProspectiveCaptureError(
            "pattern_spec_forecast_or_semantics_missing"
        )
    conditions = semantics.get("conditions")
    if not isinstance(conditions, Sequence) or isinstance(
        conditions, (str, bytes, bytearray)
    ):
        raise ProspectiveCaptureError("pattern_conditions_sequence_required")
    if not conditions:
        raise ProspectiveCaptureError("pattern_conditions_nonempty_required")
    return dict(record)


def _collect_patterns(
    snapshots: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    if isinstance(snapshots, (str, bytes, bytearray)) or not isinstance(
        snapshots, Sequence
    ):
        raise ProspectiveCaptureError("l5_snapshots_sequence_required")
    if not snapshots:
        raise ProspectiveCaptureError("at_least_one_l5_snapshot_required")

    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    hashes: list[str] = []
    for snapshot in snapshots:
        verified = verify_freeze_snapshot(snapshot)
        snapshot_hash = _sha256_text(
            verified["snapshot_hash"], "l5_snapshot_hash"
        )
        hashes.append(snapshot_hash)
        for raw in snapshot.get("frozen_patterns", []):
            record = _verify_frozen_pattern_record(raw)
            key = (
                str(record["pattern_id"]),
                str(record["pattern_version"]),
                str(record["pattern_spec_hash"]),
            )
            previous = by_key.get(key)
            if previous is not None and previous != record:
                raise ProspectiveCaptureError(
                    "conflicting_duplicate_frozen_pattern:"
                    + "::".join(key)
                )
            by_key[key] = record
    patterns = [by_key[key] for key in sorted(by_key)]
    return patterns, sorted(set(hashes))


def _history_for_symbol(
    history: Sequence[Mapping[str, Any]],
    symbol: str,
    current_as_of: str,
) -> list[Mapping[str, Any]]:
    current_dt = _as_datetime(current_as_of, "current_as_of")
    rows = [
        row
        for row in history
        if str(row["symbol"]) == symbol
        and _as_datetime(row["as_of"], "history.as_of") < current_dt
    ]
    rows.sort(
        key=lambda row: (
            _as_datetime(row["as_of"], "history.as_of"),
            _as_datetime(row["generated_at"], "history.generated_at"),
            str(row["snapshot_id"]),
        )
    )
    return rows


def _prior_row(
    symbol_history: Sequence[Mapping[str, Any]],
    lag: int,
) -> Mapping[str, Any] | None:
    if lag <= 0 or len(symbol_history) < lag:
        return None
    return symbol_history[-lag]


def _supporting_prior_identity(
    row: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "snapshot_id": row["snapshot_id"],
        "as_of": row["as_of"],
        "generated_at": row["generated_at"],
        "row_hash": row.get("_l7_row_hash") or _hash(
            {
                "symbol": row.get("symbol"),
                "as_of": row.get("as_of"),
                "generated_at": row.get("generated_at"),
                "snapshot_id": row.get("snapshot_id"),
            }
        ),
    }


def _observed_state(
    *,
    condition: Mapping[str, Any],
    row: Mapping[str, Any],
    symbol_history: Sequence[Mapping[str, Any]],
    feature_library: FeatureLibrary,
    data_cutoff: str,
    supported_transformations: set[str],
) -> dict[str, Any]:
    feature_use = {
        "feature_id": condition.get("feature_id"),
        "feature_version": condition.get("feature_version"),
        "transformation_id": condition.get("transformation_id"),
        "transformation_version": condition.get("transformation_version"),
        "parameters": condition.get("parameters"),
    }
    try:
        validated = feature_library.validate_feature_use(feature_use)
    except FeatureLibraryError as exc:
        raise ProspectiveCaptureError(
            f"frozen_condition_not_valid_in_feature_library:{condition.get('feature_id')}"
        ) from exc
    if str(validated["feature_version_hash"]) != str(
        condition.get("feature_version_hash")
    ):
        raise ProspectiveCaptureError(
            f"frozen_condition_feature_hash_mismatch:{validated['feature_id']}"
        )
    tid = str(validated["transformation_id"])
    if tid not in supported_transformations:
        raise ProspectiveCaptureError(
            f"l7_transformation_not_supported:{tid}"
        )

    availability = feature_library.pit_availability(
        feature_use,
        row,
        data_cutoff=data_cutoff,
        history=symbol_history,
    )
    evidence: dict[str, Any] = {
        "feature_id": validated["feature_id"],
        "feature_version": validated["feature_version"],
        "feature_version_hash": validated["feature_version_hash"],
        "transformation_id": tid,
        "transformation_version": validated["transformation_version"],
        "parameters": dict(validated["parameters"]),
        "expected_state": str(condition.get("state")),
        "availability_status": availability["status"],
        "observed_state": None,
        "supporting_prior_observation": None,
        "match_status": "UNAVAILABLE",
    }
    if not availability["available"]:
        return evidence

    field = str(validated["source_field"])
    current = row.get(field)
    observed: str | None = None
    prior: Mapping[str, Any] | None = None

    if tid in {"raw", "regime_context"}:
        observed = _state_text(current)
    elif tid == "change_direction":
        lag = int(validated["parameters"]["lag_observations"])
        prior = _prior_row(symbol_history, lag)
        if prior is None or _is_missing(prior.get(field)):
            evidence["availability_status"] = "INSUFFICIENT_PRIOR_OBSERVATIONS"
            return evidence
        delta = _finite(current, f"{field}.current") - _finite(
            prior.get(field), f"{field}.prior"
        )
        if delta > 0:
            observed = "UP"
        elif delta < 0:
            observed = "DOWN"
        else:
            evidence["availability_status"] = "NO_DIRECTIONAL_CHANGE"
            evidence["match_status"] = "NO_MATCH"
    elif tid == "threshold_crossing":
        threshold = float(validated["parameters"]["threshold"])
        prior = _prior_row(symbol_history, 1)
        if prior is None or _is_missing(prior.get(field)):
            evidence["availability_status"] = "INSUFFICIENT_PRIOR_OBSERVATIONS"
            return evidence
        current_value = _finite(current, f"{field}.current")
        previous_value = _finite(prior.get(field), f"{field}.prior")
        if current_value >= threshold and previous_value < threshold:
            observed = "CROSS_UP"
        elif current_value < threshold and previous_value >= threshold:
            observed = "CROSS_DOWN"
        else:
            evidence["availability_status"] = "NO_THRESHOLD_CROSSING"
            evidence["match_status"] = "NO_MATCH"
    elif tid == "state_transition":
        prior = _prior_row(symbol_history, 1)
        if prior is None or _is_missing(prior.get(field)):
            evidence["availability_status"] = "INSUFFICIENT_PRIOR_OBSERVATIONS"
            return evidence
        before = _state_text(prior.get(field))
        after = _state_text(current)
        if before == after:
            evidence["availability_status"] = "NO_STATE_CHANGE"
            evidence["match_status"] = "NO_MATCH"
        else:
            observed = f"{before}->{after}"
    else:
        raise ProspectiveCaptureError(
            f"l7_transformation_runtime_not_supported:{tid}"
        )

    evidence["supporting_prior_observation"] = _supporting_prior_identity(prior)
    if observed is not None:
        evidence["observed_state"] = observed
        evidence["match_status"] = (
            "MATCH"
            if observed == evidence["expected_state"]
            else "NO_MATCH"
        )
    return evidence


def match_frozen_pattern(
    pattern: Mapping[str, Any],
    row: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]],
    *,
    snapshot_generated_at: str,
    feature_library: FeatureLibrary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Match one exact frozen PAT version against one current PIT row."""
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    frozen = _verify_frozen_pattern_record(pattern)
    library = feature_library or FeatureLibrary()
    symbol = _text(row.get("symbol"), "row.symbol")
    current_as_of = _timestamp(row.get("as_of"), "row.as_of")
    symbol_history = _history_for_symbol(history, symbol, current_as_of)
    supported = set(str(x) for x in spec["matching"]["supported_transformations"])
    conditions = frozen["pattern_spec"]["semantics"]["conditions"]

    evidence = [
        _observed_state(
            condition=condition,
            row=row,
            symbol_history=symbol_history,
            feature_library=library,
            data_cutoff=snapshot_generated_at,
            supported_transformations=supported,
        )
        for condition in conditions
    ]
    if any(item["match_status"] == "UNAVAILABLE" for item in evidence):
        status = "UNAVAILABLE"
    elif all(item["match_status"] == "MATCH" for item in evidence):
        status = "MATCH"
    else:
        status = "NO_MATCH"

    return {
        "pattern_id": frozen["pattern_id"],
        "pattern_version": frozen["pattern_version"],
        "pattern_spec_hash": frozen["pattern_spec_hash"],
        "symbol": symbol,
        "status": status,
        "condition_evidence": evidence,
        "condition_evidence_hash": _hash(evidence),
    }


def _governance_authorization(
    pattern: Mapping[str, Any],
    *,
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        validated = validate_applied_qm_handoff(
            pattern,
            hypothesis_registry=hypothesis_registry,
            analysis_plan_registry=analysis_plan_registry,
        )
    except (CandidateFreezeError, KeyError, ValueError) as exc:
        raise ProspectiveCaptureError(
            f"l7_qm_c_authorization_invalid:{pattern.get('pattern_id')}:{pattern.get('pattern_version')}"
        ) from exc
    required_h = str(
        contract["governance"]["qm_c1_hypothesis_state_required"]
    )
    required_p = str(
        contract["governance"]["qm_c2_analysis_plan_state_required"]
    )
    if validated["states"]["hypothesis"] != required_h:
        raise ProspectiveCaptureError("l7_qm_c1_state_not_frozen")
    if validated["states"]["analysis_plan"] != required_p:
        raise ProspectiveCaptureError("l7_qm_c2_state_not_frozen")
    return {
        "hypothesis_id": validated["hypothesis_id"],
        "hypothesis_version": validated["hypothesis_version"],
        "hypothesis_version_hash": validated["hypothesis_version_hash"],
        "analysis_plan_id": validated["analysis_plan_id"],
        "analysis_plan_version": validated["analysis_plan_version"],
        "analysis_plan_hash": validated["analysis_plan_hash"],
        "hypothesis_state": validated["states"]["hypothesis"],
        "analysis_plan_state": validated["states"]["analysis_plan"],
        "qm_a_state_at_capture": validated["states"]["qm_a"],
    }


def _forbidden_payload_paths(
    value: object,
    forbidden_keys: frozenset[str],
    path: str = "$",
) -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            child = f"{path}.{key_text}"
            if key_text.lower() in forbidden_keys:
                found.append(child)
            found.extend(
                _forbidden_payload_paths(item, forbidden_keys, child)
            )
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for index, item in enumerate(value):
            found.extend(
                _forbidden_payload_paths(
                    item,
                    forbidden_keys,
                    f"{path}[{index}]",
                )
            )
    return found


def _assert_no_outcome_payload(
    payload: object,
    *,
    contract: Mapping[str, Any],
) -> None:
    forbidden = frozenset(
        str(key).lower()
        for key in contract["claim"]["forbidden_payload_keys"]
    )
    found = _forbidden_payload_paths(payload, forbidden)
    if found:
        raise ProspectiveCaptureError(
            "outcome_or_price_payload_forbidden:" + ",".join(sorted(found))
        )


def _build_claim(
    *,
    pattern: Mapping[str, Any],
    row: Mapping[str, Any],
    match: Mapping[str, Any],
    session: Mapping[str, Any],
    governance: Mapping[str, Any],
    snapshot_binding: Mapping[str, Any],
    history_binding: Mapping[str, Any],
    session_map_hash: str,
    capture_at: str,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    forecast = pattern["pattern_spec"]["forecast"]
    horizon = int(forecast["horizon_sessions"])
    allowed_horizons = {
        int(value)
        for value in contract["market_session"]["allowed_horizons_sessions"]
    }
    if horizon not in allowed_horizons:
        raise ProspectiveCaptureError(
            f"l7_horizon_not_supported:{horizon}"
        )

    event_basis = {
        "pattern_id": pattern["pattern_id"],
        "pattern_version": pattern["pattern_version"],
        "pattern_spec_hash": pattern["pattern_spec_hash"],
        "snapshot_id": snapshot_binding["snapshot_id"],
        "symbol": row["symbol"],
    }
    event_id = (
        f"{contract['claim']['event_id_prefix']}-"
        f"{_hash(event_basis)[:int(contract['claim']['id_digest_chars'])].upper()}"
    )
    claim_basis = {
        "event_id": event_id,
        "target_id": forecast["target_id"],
        "horizon_sessions": horizon,
        "baseline": forecast["baseline"],
        "start_market_session_id": session["session_id"],
        "snapshot_binding_hash": snapshot_binding["snapshot_binding_hash"],
    }
    claim_id = (
        f"{contract['claim']['claim_id_prefix']}-"
        f"{_hash(claim_basis)[:int(contract['claim']['id_digest_chars'])].upper()}"
    )

    claim: dict[str, Any] = {
        "schema_version": CLAIM_SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "capture_state": contract["claim"]["capture_state"],
        "claim_id": claim_id,
        "event_id": event_id,
        "captured_at": capture_at,
        "pattern": {
            "pattern_id": pattern["pattern_id"],
            "pattern_version": pattern["pattern_version"],
            "pattern_spec_hash": pattern["pattern_spec_hash"],
            "frozen_record_hash": pattern["frozen_record_hash"],
            "freeze_timestamp": pattern["freeze_timestamp"],
            "discovery_run_id": pattern["discovery_run_id"],
        },
        "governance": dict(governance),
        "match": {
            "symbol": row["symbol"],
            "observation_as_of": row["as_of"],
            "snapshot_id": snapshot_binding["snapshot_id"],
            "snapshot_generated_at": snapshot_binding["snapshot_generated_at"],
            "snapshot_binding_hash": snapshot_binding["snapshot_binding_hash"],
            "history_binding_hash": history_binding["history_binding_hash"],
            "session_map_hash": session_map_hash,
            "current_row_hash": row["_l7_row_hash"],
            "condition_evidence": match["condition_evidence"],
            "condition_evidence_hash": match["condition_evidence_hash"],
        },
        "forecast": {
            "target_id": forecast["target_id"],
            "expected_direction": forecast["expected_direction"],
            "horizon_sessions": horizon,
            "baseline": forecast["baseline"],
            "reference_definition": forecast["reference_definition"],
        },
        "start_market_session": {
            "session_id": session["session_id"],
            "calendar_id": session["calendar_id"],
            "start_at": session["start_at"],
            "source": session["source"],
        },
        "outcome_available_at_capture": False,
        "outcome_maturation_performed": False,
        "confirmation_evaluation_performed": False,
        "rating_assigned": False,
        "promotion_performed": False,
    }
    _assert_no_outcome_payload(claim, contract=contract)
    PatternDiscoveryBoundary().assert_research_payload(claim)
    claim["claim_hash"] = _hash(claim)
    return claim


def verify_prospective_claim(
    claim: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    if not isinstance(claim, Mapping):
        raise ProspectiveCaptureError("prospective_claim_must_be_object")
    if claim.get("schema_version") != CLAIM_SCHEMA_VERSION:
        raise ProspectiveCaptureError("prospective_claim_schema_invalid")
    if claim.get("research_only") is not True:
        raise ProspectiveCaptureError("prospective_claim_research_only_guard_missing")
    if claim.get("productive_integration_enabled") is not False:
        raise ProspectiveCaptureError(
            "prospective_claim_productive_integration_forbidden"
        )
    if claim.get("execution_allowed") is not False:
        raise ProspectiveCaptureError("prospective_claim_execution_forbidden")
    if claim.get("outcome_available_at_capture") is not False:
        raise ProspectiveCaptureError("prospective_claim_outcome_forbidden")
    if claim.get("outcome_maturation_performed") is not False:
        raise ProspectiveCaptureError("prospective_claim_maturation_forbidden")
    if claim.get("confirmation_evaluation_performed") is not False:
        raise ProspectiveCaptureError("prospective_claim_confirmation_forbidden")
    _assert_no_outcome_payload(claim, contract=spec)

    stored = _sha256_text(claim.get("claim_hash"), "claim_hash")
    body = dict(claim)
    body.pop("claim_hash", None)
    if _hash(body) != stored:
        raise ProspectiveCaptureError("prospective_claim_hash_mismatch")

    captured = _as_datetime(claim.get("captured_at"), "claim.captured_at")
    start_at = _as_datetime(
        claim.get("start_market_session", {}).get("start_at"),
        "claim.start_market_session.start_at",
    )
    if start_at <= captured:
        raise ProspectiveCaptureError(
            "prospective_claim_market_session_not_future"
        )
    freeze = _as_datetime(
        claim.get("pattern", {}).get("freeze_timestamp"),
        "claim.pattern.freeze_timestamp",
    )
    snapshot_generated = _as_datetime(
        claim.get("match", {}).get("snapshot_generated_at"),
        "claim.match.snapshot_generated_at",
    )
    if snapshot_generated <= freeze:
        raise ProspectiveCaptureError(
            "prospective_claim_snapshot_not_post_freeze"
        )
    if captured < snapshot_generated:
        raise ProspectiveCaptureError("prospective_claim_before_snapshot_generation")
    PatternDiscoveryBoundary().assert_research_payload(claim)
    return {
        "valid": True,
        "claim_id": claim.get("claim_id"),
        "event_id": claim.get("event_id"),
        "claim_hash": stored,
    }


def build_prospective_capture(
    l5_snapshots: Sequence[Mapping[str, Any]],
    snapshot_metadata: Mapping[str, Any],
    current_observations: Sequence[Mapping[str, Any]],
    history_observations: Sequence[Mapping[str, Any]],
    market_sessions: Sequence[Mapping[str, Any]],
    *,
    capture_at: str,
    snapshot_file_sha256: str,
    history_file_sha256: str,
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
    feature_library: FeatureLibrary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one deterministic L7 capture batch without mutating storage."""
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    library = feature_library or FeatureLibrary()
    metadata = _normalize_snapshot_metadata(
        snapshot_metadata,
        contract=spec,
    )
    capture_ts = _timestamp(capture_at, "capture_at")
    capture_dt = _as_datetime(capture_ts, "capture_at")
    generated_dt = _as_datetime(
        metadata["generated_at"], "snapshot.generated_at"
    )
    if capture_dt < generated_dt:
        raise ProspectiveCaptureError("capture_before_snapshot_generation")
    # Preserve the immutable v1 time window for explicit legacy replays.
    # V2 does not expire immutable scanner evidence on an elapsed-time clock.
    # The future-start-session guard in _normalize_sessions remains mandatory.
    if spec["schema_version"] == LEGACY_SCHEMA_VERSION:
        max_delay_seconds = (
            int(spec["snapshot"]["max_capture_delay_minutes"]) * 60
        )
        if (capture_dt - generated_dt).total_seconds() > max_delay_seconds:
            raise ProspectiveCaptureError("stale_snapshot_backfill_forbidden")

    current_rows = _normalize_current_rows(
        current_observations,
        metadata=metadata,
        feature_library=library,
        contract=spec,
    )
    history_rows = _normalize_history_rows(
        history_observations,
        current_generated_at=metadata["generated_at"],
        feature_library=library,
    )
    snapshot_binding, history_binding = _snapshot_binding(
        metadata,
        current_rows,
        snapshot_file_sha256=snapshot_file_sha256,
        history_rows=history_rows,
        history_file_sha256=history_file_sha256,
    )
    sessions_by_symbol, session_map_hash = _normalize_sessions(
        market_sessions,
        capture_at=capture_ts,
        contract=spec,
    )
    patterns, l5_snapshot_hashes = _collect_patterns(l5_snapshots)

    claims: list[dict[str, Any]] = []
    pattern_summaries: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    post_freeze_pattern_count = 0

    for pattern in patterns:
        pattern_freeze = _as_datetime(
            pattern["freeze_timestamp"], "pattern.freeze_timestamp"
        )
        if generated_dt <= pattern_freeze:
            exclusions.append(
                {
                    "scope": "PATTERN",
                    "pattern_id": pattern["pattern_id"],
                    "pattern_version": pattern["pattern_version"],
                    "reason": "SNAPSHOT_NOT_STRICTLY_POST_FREEZE",
                }
            )
            continue
        post_freeze_pattern_count += 1
        governance = _governance_authorization(
            pattern,
            hypothesis_registry=hypothesis_registry,
            analysis_plan_registry=analysis_plan_registry,
            contract=spec,
        )

        matched = 0
        unavailable = 0
        no_match = 0
        claim_count = 0
        session_missing = 0
        for row in current_rows:
            match = match_frozen_pattern(
                pattern,
                row,
                history_rows,
                snapshot_generated_at=metadata["generated_at"],
                feature_library=library,
                contract=spec,
            )
            if match["status"] == "UNAVAILABLE":
                unavailable += 1
                continue
            if match["status"] == "NO_MATCH":
                no_match += 1
                continue
            matched += 1
            session = sessions_by_symbol.get(str(row["symbol"]))
            if session is None:
                session_missing += 1
                exclusions.append(
                    {
                        "scope": "MATCH",
                        "pattern_id": pattern["pattern_id"],
                        "pattern_version": pattern["pattern_version"],
                        "symbol": row["symbol"],
                        "reason": "START_MARKET_SESSION_UNAVAILABLE",
                    }
                )
                continue
            claim = _build_claim(
                pattern=pattern,
                row=row,
                match=match,
                session=session,
                governance=governance,
                snapshot_binding=snapshot_binding,
                history_binding=history_binding,
                session_map_hash=session_map_hash,
                capture_at=capture_ts,
                contract=spec,
            )
            verify_prospective_claim(claim, contract=spec)
            claims.append(claim)
            claim_count += 1

        pattern_summaries.append(
            {
                "pattern_id": pattern["pattern_id"],
                "pattern_version": pattern["pattern_version"],
                "pattern_spec_hash": pattern["pattern_spec_hash"],
                "evaluated_symbol_count": len(current_rows),
                "matched_symbol_count": matched,
                "unavailable_symbol_count": unavailable,
                "no_match_symbol_count": no_match,
                "missing_start_session_count": session_missing,
                "prospective_claim_count": claim_count,
            }
        )

    claims.sort(
        key=lambda claim: (
            str(claim["pattern"]["pattern_id"]),
            str(claim["pattern"]["pattern_version"]),
            str(claim["match"]["symbol"]),
            str(claim["claim_id"]),
        )
    )
    claim_ids = [str(claim["claim_id"]) for claim in claims]
    if len(claim_ids) != len(set(claim_ids)):
        raise ProspectiveCaptureError("duplicate_claim_identity_in_capture")

    contract_hash = prospective_capture_contract_hash(spec)
    capture_identity = {
        "l7_contract_hash": contract_hash,
        "l5_snapshot_hashes": l5_snapshot_hashes,
        "snapshot_binding_hash": snapshot_binding["snapshot_binding_hash"],
        "history_binding_hash": history_binding["history_binding_hash"],
        "session_map_hash": session_map_hash,
        "captured_at": capture_ts,
    }
    capture_id = f"PCAP-{_hash(capture_identity)[:24].upper()}"
    report: dict[str, Any] = {
        "schema_version": CAPTURE_REPORT_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L7",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "capture_id": capture_id,
        "captured_at": capture_ts,
        "l7_contract_hash": contract_hash,
        "l5_snapshot_hashes": l5_snapshot_hashes,
        "snapshot_binding": snapshot_binding,
        "history_binding": history_binding,
        "session_map_hash": session_map_hash,
        "counts": {
            "frozen_pattern_count": len(patterns),
            "post_freeze_pattern_count": post_freeze_pattern_count,
            "current_symbol_count": len(current_rows),
            "prospective_claim_count": len(claims),
            "excluded_item_count": len(exclusions),
        },
        "pattern_summaries": sorted(
            pattern_summaries,
            key=lambda item: (
                str(item["pattern_id"]),
                str(item["pattern_version"]),
            ),
        ),
        "exclusions": sorted(
            exclusions,
            key=lambda item: _canonical_json(item),
        ),
        "claims": claims,
        "boundaries": {
            "outcome_information_used": False,
            "outcome_maturation_performed": False,
            "confirmation_evaluation_performed": False,
            "sequential_monitoring_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    _assert_no_outcome_payload(report, contract=spec)
    PatternDiscoveryBoundary().assert_research_payload(report)
    report["capture_hash"] = _hash(report)
    return report


def verify_capture_report(
    report: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        raise ProspectiveCaptureError("capture_report_must_be_object")
    if contract is not None:
        spec = dict(contract)
    else:
        # Historical v1 captures keep their original, immutable contract hash.
        # L8/L9 can validate them after v2 becomes the active L7 contract.
        stored_contract_hash = report.get("l7_contract_hash")
        candidates = (
            load_prospective_capture_contract(),
            load_prospective_capture_contract(LEGACY_CONTRACT_PATH),
        )
        spec = next(
            (
                item for item in candidates
                if prospective_capture_contract_hash(item) == stored_contract_hash
            ),
            candidates[0],
        )
    if report.get("l7_contract_hash") != prospective_capture_contract_hash(spec):
        raise ProspectiveCaptureError("capture_report_contract_hash_mismatch")
    if report.get("schema_version") != CAPTURE_REPORT_SCHEMA_VERSION:
        raise ProspectiveCaptureError("capture_report_schema_invalid")
    if report.get("research_only") is not True:
        raise ProspectiveCaptureError("capture_report_research_only_guard_missing")
    if report.get("productive_integration_enabled") is not False:
        raise ProspectiveCaptureError(
            "capture_report_productive_integration_forbidden"
        )
    if report.get("execution_allowed") is not False:
        raise ProspectiveCaptureError("capture_report_execution_forbidden")
    _assert_no_outcome_payload(report, contract=spec)

    stored = _sha256_text(report.get("capture_hash"), "capture_hash")
    body = dict(report)
    body.pop("capture_hash", None)
    if _hash(body) != stored:
        raise ProspectiveCaptureError("capture_report_hash_mismatch")

    claims = report.get("claims")
    if not isinstance(claims, Sequence) or isinstance(
        claims, (str, bytes, bytearray)
    ):
        raise ProspectiveCaptureError("capture_report_claims_sequence_required")
    for claim in claims:
        verify_prospective_claim(claim, contract=spec)
        if claim.get("captured_at") != report.get("captured_at"):
            raise ProspectiveCaptureError("capture_report_claim_timestamp_mismatch")
        if claim.get("match", {}).get("snapshot_binding_hash") != report.get(
            "snapshot_binding", {}
        ).get("snapshot_binding_hash"):
            raise ProspectiveCaptureError("capture_report_snapshot_binding_mismatch")
    if report.get("counts", {}).get("prospective_claim_count") != len(claims):
        raise ProspectiveCaptureError("capture_report_claim_count_mismatch")
    boundaries = report.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ProspectiveCaptureError("capture_report_boundaries_missing")
    if boundaries.get("outcome_information_used") is not False:
        raise ProspectiveCaptureError("capture_report_outcome_guard_invalid")
    PatternDiscoveryBoundary().assert_research_payload(report)
    return {
        "valid": True,
        "capture_id": report.get("capture_id"),
        "capture_hash": stored,
        "prospective_claim_count": len(claims),
    }


class ProspectiveClaimRegistry:
    """Append-only, hash-chained registry of immutable prospective claims."""

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
            else load_prospective_capture_contract()
        )

    def _read_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ProspectiveCaptureError(
                    f"prospective_registry_invalid_json:{line_number}"
                ) from exc
            if not isinstance(event, dict):
                raise ProspectiveCaptureError(
                    f"prospective_registry_event_not_object:{line_number}"
                )
            events.append(event)
        return events

    def _verify_hash_chain(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        previous: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise ProspectiveCaptureError(
                    f"prospective_registry_schema_invalid:{expected_sequence}"
                )
            if event.get("sequence") != expected_sequence:
                raise ProspectiveCaptureError(
                    f"prospective_registry_sequence_invalid:{expected_sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise ProspectiveCaptureError(
                    f"prospective_registry_previous_hash_invalid:{expected_sequence}"
                )
            stored = _sha256_text(
                event.get("entry_hash"),
                f"registry.entry_hash.{expected_sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise ProspectiveCaptureError(
                    f"prospective_registry_entry_hash_invalid:{expected_sequence}"
                )
            previous = stored

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        claims: dict[str, dict[str, Any]] = {}
        for event in events:
            if event.get("event_type") != "CLAIM_CAPTURED":
                raise ProspectiveCaptureError(
                    f"prospective_registry_event_type_unknown:{event.get('event_type')}"
                )
            claim = event.get("claim")
            if not isinstance(claim, Mapping):
                raise ProspectiveCaptureError("prospective_registry_claim_missing")
            verify_prospective_claim(claim, contract=self.contract)
            claim_id = _text(claim.get("claim_id"), "claim_id")
            if claim_id in claims:
                raise ProspectiveCaptureError(
                    f"prospective_claim_duplicate_in_registry:{claim_id}"
                )
            claims[claim_id] = dict(claim)
        return claims

    def _load(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def get_claim(self, claim_id: str) -> dict[str, Any]:
        _, claims = self._load()
        key = _text(claim_id, "claim_id")
        if key not in claims:
            raise ProspectiveCaptureError(
                f"prospective_claim_not_found:{key}"
            )
        return dict(claims[key])

    def register_claims(
        self,
        claims: Sequence[Mapping[str, Any]],
        *,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        if isinstance(claims, (str, bytes, bytearray)) or not isinstance(
            claims, Sequence
        ):
            raise ProspectiveCaptureError("claims_sequence_required")
        actor = _text(actor_id, "actor_id")
        role = _text(actor_role, "actor_role")
        normalized = [dict(claim) for claim in claims]
        for claim in normalized:
            verify_prospective_claim(claim, contract=self.contract)
        ids = [str(claim["claim_id"]) for claim in normalized]
        if len(ids) != len(set(ids)):
            raise ProspectiveCaptureError("duplicate_claim_id_in_batch")

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
                raise ProspectiveCaptureError(
                    "prospective_registry_lock_already_held"
                ) from exc

            events, existing = self._load()
            new_claims: list[dict[str, Any]] = []
            idempotent_count = 0
            for claim in normalized:
                claim_id = str(claim["claim_id"])
                if claim_id not in existing:
                    new_claims.append(claim)
                    continue
                if existing[claim_id] == claim:
                    idempotent_count += 1
                    continue
                raise ProspectiveCaptureError(
                    f"prospective_claim_identity_collision:{claim_id}"
                )

            staged_events = [dict(event) for event in events]
            previous = (
                staged_events[-1]["entry_hash"]
                if staged_events
                else None
            )
            for claim in new_claims:
                event: dict[str, Any] = {
                    "schema_version": EVENT_SCHEMA_VERSION,
                    "sequence": len(staged_events) + 1,
                    "event_id": (
                        "PCE-"
                        + _hash(
                            {
                                "claim_id": claim["claim_id"],
                                "claim_hash": claim["claim_hash"],
                            }
                        )[:24].upper()
                    ),
                    "event_type": "CLAIM_CAPTURED",
                    "recorded_at": claim["captured_at"],
                    "actor_id": actor,
                    "actor_role": role,
                    "claim": claim,
                    "previous_event_hash": previous,
                }
                event["entry_hash"] = _hash(event)
                staged_events.append(event)
                previous = event["entry_hash"]

            self._verify_hash_chain(staged_events)
            self._replay(staged_events)

            if new_claims:
                payload = "".join(
                    _canonical_json(event) + "\n"
                    for event in staged_events[len(events):]
                ).encode("utf-8")
                fd = os.open(
                    self.path,
                    os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                    0o644,
                )
                try:
                    view = memoryview(payload)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise ProspectiveCaptureError(
                                "prospective_registry_short_write"
                            )
                        view = view[written:]
                    os.fsync(fd)
                finally:
                    os.close(fd)
            status = self.verify_integrity()
            return {
                "valid": True,
                "requested_claim_count": len(normalized),
                "appended_claim_count": len(new_claims),
                "idempotent_claim_count": idempotent_count,
                **status,
            }
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def verify_integrity(self) -> dict[str, Any]:
        events, claims = self._load()
        return {
            "registry_event_count": len(events),
            "registry_claim_count": len(claims),
            "registry_head_hash": (
                events[-1]["entry_hash"] if events else None
            ),
        }


def prospective_claim_registry_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    return str(spec["storage"]["registry_path"])


def capture_report_repo_path(
    snapshot_id: str,
    capture_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    return str(spec["storage"]["capture_report_path_template"]).format(
        snapshot_id=_safe_token(snapshot_id, "snapshot_id"),
        capture_id=_safe_token(capture_id, "capture_id"),
    )


def persist_prospective_capture(
    repo_root: str | Path,
    report: Mapping[str, Any],
    *,
    actor_id: str,
    actor_role: str,
    boundary: PatternDiscoveryBoundary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically append all new claims, then persist the immutable capture report."""
    spec = (
        dict(contract)
        if contract is not None
        else load_prospective_capture_contract()
    )
    verify_capture_report(report, contract=spec)
    guard = boundary or PatternDiscoveryBoundary()
    root = Path(repo_root).resolve()

    registry_repo_path = prospective_claim_registry_repo_path(contract=spec)
    report_repo_path = capture_report_repo_path(
        str(report["snapshot_binding"]["snapshot_id"]),
        str(report["capture_id"]),
        contract=spec,
    )
    guard.assert_write_path_allowed(registry_repo_path)
    guard.assert_write_path_allowed(report_repo_path)

    registry_path = (root / registry_repo_path).resolve()
    report_path = (root / report_repo_path).resolve()
    try:
        registry_path.relative_to(root)
        report_path.relative_to(root)
    except ValueError as exc:
        raise ProspectiveCaptureError(
            "prospective_capture_path_outside_repo"
        ) from exc

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_already_exists = report_path.exists()
    if report_already_exists:
        try:
            existing = json.loads(
                report_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise ProspectiveCaptureError(
                "existing_capture_report_unreadable"
            ) from exc
        verify_capture_report(existing, contract=spec)
        if existing != dict(report):
            raise ProspectiveCaptureError(
                f"capture_report_identity_collision:{report_repo_path}"
            )

    registry = ProspectiveClaimRegistry(
        registry_path,
        contract=spec,
    )
    registry_status = registry.register_claims(
        report["claims"],
        actor_id=actor_id,
        actor_role=actor_role,
    )

    if not report_already_exists:
        with report_path.open(
            "x",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                dict(report),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")

    return {
        "valid": True,
        "capture_id": report["capture_id"],
        "capture_hash": report["capture_hash"],
        "prospective_claim_count": len(report["claims"]),
        "registry": registry_status,
        "registry_path": str(registry_path),
        "capture_report_path": str(report_path),
    }
