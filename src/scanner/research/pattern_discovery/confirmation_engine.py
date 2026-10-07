"""Phase L9 Confirmation & Falsification Engine.

L9 evaluates immutable L5 Pattern versions using only hash-valid L8 matured
outcomes and strictly post-freeze baseline/control observations.

Existing QM-C governance remains authoritative:
- QM-C1 hypothesis identity
- QM-C2 analysis-plan freeze
- QM-C3 family membership / multiplicity
- QM-C4 sequential look schedule
- QM-C5 result retention

Because the L0 Pattern Discovery boundary forbids direct writes to the central
QM registries, L9 produces exact handoff payloads for QM-C4/QM-C5 rather than
mutating them. L9 itself persists only immutable research artifacts inside
artifacts/research/pattern_discovery/.

L9 does not assign Pattern ratings, promote Patterns, or create productive
Decision/Portfolio/Execution authority.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import random
from statistics import median
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import (
    FamilyMultiplicityRegistry,
)
from scanner.research.governance.qm_c_sequential_monitoring import (
    SequentialMonitoringRegistry,
)

from .boundary import PatternDiscoveryBoundary
from .candidate_registry import validate_applied_qm_handoff
from .outcome_maturation import verify_matured_outcome


SCHEMA_VERSION = "pattern_discovery_l9_confirmation_engine_v1"
BASELINE_SCHEMA_VERSION = "pattern_discovery_l9_baseline_bundle_v1"
CONTEXT_SCHEMA_VERSION = "pattern_discovery_l9_context_bundle_v1"
LOOK_SCHEMA_VERSION = "pattern_discovery_l9_confirmation_look_v1"
EVENT_SCHEMA_VERSION = "pattern_discovery_l9_confirmation_registry_event_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l9_confirmation_engine_v1.json"
)


class ConfirmationEngineError(ValueError):
    """Raised when an L9 confirmatory invariant is violated."""


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
        raise ConfirmationEngineError(f"value_required:{field}")
    return result


def _safe_token(value: Any, field: str) -> str:
    result = _text(value, field)
    allowed = set(
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789._:-"
    )
    if any(char not in allowed for char in result):
        raise ConfirmationEngineError(f"safe_token_required:{field}")
    return result


def _sha256_text(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(ch not in "0123456789abcdef" for ch in result):
        raise ConfirmationEngineError(f"sha256_required:{field}")
    return result


def _as_datetime(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfirmationEngineError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise ConfirmationEngineError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: Any, field: str) -> str:
    return _as_datetime(value, field).isoformat().replace("+00:00", "Z")


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ConfirmationEngineError(f"finite_number_required:{field}")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ConfirmationEngineError(f"finite_number_required:{field}") from exc
    if not math.isfinite(result):
        raise ConfirmationEngineError(f"finite_number_required:{field}")
    return result


def _percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = position - lo
    return ordered[lo] * (1.0 - fraction) + ordered[hi] * fraction


def load_confirmation_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfirmationEngineError(
            f"confirmation_contract_unreadable:{target}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ConfirmationEngineError("confirmation_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise ConfirmationEngineError("confirmation_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise ConfirmationEngineError("confirmation_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise ConfirmationEngineError("confirmation_execution_forbidden")
    principles = payload.get("principles") or {}
    if principles.get("discovery_evidence_never_counts_as_confirmation") is not True:
        raise ConfirmationEngineError("discovery_confirmation_separation_required")
    if principles.get("missing_maturity_is_unresolved_not_falsified") is not True:
        raise ConfirmationEngineError("missing_maturity_guard_required")
    if principles.get("no_posthoc_subgroup_rescue") is not True:
        raise ConfirmationEngineError("posthoc_subgroup_guard_required")
    return payload


def confirmation_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    return _hash(value)


def _verify_frozen_pattern(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise ConfirmationEngineError("frozen_pattern_must_be_object")
    if record.get("schema_version") != "pattern_discovery_l5_frozen_pattern_v1":
        raise ConfirmationEngineError("frozen_pattern_schema_invalid")
    if record.get("research_only") is not True:
        raise ConfirmationEngineError("frozen_pattern_research_only_guard_missing")
    stored = _sha256_text(
        record.get("frozen_record_hash"),
        "frozen_record_hash",
    )
    body = dict(record)
    body.pop("frozen_record_hash", None)
    if _hash(body) != stored:
        raise ConfirmationEngineError("frozen_pattern_hash_mismatch")
    pattern_spec = record.get("pattern_spec")
    if not isinstance(pattern_spec, Mapping):
        raise ConfirmationEngineError("frozen_pattern_spec_missing")
    if _hash(pattern_spec) != record.get("pattern_spec_hash"):
        raise ConfirmationEngineError("frozen_pattern_spec_hash_mismatch")
    identity = pattern_spec.get("identity") or {}
    if identity.get("pattern_id") != record.get("pattern_id"):
        raise ConfirmationEngineError("frozen_pattern_id_mismatch")
    if identity.get("pattern_version") != record.get("pattern_version"):
        raise ConfirmationEngineError("frozen_pattern_version_mismatch")
    if record.get("confirmation_data_used") is not False:
        raise ConfirmationEngineError("frozen_pattern_confirmation_data_contaminated")
    return {
        "valid": True,
        "pattern_id": record["pattern_id"],
        "pattern_version": record["pattern_version"],
        "pattern_spec_hash": record["pattern_spec_hash"],
        "frozen_record_hash": stored,
    }


def build_baseline_bundle(
    pattern_baselines: Sequence[Mapping[str, Any]],
    *,
    baseline_bundle_id: str,
    generated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if isinstance(pattern_baselines, (str, bytes, bytearray)) or not isinstance(
        pattern_baselines, Sequence
    ):
        raise ConfirmationEngineError("pattern_baselines_sequence_required")
    bundle: dict[str, Any] = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "research_only": True,
        "baseline_bundle_id": _safe_token(
            baseline_bundle_id,
            "baseline_bundle_id",
        ),
        "generated_at": _timestamp(generated_at, "generated_at"),
        "pattern_baselines": [dict(value) for value in pattern_baselines],
    }
    bundle["baseline_bundle_hash"] = _hash(bundle)
    verify_baseline_bundle(bundle, contract=spec)
    return bundle


def verify_baseline_bundle(
    bundle: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if not isinstance(bundle, Mapping):
        raise ConfirmationEngineError("baseline_bundle_must_be_object")
    if bundle.get("schema_version") != BASELINE_SCHEMA_VERSION:
        raise ConfirmationEngineError("baseline_bundle_schema_invalid")
    if bundle.get("research_only") is not True:
        raise ConfirmationEngineError("baseline_bundle_research_only_guard_missing")
    _safe_token(bundle.get("baseline_bundle_id"), "baseline_bundle_id")
    _timestamp(bundle.get("generated_at"), "baseline_bundle.generated_at")
    stored = _sha256_text(
        bundle.get("baseline_bundle_hash"),
        "baseline_bundle_hash",
    )
    body = dict(bundle)
    body.pop("baseline_bundle_hash", None)
    if _hash(body) != stored:
        raise ConfirmationEngineError("baseline_bundle_hash_mismatch")

    required = tuple(
        spec["baseline_bundle"]["pattern_baseline_required_fields"]
    )
    observation_required = tuple(
        spec["baseline_bundle"]["observation_required_fields"]
    )
    values = bundle.get("pattern_baselines")
    if not isinstance(values, list):
        raise ConfirmationEngineError("pattern_baselines_list_required")
    seen_patterns: set[tuple[str, str]] = set()
    event_ids: set[str] = set()

    for index, raw in enumerate(values):
        if not isinstance(raw, Mapping):
            raise ConfirmationEngineError(
                f"pattern_baseline_must_be_object:{index}"
            )
        missing = [field for field in required if field not in raw]
        if missing:
            raise ConfirmationEngineError(
                f"pattern_baseline_fields_missing:{index}:"
                + ",".join(missing)
            )
        pid = _safe_token(raw["pattern_id"], f"pattern_baselines[{index}].pattern_id")
        pver = _safe_token(
            raw["pattern_version"],
            f"pattern_baselines[{index}].pattern_version",
        )
        _sha256_text(
            raw["pattern_spec_hash"],
            f"pattern_baselines[{index}].pattern_spec_hash",
        )
        key = (pid, pver)
        if key in seen_patterns:
            raise ConfirmationEngineError(
                f"duplicate_pattern_baseline:{pid}:{pver}"
            )
        seen_patterns.add(key)
        _text(
            raw["baseline_definition"],
            f"pattern_baselines[{index}].baseline_definition",
        )
        _safe_token(
            raw["population_source_id"],
            f"pattern_baselines[{index}].population_source_id",
        )
        _sha256_text(
            raw["population_source_hash"],
            f"pattern_baselines[{index}].population_source_hash",
        )
        _safe_token(
            raw["selection_rule_id"],
            f"pattern_baselines[{index}].selection_rule_id",
        )
        _sha256_text(
            raw["selection_rule_hash"],
            f"pattern_baselines[{index}].selection_rule_hash",
        )
        eligible_population_count = int(raw["eligible_population_count"])
        if eligible_population_count < 0:
            raise ConfirmationEngineError(
                f"baseline_eligible_population_count_invalid:{pid}:{pver}"
            )
        _text(raw["target_id"], f"pattern_baselines[{index}].target_id")
        horizon = int(raw["horizon_sessions"])
        if horizon <= 0:
            raise ConfirmationEngineError("baseline_horizon_must_be_positive")
        observations = raw["observations"]
        if not isinstance(observations, list):
            raise ConfirmationEngineError(
                f"baseline_observations_list_required:{pid}:{pver}"
            )
        if len(observations) != eligible_population_count:
            raise ConfirmationEngineError(
                f"baseline_eligible_population_not_fully_present:{pid}:{pver}"
            )
        for obs_index, obs in enumerate(observations):
            if not isinstance(obs, Mapping):
                raise ConfirmationEngineError(
                    f"baseline_observation_must_be_object:{pid}:{obs_index}"
                )
            missing_obs = [
                field for field in observation_required if field not in obs
            ]
            if missing_obs:
                raise ConfirmationEngineError(
                    f"baseline_observation_fields_missing:{pid}:{obs_index}:"
                    + ",".join(missing_obs)
                )
            event_id = _safe_token(
                obs["baseline_event_id"],
                f"baseline[{pid}].observations[{obs_index}].baseline_event_id",
            )
            if event_id in event_ids:
                raise ConfirmationEngineError(
                    f"duplicate_baseline_event_id:{event_id}"
                )
            event_ids.add(event_id)
            _text(obs["symbol"], f"baseline[{pid}].symbol")
            start = _as_datetime(obs["start_at"], f"baseline[{pid}].start_at")
            end = _as_datetime(obs["end_at"], f"baseline[{pid}].end_at")
            if end <= start:
                raise ConfirmationEngineError(
                    f"baseline_end_not_after_start:{event_id}"
                )
            _finite(obs["target_value"], f"baseline[{pid}].target_value")
            if _text(obs["target_id"], f"baseline[{pid}].target_id") != str(
                raw["target_id"]
            ):
                raise ConfirmationEngineError(
                    f"baseline_target_id_mismatch:{event_id}"
                )
            if int(obs["horizon_sessions"]) != horizon:
                raise ConfirmationEngineError(
                    f"baseline_horizon_mismatch:{event_id}"
                )
            _sha256_text(
                obs["source_hash"],
                f"baseline[{pid}].source_hash",
            )
    return {
        "valid": True,
        "baseline_bundle_id": bundle["baseline_bundle_id"],
        "baseline_bundle_hash": stored,
        "pattern_count": len(values),
        "observation_count": len(event_ids),
    }


def build_context_bundle(
    claim_contexts: Sequence[Mapping[str, Any]],
    *,
    context_bundle_id: str,
    generated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if isinstance(claim_contexts, (str, bytes, bytearray)) or not isinstance(
        claim_contexts, Sequence
    ):
        raise ConfirmationEngineError("claim_contexts_sequence_required")
    bundle: dict[str, Any] = {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "research_only": True,
        "context_bundle_id": _safe_token(
            context_bundle_id,
            "context_bundle_id",
        ),
        "generated_at": _timestamp(generated_at, "generated_at"),
        "claim_contexts": [dict(value) for value in claim_contexts],
    }
    bundle["context_bundle_hash"] = _hash(bundle)
    verify_context_bundle(bundle, contract=spec)
    return bundle


def verify_context_bundle(
    bundle: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if not isinstance(bundle, Mapping):
        raise ConfirmationEngineError("context_bundle_must_be_object")
    if bundle.get("schema_version") != CONTEXT_SCHEMA_VERSION:
        raise ConfirmationEngineError("context_bundle_schema_invalid")
    if bundle.get("research_only") is not True:
        raise ConfirmationEngineError("context_bundle_research_only_guard_missing")
    _safe_token(bundle.get("context_bundle_id"), "context_bundle_id")
    _timestamp(bundle.get("generated_at"), "context_bundle.generated_at")
    stored = _sha256_text(
        bundle.get("context_bundle_hash"),
        "context_bundle_hash",
    )
    body = dict(bundle)
    body.pop("context_bundle_hash", None)
    if _hash(body) != stored:
        raise ConfirmationEngineError("context_bundle_hash_mismatch")

    required = tuple(
        spec["context_bundle"]["claim_context_required_fields"]
    )
    allowed_optional = set(
        spec["context_bundle"]["allowed_optional_fields"]
    )
    contexts = bundle.get("claim_contexts")
    if not isinstance(contexts, list):
        raise ConfirmationEngineError("claim_contexts_list_required")
    seen: set[str] = set()
    for index, raw in enumerate(contexts):
        if not isinstance(raw, Mapping):
            raise ConfirmationEngineError(
                f"claim_context_must_be_object:{index}"
            )
        missing = [field for field in required if field not in raw]
        if missing:
            raise ConfirmationEngineError(
                f"claim_context_fields_missing:{index}:"
                + ",".join(missing)
            )
        extra = set(raw) - set(required) - allowed_optional
        if extra:
            raise ConfirmationEngineError(
                f"claim_context_unknown_fields:{index}:"
                + ",".join(sorted(extra))
            )
        claim_id = _safe_token(
            raw["claim_id"],
            f"claim_contexts[{index}].claim_id",
        )
        if claim_id in seen:
            raise ConfirmationEngineError(
                f"duplicate_claim_context:{claim_id}"
            )
        seen.add(claim_id)
        _text(raw["symbol"], f"claim_contexts[{index}].symbol")
        _timestamp(
            raw["observation_as_of"],
            f"claim_contexts[{index}].observation_as_of",
        )
        _sha256_text(
            raw["context_source_hash"],
            f"claim_contexts[{index}].context_source_hash",
        )
    return {
        "valid": True,
        "context_bundle_id": bundle["context_bundle_id"],
        "context_bundle_hash": stored,
        "context_count": len(contexts),
    }


def _frozen_minimums(pattern: Mapping[str, Any]) -> dict[str, float | int]:
    criteria = (
        pattern.get("pattern_spec", {})
        .get("statistics", {})
        .get("minimum_criteria")
    )
    if not isinstance(criteria, Mapping):
        raise ConfirmationEngineError("frozen_pattern_minimum_criteria_missing")
    required = (
        "minimum_raw_n",
        "minimum_temporal_support_regions",
        "minimum_effect_size",
        "minimum_baseline_lift",
    )
    missing = [field for field in required if field not in criteria]
    if missing:
        raise ConfirmationEngineError(
            "frozen_pattern_minimum_criteria_fields_missing:"
            + ",".join(missing)
        )
    return {
        "minimum_raw_n": int(criteria["minimum_raw_n"]),
        "minimum_temporal_support_regions": int(
            criteria["minimum_temporal_support_regions"]
        ),
        "minimum_effect_size": _finite(
            criteria["minimum_effect_size"],
            "minimum_effect_size",
        ),
        "minimum_baseline_lift": _finite(
            criteria["minimum_baseline_lift"],
            "minimum_baseline_lift",
        ),
    }


def _pattern_forecast(pattern: Mapping[str, Any]) -> dict[str, Any]:
    forecast = pattern.get("pattern_spec", {}).get("forecast")
    if not isinstance(forecast, Mapping):
        raise ConfirmationEngineError("frozen_pattern_forecast_missing")
    return {
        "target_id": _text(forecast.get("target_id"), "pattern.target_id"),
        "expected_direction": _text(
            forecast.get("expected_direction"),
            "pattern.expected_direction",
        ),
        "horizon_sessions": int(forecast.get("horizon_sessions")),
        "baseline": _text(forecast.get("baseline"), "pattern.baseline"),
        "reference_definition": _text(
            forecast.get("reference_definition"),
            "pattern.reference_definition",
        ),
    }


def _pattern_key(pattern: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _safe_token(pattern.get("pattern_id"), "pattern_id"),
        _safe_token(pattern.get("pattern_version"), "pattern_version"),
    )


def _expected_baseline_selection_rule(
    pattern: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> tuple[str, str]:
    forecast = _pattern_forecast(pattern)
    baseline = forecast["baseline"]
    rules = contract["baseline_bundle"].get(
        "supported_baseline_selection_rules"
    ) or {}
    rule_id = rules.get(baseline)
    if not rule_id:
        raise ConfirmationEngineError(
            f"l9_baseline_selection_rule_not_supported:{baseline}"
        )
    universe_version = _text(
        pattern.get("pattern_spec", {}).get("data", {}).get("universe_version"),
        "pattern.universe_version",
    )
    rule_hash = _hash(
        {
            "selection_rule_id": rule_id,
            "baseline_definition": baseline,
            "universe_version": universe_version,
            "target_id": forecast["target_id"],
            "horizon_sessions": forecast["horizon_sessions"],
        }
    )
    return str(rule_id), rule_hash


def _pattern_baseline_map(
    bundle: Mapping[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (
            str(item["pattern_id"]),
            str(item["pattern_version"]),
        ): dict(item)
        for item in bundle["pattern_baselines"]
    }


def _context_map(bundle: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if bundle is None:
        return {}
    return {
        str(item["claim_id"]): dict(item)
        for item in bundle["claim_contexts"]
    }


def _aligned(value: float, direction: str) -> float:
    if direction == "POSITIVE":
        return float(value)
    if direction == "NEGATIVE":
        return -float(value)
    raise ConfirmationEngineError(f"expected_direction_invalid:{direction}")


def _date_key(timestamp: str) -> str:
    return str(timestamp)[:10]


def _support_regions(
    candidate_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    *,
    block_length: int,
) -> tuple[int, dict[str, int], list[str]]:
    date_axis = sorted(
        {
            *(_date_key(str(row["start_at"])) for row in candidate_rows),
            *(_date_key(str(row["start_at"])) for row in baseline_rows),
        }
    )
    positions = {day: index for index, day in enumerate(date_axis)}
    candidate_dates = sorted(
        {_date_key(str(row["start_at"])) for row in candidate_rows},
        key=lambda day: positions[day],
    )
    region_by_date: dict[str, int] = {}
    region = -1
    region_start: int | None = None
    for day in candidate_dates:
        pos = positions[day]
        if region_start is None or pos - region_start >= block_length:
            region += 1
            region_start = pos
        region_by_date[day] = region
    return (
        len(set(region_by_date.values())),
        region_by_date,
        date_axis,
    )


def _hhi(counter: Counter[str]) -> float | None:
    total = sum(counter.values())
    if total <= 0:
        return None
    return float(sum((count / total) ** 2 for count in counter.values()))


def _concentration(
    rows: Sequence[Mapping[str, Any]],
    region_by_date: Mapping[str, int],
) -> dict[str, Any]:
    symbols = Counter(str(row["symbol"]) for row in rows)
    dates = Counter(_date_key(str(row["start_at"])) for row in rows)
    regions = Counter(
        str(region_by_date[_date_key(str(row["start_at"]))])
        for row in rows
        if _date_key(str(row["start_at"])) in region_by_date
    )

    def top_share(counter: Counter[str]) -> float | None:
        total = sum(counter.values())
        return (
            max(counter.values()) / total
            if total and counter
            else None
        )

    return {
        "top_symbol_share": top_share(symbols),
        "symbol_hhi": _hhi(symbols),
        "top_observation_date_share": top_share(dates),
        "observation_date_hhi": _hhi(dates),
        "top_support_region_share": top_share(regions),
        "support_region_hhi": _hhi(regions),
    }


def _effective_n(
    rows: Sequence[Mapping[str, Any]],
    region_by_date: Mapping[str, int],
) -> int:
    clusters = {
        (
            str(row["symbol"]),
            int(region_by_date[_date_key(str(row["start_at"]))]),
        )
        for row in rows
        if _date_key(str(row["start_at"])) in region_by_date
    }
    return len(clusters)


def _bootstrap_intervals(
    candidate_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    *,
    horizon: int,
    seed_material: str,
    support_region_count: int,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    stats = contract["statistics"]
    repetitions = int(stats["bootstrap_repetitions"])
    block_length = max(
        1,
        int(stats["block_length_sessions_multiplier"]) * int(horizon),
    )
    low_q, high_q = [
        float(value) for value in stats["bootstrap_interval"]
    ]
    date_axis = sorted(
        {
            *(_date_key(str(row["start_at"])) for row in candidate_rows),
            *(_date_key(str(row["start_at"])) for row in baseline_rows),
        }
    )
    diagnostics: dict[str, Any] = {
        "method": stats["bootstrap_method"],
        "block_length_sessions": block_length,
        "date_axis_count": len(date_axis),
        "support_region_count": support_region_count,
        "repetitions_requested": repetitions,
        "repetitions_completed": 0,
        "effect_size_interval_95": None,
        "probability_lift_interval_95": None,
        "direction_probability_interval_95": None,
        "baseline_probability_interval_95": None,
    }
    if (
        repetitions <= 0
        or len(date_axis) < 2
        or not candidate_rows
        or not baseline_rows
        or support_region_count < 2
    ):
        return diagnostics

    candidate_by_day: dict[str, list[float]] = defaultdict(list)
    baseline_by_day: dict[str, list[float]] = defaultdict(list)
    for row in candidate_rows:
        candidate_by_day[_date_key(str(row["start_at"]))].append(
            float(row["aligned_value"])
        )
    for row in baseline_rows:
        baseline_by_day[_date_key(str(row["start_at"]))].append(
            float(row["aligned_value"])
        )

    span = min(block_length, len(date_axis))
    blocks = [
        [
            date_axis[(start + offset) % len(date_axis)]
            for offset in range(span)
        ]
        for start in range(len(date_axis))
    ]
    draws = int(math.ceil(len(date_axis) / span))
    seed = int.from_bytes(
        sha256(seed_material.encode("utf-8")).digest()[:8],
        "big",
    )
    rng = random.Random(seed)

    effects: list[float] = []
    lifts: list[float] = []
    candidate_probabilities: list[float] = []
    baseline_probabilities: list[float] = []
    for _ in range(repetitions):
        chosen = [rng.randrange(len(blocks)) for _ in range(draws)]
        sampled_dates = [
            day
            for block_index in chosen
            for day in blocks[block_index]
        ][: len(date_axis)]
        candidate = [
            value
            for day in sampled_dates
            for value in candidate_by_day.get(day, [])
        ]
        baseline = [
            value
            for day in sampled_dates
            for value in baseline_by_day.get(day, [])
        ]
        if not candidate or not baseline:
            continue
        candidate_mean = sum(candidate) / len(candidate)
        baseline_mean = sum(baseline) / len(baseline)
        candidate_p = sum(value > 0 for value in candidate) / len(candidate)
        baseline_p = sum(value > 0 for value in baseline) / len(baseline)
        effects.append(candidate_mean - baseline_mean)
        lifts.append(candidate_p - baseline_p)
        candidate_probabilities.append(candidate_p)
        baseline_probabilities.append(baseline_p)

    diagnostics["repetitions_completed"] = len(effects)
    if effects:
        diagnostics["effect_size_interval_95"] = [
            _percentile(effects, low_q),
            _percentile(effects, high_q),
        ]
        diagnostics["probability_lift_interval_95"] = [
            _percentile(lifts, low_q),
            _percentile(lifts, high_q),
        ]
        diagnostics["direction_probability_interval_95"] = [
            _percentile(candidate_probabilities, low_q),
            _percentile(candidate_probabilities, high_q),
        ]
        diagnostics["baseline_probability_interval_95"] = [
            _percentile(baseline_probabilities, low_q),
            _percentile(baseline_probabilities, high_q),
        ]
    return diagnostics


def _raw_dependency_aware_p(
    direction_probability: float | None,
    baseline_probability: float | None,
    effective_n: int,
) -> float:
    if (
        direction_probability is None
        or baseline_probability is None
        or effective_n <= 0
    ):
        return 1.0
    p0 = min(1.0, max(0.0, float(baseline_probability)))
    observed = min(1.0, max(0.0, float(direction_probability)))
    variance = p0 * (1.0 - p0) / effective_n
    if variance <= 0:
        return 0.0 if observed > p0 else 1.0
    z = (observed - p0) / math.sqrt(variance)
    return float(
        min(
            1.0,
            max(0.0, 0.5 * math.erfc(z / math.sqrt(2.0))),
        )
    )


def _adjust_family_pvalues(
    raw: Sequence[tuple[str, float]],
    *,
    strategy: str,
    parameters: Mapping[str, Any],
    planned_look_count: int,
    contract: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        (
            (key, min(1.0, max(0.0, float(p))))
            for key, p in raw
        ),
        key=lambda item: (item[1], item[0]),
    )
    if not ordered:
        return {}
    if planned_look_count <= 0:
        raise ConfirmationEngineError("planned_look_count_must_be_positive")
    supported = set(contract["multiplicity"]["supported_strategies"])
    if strategy not in supported:
        raise ConfirmationEngineError(
            f"l9_multiplicity_strategy_not_supported:{strategy}"
        )

    m = len(ordered)
    base_threshold: float
    adjusted: dict[str, tuple[float, float]] = {}

    if strategy == "PREDECLARED_SINGLE_PRIMARY":
        if m != 1:
            raise ConfirmationEngineError(
                "single_primary_requires_exactly_one_family_member"
            )
        base_threshold = float(
            contract["statistics"]["single_primary_alpha"]
        )
        key, p = ordered[0]
        adjusted[key] = (p, p)
    elif strategy == "BONFERRONI_FWER":
        base_threshold = _finite(
            parameters.get("family_alpha"),
            "multiplicity.family_alpha",
        )
        for key, p in ordered:
            adjusted[key] = (p, min(1.0, p * m))
    elif strategy == "HOLM_FWER":
        base_threshold = _finite(
            parameters.get("family_alpha"),
            "multiplicity.family_alpha",
        )
        running = 0.0
        for rank, (key, p) in enumerate(ordered, start=1):
            value = min(1.0, (m - rank + 1) * p)
            running = max(running, value)
            adjusted[key] = (p, min(1.0, running))
    else:
        base_threshold = _finite(
            parameters.get("fdr_q"),
            "multiplicity.fdr_q",
        )
        adjusted_values = [1.0] * m
        running = 1.0
        for index in range(m - 1, -1, -1):
            rank = index + 1
            p = ordered[index][1]
            running = min(running, p * m / rank)
            adjusted_values[index] = min(1.0, running)
        for index, (key, p) in enumerate(ordered):
            adjusted[key] = (p, adjusted_values[index])

    sequential_threshold = base_threshold / planned_look_count
    return {
        key: {
            "strategy": strategy,
            "raw_p_value": raw_p,
            "adjusted_p_value": adjusted_p,
            "base_threshold": base_threshold,
            "planned_look_count": planned_look_count,
            "sequential_alpha_method": contract["statistics"][
                "sequential_alpha_method"
            ],
            "sequential_threshold": sequential_threshold,
            "passed": adjusted_p <= sequential_threshold,
        }
        for key, (raw_p, adjusted_p) in adjusted.items()
    }


def _temporal_stability(
    rows: Sequence[Mapping[str, Any]],
    *,
    minimum_split_n: int,
) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row["start_at"]),
            str(row["symbol"]),
            str(row["claim_id"]),
        ),
    )
    if len(ordered) < minimum_split_n * 2:
        return {
            "status": "INSUFFICIENT",
            "minimum_split_n": minimum_split_n,
            "halves": {},
            "sign_reversal": None,
        }
    midpoint = len(ordered) // 2
    halves = {
        "FIRST_HALF": ordered[:midpoint],
        "SECOND_HALF": ordered[midpoint:],
    }
    diagnostics: dict[str, Any] = {}
    reversal = False
    for name, values in halves.items():
        aligned = [float(row["aligned_value"]) for row in values]
        mean_value = sum(aligned) / len(aligned)
        hit = sum(value > 0 for value in aligned) / len(aligned)
        diagnostics[name] = {
            "raw_n": len(values),
            "mean_aligned_outcome": mean_value,
            "direction_probability": hit,
            "start_min": min(str(row["start_at"]) for row in values),
            "start_max": max(str(row["start_at"]) for row in values),
        }
        if mean_value <= 0:
            reversal = True
    return {
        "status": "AVAILABLE",
        "minimum_split_n": minimum_split_n,
        "halves": diagnostics,
        "sign_reversal": reversal,
    }


def _regime_diagnostics(
    rows: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    *,
    minimum_split_n: int,
) -> dict[str, Any]:
    fields = ("market_regime_stock", "market_regime_crypto")
    with_context = [
        row for row in rows if str(row["claim_id"]) in contexts
    ]
    with_regime_context = []
    for row in rows:
        context = contexts.get(str(row["claim_id"]))
        if not context:
            continue
        if any(
            context.get(field) is not None
            and str(context.get(field)).strip()
            for field in fields
        ):
            with_regime_context.append(row)
    coverage = (
        len(with_context) / len(rows)
        if rows
        else 0.0
    )
    regime_coverage = (
        len(with_regime_context) / len(rows)
        if rows
        else 0.0
    )
    result: dict[str, Any] = {
        "context_coverage": coverage,
        "regime_context_coverage": regime_coverage,
        "fields": {},
        "qualified_sign_reversal": False,
    }
    for field in fields:
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            context = contexts.get(str(row["claim_id"]))
            if not context:
                continue
            value = context.get(field)
            if value is None or not str(value).strip():
                continue
            grouped[str(value)].append(float(row["aligned_value"]))
        field_result: dict[str, Any] = {}
        for state, values in sorted(grouped.items()):
            mean_value = sum(values) / len(values)
            field_result[state] = {
                "raw_n": len(values),
                "mean_aligned_outcome": mean_value,
                "direction_probability": (
                    sum(value > 0 for value in values) / len(values)
                ),
                "qualified_for_stability_gate": len(values) >= minimum_split_n,
            }
            if len(values) >= minimum_split_n and mean_value <= 0:
                result["qualified_sign_reversal"] = True
        result["fields"][field] = field_result
    return result


def _other_context_splits(
    rows: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    fields = (
        "sector",
        "segment",
        "pillar_primary",
        "cluster_official",
    )
    result: dict[str, Any] = {}
    for field in fields:
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            context = contexts.get(str(row["claim_id"]))
            if not context:
                continue
            value = context.get(field)
            if value is None or not str(value).strip():
                continue
            grouped[str(value)].append(float(row["aligned_value"]))
        if grouped:
            total = sum(len(values) for values in grouped.values())
            result[field] = {
                key: {
                    "raw_n": len(values),
                    "share": len(values) / total,
                    "mean_aligned_outcome": sum(values) / len(values),
                    "direction_probability": (
                        sum(value > 0 for value in values) / len(values)
                    ),
                }
                for key, values in sorted(grouped.items())
            }
    return result


def _normalize_pattern_outcomes(
    pattern: Mapping[str, Any],
    matured_outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    _verify_frozen_pattern(pattern)
    forecast = _pattern_forecast(pattern)
    freeze_at = _as_datetime(
        pattern["freeze_timestamp"],
        "pattern.freeze_timestamp",
    )
    result: list[dict[str, Any]] = []
    seen_claims: dict[str, str] = {}
    for raw in matured_outcomes:
        verify_matured_outcome(raw)
        claim = raw["claim"]
        if claim["pattern_id"] != pattern["pattern_id"]:
            continue
        if claim["pattern_version"] != pattern["pattern_version"]:
            continue
        if claim["pattern_spec_hash"] != pattern["pattern_spec_hash"]:
            raise ConfirmationEngineError(
                f"l8_outcome_pattern_spec_hash_mismatch:{claim['claim_id']}"
            )
        target = raw["target"]
        expected_checks = {
            "target_id": forecast["target_id"],
            "expected_direction": forecast["expected_direction"],
            "horizon_sessions": forecast["horizon_sessions"],
            "baseline": forecast["baseline"],
            "reference_definition": forecast["reference_definition"],
        }
        for field, expected in expected_checks.items():
            if target.get(field) != expected:
                raise ConfirmationEngineError(
                    f"l8_outcome_frozen_forecast_mismatch:{claim['claim_id']}:{field}"
                )
        start_at = _as_datetime(
            raw["horizon_provenance"]["start_at"],
            "matured_outcome.start_at",
        )
        if start_at <= freeze_at:
            raise ConfirmationEngineError(
                f"nonprospective_l8_outcome_after_freeze_required:{claim['claim_id']}"
            )
        claim_id = _safe_token(claim["claim_id"], "claim_id")
        outcome_hash = _sha256_text(raw["outcome_hash"], "outcome_hash")
        if claim_id in seen_claims:
            if seen_claims[claim_id] == outcome_hash:
                continue
            raise ConfirmationEngineError(
                f"claim_has_conflicting_matured_outcomes:{claim_id}"
            )
        seen_claims[claim_id] = outcome_hash
        target_value = _finite(
            raw["outcome"]["target_value"],
            f"outcome.target_value:{claim_id}",
        )
        result.append(
            {
                "claim_id": claim_id,
                "outcome_hash": outcome_hash,
                "symbol": _text(claim["symbol"], f"claim.symbol:{claim_id}"),
                "capture_snapshot_id": _safe_token(
                    claim["capture_snapshot_id"],
                    f"claim.capture_snapshot_id:{claim_id}",
                ),
                "capture_snapshot_binding_hash": _sha256_text(
                    claim["capture_snapshot_binding_hash"],
                    f"claim.capture_snapshot_binding_hash:{claim_id}",
                ),
                "start_at": raw["horizon_provenance"]["start_at"],
                "target_session_date": raw["horizon_provenance"][
                    "target_session_date"
                ],
                "target_value": target_value,
                "aligned_value": _aligned(
                    target_value,
                    forecast["expected_direction"],
                ),
                "return": raw["outcome"]["return"],
                "peer_excess": raw["outcome"]["peer_excess"],
                "adverse_excursion": raw["outcome"]["adverse_excursion"],
                "path_max_drawdown": raw["outcome"]["path_max_drawdown"],
            }
        )
    result.sort(
        key=lambda row: (
            str(row["start_at"]),
            str(row["symbol"]),
            str(row["claim_id"]),
        )
    )
    return result


def _normalize_pattern_baseline(
    pattern: Mapping[str, Any],
    baseline_record: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    forecast = _pattern_forecast(pattern)
    freeze_at = _as_datetime(
        pattern["freeze_timestamp"],
        "pattern.freeze_timestamp",
    )
    rule_id, rule_hash = _expected_baseline_selection_rule(
        pattern,
        contract=contract,
    )
    checks = {
        "pattern_id": pattern["pattern_id"],
        "pattern_version": pattern["pattern_version"],
        "pattern_spec_hash": pattern["pattern_spec_hash"],
        "baseline_definition": forecast["baseline"],
        "universe_version": pattern["pattern_spec"]["data"]["universe_version"],
        "selection_rule_id": rule_id,
        "selection_rule_hash": rule_hash,
        "target_id": forecast["target_id"],
        "horizon_sessions": forecast["horizon_sessions"],
    }
    for field, expected in checks.items():
        if baseline_record.get(field) != expected:
            raise ConfirmationEngineError(
                f"baseline_frozen_pattern_mismatch:{pattern['pattern_id']}:{field}"
            )
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for raw in baseline_record["observations"]:
        event_id = _safe_token(raw["baseline_event_id"], "baseline_event_id")
        if event_id in seen:
            raise ConfirmationEngineError(
                f"duplicate_baseline_event_id_for_pattern:{event_id}"
            )
        seen.add(event_id)
        start_at = _as_datetime(raw["start_at"], f"baseline.start_at:{event_id}")
        end_at = _as_datetime(raw["end_at"], f"baseline.end_at:{event_id}")
        if start_at <= freeze_at:
            raise ConfirmationEngineError(
                f"baseline_not_strictly_post_freeze:{event_id}"
            )
        if end_at <= start_at:
            raise ConfirmationEngineError(
                f"baseline_end_not_after_start:{event_id}"
            )
        if raw["target_id"] != forecast["target_id"]:
            raise ConfirmationEngineError(
                f"baseline_target_mismatch:{event_id}"
            )
        if int(raw["horizon_sessions"]) != forecast["horizon_sessions"]:
            raise ConfirmationEngineError(
                f"baseline_horizon_mismatch:{event_id}"
            )
        value = _finite(raw["target_value"], f"baseline.target_value:{event_id}")
        result.append(
            {
                "baseline_event_id": event_id,
                "symbol": _text(raw["symbol"], f"baseline.symbol:{event_id}"),
                "start_at": _timestamp(
                    raw["start_at"],
                    f"baseline.start_at:{event_id}",
                ),
                "end_at": _timestamp(
                    raw["end_at"],
                    f"baseline.end_at:{event_id}",
                ),
                "target_value": value,
                "aligned_value": _aligned(
                    value,
                    forecast["expected_direction"],
                ),
                "source_hash": _sha256_text(
                    raw["source_hash"],
                    f"baseline.source_hash:{event_id}",
                ),
            }
        )
    result.sort(
        key=lambda row: (
            row["start_at"],
            row["symbol"],
            row["baseline_event_id"],
        )
    )
    return result


def _pattern_statistics(
    pattern: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    *,
    look_id: str,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    forecast = _pattern_forecast(pattern)
    minimums = _frozen_minimums(pattern)
    horizon = int(forecast["horizon_sessions"])
    block_length = max(
        1,
        int(contract["statistics"]["block_length_sessions_multiplier"])
        * horizon,
    )
    support_count, region_by_date, date_axis = _support_regions(
        candidate_rows,
        baseline_rows,
        block_length=block_length,
    )
    effective_n = _effective_n(candidate_rows, region_by_date)
    concentration = _concentration(candidate_rows, region_by_date)

    candidate_values = [float(row["aligned_value"]) for row in candidate_rows]
    baseline_values = [float(row["aligned_value"]) for row in baseline_rows]
    direction_probability = (
        sum(value > 0 for value in candidate_values) / len(candidate_values)
        if candidate_values
        else None
    )
    baseline_probability = (
        sum(value > 0 for value in baseline_values) / len(baseline_values)
        if baseline_values
        else None
    )
    probability_lift = (
        direction_probability - baseline_probability
        if direction_probability is not None
        and baseline_probability is not None
        else None
    )
    mean_aligned = (
        sum(candidate_values) / len(candidate_values)
        if candidate_values
        else None
    )
    baseline_mean = (
        sum(baseline_values) / len(baseline_values)
        if baseline_values
        else None
    )
    effect_size = (
        mean_aligned - baseline_mean
        if mean_aligned is not None and baseline_mean is not None
        else None
    )

    seed_material = "|".join(
        [
            str(pattern["pattern_id"]),
            str(pattern["pattern_version"]),
            str(pattern["pattern_spec_hash"]),
            look_id,
            "L9-MBB-v1",
        ]
    )
    robust = _bootstrap_intervals(
        candidate_rows,
        baseline_rows,
        horizon=horizon,
        seed_material=seed_material,
        support_region_count=support_count,
        contract=contract,
    )
    minimum_split_n = int(
        contract["confirmation_gates"][
            "minimum_split_n_for_stability_diagnostic"
        ]
    )
    temporal = _temporal_stability(
        candidate_rows,
        minimum_split_n=minimum_split_n,
    )
    regime = _regime_diagnostics(
        candidate_rows,
        contexts,
        minimum_split_n=minimum_split_n,
    )
    raw_p = _raw_dependency_aware_p(
        direction_probability,
        baseline_probability,
        effective_n,
    )
    return {
        "raw_n": len(candidate_rows),
        "baseline_raw_n": len(baseline_rows),
        "effective_n": effective_n,
        "effective_n_method": contract["statistics"]["effective_n_method"],
        "symbol_count": len({str(row["symbol"]) for row in candidate_rows}),
        "observation_date_count": len(
            {_date_key(str(row["start_at"])) for row in candidate_rows}
        ),
        "support_region_count": support_count,
        "support_region_method": contract["statistics"]["support_region_method"],
        "combined_prospective_date_axis_count": len(date_axis),
        "direction_probability": direction_probability,
        "baseline_probability": baseline_probability,
        "probability_advantage_lift": probability_lift,
        "mean_aligned_outcome": mean_aligned,
        "median_aligned_outcome": (
            float(median(candidate_values)) if candidate_values else None
        ),
        "mean_baseline_aligned_outcome": baseline_mean,
        "median_baseline_aligned_outcome": (
            float(median(baseline_values)) if baseline_values else None
        ),
        "effect_size_vs_baseline": effect_size,
        "mean_outcome": (
            sum(float(row["target_value"]) for row in candidate_rows)
            / len(candidate_rows)
            if candidate_rows
            else None
        ),
        "median_outcome": (
            float(median(float(row["target_value"]) for row in candidate_rows))
            if candidate_rows
            else None
        ),
        "mean_peer_excess": (
            sum(
                float(row["peer_excess"])
                for row in candidate_rows
                if row["peer_excess"] is not None
            )
            / len(
                [
                    row
                    for row in candidate_rows
                    if row["peer_excess"] is not None
                ]
            )
            if any(row["peer_excess"] is not None for row in candidate_rows)
            else None
        ),
        "adverse_excursion_median": (
            float(median(float(row["adverse_excursion"]) for row in candidate_rows))
            if candidate_rows
            else None
        ),
        "path_max_drawdown_median": (
            float(
                median(
                    float(row["path_max_drawdown"])
                    for row in candidate_rows
                )
            )
            if candidate_rows
            else None
        ),
        "robust_uncertainty": robust,
        "concentration": concentration,
        "temporal_stability": temporal,
        "regime_diagnostics": regime,
        "context_splits": _other_context_splits(candidate_rows, contexts),
        "raw_dependency_aware_p_value": raw_p,
        "frozen_minimum_criteria": minimums,
        "confirmation_period": {
            "start": (
                min(str(row["start_at"]) for row in candidate_rows)
                if candidate_rows
                else None
            ),
            "end": (
                max(str(row["target_session_date"]) for row in candidate_rows)
                if candidate_rows
                else None
            ),
        },
    }


def _data_quality_blockers(
    metrics: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    robust = metrics["robust_uncertainty"]
    if robust.get("effect_size_interval_95") is None:
        blockers.append("ROBUST_EFFECT_INTERVAL_UNAVAILABLE")
    if robust.get("probability_lift_interval_95") is None:
        blockers.append("ROBUST_PROBABILITY_LIFT_INTERVAL_UNAVAILABLE")
    context_coverage = float(
        metrics["regime_diagnostics"]["context_coverage"]
    )
    required = float(
        contract["confirmation_gates"][
            "context_coverage_minimum_for_confirmation"
        ]
    )
    if context_coverage < required:
        blockers.append("CONTEXT_COVERAGE_INSUFFICIENT")
    regime_coverage = float(
        metrics["regime_diagnostics"]["regime_context_coverage"]
    )
    regime_required = float(
        contract["confirmation_gates"][
            "regime_context_coverage_minimum_for_confirmation"
        ]
    )
    if regime_coverage < regime_required:
        blockers.append("REGIME_CONTEXT_COVERAGE_INSUFFICIENT")
    temporal = metrics["temporal_stability"]
    if temporal.get("status") != "AVAILABLE":
        blockers.append("TEMPORAL_STABILITY_DIAGNOSTIC_INSUFFICIENT")
    return blockers


def _confirmation_gate_reasons(
    metrics: Mapping[str, Any],
    multiplicity: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    minimums = metrics["frozen_minimum_criteria"]
    if metrics["effect_size_vs_baseline"] is None or float(
        metrics["effect_size_vs_baseline"]
    ) < float(minimums["minimum_effect_size"]):
        reasons.append("MINIMUM_EFFECT_SIZE_NOT_MET")
    if metrics["probability_advantage_lift"] is None or float(
        metrics["probability_advantage_lift"]
    ) < float(minimums["minimum_baseline_lift"]):
        reasons.append("MINIMUM_BASELINE_LIFT_NOT_MET")
    if int(metrics["support_region_count"]) < int(
        minimums["minimum_temporal_support_regions"]
    ):
        reasons.append("MINIMUM_TEMPORAL_SUPPORT_REGIONS_NOT_MET")

    cfg = contract["confirmation_gates"]
    if int(metrics["symbol_count"]) < int(cfg["minimum_symbol_count"]):
        reasons.append("MINIMUM_SYMBOL_BREADTH_NOT_MET")
    concentration = metrics["concentration"]
    concentration_checks = (
        (
            "top_symbol_share",
            "maximum_top_symbol_share",
            "SYMBOL_CONCENTRATION_EXCEEDED",
        ),
        (
            "top_observation_date_share",
            "maximum_top_observation_date_share",
            "DATE_CONCENTRATION_EXCEEDED",
        ),
        (
            "top_support_region_share",
            "maximum_top_support_region_share",
            "SUPPORT_REGION_CONCENTRATION_EXCEEDED",
        ),
    )
    for metric_name, threshold_name, reason in concentration_checks:
        value = concentration.get(metric_name)
        if value is None or float(value) > float(cfg[threshold_name]):
            reasons.append(reason)

    effect_interval = metrics["robust_uncertainty"].get(
        "effect_size_interval_95"
    )
    if (
        cfg["effect_interval_lower_must_exceed_zero"]
        and (
            not effect_interval
            or effect_interval[0] is None
            or float(effect_interval[0]) <= 0
        )
    ):
        reasons.append("ROBUST_EFFECT_INTERVAL_DOES_NOT_EXCLUDE_ZERO")
    lift_interval = metrics["robust_uncertainty"].get(
        "probability_lift_interval_95"
    )
    if (
        cfg["probability_lift_interval_lower_must_exceed_zero"]
        and (
            not lift_interval
            or lift_interval[0] is None
            or float(lift_interval[0]) <= 0
        )
    ):
        reasons.append("ROBUST_LIFT_INTERVAL_DOES_NOT_EXCLUDE_ZERO")
    if (
        cfg["sequentially_adjusted_multiple_testing_must_pass"]
        and multiplicity.get("passed") is not True
    ):
        reasons.append("MULTIPLE_TESTING_CONFIRMATION_GATE_NOT_PASSED")

    temporal = metrics["temporal_stability"]
    if (
        cfg["temporal_half_sign_reversal_blocks_confirmation"]
        and temporal.get("sign_reversal") is True
    ):
        reasons.append("TEMPORAL_SIGN_REVERSAL")
    regime = metrics["regime_diagnostics"]
    if (
        cfg["regime_sign_reversal_blocks_confirmation"]
        and regime.get("qualified_sign_reversal") is True
    ):
        reasons.append("REGIME_SIGN_REVERSAL")
    reasons.extend(_data_quality_blockers(metrics, contract=contract))
    return sorted(set(reasons))


def _strong_falsification(
    metrics: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> bool:
    robust = metrics["robust_uncertainty"]
    effect = robust.get("effect_size_interval_95")
    lift = robust.get("probability_lift_interval_95")
    if not effect or not lift:
        return False
    effect_upper = effect[1]
    lift_upper = lift[1]
    if effect_upper is None or lift_upper is None:
        return False
    return float(effect_upper) <= 0 and float(lift_upper) <= 0


def _pattern_result_class(
    metrics: Mapping[str, Any],
    multiplicity: Mapping[str, Any],
    *,
    is_final: bool,
    early_stop_allowed: bool,
    contract: Mapping[str, Any],
) -> tuple[str, list[str]]:
    blockers = _data_quality_blockers(metrics, contract=contract)
    falsified = _strong_falsification(metrics, contract=contract)
    if falsified and (
        is_final
        or (
            early_stop_allowed
            and contract["falsification"][
                "strong_falsification_requires_final_or_predeclared_futility_look"
            ]
        )
    ):
        return "FALSIFIED", ["ROBUST_SIGN_REVERSAL"]

    reasons = _confirmation_gate_reasons(
        metrics,
        multiplicity,
        contract=contract,
    )
    if not reasons:
        return "SUPPORTED", []
    if blockers:
        return "INCONCLUSIVE", reasons
    if is_final:
        return (
            str(
                contract["falsification"][
                    "final_nonconfirmed_without_strong_reversal_classification"
                ]
            ),
            reasons,
        )
    return "INCONCLUSIVE", reasons


def _validate_context_against_outcomes(
    contexts: Mapping[str, Mapping[str, Any]],
    outcome_rows_by_claim: Mapping[str, Mapping[str, Any]],
) -> None:
    for claim_id, context in contexts.items():
        row = outcome_rows_by_claim.get(claim_id)
        if row is None:
            continue
        if context["symbol"] != row["symbol"]:
            raise ConfirmationEngineError(
                f"context_symbol_mismatch:{claim_id}"
            )
        if context["capture_snapshot_id"] != row["capture_snapshot_id"]:
            raise ConfirmationEngineError(
                f"context_capture_snapshot_id_mismatch:{claim_id}"
            )
        if (
            context["capture_snapshot_binding_hash"]
            != row["capture_snapshot_binding_hash"]
        ):
            raise ConfirmationEngineError(
                f"context_capture_snapshot_binding_hash_mismatch:{claim_id}"
            )
        if _as_datetime(
            context["observation_as_of"],
            f"context.observation_as_of:{claim_id}",
        ) >= _as_datetime(
            row["start_at"],
            f"outcome.start_at:{claim_id}",
        ):
            raise ConfirmationEngineError(
                f"context_not_strictly_pre_start_session:{claim_id}"
            )


def _validate_family_bindings(
    frozen_patterns: Sequence[Mapping[str, Any]],
    *,
    control_plan: Mapping[str, Any],
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
    qm_a_ledger: GovernanceLedger,
    look_index: int,
) -> dict[str, Mapping[str, Any]]:
    pattern_by_hypothesis: dict[str, Mapping[str, Any]] = {}
    for pattern in frozen_patterns:
        _verify_frozen_pattern(pattern)
        validate_applied_qm_handoff(
            pattern,
            hypothesis_registry=hypothesis_registry,
            analysis_plan_registry=analysis_plan_registry,
        )
        handoff = pattern["qm_c_handoff"]
        hid = str(handoff["hypothesis_record"]["hypothesis_id"])
        if hid in pattern_by_hypothesis:
            raise ConfirmationEngineError(
                f"duplicate_pattern_hypothesis_binding:{hid}"
            )
        pattern_by_hypothesis[hid] = pattern

    members = control_plan["family_members"]
    if len(pattern_by_hypothesis) != len(members):
        raise ConfirmationEngineError(
            "l9_patterns_must_exactly_cover_qm_c3_family"
        )
    for member in members:
        hid = str(member["hypothesis_id"])
        pattern = pattern_by_hypothesis.get(hid)
        if pattern is None:
            raise ConfirmationEngineError(
                f"qm_c3_member_pattern_missing:{hid}"
            )
        handoff = pattern["qm_c_handoff"]
        checks = {
            "hypothesis_version": handoff["hypothesis_record"][
                "hypothesis_version"
            ],
            "hypothesis_version_hash": handoff["hypothesis_version_hash"],
            "analysis_plan_id": handoff["analysis_plan_record"][
                "analysis_plan_id"
            ],
            "analysis_plan_version": handoff["analysis_plan_record"][
                "analysis_plan_version"
            ],
            "analysis_plan_hash": handoff["analysis_plan_hash"],
            "qm_a_analysis_id": handoff["qm_a_analysis_id"],
        }
        for field, expected in checks.items():
            if member.get(field) != expected:
                raise ConfirmationEngineError(
                    f"qm_c3_member_l5_binding_mismatch:{hid}:{field}"
                )
        analysis = qm_a_ledger.get_analysis(
            member["qm_a_analysis_id"],
            member["qm_a_version_id"],
        )
        state = str(analysis["state"])
        if look_index == 0:
            if state != "FROZEN_FOR_CONFIRMATION":
                raise ConfirmationEngineError(
                    f"first_l9_look_requires_frozen_qm_a:{hid}:{state}"
                )
        elif state not in qm_a_ledger.spent_states:
            raise ConfirmationEngineError(
                f"subsequent_l9_look_requires_spent_qm_a:{hid}:{state}"
            )
    return pattern_by_hypothesis


def _confirmation_evidence_payload(
    report: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the immutable statistical/governance evidence core.

    QM handoff payloads and outer artifact hashes are excluded to avoid a
    circular hash dependency. QM-C4/QM-C5 bind to this evidence hash.
    """
    return {
        key: value
        for key, value in report.items()
        if key not in {
            "look_hash",
            "confirmation_evidence_hash",
            "qm_c_handoff",
        }
    }


def _look_identity(
    *,
    control_plan: Mapping[str, Any],
    monitoring_plan: Mapping[str, Any],
    look: Mapping[str, Any],
    baseline_bundle_hash: str,
    context_bundle_hash: str | None,
    contract_hash: str,
    evaluated_at: str,
) -> dict[str, Any]:
    return {
        "control_plan_id": control_plan["control_plan_id"],
        "control_plan_version": control_plan["control_plan_version"],
        "control_plan_hash": control_plan["control_plan_hash"],
        "monitoring_plan_id": monitoring_plan["monitoring_plan_id"],
        "monitoring_plan_version": monitoring_plan["monitoring_plan_version"],
        "monitoring_plan_hash": monitoring_plan["monitoring_plan_hash"],
        "look_id": look["look_id"],
        "information_fraction": look["information_fraction"],
        "baseline_bundle_hash": baseline_bundle_hash,
        "context_bundle_hash": context_bundle_hash,
        "l9_contract_hash": contract_hash,
        "evaluated_at": evaluated_at,
    }


def build_confirmation_look(
    frozen_patterns: Sequence[Mapping[str, Any]],
    matured_outcomes: Sequence[Mapping[str, Any]],
    baseline_bundle: Mapping[str, Any],
    *,
    control_plan_id: str,
    control_plan_version: str,
    monitoring_plan_id: str,
    monitoring_plan_version: str,
    hypothesis_registry: HypothesisRegistry,
    analysis_plan_registry: AnalysisPlanRegistry,
    control_registry: FamilyMultiplicityRegistry,
    monitoring_registry: SequentialMonitoringRegistry,
    qm_a_ledger: GovernanceLedger,
    evaluated_at: str,
    context_bundle: Mapping[str, Any] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the next permitted family-level confirmation look.

    The function is read-only with respect to QM-C/QM-A registries.
    """
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if isinstance(frozen_patterns, (str, bytes, bytearray)) or not isinstance(
        frozen_patterns, Sequence
    ):
        raise ConfirmationEngineError("frozen_patterns_sequence_required")
    if isinstance(matured_outcomes, (str, bytes, bytearray)) or not isinstance(
        matured_outcomes, Sequence
    ):
        raise ConfirmationEngineError("matured_outcomes_sequence_required")
    verify_baseline_bundle(baseline_bundle, contract=spec)
    if context_bundle is not None:
        verify_context_bundle(context_bundle, contract=spec)

    control = control_registry.get_control_plan(
        _safe_token(control_plan_id, "control_plan_id"),
        _safe_token(control_plan_version, "control_plan_version"),
    )
    if control.get("state") != "FROZEN_FOR_CONFIRMATION":
        raise ConfirmationEngineError(
            f"l9_requires_frozen_qm_c3_control_plan:{control.get('state')}"
        )
    monitoring = monitoring_registry.get_monitoring_plan(
        _safe_token(monitoring_plan_id, "monitoring_plan_id"),
        _safe_token(monitoring_plan_version, "monitoring_plan_version"),
    )
    if monitoring.get("control_plan_id") != control["control_plan_id"]:
        raise ConfirmationEngineError("qm_c4_control_plan_id_mismatch")
    if monitoring.get("control_plan_version") != control["control_plan_version"]:
        raise ConfirmationEngineError("qm_c4_control_plan_version_mismatch")
    if monitoring.get("control_plan_hash") != control["control_plan_hash"]:
        raise ConfirmationEngineError("qm_c4_control_plan_hash_mismatch")
    if monitoring.get("state") not in {
        "FROZEN_FOR_CONFIRMATION",
        "MONITORING",
    }:
        raise ConfirmationEngineError(
            f"no_next_l9_look_in_monitoring_state:{monitoring.get('state')}"
        )
    if (
        monitoring.get("early_stop_allowed") is True
        and spec["statistics"].get("automatic_early_stop_supported") is not True
    ):
        raise ConfirmationEngineError(
            "l9_v1_automatic_early_stop_not_supported_without_machine_readable_boundary"
        )

    look_index = len(monitoring["recorded_looks"])
    planned = monitoring["planned_looks"]
    if look_index >= len(planned):
        raise ConfirmationEngineError("qm_c4_no_remaining_planned_look")
    next_look = dict(planned[look_index])
    pattern_by_hypothesis = _validate_family_bindings(
        frozen_patterns,
        control_plan=control,
        hypothesis_registry=hypothesis_registry,
        analysis_plan_registry=analysis_plan_registry,
        qm_a_ledger=qm_a_ledger,
        look_index=look_index,
    )

    baseline_map = _pattern_baseline_map(baseline_bundle)
    contexts = _context_map(context_bundle)
    all_outcome_rows_by_claim: dict[str, Mapping[str, Any]] = {}
    per_member: dict[str, dict[str, Any]] = {}
    readiness: dict[str, Any] = {}
    info_fraction = float(next_look["information_fraction"])

    for member in control["family_members"]:
        hid = str(member["hypothesis_id"])
        pattern = pattern_by_hypothesis[hid]
        key = _pattern_key(pattern)
        baseline_record = baseline_map.get(key)
        if baseline_record is None:
            raise ConfirmationEngineError(
                f"baseline_record_missing_for_pattern:{key[0]}:{key[1]}"
            )
        candidate_rows = _normalize_pattern_outcomes(
            pattern,
            matured_outcomes,
        )
        baseline_rows = _normalize_pattern_baseline(
            pattern,
            baseline_record,
            contract=spec,
        )
        for row in candidate_rows:
            all_outcome_rows_by_claim[str(row["claim_id"])] = row
        minimums = _frozen_minimums(pattern)
        required_n = max(
            1,
            int(math.ceil(int(minimums["minimum_raw_n"]) * info_fraction)),
        )
        ready = (
            len(candidate_rows) >= required_n
            and len(baseline_rows) >= required_n
        )
        readiness[hid] = {
            "pattern_id": pattern["pattern_id"],
            "pattern_version": pattern["pattern_version"],
            "required_raw_n": required_n,
            "candidate_raw_n": len(candidate_rows),
            "baseline_raw_n": len(baseline_rows),
            "ready": ready,
        }
        per_member[hid] = {
            "member": dict(member),
            "pattern": pattern,
            "candidate_rows": candidate_rows,
            "baseline_rows": baseline_rows,
        }

    _validate_context_against_outcomes(
        contexts,
        all_outcome_rows_by_claim,
    )

    evaluated = _timestamp(evaluated_at, "evaluated_at")
    contract_hash = confirmation_contract_hash(spec)
    identity = _look_identity(
        control_plan=control,
        monitoring_plan=monitoring,
        look=next_look,
        baseline_bundle_hash=baseline_bundle["baseline_bundle_hash"],
        context_bundle_hash=(
            context_bundle["context_bundle_hash"]
            if context_bundle is not None
            else None
        ),
        contract_hash=contract_hash,
        evaluated_at=evaluated,
    )
    local_id = f"PCL9-{_hash(identity)[:24].upper()}"

    if not all(item["ready"] for item in readiness.values()):
        report: dict[str, Any] = {
            "schema_version": LOOK_SCHEMA_VERSION,
            "module": "pattern_discovery_lab",
            "phase": "L9",
            "research_only": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "confirmation_look_id": local_id,
            "evaluated_at": evaluated,
            "l9_contract_hash": contract_hash,
            "qm_governance": {
                "control_plan_id": control["control_plan_id"],
                "control_plan_version": control["control_plan_version"],
                "control_plan_hash": control["control_plan_hash"],
                "monitoring_plan_id": monitoring["monitoring_plan_id"],
                "monitoring_plan_version": monitoring["monitoring_plan_version"],
                "monitoring_plan_hash": monitoring["monitoring_plan_hash"],
                "next_look": next_look,
                "recorded_look_count_before": look_index,
            },
            "input_bindings": {
                "baseline_bundle_id": baseline_bundle["baseline_bundle_id"],
                "baseline_bundle_hash": baseline_bundle["baseline_bundle_hash"],
                "context_bundle_id": (
                    context_bundle["context_bundle_id"]
                    if context_bundle is not None
                    else None
                ),
                "context_bundle_hash": (
                    context_bundle["context_bundle_hash"]
                    if context_bundle is not None
                    else None
                ),
                "matured_outcome_hashes": sorted(
                    {
                        str(row["outcome_hash"])
                        for row in matured_outcomes
                        if isinstance(row, Mapping)
                        and row.get("outcome_hash")
                    }
                ),
            },
            "look_status": "UNRESOLVED_NOT_DUE",
            "look_consumes_qm_c4_schedule": False,
            "family_readiness": readiness,
            "pattern_results": [],
            "family_decision": None,
            "qm_c_handoff": {
                "application_status": "NOT_READY_NO_QM_WRITE",
                "qm_c4_record_look": None,
                "qm_a_transitions": [],
                "qm_c5_results": [],
                "direct_qm_registry_write_performed": False,
            },
            "boundaries": {
                "discovery_evidence_used_as_confirmation": False,
                "l8_outcome_mutation_performed": False,
                "qm_c_registry_write_performed": False,
                "rating_assigned": False,
                "promotion_performed": False,
                "decision_layer_integration_performed": False,
            },
        }
        PatternDiscoveryBoundary().assert_research_payload(report)
        report["look_hash"] = _hash(report)
        return report

    pattern_metrics: dict[str, dict[str, Any]] = {}
    raw_pvalues: list[tuple[str, float]] = []
    for hid, item in per_member.items():
        metrics = _pattern_statistics(
            item["pattern"],
            item["candidate_rows"],
            item["baseline_rows"],
            contexts,
            look_id=str(next_look["look_id"]),
            contract=spec,
        )
        pattern_metrics[hid] = metrics
        raw_pvalues.append(
            (hid, float(metrics["raw_dependency_aware_p_value"]))
        )

    multiplicity = _adjust_family_pvalues(
        raw_pvalues,
        strategy=str(control["multiplicity_strategy"]),
        parameters=control["multiplicity_parameters"],
        planned_look_count=len(planned),
        contract=spec,
    )
    is_final = look_index == len(planned) - 1
    pattern_results: list[dict[str, Any]] = []
    for member in control["family_members"]:
        hid = str(member["hypothesis_id"])
        item = per_member[hid]
        metrics = pattern_metrics[hid]
        result_class, reasons = _pattern_result_class(
            metrics,
            multiplicity[hid],
            is_final=is_final,
            early_stop_allowed=bool(monitoring["early_stop_allowed"]),
            contract=spec,
        )
        pattern = item["pattern"]
        result = {
            "hypothesis_id": hid,
            "hypothesis_version": member["hypothesis_version"],
            "hypothesis_version_hash": member["hypothesis_version_hash"],
            "analysis_plan_id": member["analysis_plan_id"],
            "analysis_plan_version": member["analysis_plan_version"],
            "analysis_plan_hash": member["analysis_plan_hash"],
            "qm_a_analysis_id": member["qm_a_analysis_id"],
            "qm_a_version_id": member["qm_a_version_id"],
            "pattern_id": pattern["pattern_id"],
            "pattern_version": pattern["pattern_version"],
            "pattern_spec_hash": pattern["pattern_spec_hash"],
            "target": _pattern_forecast(pattern),
            "prospective_evidence": metrics,
            "multiple_testing": multiplicity[hid],
            "result_class": result_class,
            "result_reasons": reasons,
            "discovery_evidence_used_as_confirmation": False,
        }
        pattern_results.append(result)

    result_classes = [row["result_class"] for row in pattern_results]
    if is_final:
        family_decision = spec["sequential_decisions"]["final_required"]
    else:
        family_decision = spec["sequential_decisions"]["interim_default"]

    outcome_hashes = sorted(
        {
            row["outcome_hash"]
            for item in per_member.values()
            for row in item["candidate_rows"]
        }
    )
    report = {
        "schema_version": LOOK_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L9",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "confirmation_look_id": local_id,
        "evaluated_at": evaluated,
        "l9_contract_hash": contract_hash,
        "qm_governance": {
            "control_plan_id": control["control_plan_id"],
            "control_plan_version": control["control_plan_version"],
            "control_plan_hash": control["control_plan_hash"],
            "control_plan_freeze_binding_hash": control.get(
                "freeze_binding_hash"
            ),
            "monitoring_plan_id": monitoring["monitoring_plan_id"],
            "monitoring_plan_version": monitoring["monitoring_plan_version"],
            "monitoring_plan_hash": monitoring["monitoring_plan_hash"],
            "monitoring_mode": monitoring["mode"],
            "planned_look_count": len(planned),
            "recorded_look_count_before": look_index,
            "look_id": next_look["look_id"],
            "information_fraction": next_look["information_fraction"],
            "is_final_look": is_final,
            "early_stop_allowed": monitoring["early_stop_allowed"],
            "stopping_rule": monitoring["stopping_rule"],
        },
        "input_bindings": {
            "baseline_bundle_id": baseline_bundle["baseline_bundle_id"],
            "baseline_bundle_hash": baseline_bundle["baseline_bundle_hash"],
            "context_bundle_id": (
                context_bundle["context_bundle_id"]
                if context_bundle is not None
                else None
            ),
            "context_bundle_hash": (
                context_bundle["context_bundle_hash"]
                if context_bundle is not None
                else None
            ),
            "matured_outcome_hashes": outcome_hashes,
            "matured_outcome_set_hash": _hash(outcome_hashes),
        },
        "look_status": "EVALUATED",
        "look_consumes_qm_c4_schedule": True,
        "family_readiness": readiness,
        "pattern_results": pattern_results,
        "family_decision": family_decision,
        "qm_c_handoff": None,
        "boundaries": {
            "discovery_evidence_used_as_confirmation": False,
            "l8_outcome_mutation_performed": False,
            "qm_c_registry_write_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(report)
    evidence_hash = _hash(_confirmation_evidence_payload(report))
    report["confirmation_evidence_hash"] = evidence_hash

    c4_payload = {
        "monitoring_plan_id": monitoring["monitoring_plan_id"],
        "monitoring_plan_version": monitoring["monitoring_plan_version"],
        "look_id": next_look["look_id"],
        "artifact_hash": evidence_hash,
        "decision": family_decision,
        "observed_at": evaluated,
    }
    terminal_after_application = family_decision in {
        "FINAL_COMPLETE",
        "STOP_EFFICACY",
        "STOP_FUTILITY",
    }
    c5_results: list[dict[str, Any]] = []
    if terminal_after_application:
        mapping = spec["qm_c5_mapping"]
        for row in pattern_results:
            classification = mapping[row["result_class"]]
            if classification is None:
                continue
            result_id = f"R-{row['hypothesis_id']}"
            conclusion = (
                f"L9 {row['result_class']} at predeclared QM-C4 look "
                f"{next_look['look_id']}; reasons="
                f"{','.join(row['result_reasons']) if row['result_reasons'] else 'NONE'}."
            )
            c5_results.append(
                {
                    "result_id": result_id,
                    "result_version": "v1",
                    "hypothesis_id": row["hypothesis_id"],
                    "hypothesis_version": row["hypothesis_version"],
                    "hypothesis_version_hash": row["hypothesis_version_hash"],
                    "evidence_scope": "CONFIRMATORY",
                    "outcome_classification": classification,
                    "conclusion": conclusion,
                    "evidence_artifact_hash": evidence_hash,
                    "analysis_plan_id": row["analysis_plan_id"],
                    "analysis_plan_version": row["analysis_plan_version"],
                    "analysis_plan_hash": row["analysis_plan_hash"],
                    "control_plan_id": control["control_plan_id"],
                    "control_plan_version": control["control_plan_version"],
                    "control_plan_hash": control["control_plan_hash"],
                    "monitoring_plan_id": monitoring["monitoring_plan_id"],
                    "monitoring_plan_version": monitoring["monitoring_plan_version"],
                    "monitoring_plan_hash": monitoring["monitoring_plan_hash"],
                    "qm_a_analysis_id": row["qm_a_analysis_id"],
                    "qm_a_version_id": row["qm_a_version_id"],
                }
            )

    report["qm_c_handoff"] = {
        "application_status": "READY_NOT_APPLIED_BY_L9",
        "direct_qm_registry_write_performed": False,
        "l0_write_boundary_preserved": True,
        "application_order": [
            "QM_C4_RECORD_MONITORING_LOOK",
            "QM_A_TRANSITION_FROZEN_TO_CONFIRMATORY_EVALUATED_IF_FIRST_LOOK",
            "QM_C5_REGISTER_TERMINAL_RESULTS_IF_TERMINAL_LOOK",
        ],
        "qm_c4_record_look": c4_payload,
        "qm_a_transitions": [
            {
                "analysis_id": member["qm_a_analysis_id"],
                "version_id": member["qm_a_version_id"],
                "requested_to_state": (
                    "CONFIRMATORY_EVALUATED"
                    if look_index == 0
                    else None
                ),
                "reason": (
                    "Consume the predeclared L9 confirmatory look."
                    if look_index == 0
                    else "Already in spent confirmatory state."
                ),
            }
            for member in control["family_members"]
        ],
        "qm_c5_results": c5_results,
        "terminal_after_application": terminal_after_application,
    }
    PatternDiscoveryBoundary().assert_research_payload(report)
    report["look_hash"] = _hash(report)
    return report


def verify_confirmation_look(
    report: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    if not isinstance(report, Mapping):
        raise ConfirmationEngineError("confirmation_look_must_be_object")
    if report.get("schema_version") != LOOK_SCHEMA_VERSION:
        raise ConfirmationEngineError("confirmation_look_schema_invalid")
    if report.get("research_only") is not True:
        raise ConfirmationEngineError("confirmation_look_research_only_guard_missing")
    if report.get("productive_integration_enabled") is not False:
        raise ConfirmationEngineError(
            "confirmation_look_productive_integration_forbidden"
        )
    if report.get("execution_allowed") is not False:
        raise ConfirmationEngineError("confirmation_look_execution_forbidden")
    if report.get("l9_contract_hash") != confirmation_contract_hash(spec):
        raise ConfirmationEngineError("confirmation_look_contract_hash_mismatch")

    stored = _sha256_text(report.get("look_hash"), "look_hash")
    body = dict(report)
    body.pop("look_hash", None)
    if _hash(body) != stored:
        raise ConfirmationEngineError("confirmation_look_hash_mismatch")

    status = report.get("look_status")
    if status not in {"UNRESOLVED_NOT_DUE", "EVALUATED"}:
        raise ConfirmationEngineError("confirmation_look_status_invalid")
    consumes = report.get("look_consumes_qm_c4_schedule")
    if status == "UNRESOLVED_NOT_DUE" and consumes is not False:
        raise ConfirmationEngineError("unresolved_not_due_cannot_consume_look")
    if status == "EVALUATED" and consumes is not True:
        raise ConfirmationEngineError("evaluated_look_must_consume_schedule")

    boundaries = report.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ConfirmationEngineError("confirmation_look_boundaries_missing")
    expected_false = (
        "discovery_evidence_used_as_confirmation",
        "l8_outcome_mutation_performed",
        "qm_c_registry_write_performed",
        "rating_assigned",
        "promotion_performed",
        "decision_layer_integration_performed",
    )
    for field in expected_false:
        if boundaries.get(field) is not False:
            raise ConfirmationEngineError(
                f"confirmation_look_boundary_invalid:{field}"
            )
    if status == "EVALUATED":
        results = report.get("pattern_results")
        if not isinstance(results, list) or not results:
            raise ConfirmationEngineError("evaluated_look_results_required")
        for row in results:
            if row.get("result_class") not in set(spec["result_classes"]):
                raise ConfirmationEngineError(
                    "confirmation_result_class_invalid"
                )
            if row.get("discovery_evidence_used_as_confirmation") is not False:
                raise ConfirmationEngineError(
                    "discovery_evidence_confirmation_leakage"
                )
        handoff = report.get("qm_c_handoff")
        if not isinstance(handoff, Mapping):
            raise ConfirmationEngineError("confirmation_qm_handoff_missing")
        if handoff.get("direct_qm_registry_write_performed") is not False:
            raise ConfirmationEngineError("direct_qm_write_forbidden")
        evidence_hash = _sha256_text(
            report.get("confirmation_evidence_hash"),
            "confirmation_evidence_hash",
        )
        if _hash(_confirmation_evidence_payload(report)) != evidence_hash:
            raise ConfirmationEngineError(
                "confirmation_evidence_hash_mismatch"
            )
        c4 = handoff.get("qm_c4_record_look")
        if not isinstance(c4, Mapping):
            raise ConfirmationEngineError("qm_c4_handoff_missing")
        if c4.get("artifact_hash") != evidence_hash:
            raise ConfirmationEngineError("qm_c4_handoff_artifact_hash_mismatch")
        for record in handoff.get("qm_c5_results") or []:
            if record.get("evidence_artifact_hash") != evidence_hash:
                raise ConfirmationEngineError(
                    "qm_c5_handoff_artifact_hash_mismatch"
                )
    PatternDiscoveryBoundary().assert_research_payload(report)
    return {
        "valid": True,
        "confirmation_look_id": report["confirmation_look_id"],
        "look_hash": stored,
        "look_status": status,
        "result_count": len(report.get("pattern_results") or []),
    }


class ConfirmationLookRegistry:
    """Append-only local mirror of consumed L9/QM-C4 confirmation looks."""

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
            else load_confirmation_contract()
        )

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        values: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ConfirmationEngineError(
                    f"confirmation_registry_invalid_json:{line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise ConfirmationEngineError(
                    f"confirmation_registry_event_not_object:{line_number}"
                )
            values.append(value)
        return values

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous: str | None = None
        for sequence, raw in enumerate(events, start=1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise ConfirmationEngineError(
                    f"confirmation_registry_schema_invalid:{sequence}"
                )
            if event.get("sequence") != sequence:
                raise ConfirmationEngineError(
                    f"confirmation_registry_sequence_invalid:{sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise ConfirmationEngineError(
                    f"confirmation_registry_previous_hash_invalid:{sequence}"
                )
            stored = _sha256_text(
                event.get("entry_hash"),
                f"confirmation_registry.entry_hash.{sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise ConfirmationEngineError(
                    f"confirmation_registry_entry_hash_invalid:{sequence}"
                )
            previous = stored

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        looks: dict[str, dict[str, Any]] = {}
        for raw in events:
            if raw.get("event_type") != "CONFIRMATION_LOOK_PERSISTED":
                raise ConfirmationEngineError(
                    "confirmation_registry_event_type_unknown"
                )
            report = raw.get("report")
            if not isinstance(report, Mapping):
                raise ConfirmationEngineError(
                    "confirmation_registry_report_missing"
                )
            verify_confirmation_look(report, contract=self.contract)
            if report["look_status"] != "EVALUATED":
                raise ConfirmationEngineError(
                    "confirmation_registry_only_consumed_looks_allowed"
                )
            governance = report["qm_governance"]
            key = "::".join(
                [
                    str(governance["monitoring_plan_id"]),
                    str(governance["monitoring_plan_version"]),
                    str(governance["look_id"]),
                ]
            )
            if key in looks:
                raise ConfirmationEngineError(
                    f"confirmation_look_already_persisted:{key}"
                )
            looks[key] = dict(report)
        return looks

    def _load(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read()
        self._verify_chain(events)
        return events, self._replay(events)

    def register(
        self,
        report: Mapping[str, Any],
        *,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        verify_confirmation_look(report, contract=self.contract)
        if report["look_status"] != "EVALUATED":
            raise ConfirmationEngineError(
                "unresolved_not_due_look_is_not_persisted_as_consumed"
            )
        governance = report["qm_governance"]
        key = "::".join(
            [
                str(governance["monitoring_plan_id"]),
                str(governance["monitoring_plan_version"]),
                str(governance["look_id"]),
            ]
        )
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
                raise ConfirmationEngineError(
                    "confirmation_registry_lock_already_held"
                ) from exc
            events, looks = self._load()
            if key in looks:
                if looks[key] == dict(report):
                    return {
                        "valid": True,
                        "idempotent": True,
                        "registry_event_count": len(events),
                        "look_count": len(looks),
                        "head_hash": (
                            events[-1]["entry_hash"] if events else None
                        ),
                    }
                raise ConfirmationEngineError(
                    f"confirmation_look_identity_collision:{key}"
                )
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": (
                    "PCE-"
                    + _hash(
                        {
                            "key": key,
                            "look_hash": report["look_hash"],
                        }
                    )[:24].upper()
                ),
                "event_type": "CONFIRMATION_LOOK_PERSISTED",
                "recorded_at": report["evaluated_at"],
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "report": dict(report),
                "previous_event_hash": (
                    events[-1]["entry_hash"] if events else None
                ),
            }
            event["entry_hash"] = _hash(event)
            candidate = [*events, event]
            self._verify_chain(candidate)
            self._replay(candidate)
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(
                    fd,
                    (_canonical_json(event) + "\n").encode("utf-8"),
                )
                os.fsync(fd)
            finally:
                os.close(fd)
            return {
                "valid": True,
                "idempotent": False,
                "registry_event_count": len(candidate),
                "look_count": len(looks) + 1,
                "head_hash": event["entry_hash"],
            }
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def verify_integrity(self) -> dict[str, Any]:
        events, looks = self._load()
        return {
            "valid": True,
            "registry_event_count": len(events),
            "look_count": len(looks),
            "head_hash": (
                events[-1]["entry_hash"] if events else None
            ),
        }


def baseline_bundle_repo_path(
    baseline_bundle_hash: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    return str(spec["storage"]["baseline_bundle_path_template"]).format(
        baseline_bundle_hash=_sha256_text(
            baseline_bundle_hash,
            "baseline_bundle_hash",
        )
    )


def context_bundle_repo_path(
    context_bundle_hash: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    return str(spec["storage"]["context_bundle_path_template"]).format(
        context_bundle_hash=_sha256_text(
            context_bundle_hash,
            "context_bundle_hash",
        )
    )


def _write_immutable_json(
    root: Path,
    repo_path: str,
    payload: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary,
) -> Path:
    boundary.assert_write_path_allowed(repo_path)
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ConfirmationEngineError(
            "confirmation_input_path_outside_repo"
        ) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfirmationEngineError(
                f"existing_confirmation_input_unreadable:{repo_path}"
            ) from exc
        if existing != dict(payload):
            raise ConfirmationEngineError(
                f"confirmation_input_identity_collision:{repo_path}"
            )
        return target
    with target.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(
            dict(payload),
            handle,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        handle.write("\n")
    return target


def confirmation_registry_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    return str(spec["storage"]["look_registry_path"])


def confirmation_report_repo_path(
    report: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    governance = report["qm_governance"]
    return str(spec["storage"]["look_report_path_template"]).format(
        monitoring_plan_id=_safe_token(
            governance["monitoring_plan_id"],
            "monitoring_plan_id",
        ),
        look_id=_safe_token(governance["look_id"], "look_id"),
        look_hash=_sha256_text(report["look_hash"], "look_hash"),
    )


def persist_confirmation_look(
    repo_root: str | Path,
    report: Mapping[str, Any],
    baseline_bundle: Mapping[str, Any],
    *,
    context_bundle: Mapping[str, Any] | None = None,
    actor_id: str,
    actor_role: str,
    boundary: PatternDiscoveryBoundary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_confirmation_contract()
    )
    verify_confirmation_look(report, contract=spec)
    if report["look_status"] != "EVALUATED":
        raise ConfirmationEngineError(
            "confirmation_not_due_report_not_persistable"
        )
    verify_baseline_bundle(baseline_bundle, contract=spec)
    if baseline_bundle["baseline_bundle_hash"] != report["input_bindings"][
        "baseline_bundle_hash"
    ]:
        raise ConfirmationEngineError(
            "persisted_baseline_bundle_hash_mismatch_report"
        )
    if context_bundle is not None:
        verify_context_bundle(context_bundle, contract=spec)
        if context_bundle["context_bundle_hash"] != report["input_bindings"][
            "context_bundle_hash"
        ]:
            raise ConfirmationEngineError(
                "persisted_context_bundle_hash_mismatch_report"
            )
    elif report["input_bindings"].get("context_bundle_hash") is not None:
        raise ConfirmationEngineError(
            "confirmation_report_requires_context_bundle_for_persistence"
        )

    guard = boundary or PatternDiscoveryBoundary()
    root = Path(repo_root).resolve()

    baseline_repo = baseline_bundle_repo_path(
        baseline_bundle["baseline_bundle_hash"],
        contract=spec,
    )
    baseline_path = _write_immutable_json(
        root,
        baseline_repo,
        baseline_bundle,
        boundary=guard,
    )
    context_path: Path | None = None
    if context_bundle is not None:
        context_repo = context_bundle_repo_path(
            context_bundle["context_bundle_hash"],
            contract=spec,
        )
        context_path = _write_immutable_json(
            root,
            context_repo,
            context_bundle,
            boundary=guard,
        )
    registry_repo = confirmation_registry_repo_path(contract=spec)
    report_repo = confirmation_report_repo_path(report, contract=spec)
    guard.assert_write_path_allowed(registry_repo)
    guard.assert_write_path_allowed(report_repo)
    registry_path = (root / registry_repo).resolve()
    report_path = (root / report_repo).resolve()
    try:
        registry_path.relative_to(root)
        report_path.relative_to(root)
    except ValueError as exc:
        raise ConfirmationEngineError(
            "confirmation_path_outside_repo"
        ) from exc

    registry = ConfirmationLookRegistry(
        registry_path,
        contract=spec,
    )
    registry_status = registry.register(
        report,
        actor_id=actor_id,
        actor_role=actor_role,
    )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path.exists():
        try:
            existing = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfirmationEngineError(
                "existing_confirmation_report_unreadable"
            ) from exc
        verify_confirmation_look(existing, contract=spec)
        if existing != dict(report):
            raise ConfirmationEngineError(
                "confirmation_report_identity_collision"
            )
    else:
        with report_path.open("x", encoding="utf-8", newline="\n") as handle:
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
        "confirmation_look_id": report["confirmation_look_id"],
        "look_hash": report["look_hash"],
        "report_path": str(report_path),
        "baseline_bundle_path": str(baseline_path),
        "context_bundle_path": (
            str(context_path) if context_path is not None else None
        ),
        "registry_path": str(registry_path),
        "registry": registry_status,
    }


def validate_applied_qm_confirmation_handoff(
    report: Mapping[str, Any],
    *,
    monitoring_registry: SequentialMonitoringRegistry,
) -> dict[str, Any]:
    """Read-only verification that the L9 QM-C4 handoff was applied exactly.

    QM-C5 application is intentionally verified by the central QM-C5 registry
    itself; L9 never writes that registry.
    """
    verify_confirmation_look(report)
    if report["look_status"] != "EVALUATED":
        raise ConfirmationEngineError("unresolved_look_has_no_qm_handoff")
    handoff = report["qm_c_handoff"]["qm_c4_record_look"]
    monitor = monitoring_registry.get_monitoring_plan(
        handoff["monitoring_plan_id"],
        handoff["monitoring_plan_version"],
    )
    matches = [
        look
        for look in monitor["recorded_looks"]
        if look["look_id"] == handoff["look_id"]
        and look["artifact_hash"] == report["confirmation_evidence_hash"]
        and look["decision"] == handoff["decision"]
    ]
    if len(matches) != 1:
        raise ConfirmationEngineError(
            "qm_c4_applied_look_does_not_match_l9_handoff"
        )
    return {
        "valid": True,
        "monitoring_plan_id": monitor["monitoring_plan_id"],
        "monitoring_plan_version": monitor["monitoring_plan_version"],
        "look_id": handoff["look_id"],
        "look_hash": report["look_hash"],
        "monitoring_state": monitor["state"],
    }
