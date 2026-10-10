"""Phase L3 bounded Pattern Discovery search engine.

L3 is discovery-only. It consumes a frozen L1 run, the exact L2 Feature Library
and discovery observations whose outcomes have fully matured by the L1 cutoff.
It enumerates only registered atomic conditions, tests 1-3 condition
combinations inside hard budgets, applies the simple L1 minimum-N/effect gates,
ranks candidates only inside their discovery family and retains every tested
candidate including rejections.

L3 does not perform multiple-testing correction, robust uncertainty,
dependency/redundancy analysis, hard candidate freeze, QM-C registration,
prospective confirmation, promotion or productive decision integration.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from itertools import combinations
import json
import math
from pathlib import Path
import re
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .feature_library import FeatureLibrary
from .run_contract import verify_run_manifest


SCHEMA_VERSION = "pattern_discovery_l3_search_contract_v1"
RESULT_SCHEMA_VERSION = "pattern_discovery_l3_result_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l3_search_contract_v1.json"
)


class DiscoverySearchError(ValueError):
    """Raised when L3 search invariants are violated."""


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
        raise DiscoverySearchError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise DiscoverySearchError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DiscoverySearchError(f"finite_number_required:{field}")
    result = float(value)
    if not math.isfinite(result):
        raise DiscoverySearchError(f"finite_number_required:{field}")
    return result


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def load_search_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoverySearchError(f"search_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise DiscoverySearchError("search_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise DiscoverySearchError("search_contract_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise DiscoverySearchError("search_contract_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise DiscoverySearchError("search_contract_execution_forbidden")
    return payload


def search_contract_hash(contract: Mapping[str, Any] | None = None) -> str:
    value = dict(contract) if contract is not None else load_search_contract()
    return _hash(value)


def _target_definition(target_id: str, contract: Mapping[str, Any]) -> dict[str, Any]:
    value = _text(target_id, "target_id")
    for pattern_type, regex in contract["target_id_patterns"].items():
        match = re.fullmatch(str(regex), value)
        if not match:
            continue
        horizon = int(match.group(1))
        direction_code = match.group(2).lower()
        return {
            "target_id": value,
            "pattern_type": str(pattern_type),
            "horizon_sessions": horizon,
            "expected_direction": "POSITIVE" if direction_code == "gt" else "NEGATIVE",
        }
    raise DiscoverySearchError(f"target_id_not_supported_by_l3:{value}")


def _normalize_observations(
    observations: Sequence[Mapping[str, Any]],
    *,
    cutoff: datetime,
    target_ids: Sequence[str],
) -> list[dict[str, Any]]:
    if isinstance(observations, (str, bytes, bytearray)) or not isinstance(
        observations, Sequence
    ):
        raise DiscoverySearchError("observations_sequence_required")
    normalized: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()

    for index, raw in enumerate(observations):
        if not isinstance(raw, Mapping):
            raise DiscoverySearchError(f"observation_must_be_object:{index}")
        symbol = _text(raw.get("symbol"), f"observations[{index}].symbol")
        as_of_dt = _timestamp(raw.get("as_of"), f"observations[{index}].as_of")
        if as_of_dt > cutoff:
            raise DiscoverySearchError(
                f"observation_after_discovery_cutoff:{symbol}:{as_of_dt.isoformat()}"
            )
        identity = (symbol, as_of_dt.isoformat())
        if identity in identities:
            raise DiscoverySearchError(
                f"duplicate_observation_identity:{symbol}:{as_of_dt.isoformat()}"
            )
        identities.add(identity)

        outcomes = raw.get("outcomes")
        if not isinstance(outcomes, Mapping):
            raise DiscoverySearchError(f"outcomes_object_required:{index}")

        normalized_outcomes: dict[str, dict[str, Any]] = {}
        for target_id in target_ids:
            if target_id not in outcomes:
                continue
            outcome = outcomes[target_id]
            if not isinstance(outcome, Mapping):
                raise DiscoverySearchError(
                    f"outcome_object_required:{index}:{target_id}"
                )
            if set(outcome) != {"value", "end_at"}:
                raise DiscoverySearchError(
                    f"outcome_fields_invalid:{index}:{target_id}"
                )
            value = _finite(
                outcome["value"],
                f"observations[{index}].outcomes.{target_id}.value",
            )
            end_at = _timestamp(
                outcome["end_at"],
                f"observations[{index}].outcomes.{target_id}.end_at",
            )
            if end_at < as_of_dt:
                raise DiscoverySearchError(
                    f"outcome_end_before_observation:{index}:{target_id}"
                )
            if end_at > cutoff:
                raise DiscoverySearchError(
                    f"future_outcome_after_cutoff_forbidden:{index}:{target_id}"
                )
            normalized_outcomes[target_id] = {
                "value": value,
                "end_at": end_at.isoformat(),
            }

        copy = dict(raw)
        copy["symbol"] = symbol
        copy["as_of"] = as_of_dt.isoformat()
        copy["outcomes"] = normalized_outcomes
        normalized.append(copy)

    normalized.sort(key=lambda row: (row["symbol"], row["as_of"]))
    return normalized


def _feature_use_specs(
    library: FeatureLibrary,
    prereg: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    allowed_run = set(str(x) for x in prereg["allowed_transformations"])
    included = set(str(x) for x in contract["atom_generation"]["included_transformations"])
    specs: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []

    for feature_key in sorted(library.features):
        feature = library.features[feature_key]
        feature_id = str(feature["feature_id"])
        semantic_type = str(feature["semantic_type"])
        transformations = [
            str(value)
            for value in feature["allowed_transformations"]
            if str(value) in allowed_run and str(value) in included
        ]

        if "regime_context" in transformations and "raw" in transformations:
            transformations.remove("raw")
            exclusions.append(
                {
                    "scope": "ATOM_GENERATION",
                    "feature_id": feature_id,
                    "reason": "RAW_REDUNDANT_WITH_REGIME_CONTEXT",
                }
            )

        for transformation_id in sorted(transformations):
            if transformation_id == "raw" and semantic_type == "CONTINUOUS":
                exclusions.append(
                    {
                        "scope": "ATOM_GENERATION",
                        "feature_id": feature_id,
                        "transformation_id": transformation_id,
                        "reason": "CONTINUOUS_RAW_NOT_ATOMIZED_WITHOUT_PREDECLARED_BIN",
                    }
                )
                continue

            if transformation_id in {"change_direction"}:
                transformation = library.get_transformation(transformation_id)
                for lag in transformation["allowed_lag_observations"]:
                    specs.append(
                        library.validate_feature_use(
                            {
                                "feature_id": feature_id,
                                "feature_version": feature["feature_version"],
                                "transformation_id": transformation_id,
                                "transformation_version": "v1",
                                "parameters": {"lag_observations": int(lag)},
                            }
                        )
                    )
                continue

            if transformation_id == "threshold_crossing":
                for threshold in feature.get("allowed_thresholds", []):
                    specs.append(
                        library.validate_feature_use(
                            {
                                "feature_id": feature_id,
                                "feature_version": feature["feature_version"],
                                "transformation_id": transformation_id,
                                "transformation_version": "v1",
                                "parameters": {"threshold": float(threshold)},
                            }
                        )
                    )
                continue

            specs.append(
                library.validate_feature_use(
                    {
                        "feature_id": feature_id,
                        "feature_version": feature["feature_version"],
                        "transformation_id": transformation_id,
                        "transformation_version": "v1",
                        "parameters": {},
                    }
                )
            )

    specs.sort(
        key=lambda item: (
            item["feature_id"],
            item["transformation_id"],
            _canonical_json(item["parameters"]),
        )
    )
    return specs, exclusions


def _history_by_symbol(
    observations: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, list[Mapping[str, Any]]], dict[tuple[str, str], int]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in observations:
        grouped[str(row["symbol"])].append(row)
    positions: dict[tuple[str, str], int] = {}
    for symbol, rows in grouped.items():
        rows.sort(key=lambda row: str(row["as_of"]))
        for index, row in enumerate(rows):
            positions[(symbol, str(row["as_of"]))] = index
    return dict(grouped), positions


def _state_text(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, ".12g")
    return str(value).strip()


def _atom_state(
    validated_use: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    symbol_history: Sequence[Mapping[str, Any]],
    position: int,
) -> tuple[str | None, str | None]:
    field = str(validated_use["source_field"])
    tid = str(validated_use["transformation_id"])
    current = row.get(field)

    if _is_missing(current):
        return None, "SOURCE_VALUE_MISSING"

    if tid in {"raw", "regime_context"}:
        return _state_text(current), None

    if tid == "level_band":
        # Exactly the preregistered Cycle bands. 25/50/75 belong to the
        # upper band. No outcome-dependent fitting or arbitrary binning.
        if isinstance(current, bool):
            return None, "CYCLE_LEVEL_NOT_FINITE"
        try:
            level = float(current)
        except (TypeError, ValueError):
            return None, "CYCLE_LEVEL_NOT_FINITE"
        if not math.isfinite(level) or not 0.0 <= level <= 100.0:
            return None, "CYCLE_LEVEL_OUT_OF_RANGE"
        if level < 25.0:
            return "LEVEL_LT_25", None
        if level < 50.0:
            return "LEVEL_25_LT_50", None
        if level < 75.0:
            return "LEVEL_50_LT_75", None
        return "LEVEL_GTE_75", None

    if tid == "change_direction":
        lag = int(validated_use["parameters"]["lag_observations"])
        if position < lag:
            return None, "INSUFFICIENT_PRIOR_OBSERVATIONS"
        previous = symbol_history[position - lag].get(field)
        if _is_missing(previous):
            return None, "PRIOR_SOURCE_VALUE_MISSING"
        try:
            delta = float(current) - float(previous)
        except (TypeError, ValueError) as exc:
            raise DiscoverySearchError(
                f"continuous_transform_non_numeric:{validated_use['feature_id']}"
            ) from exc
        if delta > 0:
            return "UP", None
        if delta < 0:
            return "DOWN", None
        return None, "NO_DIRECTIONAL_CHANGE"

    if tid == "threshold_crossing":
        if position < 1:
            return None, "INSUFFICIENT_PRIOR_OBSERVATIONS"
        previous = symbol_history[position - 1].get(field)
        if _is_missing(previous):
            return None, "PRIOR_SOURCE_VALUE_MISSING"
        threshold = float(validated_use["parameters"]["threshold"])
        try:
            current_value = float(current)
            previous_value = float(previous)
        except (TypeError, ValueError) as exc:
            raise DiscoverySearchError(
                f"threshold_transform_non_numeric:{validated_use['feature_id']}"
            ) from exc
        if current_value >= threshold and previous_value < threshold:
            return "CROSS_UP", None
        if current_value < threshold and previous_value >= threshold:
            return "CROSS_DOWN", None
        return None, "NO_THRESHOLD_CROSSING"

    if tid == "state_transition":
        if position < 1:
            return None, "INSUFFICIENT_PRIOR_OBSERVATIONS"
        previous = symbol_history[position - 1].get(field)
        if _is_missing(previous):
            return None, "PRIOR_SOURCE_VALUE_MISSING"
        before = _state_text(previous)
        after = _state_text(current)
        if before == after:
            return None, "NO_STATE_CHANGE"
        return f"{before}->{after}", None

    raise DiscoverySearchError(f"atom_transform_not_supported:{tid}")


def _atom_id(validated_use: Mapping[str, Any], state: str) -> tuple[str, str]:
    basis = {
        "feature_id": validated_use["feature_id"],
        "feature_version": validated_use["feature_version"],
        "feature_version_hash": validated_use["feature_version_hash"],
        "transformation_id": validated_use["transformation_id"],
        "transformation_version": validated_use["transformation_version"],
        "parameters": validated_use["parameters"],
    }
    basis_hash = _hash(basis)
    atom_hash = _hash({"basis_hash": basis_hash, "state": state})
    return f"ATOM-{atom_hash[:24].upper()}", basis_hash


def _build_atoms(
    library: FeatureLibrary,
    prereg: Mapping[str, Any],
    contract: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    specs, exclusions = _feature_use_specs(library, prereg, contract)
    grouped, positions = _history_by_symbol(observations)
    atom_rows: dict[str, dict[str, Any]] = {}

    for validated_use in specs:
        for row_index, row in enumerate(observations):
            symbol = str(row["symbol"])
            position = positions[(symbol, str(row["as_of"]))]
            symbol_history = grouped[symbol]
            history = symbol_history[:position]
            availability = library.pit_availability(
                {
                    "feature_id": validated_use["feature_id"],
                    "feature_version": validated_use["feature_version"],
                    "transformation_id": validated_use["transformation_id"],
                    "transformation_version": validated_use["transformation_version"],
                    "parameters": validated_use["parameters"],
                },
                row,
                data_cutoff=cutoff.isoformat(),
                history=history,
            )
            if not availability["available"]:
                continue
            state, reason = _atom_state(
                validated_use,
                row,
                symbol_history=symbol_history,
                position=position,
            )
            if state is None:
                continue

            atom_id, basis_hash = _atom_id(validated_use, state)
            atom = atom_rows.setdefault(
                atom_id,
                {
                    "atom_id": atom_id,
                    "basis_hash": basis_hash,
                    "feature_id": validated_use["feature_id"],
                    "feature_version": validated_use["feature_version"],
                    "feature_version_hash": validated_use["feature_version_hash"],
                    "transformation_id": validated_use["transformation_id"],
                    "transformation_version": validated_use["transformation_version"],
                    "parameters": dict(validated_use["parameters"]),
                    "state": state,
                    "matching_observation_indices": [],
                },
            )
            atom["matching_observation_indices"].append(row_index)

    atoms: list[dict[str, Any]] = []
    for atom_id in sorted(atom_rows):
        atom = atom_rows[atom_id]
        atom["matching_observation_indices"] = sorted(
            set(atom["matching_observation_indices"])
        )
        atom["raw_support"] = len(atom["matching_observation_indices"])
        atoms.append(atom)
    return atoms, exclusions


def _mutually_exclusive(atoms: Sequence[Mapping[str, Any]]) -> bool:
    seen: dict[str, str] = {}
    for atom in atoms:
        basis = str(atom["basis_hash"])
        state = str(atom["state"])
        if basis in seen and seen[basis] != state:
            return True
        seen[basis] = state
    return False


def _candidate_id(
    family: Mapping[str, Any],
    atom_ids: Sequence[str],
) -> tuple[str, str]:
    spec = {
        "family_id": family["family_id"],
        "conditions": sorted(atom_ids),
    }
    digest = _hash(spec)
    return f"CAND-{digest[:24].upper()}", digest


def _family_id(target: Mapping[str, Any], baseline: str) -> str:
    body = {
        "pattern_type": target["pattern_type"],
        "target_id": target["target_id"],
        "horizon_sessions": target["horizon_sessions"],
        "expected_direction": target["expected_direction"],
        "baseline": baseline,
    }
    return f"FAM-{_hash(body)[:20].upper()}"


def _family_budgets(
    family_ids: Sequence[str],
    *,
    total_budget: int,
    per_family_budget: int,
) -> dict[str, int]:
    if not family_ids:
        return {}
    ordered = sorted(family_ids)
    base = min(per_family_budget, total_budget // len(ordered))
    budgets = {family_id: base for family_id in ordered}
    remaining = total_budget - base * len(ordered)
    while remaining > 0:
        changed = False
        for family_id in ordered:
            if remaining <= 0:
                break
            if budgets[family_id] >= per_family_budget:
                continue
            budgets[family_id] += 1
            remaining -= 1
            changed = True
        if not changed:
            break
    return budgets


def _candidate_stats(
    indices: set[int],
    observations: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any],
) -> dict[str, Any]:
    values: list[float] = []
    symbols: set[str] = set()
    dates: set[str] = set()
    target_id = str(target["target_id"])

    for index in sorted(indices):
        outcome = observations[index]["outcomes"].get(target_id)
        if not outcome:
            continue
        values.append(float(outcome["value"]))
        symbols.add(str(observations[index]["symbol"]))
        dates.add(str(observations[index]["as_of"])[:10])

    if not values:
        return {
            "raw_n": 0,
            "symbol_count": 0,
            "observation_date_count": 0,
            "mean_outcome": None,
            "median_outcome": None,
            "hit_rate": None,
            "aligned_effect": None,
        }

    mean_outcome = sum(values) / len(values)
    expected_positive = target["expected_direction"] == "POSITIVE"
    hits = (
        sum(1 for value in values if value > 0)
        if expected_positive
        else sum(1 for value in values if value < 0)
    )
    return {
        "raw_n": len(values),
        "symbol_count": len(symbols),
        "observation_date_count": len(dates),
        "mean_outcome": mean_outcome,
        "median_outcome": float(median(values)),
        "hit_rate": hits / len(values),
        "aligned_effect": mean_outcome if expected_positive else -mean_outcome,
    }


def _candidate_record(
    *,
    family: Mapping[str, Any],
    atoms: Sequence[Mapping[str, Any]],
    stats: Mapping[str, Any],
    minimum_raw_n: int,
    minimum_effect_size: float,
) -> dict[str, Any]:
    atom_ids = [str(atom["atom_id"]) for atom in atoms]
    candidate_id, spec_hash = _candidate_id(family, atom_ids)
    reasons: list[str] = []
    raw_n = int(stats["raw_n"])
    aligned_effect = stats["aligned_effect"]
    if raw_n < minimum_raw_n:
        reasons.append("MINIMUM_RAW_N_NOT_MET")
    if aligned_effect is None or float(aligned_effect) < minimum_effect_size:
        reasons.append("MINIMUM_EFFECT_SIZE_NOT_MET")

    return {
        "candidate_id": candidate_id,
        "spec_hash": spec_hash,
        "family_id": family["family_id"],
        "pattern_type": family["pattern_type"],
        "target_id": family["target_id"],
        "horizon_sessions": family["horizon_sessions"],
        "expected_direction": family["expected_direction"],
        "baseline": family["baseline"],
        "condition_count": len(atoms),
        "conditions": [
            {
                key: atom[key]
                for key in (
                    "atom_id",
                    "feature_id",
                    "feature_version",
                    "feature_version_hash",
                    "transformation_id",
                    "transformation_version",
                    "parameters",
                    "state",
                )
            }
            for atom in atoms
        ],
        "discovery_stats": dict(stats),
        "l3_gate_status": "ELIGIBLE_FOR_L4" if not reasons else "REJECTED_L3",
        "rejection_reasons": reasons,
        "family_rank": None,
        "shortlist_status": "NOT_SHORTLISTED",
    }


def _rank_family(candidates: list[dict[str, Any]]) -> None:
    eligible = [
        candidate
        for candidate in candidates
        if candidate["l3_gate_status"] == "ELIGIBLE_FOR_L4"
    ]
    eligible.sort(
        key=lambda candidate: (
            -float(candidate["discovery_stats"]["aligned_effect"]),
            -float(candidate["discovery_stats"]["hit_rate"]),
            -int(candidate["discovery_stats"]["raw_n"]),
            str(candidate["candidate_id"]),
        )
    )
    for rank, candidate in enumerate(eligible, start=1):
        candidate["family_rank"] = rank


def _apply_shortlist_budget(
    candidates: list[dict[str, Any]],
    prereg: Mapping[str, Any],
) -> list[str]:
    eligible = [
        candidate
        for candidate in candidates
        if candidate["l3_gate_status"] == "ELIGIBLE_FOR_L4"
        and candidate["family_rank"] is not None
    ]
    eligible.sort(
        key=lambda candidate: (
            int(candidate["family_rank"]),
            int(candidate["horizon_sessions"]),
            str(candidate["family_id"]),
            str(candidate["candidate_id"]),
        )
    )
    total_limit = int(prereg["candidate_budget"]["max_frozen_candidates_total"])
    per_horizon_limit = int(
        prereg["candidate_budget"]["max_frozen_candidates_per_horizon"]
    )
    horizon_counts: dict[int, int] = defaultdict(int)
    shortlisted: list[str] = []
    for candidate in eligible:
        if len(shortlisted) >= total_limit:
            break
        horizon = int(candidate["horizon_sessions"])
        if horizon_counts[horizon] >= per_horizon_limit:
            continue
        candidate["shortlist_status"] = "DISCOVERY_SHORTLIST"
        shortlisted.append(str(candidate["candidate_id"]))
        horizon_counts[horizon] += 1
    return shortlisted


def _verify_cy05_l3_contract_binding(
    manifest: Mapping[str, Any],
    contract: Mapping[str, Any],
    repo_root: str | Path,
) -> None:
    """Bind exact opt-in L3 contract bytes to the frozen L1 source fingerprints.

    The old L3 v1 schema and all of its historical run hashes are untouched.
    CY-05 may not select or edit its research search policy after a run freezes.
    """
    source_path = "configs/pattern_discovery/l3_search_contract_cycle_v2.json"
    if contract.get("identity", {}).get("source_repo_path") != source_path:
        raise DiscoverySearchError("cy05_l3_contract_source_unregistered")
    prereg = manifest["preregistration"]
    if source_path not in prereg["data_sources"]:
        raise DiscoverySearchError("cy05_l3_contract_not_fingerprinted")
    matching = [
        item for item in manifest["input_fingerprints"]
        if item.get("path") == source_path
    ]
    if len(matching) != 1:
        raise DiscoverySearchError("cy05_l3_contract_fingerprint_missing_or_duplicate")
    root = Path(repo_root).resolve()
    file_path = (root / source_path).resolve()
    try:
        file_path.relative_to(root)
    except ValueError as exc:
        raise DiscoverySearchError("cy05_l3_contract_path_outside_repo") from exc
    if not file_path.is_file() or file_path.is_symlink():
        raise DiscoverySearchError("cy05_l3_contract_file_missing")
    current_bytes = file_path.read_bytes()
    if sha256(current_bytes).hexdigest() != matching[0]["sha256"]:
        raise DiscoverySearchError("cy05_l3_contract_bytes_not_frozen")
    on_disk_contract = load_search_contract(file_path)
    if search_contract_hash(on_disk_contract) != search_contract_hash(contract):
        raise DiscoverySearchError("cy05_l3_contract_parameter_mismatch")
    if "level_band" not in contract.get("atom_generation", {}).get(
        "included_transformations", []
    ):
        raise DiscoverySearchError("cy05_l3_level_band_not_enabled")


def run_discovery_search(
    manifest: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    *,
    repo_root: str | Path,
    feature_library: FeatureLibrary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run one deterministic bounded L3 search using discovery data only."""
    verify_run_manifest(manifest)
    run_contract = dict(contract) if contract is not None else load_search_contract()
    library = feature_library or FeatureLibrary()
    cy05_variant = run_contract.get("contract_variant") == "CYCLE-DIR-CY05-L3-v2"
    cy05_library = library.version == "PDL-FEATURE-LIBRARY-CYCLE-v2"
    if cy05_variant != cy05_library:
        raise DiscoverySearchError("cy05_l2_l3_variant_binding_mismatch")
    library.validate_run_binding(manifest, repo_root=repo_root)
    if cy05_variant:
        _verify_cy05_l3_contract_binding(manifest, run_contract, repo_root)

    prereg = manifest["preregistration"]
    requested_pattern_types = set(str(x) for x in prereg["pattern_types"])
    supported_pattern_types = set(str(x) for x in run_contract["supported_pattern_types"])
    unsupported = sorted(requested_pattern_types - supported_pattern_types)
    if unsupported:
        raise DiscoverySearchError(
            "l3_pattern_types_not_yet_supported:" + ",".join(unsupported)
        )

    requested_horizons = set(int(x) for x in prereg["horizons_sessions"])
    supported_horizons = set(int(x) for x in run_contract["supported_horizons_sessions"])
    invalid_horizons = sorted(requested_horizons - supported_horizons)
    if invalid_horizons:
        raise DiscoverySearchError(
            "l3_horizons_not_supported:" + ",".join(str(x) for x in invalid_horizons)
        )

    targets = [
        _target_definition(target_id, run_contract)
        for target_id in prereg["targets"]
    ]
    for target in targets:
        if target["pattern_type"] not in requested_pattern_types:
            raise DiscoverySearchError(
                f"target_pattern_type_not_preregistered:{target['target_id']}"
            )
        if target["horizon_sessions"] not in requested_horizons:
            raise DiscoverySearchError(
                f"target_horizon_not_preregistered:{target['target_id']}"
            )

    cutoff = _timestamp(prereg["data_cutoff"], "manifest.preregistration.data_cutoff")
    normalized = _normalize_observations(
        observations,
        cutoff=cutoff,
        target_ids=[target["target_id"] for target in targets],
    )
    atoms, search_space_exclusions = _build_atoms(
        library,
        prereg,
        run_contract,
        normalized,
        cutoff,
    )
    atom_by_id = {str(atom["atom_id"]): atom for atom in atoms}
    atom_support_sets = {
        atom_id: set(atom["matching_observation_indices"])
        for atom_id, atom in atom_by_id.items()
    }

    families: list[dict[str, Any]] = []
    for target in targets:
        baseline = str(prereg["baselines"][target["target_id"]])
        family = {
            **target,
            "baseline": baseline,
            "family_id": _family_id(target, baseline),
        }
        families.append(family)
    families.sort(key=lambda family: str(family["family_id"]))

    search_budget_total = int(
        prereg["search_budget"]["max_tested_candidates_total"]
    )
    search_budget_per_family = int(
        prereg["search_budget"]["max_tested_candidates_per_family"]
    )
    budgets = _family_budgets(
        [str(family["family_id"]) for family in families],
        total_budget=search_budget_total,
        per_family_budget=search_budget_per_family,
    )

    minimum_raw_n = int(prereg["minimum_criteria"]["minimum_raw_n"])
    minimum_effect = float(prereg["minimum_criteria"]["minimum_effect_size"])
    min_conditions = int(prereg["pattern_complexity"]["min_atomic_conditions"])
    max_conditions = int(prereg["pattern_complexity"]["max_atomic_conditions"])

    candidates: list[dict[str, Any]] = []
    family_summaries: list[dict[str, Any]] = []
    atom_ids = sorted(atom_by_id)

    for family in families:
        family_id = str(family["family_id"])
        family_budget = int(budgets.get(family_id, 0))
        tested = 0
        mutually_exclusive_skipped = 0
        possible_nonexclusive_seen = 0
        family_candidates: list[dict[str, Any]] = []

        target_eligible_indices = {
            index
            for index, row in enumerate(normalized)
            if family["target_id"] in row["outcomes"]
        }

        budget_exhausted = False
        for size in range(min_conditions, max_conditions + 1):
            if budget_exhausted:
                break
            for combo_ids in combinations(atom_ids, size):
                combo_atoms = [atom_by_id[atom_id] for atom_id in combo_ids]
                if _mutually_exclusive(combo_atoms):
                    mutually_exclusive_skipped += 1
                    continue
                possible_nonexclusive_seen += 1
                if tested >= family_budget:
                    budget_exhausted = True
                    break

                match_indices = set(target_eligible_indices)
                for atom_id in combo_ids:
                    match_indices &= atom_support_sets[atom_id]
                    if not match_indices:
                        break
                stats = _candidate_stats(match_indices, normalized, family)
                candidate = _candidate_record(
                    family=family,
                    atoms=combo_atoms,
                    stats=stats,
                    minimum_raw_n=minimum_raw_n,
                    minimum_effect_size=minimum_effect,
                )
                family_candidates.append(candidate)
                tested += 1

        _rank_family(family_candidates)
        candidates.extend(family_candidates)
        family_summaries.append(
            {
                "family_id": family_id,
                "pattern_type": family["pattern_type"],
                "target_id": family["target_id"],
                "horizon_sessions": family["horizon_sessions"],
                "expected_direction": family["expected_direction"],
                "baseline": family["baseline"],
                "allocated_test_budget": family_budget,
                "tested_candidates": tested,
                "eligible_candidates": sum(
                    1
                    for candidate in family_candidates
                    if candidate["l3_gate_status"] == "ELIGIBLE_FOR_L4"
                ),
                "rejected_candidates": sum(
                    1
                    for candidate in family_candidates
                    if candidate["l3_gate_status"] == "REJECTED_L3"
                ),
                "mutually_exclusive_specs_skipped": mutually_exclusive_skipped,
                "budget_exhausted": budget_exhausted,
                "nonexclusive_specs_seen_before_stop": possible_nonexclusive_seen,
                "target_eligible_observations": len(target_eligible_indices),
            }
        )

    candidates.sort(
        key=lambda candidate: (
            str(candidate["family_id"]),
            len(candidate["conditions"]),
            str(candidate["candidate_id"]),
        )
    )
    shortlisted = _apply_shortlist_budget(candidates, prereg)

    result: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L3",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "run_id": manifest["run_id"],
        "run_identity_hash": manifest["run_identity_hash"],
        "l1_manifest_hash": manifest["manifest_hash"],
        "l3_search_contract_hash": search_contract_hash(run_contract),
        "feature_library_version": library.version,
        "feature_library_hash": library.library_hash,
        "data_cutoff": prereg["data_cutoff"],
        "discovery_only": True,
        "confirmation_data_used": False,
        "l4_statistical_guard_applied": False,
        "l5_candidate_freeze_applied": False,
        "search_space": {
            "condition_count_range": [min_conditions, max_conditions],
            "registered_atom_count": len(atoms),
            "atom_catalog": [
                {
                    key: atom[key]
                    for key in (
                        "atom_id",
                        "basis_hash",
                        "feature_id",
                        "feature_version",
                        "feature_version_hash",
                        "transformation_id",
                        "transformation_version",
                        "parameters",
                        "state",
                        "raw_support",
                    )
                }
                for atom in atoms
            ],
            "exclusions": search_space_exclusions,
            "search_budget_total": search_budget_total,
            "search_budget_per_family": search_budget_per_family,
            "family_budget_allocation": budgets,
        },
        "observations": {
            "input_count": len(normalized),
            "symbols": len({str(row["symbol"]) for row in normalized}),
            "as_of_min": min((row["as_of"] for row in normalized), default=None),
            "as_of_max": max((row["as_of"] for row in normalized), default=None),
        },
        "families": family_summaries,
        "candidates": candidates,
        "shortlisted_candidate_ids": shortlisted,
        "candidate_counts": {
            "tested": len(candidates),
            "eligible_for_l4": sum(
                1
                for candidate in candidates
                if candidate["l3_gate_status"] == "ELIGIBLE_FOR_L4"
            ),
            "rejected_l3": sum(
                1
                for candidate in candidates
                if candidate["l3_gate_status"] == "REJECTED_L3"
            ),
            "discovery_shortlist": len(shortlisted),
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(result)
    result["result_hash"] = _hash(result)
    return result


def verify_search_result(result: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise DiscoverySearchError("search_result_must_be_object")
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        raise DiscoverySearchError("search_result_schema_invalid")
    if result.get("research_only") is not True:
        raise DiscoverySearchError("search_result_research_only_guard_missing")
    if result.get("productive_integration_enabled") is not False:
        raise DiscoverySearchError("search_result_productive_integration_forbidden")
    if result.get("execution_allowed") is not False:
        raise DiscoverySearchError("search_result_execution_forbidden")
    if result.get("confirmation_data_used") is not False:
        raise DiscoverySearchError("search_result_confirmation_data_forbidden")
    if result.get("l4_statistical_guard_applied") is not False:
        raise DiscoverySearchError("search_result_l4_boundary_violation")
    if result.get("l5_candidate_freeze_applied") is not False:
        raise DiscoverySearchError("search_result_l5_boundary_violation")

    stored = _text(result.get("result_hash"), "result_hash")
    body = dict(result)
    body.pop("result_hash", None)
    if _hash(body) != stored:
        raise DiscoverySearchError("search_result_hash_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(result)
    return {
        "valid": True,
        "run_id": result.get("run_id"),
        "result_hash": stored,
        "tested_candidates": result.get("candidate_counts", {}).get("tested"),
    }


def search_result_repo_path(
    run_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_search_contract()
    return str(spec["identity"]["result_path_template"]).format(
        run_id=_text(run_id, "run_id")
    )


def write_search_result(
    repo_root: str | Path,
    result: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
) -> Path:
    """Persist an L3 result exactly once inside the L0 research namespace."""
    verify_search_result(result)
    guard = boundary or PatternDiscoveryBoundary()
    repo_path = search_result_repo_path(str(result["run_id"]))
    guard.assert_write_path_allowed(repo_path)

    root = Path(repo_root).resolve()
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise DiscoverySearchError("search_result_path_outside_repo") from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(result),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
    except FileExistsError as exc:
        raise DiscoverySearchError(
            f"search_result_already_exists:{repo_path}"
        ) from exc
    return target
