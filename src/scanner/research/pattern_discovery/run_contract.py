"""Phase L1 deterministic Discovery Run pre-registration.

L1 freezes the complete search declaration and exact input fingerprints before
Pattern Discovery search begins. It deliberately does not implement feature
generation, pattern search, candidate selection, statistical testing,
confirmation, promotion or Decision-Layer integration.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary, boundary_contract_hash


CONTRACT_SCHEMA_VERSION = "pattern_discovery_l1_run_contract_v1"
MANIFEST_SCHEMA_VERSION = "pattern_discovery_run_manifest_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l1_discovery_run_contract_v1.json"
)


class DiscoveryRunContractError(ValueError):
    """Raised when an L1 pre-registration or frozen manifest is invalid."""


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


def _nonblank(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DiscoveryRunContractError(f"value_required:{field}")
    return result


def _aware_timestamp(value: Any, field: str) -> str:
    text = _nonblank(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise DiscoveryRunContractError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise DiscoveryRunContractError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat()


def _string_list(value: Any, field: str, *, nonempty: bool) -> list[str]:
    if not isinstance(value, list):
        raise DiscoveryRunContractError(f"list_required:{field}")
    result = [_nonblank(item, f"{field}[{index}]") for index, item in enumerate(value)]
    if nonempty and not result:
        raise DiscoveryRunContractError(f"nonempty_list_required:{field}")
    if len(set(result)) != len(result):
        raise DiscoveryRunContractError(f"duplicate_list_value:{field}")
    return result


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DiscoveryRunContractError(f"positive_integer_required:{field}")
    return value


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DiscoveryRunContractError(f"finite_number_required:{field}")
    result = float(value)
    if not math.isfinite(result):
        raise DiscoveryRunContractError(f"finite_number_required:{field}")
    return result


def _exact_mapping(
    value: Any,
    field: str,
    required_fields: Sequence[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DiscoveryRunContractError(f"object_required:{field}")
    missing = [name for name in required_fields if name not in value]
    if missing:
        raise DiscoveryRunContractError(
            f"fields_missing:{field}:" + ",".join(missing)
        )
    extra = sorted(set(value) - set(required_fields))
    if extra:
        raise DiscoveryRunContractError(
            f"unknown_fields:{field}:" + ",".join(str(item) for item in extra)
        )
    return dict(value)


def load_run_contract(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate the versioned L1 contract."""
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoveryRunContractError(f"run_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        raise DiscoveryRunContractError("run_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise DiscoveryRunContractError("run_contract_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise DiscoveryRunContractError("productive_integration_must_be_disabled")
    if payload.get("execution_allowed") is not False:
        raise DiscoveryRunContractError("execution_must_be_disabled")
    return payload


def run_contract_hash(contract: Mapping[str, Any] | None = None) -> str:
    """Return deterministic identity of the complete L1 contract."""
    value = dict(contract) if contract is not None else load_run_contract()
    return _hash(value)


def normalize_preregistration(
    raw: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
    boundary: PatternDiscoveryBoundary | None = None,
) -> dict[str, Any]:
    """Validate and canonicalize one complete pre-search declaration."""
    spec = dict(contract) if contract is not None else load_run_contract()
    if not isinstance(raw, Mapping):
        raise DiscoveryRunContractError("preregistration_must_be_object")

    prereg = spec["preregistration"]
    required = tuple(prereg["required_fields"])
    missing = [field for field in required if field not in raw]
    if missing:
        raise DiscoveryRunContractError(
            "preregistration_fields_missing:" + ",".join(missing)
        )
    if prereg.get("reject_unknown_fields") is True:
        extra = sorted(set(raw) - set(required))
        if extra:
            raise DiscoveryRunContractError(
                "preregistration_unknown_fields:" + ",".join(str(item) for item in extra)
            )

    start_at = _aware_timestamp(raw["declared_start_at"], "declared_start_at")
    cutoff = _aware_timestamp(raw["data_cutoff"], "data_cutoff")
    if datetime.fromisoformat(cutoff) > datetime.fromisoformat(start_at):
        raise DiscoveryRunContractError("data_cutoff_after_declared_start")

    guard = boundary or PatternDiscoveryBoundary()
    data_sources = _string_list(raw["data_sources"], "data_sources", nonempty=True)
    data_sources = [guard.assert_read_path_allowed(path) for path in data_sources]

    pit_rules = _string_list(raw["pit_rules"], "pit_rules", nonempty=True)
    transformations = _string_list(
        raw["allowed_transformations"], "allowed_transformations", nonempty=True
    )
    pattern_types = [
        value.upper()
        for value in _string_list(raw["pattern_types"], "pattern_types", nonempty=True)
    ]
    allowed_pattern_types = set(prereg["allowed_pattern_types"])
    invalid_types = [value for value in pattern_types if value not in allowed_pattern_types]
    if invalid_types:
        raise DiscoveryRunContractError(
            "unsupported_pattern_types:" + ",".join(invalid_types)
        )

    horizons_raw = raw["horizons_sessions"]
    if not isinstance(horizons_raw, list) or not horizons_raw:
        raise DiscoveryRunContractError("nonempty_list_required:horizons_sessions")
    horizons = [_positive_int(value, f"horizons_sessions[{index}]") for index, value in enumerate(horizons_raw)]
    if len(set(horizons)) != len(horizons):
        raise DiscoveryRunContractError("duplicate_list_value:horizons_sessions")
    allowed_horizons = set(prereg["allowed_horizons_sessions"])
    invalid_horizons = [value for value in horizons if value not in allowed_horizons]
    if invalid_horizons:
        raise DiscoveryRunContractError(
            "unsupported_horizons:" + ",".join(str(value) for value in invalid_horizons)
        )

    complexity = _exact_mapping(
        raw["pattern_complexity"],
        "pattern_complexity",
        ("min_atomic_conditions", "max_atomic_conditions"),
    )
    min_conditions = _positive_int(
        complexity["min_atomic_conditions"], "pattern_complexity.min_atomic_conditions"
    )
    max_conditions = _positive_int(
        complexity["max_atomic_conditions"], "pattern_complexity.max_atomic_conditions"
    )
    if min_conditions > max_conditions:
        raise DiscoveryRunContractError("pattern_complexity_min_exceeds_max")
    if max_conditions > int(prereg["max_atomic_conditions_initial"]):
        raise DiscoveryRunContractError("pattern_complexity_exceeds_l1_initial_limit")

    targets = sorted(_string_list(raw["targets"], "targets", nonempty=True))
    baselines = raw["baselines"]
    if not isinstance(baselines, Mapping) or not baselines:
        raise DiscoveryRunContractError("nonempty_object_required:baselines")
    normalized_baselines = {
        _nonblank(key, "baselines.key"): _nonblank(value, f"baselines.{key}")
        for key, value in baselines.items()
    }
    if set(normalized_baselines) != set(targets):
        missing_baselines = sorted(set(targets) - set(normalized_baselines))
        extra_baselines = sorted(set(normalized_baselines) - set(targets))
        detail = []
        if missing_baselines:
            detail.append("missing=" + ",".join(missing_baselines))
        if extra_baselines:
            detail.append("extra=" + ",".join(extra_baselines))
        raise DiscoveryRunContractError(
            "baseline_target_mismatch:" + ";".join(detail)
        )

    minimum_spec = spec["minimum_criteria"]
    minimum = _exact_mapping(
        raw["minimum_criteria"],
        "minimum_criteria",
        tuple(minimum_spec["required_fields"]),
    )
    minimum_normalized = {
        "minimum_raw_n": _positive_int(
            minimum["minimum_raw_n"], "minimum_criteria.minimum_raw_n"
        ),
        "minimum_temporal_support_regions": _positive_int(
            minimum["minimum_temporal_support_regions"],
            "minimum_criteria.minimum_temporal_support_regions",
        ),
        "minimum_effect_size": _finite_number(
            minimum["minimum_effect_size"], "minimum_criteria.minimum_effect_size"
        ),
        "minimum_baseline_lift": _finite_number(
            minimum["minimum_baseline_lift"], "minimum_criteria.minimum_baseline_lift"
        ),
    }
    if minimum_normalized["minimum_effect_size"] < 0.0:
        raise DiscoveryRunContractError("minimum_effect_size_must_be_nonnegative")
    if minimum_normalized["minimum_baseline_lift"] < 0.0:
        raise DiscoveryRunContractError("minimum_baseline_lift_must_be_nonnegative")

    search = _exact_mapping(
        raw["search_budget"],
        "search_budget",
        tuple(spec["search_budget"]["required_fields"]),
    )
    search_normalized = {
        key: _positive_int(value, f"search_budget.{key}")
        for key, value in search.items()
    }
    if search_normalized["max_tested_candidates_per_family"] > search_normalized["max_tested_candidates_total"]:
        raise DiscoveryRunContractError("search_budget_per_family_exceeds_total")

    candidate = _exact_mapping(
        raw["candidate_budget"],
        "candidate_budget",
        tuple(spec["candidate_budget"]["required_fields"]),
    )
    candidate_normalized = {
        key: _positive_int(value, f"candidate_budget.{key}")
        for key, value in candidate.items()
    }
    if candidate_normalized["max_frozen_candidates_total"] > search_normalized["max_tested_candidates_total"]:
        raise DiscoveryRunContractError("candidate_budget_exceeds_search_budget")
    if candidate_normalized["max_frozen_candidates_per_horizon"] > candidate_normalized["max_frozen_candidates_total"]:
        raise DiscoveryRunContractError("candidate_budget_per_horizon_exceeds_total")

    mt = _exact_mapping(
        raw["multiple_testing"],
        "multiple_testing",
        tuple(spec["multiple_testing"]["required_fields"]),
    )
    primary_method = _nonblank(
        mt["primary_method"], "multiple_testing.primary_method"
    ).upper()
    if primary_method not in set(spec["multiple_testing"]["allowed_primary_methods"]):
        raise DiscoveryRunContractError("multiple_testing_primary_method_invalid")
    if not isinstance(mt["parameters"], Mapping):
        raise DiscoveryRunContractError("object_required:multiple_testing.parameters")
    mt_parameters = dict(mt["parameters"])
    if primary_method in {"BONFERRONI_FWER", "HOLM_FWER"}:
        if set(mt_parameters) != {"family_alpha"}:
            raise DiscoveryRunContractError("multiple_testing_family_alpha_required")
        alpha = _finite_number(
            mt_parameters["family_alpha"], "multiple_testing.parameters.family_alpha"
        )
        if not 0.0 < alpha < 1.0:
            raise DiscoveryRunContractError("multiple_testing_family_alpha_out_of_range")
        mt_parameters = {"family_alpha": alpha}
    elif primary_method == "BENJAMINI_HOCHBERG_FDR":
        if set(mt_parameters) != {"fdr_q"}:
            raise DiscoveryRunContractError("multiple_testing_fdr_q_required")
        q = _finite_number(
            mt_parameters["fdr_q"], "multiple_testing.parameters.fdr_q"
        )
        if not 0.0 < q < 1.0:
            raise DiscoveryRunContractError("multiple_testing_fdr_q_out_of_range")
        mt_parameters = {"fdr_q": q}
    elif primary_method == "PREDECLARED_SINGLE_PRIMARY":
        if mt_parameters:
            raise DiscoveryRunContractError("single_primary_parameters_must_be_empty")
    else:
        required_custom = {"method_name", "rule_hash", "rationale"}
        if set(mt_parameters) != required_custom:
            raise DiscoveryRunContractError("custom_multiple_testing_parameters_invalid")
        mt_parameters = {
            field: _nonblank(mt_parameters[field], f"multiple_testing.parameters.{field}")
            for field in sorted(required_custom)
        }

    determinism = _exact_mapping(
        raw["determinism"],
        "determinism",
        tuple(spec["determinism"]["required_fields"]),
    )
    randomness = determinism["randomness_allowed"]
    if not isinstance(randomness, bool):
        raise DiscoveryRunContractError("boolean_required:determinism.randomness_allowed")
    seed = determinism["seed"]
    if randomness:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise DiscoveryRunContractError("integer_seed_required_when_randomness_allowed")
    elif seed is not None:
        raise DiscoveryRunContractError("seed_forbidden_when_randomness_disabled")

    return {
        "declared_start_at": start_at,
        "data_cutoff": cutoff,
        "data_sources": sorted(data_sources),
        "pit_rules": sorted(pit_rules),
        "universe_version": _nonblank(raw["universe_version"], "universe_version"),
        "feature_library_version": _nonblank(
            raw["feature_library_version"], "feature_library_version"
        ),
        "allowed_transformations": sorted(transformations),
        "pattern_complexity": {
            "min_atomic_conditions": min_conditions,
            "max_atomic_conditions": max_conditions,
        },
        "pattern_types": sorted(pattern_types),
        "targets": targets,
        "horizons_sessions": sorted(horizons),
        "baselines": dict(sorted(normalized_baselines.items())),
        "minimum_criteria": minimum_normalized,
        "search_budget": search_normalized,
        "candidate_budget": candidate_normalized,
        "multiple_testing": {
            "primary_method": primary_method,
            "parameters": mt_parameters,
        },
        "statistical_primary_method": _nonblank(
            raw["statistical_primary_method"], "statistical_primary_method"
        ),
        "robustness_checks": sorted(
            _string_list(raw["robustness_checks"], "robustness_checks", nonempty=True)
        ),
        "exclusion_rules": sorted(
            _string_list(raw["exclusion_rules"], "exclusion_rules", nonempty=False)
        ),
        "dependency_rules": sorted(
            _string_list(raw["dependency_rules"], "dependency_rules", nonempty=True)
        ),
        "code_version": _nonblank(raw["code_version"], "code_version"),
        "determinism": {
            "randomness_allowed": randomness,
            "seed": seed,
        },
    }


def fingerprint_inputs(
    repo_root: str | Path,
    data_sources: Sequence[str],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
) -> list[dict[str, Any]]:
    """Fingerprint exact approved input bytes in deterministic path order."""
    root = Path(repo_root).resolve()
    guard = boundary or PatternDiscoveryBoundary()
    records: list[dict[str, Any]] = []
    for raw_path in sorted(data_sources):
        repo_path = guard.assert_read_path_allowed(raw_path)
        candidate = (root / repo_path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise DiscoveryRunContractError(
                f"input_resolves_outside_repo:{repo_path}"
            ) from exc
        if not candidate.is_file():
            raise DiscoveryRunContractError(f"input_file_missing:{repo_path}")
        content = candidate.read_bytes()
        records.append(
            {
                "path": repo_path,
                "sha256": sha256(content).hexdigest(),
                "size_bytes": len(content),
            }
        )
    if not records:
        raise DiscoveryRunContractError("input_fingerprints_required")
    return records


def build_run_manifest(
    preregistration: Mapping[str, Any],
    *,
    repo_root: str | Path,
    contract: Mapping[str, Any] | None = None,
    boundary: PatternDiscoveryBoundary | None = None,
) -> dict[str, Any]:
    """Build one fully frozen deterministic run manifest before search."""
    run_contract = dict(contract) if contract is not None else load_run_contract()
    guard = boundary or PatternDiscoveryBoundary()
    normalized = normalize_preregistration(
        preregistration,
        contract=run_contract,
        boundary=guard,
    )
    fingerprints = fingerprint_inputs(
        repo_root,
        normalized["data_sources"],
        boundary=guard,
    )
    config_hash = _hash(normalized)
    input_fingerprint_hash = _hash(fingerprints)
    l0_hash = guard.contract_hash
    l1_hash = run_contract_hash(run_contract)
    identity_body = {
        "config_hash": config_hash,
        "input_fingerprint_hash": input_fingerprint_hash,
        "l0_boundary_contract_hash": l0_hash,
        "l1_run_contract_hash": l1_hash,
    }
    run_identity_hash = _hash(identity_body)
    identity = run_contract["identity"]
    digest_chars = int(identity["run_id_digest_chars"])
    run_id = f"{identity['run_id_prefix']}-{run_identity_hash[:digest_chars].upper()}"

    body: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L1",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "state": "FROZEN_PRE_RUN",
        "run_id": run_id,
        "run_identity_hash": run_identity_hash,
        "config_hash": config_hash,
        "input_fingerprint_hash": input_fingerprint_hash,
        "l0_boundary_contract_hash": l0_hash,
        "l1_run_contract_hash": l1_hash,
        "preregistration": normalized,
        "input_fingerprints": fingerprints,
    }
    body["manifest_hash"] = _hash(body)
    return body


def verify_run_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed if any frozen manifest field was changed after construction."""
    if not isinstance(manifest, Mapping):
        raise DiscoveryRunContractError("manifest_must_be_object")
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise DiscoveryRunContractError("manifest_schema_invalid")
    if manifest.get("state") != "FROZEN_PRE_RUN":
        raise DiscoveryRunContractError("manifest_state_invalid")
    if manifest.get("research_only") is not True:
        raise DiscoveryRunContractError("manifest_research_only_guard_missing")
    if manifest.get("productive_integration_enabled") is not False:
        raise DiscoveryRunContractError("manifest_productive_integration_forbidden")
    if manifest.get("execution_allowed") is not False:
        raise DiscoveryRunContractError("manifest_execution_forbidden")

    stored_manifest_hash = _nonblank(manifest.get("manifest_hash"), "manifest_hash")
    body = dict(manifest)
    body.pop("manifest_hash", None)
    if _hash(body) != stored_manifest_hash:
        raise DiscoveryRunContractError("manifest_hash_mismatch")

    contract = load_run_contract()
    if manifest.get("l1_run_contract_hash") != run_contract_hash(contract):
        raise DiscoveryRunContractError("manifest_l1_contract_hash_mismatch")
    if manifest.get("l0_boundary_contract_hash") != boundary_contract_hash():
        raise DiscoveryRunContractError("manifest_l0_contract_hash_mismatch")

    prereg = manifest.get("preregistration")
    fingerprints = manifest.get("input_fingerprints")
    if not isinstance(prereg, Mapping) or not isinstance(fingerprints, list):
        raise DiscoveryRunContractError("manifest_identity_components_missing")
    normalized_prereg = normalize_preregistration(prereg, contract=contract)
    if normalized_prereg != dict(prereg):
        raise DiscoveryRunContractError("manifest_preregistration_not_canonical")
    if _hash(prereg) != manifest.get("config_hash"):
        raise DiscoveryRunContractError("manifest_config_hash_mismatch")

    if not fingerprints:
        raise DiscoveryRunContractError("manifest_input_fingerprints_empty")
    fingerprint_paths = []
    for index, item in enumerate(fingerprints):
        if not isinstance(item, Mapping) or set(item) != {"path", "sha256", "size_bytes"}:
            raise DiscoveryRunContractError(
                f"manifest_input_fingerprint_schema_invalid:{index}"
            )
        path = str(item["path"])
        digest = str(item["sha256"])
        size = item["size_bytes"]
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest.lower()):
            raise DiscoveryRunContractError(
                f"manifest_input_fingerprint_sha_invalid:{index}"
            )
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise DiscoveryRunContractError(
                f"manifest_input_fingerprint_size_invalid:{index}"
            )
        fingerprint_paths.append(path)
    if fingerprint_paths != list(prereg["data_sources"]):
        raise DiscoveryRunContractError("manifest_input_paths_mismatch")
    if _hash(fingerprints) != manifest.get("input_fingerprint_hash"):
        raise DiscoveryRunContractError("manifest_input_fingerprint_hash_mismatch")

    identity_body = {
        "config_hash": manifest.get("config_hash"),
        "input_fingerprint_hash": manifest.get("input_fingerprint_hash"),
        "l0_boundary_contract_hash": manifest.get("l0_boundary_contract_hash"),
        "l1_run_contract_hash": manifest.get("l1_run_contract_hash"),
    }
    run_identity_hash = _hash(identity_body)
    if run_identity_hash != manifest.get("run_identity_hash"):
        raise DiscoveryRunContractError("manifest_run_identity_hash_mismatch")

    digest_chars = int(contract["identity"]["run_id_digest_chars"])
    expected_run_id = (
        f"{contract['identity']['run_id_prefix']}-"
        f"{run_identity_hash[:digest_chars].upper()}"
    )
    if manifest.get("run_id") != expected_run_id:
        raise DiscoveryRunContractError("manifest_run_id_mismatch")

    return {
        "valid": True,
        "run_id": expected_run_id,
        "manifest_hash": stored_manifest_hash,
        "input_count": len(fingerprints),
    }


def manifest_repo_path(
    run_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    """Return the contract-defined immutable manifest repository path."""
    value = dict(contract) if contract is not None else load_run_contract()
    run_id_value = _nonblank(run_id, "run_id")
    return str(value["identity"]["manifest_path_template"]).format(run_id=run_id_value)


def write_run_manifest(
    repo_root: str | Path,
    manifest: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
) -> Path:
    """Persist a frozen manifest exactly once; no overwrite API exists in L1."""
    verify_run_manifest(manifest)
    guard = boundary or PatternDiscoveryBoundary()
    repo_path = manifest_repo_path(str(manifest["run_id"]))
    guard.assert_write_path_allowed(repo_path)

    root = Path(repo_root).resolve()
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise DiscoveryRunContractError("manifest_path_resolves_outside_repo") from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(manifest),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
    except FileExistsError as exc:
        raise DiscoveryRunContractError(
            f"frozen_manifest_already_exists:{repo_path}"
        ) from exc
    return target
