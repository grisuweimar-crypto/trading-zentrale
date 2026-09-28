"""Phase 8H-C outcome-blind interaction specification freeze.

The module freezes only explicit, preregistered interactions built from components
already authorized by Phase 8H-B. It never reads an interaction outcome.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence

from scanner.research.external_evidence.interaction_eligibility_8h import (
    BOUND_STATE,
    ELIGIBILITY_RESULT_SCHEMA,
    NO_PROMOTED_STATE,
    WAITING_STATE,
)


INTERACTION_SPEC_CONTRACT_SCHEMA = "external_evidence_8h_interaction_spec_freeze_v1"
CANDIDATE_MANIFEST_SCHEMA = "external_evidence_8h_interaction_candidate_manifest_v1"
INTERACTION_FREEZE_RESULT_SCHEMA = "external_evidence_8h_interaction_spec_freeze_result_v1"
WAITING_FOR_ELIGIBILITY = "WAITING_FOR_ELIGIBLE_FACTOR_HORIZONS"
NO_ELIGIBLE_COMPONENTS = "NO_ELIGIBLE_INTERACTION_COMPONENTS"
NO_PREREGISTERED_SPECS = "NO_PREREGISTERED_INTERACTION_SPECS"
SPECS_FROZEN = "INTERACTION_SPECS_FROZEN"


class ExternalEvidence8HSpecError(ValueError):
    """Raised when Phase-8H-C outcome-blind specification rules are violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_embedded_digest(value: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(value.get(field) or "")
    if len(recorded) != 64:
        raise ExternalEvidence8HSpecError(error)
    payload = dict(value)
    payload.pop(field, None)
    if _digest(payload) != recorded:
        raise ExternalEvidence8HSpecError(error)
    return recorded


def _timezone_aware_timestamp(value: object, error: str) -> str:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8HSpecError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8HSpecError(error) from exc
    if parsed.tzinfo is None:
        raise ExternalEvidence8HSpecError(error)
    return str(value)


def _sequence(value: object, error: str) -> list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ExternalEvidence8HSpecError(error)
    return list(value)


def validate_spec_contract(
    contract: Mapping[str, Any], challenger_specs: Mapping[str, Any]
) -> None:
    if contract.get("schema_version") != INTERACTION_SPEC_CONTRACT_SCHEMA:
        raise ExternalEvidence8HSpecError("unsupported_8h_c_contract")
    if contract.get("phase") != "8H-C":
        raise ExternalEvidence8HSpecError("8h_c_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HSpecError("8h_c_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HSpecError(f"8h_c_boundary_must_remain_false:{key}")

    allowed_classes = tuple(contract.get("allowed_interaction_classes") or ())
    if allowed_classes != ("CORE_X_EXTERNAL", "EXTERNAL_X_EXTERNAL"):
        raise ExternalEvidence8HSpecError("8h_c_interaction_class_contract_changed")

    baseline = challenger_specs.get("scanner_baseline") or {}
    frozen_numeric = set(str(x) for x in baseline.get("numeric_features") or ())
    catalog = contract.get("core_numeric_component_catalog")
    if not isinstance(catalog, Mapping) or not catalog:
        raise ExternalEvidence8HSpecError("8h_c_core_catalog_missing")
    for component_id, row in catalog.items():
        if not isinstance(row, Mapping):
            raise ExternalEvidence8HSpecError(f"8h_c_core_catalog_row_invalid:{component_id}")
        template = str(row.get("field_template") or "")
        probe = template.replace("{H}", "5")
        frozen_probe = {str(x).replace("{H}", "5") for x in frozen_numeric}
        if probe not in frozen_probe:
            raise ExternalEvidence8HSpecError(f"8h_c_core_component_not_in_frozen_baseline:{component_id}")

    math = contract.get("interaction_math") or {}
    if math.get("operator") != "ELEMENTWISE_PRODUCT":
        raise ExternalEvidence8HSpecError("8h_c_operator_must_remain_product")
    if math.get("exactly_one_new_interaction_term_per_spec") is not True:
        raise ExternalEvidence8HSpecError("8h_c_exactly_one_term_required")
    if math.get("both_underlying_main_effects_remain_present") is not True:
        raise ExternalEvidence8HSpecError("8h_c_main_effects_must_remain")
    for key in (
        "main_effect_removal_allowed",
        "quadratic_or_self_interaction_allowed",
        "threshold_or_bin_interaction_allowed",
        "sign_flip_allowed",
        "absolute_value_transform_allowed",
        "nonlinear_transform_search_allowed",
        "multiple_variants_per_spec_allowed",
        "fit_or_scaling_parameters_may_use_validation_or_holdout",
    ):
        if math.get(key) is not False:
            raise ExternalEvidence8HSpecError(f"8h_c_forbidden_math_enabled:{key}")


def _result(
    *,
    state: str,
    eligibility_binding_sha256: str | None,
    manifest_sha256: str | None,
    frozen_at: str | None,
    specs: list[dict[str, Any]],
    wait_reason: str | None = None,
) -> dict[str, Any]:
    family = [
        {
            "hypothesis_id": f"{spec['interaction_spec_id']}__peer_excess_{spec['horizon_sessions']}t",
            "interaction_spec_id": spec["interaction_spec_id"],
            "horizon_sessions": spec["horizon_sessions"],
            "primary_outcome": "peer_excess",
        }
        for spec in specs
    ]
    result: dict[str, Any] = {
        "schema_version": INTERACTION_FREEZE_RESULT_SCHEMA,
        "phase": "8H-C",
        "state": state,
        "eligibility_binding_sha256": eligibility_binding_sha256,
        "candidate_manifest_sha256": manifest_sha256,
        "frozen_at": frozen_at,
        "wait_reason": wait_reason,
        "frozen_interaction_specs": specs,
        "confirmatory_family": family,
        "holm_method": "Holm",
        "family_wise_alpha": 0.05,
        "interaction_dataset_model_construction_authorized": bool(specs),
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-D_DATASET_MODEL_CONSTRUCTION",
    }
    result["freeze_sha256"] = _digest(result)
    return result


def _validate_manifest(manifest: Mapping[str, Any]) -> str:
    if manifest.get("schema_version") != CANDIDATE_MANIFEST_SCHEMA:
        raise ExternalEvidence8HSpecError("8h_c_candidate_manifest_schema_mismatch")
    if manifest.get("phase") != "8H-C":
        raise ExternalEvidence8HSpecError("8h_c_candidate_manifest_phase_mismatch")
    manifest_sha = _verify_embedded_digest(
        manifest, "manifest_sha256", "8h_c_candidate_manifest_digest_mismatch"
    )
    if not str(manifest.get("author_identity") or "").strip():
        raise ExternalEvidence8HSpecError("8h_c_candidate_manifest_author_required")
    _timezone_aware_timestamp(manifest.get("authored_at"), "8h_c_candidate_manifest_timestamp_invalid")
    required_false = (
        "outcome_values_read",
        "economic_theory_used_to_rank_or_select",
        "candidate_ranking_used",
        "automatic_cartesian_generation_used",
        "threshold_search_used",
        "sign_search_used",
        "hyperparameter_search_used",
    )
    for key in required_false:
        if manifest.get(key) is not False:
            raise ExternalEvidence8HSpecError(f"8h_c_manifest_guard_not_false:{key}")
    _sequence(manifest.get("entries") or [], "8h_c_candidate_manifest_entries_invalid")
    return manifest_sha


def _external_component(
    row: Mapping[str, Any],
    eligible: Mapping[str, Mapping[str, Any]],
    horizon: int,
) -> dict[str, Any]:
    factor_horizon_id = str(row.get("factor_horizon_id") or "")
    feature_field = str(row.get("feature_field") or "")
    if factor_horizon_id not in eligible:
        raise ExternalEvidence8HSpecError(f"8h_c_external_component_not_eligible:{factor_horizon_id}")
    bound = eligible[factor_horizon_id]
    if int(bound.get("horizon_sessions", -1)) != horizon:
        raise ExternalEvidence8HSpecError(f"8h_c_cross_horizon_component_forbidden:{factor_horizon_id}")
    allowed_fields = [str(x) for x in bound.get("factor_spec_feature_fields") or ()]
    if feature_field not in allowed_fields:
        raise ExternalEvidence8HSpecError(
            f"8h_c_external_feature_not_in_bound_factor_spec:{factor_horizon_id}:{feature_field}"
        )
    return {
        "kind": "EXTERNAL_NUMERIC",
        "factor_horizon_id": factor_horizon_id,
        "factor_id": str(bound.get("factor_id") or ""),
        "horizon_sessions": horizon,
        "feature_field": feature_field,
        "component_identity_sha256": _digest(
            {
                "factor_horizon_id": factor_horizon_id,
                "factor_spec_sha256": bound.get("factor_spec_sha256"),
                "feature_field": feature_field,
                "promotion_completion_sha256": bound.get("promotion_completion_sha256"),
            }
        ),
    }


def _core_component(row: Mapping[str, Any], contract: Mapping[str, Any], horizon: int) -> dict[str, Any]:
    component_id = str(row.get("component_id") or "")
    catalog = contract["core_numeric_component_catalog"]
    if component_id not in catalog:
        raise ExternalEvidence8HSpecError(f"8h_c_core_component_not_allowed:{component_id}")
    template = str(catalog[component_id]["field_template"])
    return {
        "kind": "CORE_NUMERIC",
        "component_id": component_id,
        "resolved_field": template.replace("{H}", str(horizon)),
        "horizon_sessions": horizon,
    }


def _component_token(row: Mapping[str, Any]) -> str:
    if row.get("kind") == "CORE_NUMERIC":
        return f"CORE:{row['component_id']}:{row['resolved_field']}"
    return f"EXTERNAL:{row['factor_horizon_id']}:{row['feature_field']}"


def freeze_interaction_specs(
    *,
    contract: Mapping[str, Any],
    challenger_specs: Mapping[str, Any],
    eligibility_result: Mapping[str, Any],
    candidate_manifest: Mapping[str, Any] | None = None,
    frozen_at: str | None = None,
) -> dict[str, Any]:
    """Freeze explicit 8H-C specs and the full Holm family without reading outcomes."""
    validate_spec_contract(contract, challenger_specs)
    if eligibility_result.get("schema_version") != ELIGIBILITY_RESULT_SCHEMA:
        raise ExternalEvidence8HSpecError("8h_c_eligibility_result_schema_mismatch")
    eligibility_sha = _verify_embedded_digest(
        eligibility_result, "binding_sha256", "8h_c_eligibility_binding_digest_mismatch"
    )
    if eligibility_result.get("interaction_outcome_access_authorized") is not False:
        raise ExternalEvidence8HSpecError("8h_c_upstream_must_not_authorize_outcome_access")
    if eligibility_result.get("empirical_interaction_research_enabled") is not False:
        raise ExternalEvidence8HSpecError("8h_c_upstream_empirical_research_must_remain_closed")

    upstream_state = str(eligibility_result.get("state") or "")
    if upstream_state == WAITING_STATE:
        if candidate_manifest is not None:
            raise ExternalEvidence8HSpecError("8h_c_manifest_forbidden_while_upstream_waiting")
        return _result(
            state=WAITING_FOR_ELIGIBILITY,
            eligibility_binding_sha256=eligibility_sha,
            manifest_sha256=None,
            frozen_at=None,
            specs=[],
            wait_reason="8H_B_ELIGIBILITY_NOT_TERMINAL",
        )
    if upstream_state == NO_PROMOTED_STATE:
        if candidate_manifest is not None:
            raise ExternalEvidence8HSpecError("8h_c_manifest_forbidden_without_eligible_components")
        return _result(
            state=NO_ELIGIBLE_COMPONENTS,
            eligibility_binding_sha256=eligibility_sha,
            manifest_sha256=None,
            frozen_at=None,
            specs=[],
            wait_reason="NO_PROMOTED_FACTORS",
        )
    if upstream_state != BOUND_STATE:
        raise ExternalEvidence8HSpecError(f"8h_c_unbindable_upstream_state:{upstream_state}")
    if eligibility_result.get("interaction_spec_freeze_authorized") is not True:
        raise ExternalEvidence8HSpecError("8h_c_spec_freeze_not_authorized_by_8h_b")

    eligible_rows = _sequence(
        eligibility_result.get("eligible_components") or [], "8h_c_eligible_components_invalid"
    )
    eligible: dict[str, Mapping[str, Any]] = {}
    for row in eligible_rows:
        if not isinstance(row, Mapping):
            raise ExternalEvidence8HSpecError("8h_c_eligible_component_row_invalid")
        key = str(row.get("factor_horizon_id") or "")
        if not key or key in eligible:
            raise ExternalEvidence8HSpecError("8h_c_duplicate_or_missing_eligible_component_id")
        if row.get("eligibility_state") != "ELIGIBLE_FOR_8H_INTERACTION_SPEC_FREEZE_ONLY":
            raise ExternalEvidence8HSpecError(f"8h_c_component_not_spec_freeze_eligible:{key}")
        if row.get("interaction_outcome_access_authorized") is not False:
            raise ExternalEvidence8HSpecError(f"8h_c_component_outcome_access_must_be_false:{key}")
        eligible[key] = row
    if set(eligible) != set(str(x) for x in eligibility_result.get("eligible_factor_horizon_ids") or ()):
        raise ExternalEvidence8HSpecError("8h_c_eligible_component_identity_mismatch")
    if not eligible:
        raise ExternalEvidence8HSpecError("8h_c_bound_state_requires_eligible_components")
    if candidate_manifest is None:
        raise ExternalEvidence8HSpecError("8h_c_candidate_manifest_required")

    manifest_sha = _validate_manifest(candidate_manifest)
    entries = _sequence(candidate_manifest.get("entries") or [], "8h_c_candidate_manifest_entries_invalid")
    if not entries:
        return _result(
            state=NO_PREREGISTERED_SPECS,
            eligibility_binding_sha256=eligibility_sha,
            manifest_sha256=manifest_sha,
            frozen_at=None,
            specs=[],
            wait_reason="EXPLICIT_CANDIDATE_MANIFEST_EMPTY",
        )

    frozen_at_text = _timezone_aware_timestamp(frozen_at, "8h_c_freeze_timestamp_invalid")
    allowed_classes = set(contract["allowed_interaction_classes"])
    specs: list[dict[str, Any]] = []
    seen_pairs: set[tuple[int, tuple[str, str]]] = set()

    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ExternalEvidence8HSpecError("8h_c_candidate_entry_invalid")
        interaction_class = str(entry.get("interaction_class") or "")
        if interaction_class not in allowed_classes:
            raise ExternalEvidence8HSpecError(f"8h_c_interaction_class_not_allowed:{interaction_class}")
        try:
            horizon = int(entry.get("horizon_sessions"))
        except (TypeError, ValueError) as exc:
            raise ExternalEvidence8HSpecError("8h_c_candidate_horizon_invalid") from exc
        components = _sequence(entry.get("components") or [], "8h_c_candidate_components_invalid")
        if len(components) != 2 or any(not isinstance(row, Mapping) for row in components):
            raise ExternalEvidence8HSpecError("8h_c_exactly_two_components_required")

        if interaction_class == "CORE_X_EXTERNAL":
            core_rows = [row for row in components if row.get("kind") == "CORE_NUMERIC"]
            external_rows = [row for row in components if row.get("kind") == "EXTERNAL_NUMERIC"]
            if len(core_rows) != 1 or len(external_rows) != 1:
                raise ExternalEvidence8HSpecError("8h_c_core_external_component_kinds_invalid")
            normalized = [
                _core_component(core_rows[0], contract, horizon),
                _external_component(external_rows[0], eligible, horizon),
            ]
        else:
            if any(row.get("kind") != "EXTERNAL_NUMERIC" for row in components):
                raise ExternalEvidence8HSpecError("8h_c_external_external_component_kinds_invalid")
            normalized = [
                _external_component(components[0], eligible, horizon),
                _external_component(components[1], eligible, horizon),
            ]
            factor_ids = {row["factor_id"] for row in normalized}
            if len(factor_ids) != 2:
                raise ExternalEvidence8HSpecError("8h_c_external_external_requires_distinct_factor_ids")

        tokens = tuple(sorted(_component_token(row) for row in normalized))
        if tokens[0] == tokens[1]:
            raise ExternalEvidence8HSpecError("8h_c_self_interaction_forbidden")
        pair_key = (horizon, tokens)
        if pair_key in seen_pairs:
            raise ExternalEvidence8HSpecError("8h_c_duplicate_or_commutative_duplicate_spec")
        seen_pairs.add(pair_key)
        by_token = {_component_token(row): row for row in normalized}
        canonical_components = [by_token[token] for token in tokens]
        identity_payload = {
            "interaction_class": interaction_class,
            "horizon_sessions": horizon,
            "components": canonical_components,
            "operator": "ELEMENTWISE_PRODUCT",
            "eligibility_binding_sha256": eligibility_sha,
            "candidate_manifest_sha256": manifest_sha,
        }
        interaction_spec_id = f"ix_{_digest(identity_payload)[:20]}"
        spec = {
            "interaction_spec_id": interaction_spec_id,
            **identity_payload,
            "main_effects_retained": True,
            "exactly_one_new_interaction_term": True,
            "outcome_values_read_during_spec_freeze": False,
            "production_authorized": False,
            "phase7_mutation_authorized": False,
            "phase8i_integration_authorized": False,
            "orders_or_trades_authorized": False,
        }
        spec["interaction_spec_sha256"] = _digest(spec)
        specs.append(spec)

    specs.sort(key=lambda row: row["interaction_spec_id"])
    return _result(
        state=SPECS_FROZEN,
        eligibility_binding_sha256=eligibility_sha,
        manifest_sha256=manifest_sha,
        frozen_at=frozen_at_text,
        specs=specs,
    )
