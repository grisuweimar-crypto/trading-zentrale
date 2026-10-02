"""Phase 8I-E extended reliability research.

8I-E preserves Phase-7G reliability exactly and adds only a research annotation
from the frozen 8I-C external relation. Real outcome access remains closed.
Synthetic helpers exercise the prospective preregistration without creating a
reliability score, changing Phase 7, or enabling stance/action effects.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from hashlib import sha256
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.research.decision_layer.reliability_explainability import (
    SCHEMA_VERSION as PHASE7_RELIABILITY_SCHEMA,
    validate_reliability_explanation,
)
from scanner.research.external_evidence.external_aggregation_8i import (
    AGGREGATION_RESULT_SCHEMA,
    classify_relation,
)


RELIABILITY_CONTRACT_SCHEMA = "external_evidence_8i_reliability_extension_research_v1"
ANNOTATION_SCHEMA = "external_evidence_8i_reliability_annotation_v1"
MANIFEST_SCHEMA = "external_evidence_8i_reliability_prospective_manifest_v1"
GATE_SCHEMA = "external_evidence_8i_reliability_terminal_gate_v1"
RESULT_SCHEMA = "external_evidence_8i_reliability_terminal_result_v1"

PRIMARY_CORE_STATES = ("POSITIVE", "NEGATIVE")
PRIMARY_PHASE7_STATES = (
    "pending_confirmation",
    "provisional_cross_family_support",
    "provisional_same_family_support",
    "provisional_unopposed_support",
)
PRIMARY_RELATIONS = ("CONFIRMING", "CONFLICTING", "INSUFFICIENT_EXTERNAL")
SECONDARY_RELATIONS = ("MIXED_EXTERNAL", "UNKNOWN")
HORIZONS = (5, 20, 40, 60)
FORBIDDEN_PRE_GATE_PARTS = (
    "peer_excess",
    "future_return",
    "adverse_excursion",
    "path_max_drawdown",
    "label_",
    "outcome",
    "p_value",
    "confidence_interval",
    "holm_adjusted_p",
)


class ExternalEvidence8IReliabilityError(ValueError):
    """Raised when the 8I-E reliability-research boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8IReliabilityError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8IReliabilityError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IReliabilityError(error + "_timezone_required")
    return parsed


def _verify_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(row.get(field) or "")
    if len(recorded) != 64:
        raise ExternalEvidence8IReliabilityError(error)
    payload = dict(row)
    payload.pop(field, None)
    if digest(payload) != recorded:
        raise ExternalEvidence8IReliabilityError(error)
    return recorded


def _expected_family() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        for state in PRIMARY_PHASE7_STATES:
            rows.extend(
                [
                    {
                        "hypothesis_id": f"{state}__CONFIRMING_MINUS_INSUFFICIENT_EXTERNAL__{horizon}t",
                        "phase7_reliability_state": state,
                        "horizon_sessions": horizon,
                        "left_relation": "CONFIRMING",
                        "right_relation": "INSUFFICIENT_EXTERNAL",
                        "expected_effect_sign": "positive",
                    },
                    {
                        "hypothesis_id": f"{state}__CONFLICTING_MINUS_INSUFFICIENT_EXTERNAL__{horizon}t",
                        "phase7_reliability_state": state,
                        "horizon_sessions": horizon,
                        "left_relation": "CONFLICTING",
                        "right_relation": "INSUFFICIENT_EXTERNAL",
                        "expected_effect_sign": "negative",
                    },
                ]
            )
    return rows


def validate_reliability_contract(
    contract: Mapping[str, Any],
    phase7_contract: Mapping[str, Any],
    direction_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
    dataset_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != RELIABILITY_CONTRACT_SCHEMA or contract.get("phase") != "8I-E":
        raise ExternalEvidence8IReliabilityError("unsupported_8i_e_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_must_be_research_only_shadow")
    for key in (
        "productive_integration_enabled",
        "phase7_mutation_enabled",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_enabled",
        "orders_or_trades_enabled",
        "real_decision_outcome_read_allowed",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8IReliabilityError(f"8i_e_boundary_must_remain_false:{key}")

    if phase7_contract.get("schema_version") != PHASE7_RELIABILITY_SCHEMA or phase7_contract.get("phase") != "7G":
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_reliability_contract_required")
    if phase7_contract.get("reliability_policy", {}).get("numeric_reliability_score_allowed") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_numeric_reliability_boundary_drift")
    if phase7_contract.get("reliability_policy", {}).get("assessment_is_success_probability") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_probability_boundary_drift")

    if direction_contract.get("schema_version") != "external_evidence_8i_component_direction_state_engine_v1":
        raise ExternalEvidence8IReliabilityError("8i_e_8i_d_contract_required")
    if direction_contract.get("extended_reliability_enabled") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_8i_d_must_not_pre_enable_reliability")
    if direction_contract.get("evidence_consumption", {}).get(
        "future_reliability_research_requires_separately_frozen_fresh_evidence"
    ) is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_fresh_evidence_handoff_missing")

    if aggregation_contract.get("schema_version") != "external_evidence_8i_external_aggregation_design_v1":
        raise ExternalEvidence8IReliabilityError("8i_e_8i_c_contract_required")
    if aggregation_contract.get("relation_mapping", {}).get("relation_is_descriptive_only") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_relation_must_enter_as_descriptive")

    if dataset_contract.get("schema_version") != "decision_research_dataset_v1":
        raise ExternalEvidence8IReliabilityError("8i_e_dataset_contract_required")
    if tuple(int(x) for x in dataset_contract.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8IReliabilityError("8i_e_horizon_family_drift")
    if "peer_excess" not in (dataset_contract.get("forward_labels") or {}):
        raise ExternalEvidence8IReliabilityError("8i_e_peer_excess_label_required")

    if conflict_matrix.get("schema_version") != "external_conflict_matrix_v1":
        raise ExternalEvidence8IReliabilityError("8i_e_conflict_matrix_required")
    if conflict_matrix.get("status") != "descriptive_not_policy":
        raise ExternalEvidence8IReliabilityError("8i_e_conflict_matrix_must_remain_descriptive")

    eligibility = contract.get("eligibility") or {}
    if tuple(eligibility.get("primary_core_states") or ()) != PRIMARY_CORE_STATES:
        raise ExternalEvidence8IReliabilityError("8i_e_primary_core_state_drift")
    if tuple(eligibility.get("primary_phase7_reliability_states") or ()) != PRIMARY_PHASE7_STATES:
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_reliability_family_drift")
    if tuple(eligibility.get("primary_external_relations") or ()) != PRIMARY_RELATIONS:
        raise ExternalEvidence8IReliabilityError("8i_e_primary_relation_family_drift")
    if tuple(eligibility.get("secondary_descriptive_external_relations") or ()) != SECONDARY_RELATIONS:
        raise ExternalEvidence8IReliabilityError("8i_e_secondary_relation_family_drift")

    annotation = contract.get("annotation_model") or {}
    for key in (
        "ordinal_reliability_ranking_allowed",
        "numeric_reliability_score_allowed",
        "external_relation_may_change_phase7_reliability_now",
        "external_relation_may_change_phase7_stance_now",
    ):
        if annotation.get(key) is not False:
            raise ExternalEvidence8IReliabilityError(f"8i_e_annotation_policy_leak:{key}")
    for key in (
        "extended_reliability_research_cell_only",
        "phase7_reliability_assessment_must_be_preserved_exactly",
        "phase7_stance_must_be_preserved_exactly",
        "phase7_portfolio_action_must_be_preserved_exactly",
    ):
        if annotation.get(key) is not True:
            raise ExternalEvidence8IReliabilityError(f"8i_e_annotation_guard_required:{key}")

    outcome = contract.get("outcome_definition") or {}
    if outcome.get("primary_source") != "peer_excess_{H}t":
        raise ExternalEvidence8IReliabilityError("8i_e_primary_outcome_drift")
    if outcome.get("primary_metric") != "direction_aligned_peer_excess":
        raise ExternalEvidence8IReliabilityError("8i_e_primary_metric_drift")
    if outcome.get("formula") != {
        "POSITIVE": "peer_excess_{H}t",
        "NEGATIVE": "-1 * peer_excess_{H}t",
    }:
        raise ExternalEvidence8IReliabilityError("8i_e_direction_alignment_formula_drift")
    if outcome.get("metric_selection_after_outcome_read_allowed") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_post_outcome_metric_selection_forbidden")

    family = contract.get("primary_hypothesis_family") or {}
    if int(family.get("family_size", -1)) != 32:
        raise ExternalEvidence8IReliabilityError("8i_e_hypothesis_family_size_drift")
    if list(family.get("family_members") or ()) != _expected_family():
        raise ExternalEvidence8IReliabilityError("8i_e_hypothesis_family_membership_or_order_drift")
    if family.get("unexpected_sign_may_be_inverted_or_relabelled") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_failed_hypothesis_inversion_forbidden")

    stats = contract.get("statistics") or {}
    expected_stats = {
        "bootstrap_repetitions": 1000,
        "bootstrap_seed": 20260928,
        "minimum_snapshot_n_per_side": 30,
        "minimum_temporal_support_regions_per_side": 2,
        "multiple_testing_method": "Holm",
        "holm_family_size": 32,
    }
    for key, expected in expected_stats.items():
        if stats.get(key) != expected:
            raise ExternalEvidence8IReliabilityError(f"8i_e_statistical_contract_drift:{key}")
    if float(stats.get("family_wise_alpha", -1)) != 0.05:
        raise ExternalEvidence8IReliabilityError("8i_e_holm_alpha_must_remain_0_05")
    if stats.get("significance_is_automatic_promotion") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_significance_cannot_promote")

    prospective = contract.get("prospective_evidence") or {}
    if prospective.get("single_partition") != "PROSPECTIVE":
        raise ExternalEvidence8IReliabilityError("8i_e_single_prospective_partition_required")
    if prospective.get("one_shot_terminal_family_evaluation") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_one_shot_terminal_family_required")
    if prospective.get("repeated_interim_significance_looks_allowed") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_repeated_interim_looks_forbidden")
    if prospective.get("outcome_values_may_not_be_read_before_terminal_family_gate") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_outcome_gate_required")


def current_reliability_status(
    contract: Mapping[str, Any],
    phase7_contract: Mapping[str, Any],
    direction_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
    dataset_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    validate_reliability_contract(
        contract,
        phase7_contract,
        direction_contract,
        aggregation_contract,
        dataset_contract,
        conflict_matrix,
    )
    current = dict(contract.get("current_repository_state") or {})
    result = {
        "schema_version": ANNOTATION_SCHEMA,
        "phase": "8I-E",
        "state": str(current.get("state") or ""),
        "synthetic": False,
        "real_bound_component_ids": list(current.get("real_bound_component_ids") or ()),
        "real_reliability_annotations_authorized": False,
        "prospective_manifest_frozen": False,
        "real_outcomes_opened": False,
        "terminal_family_evaluation_complete": False,
        "phase7_reliability_preserved": True,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["status_sha256"] = digest(result)
    return result


def _verify_aggregation_result(result: Mapping[str, Any]) -> str:
    if result.get("schema_version") != AGGREGATION_RESULT_SCHEMA or result.get("phase") != "8I-C":
        raise ExternalEvidence8IReliabilityError("8i_e_aggregation_result_required")
    recorded = _verify_digest(result, "aggregation_sha256", "8i_e_aggregation_digest_mismatch")
    for key in (
        "external_decision_influence_enabled",
        "phase7_mutation_authorized",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_authorized",
        "orders_or_trades_authorized",
    ):
        if result.get(key) is not False:
            raise ExternalEvidence8IReliabilityError(f"8i_e_aggregation_scope_drift:{key}")
    return recorded


def _phase7_core_state(explanation: Mapping[str, Any]) -> tuple[str, str | None]:
    decision = explanation.get("decision_context")
    if not isinstance(decision, Mapping):
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_decision_context_required")
    raw_state = str(decision.get("universal_stance_state") or "")
    raw_direction = decision.get("universal_stance_direction")
    mapping = {
        "positive": "POSITIVE",
        "negative": "NEGATIVE",
        "conflicted": "CONFLICTED",
        "insufficient_evidence": "INSUFFICIENT_EVIDENCE",
    }
    if raw_state not in mapping:
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_core_state_invalid")
    core = mapping[raw_state]
    direction = None if raw_direction is None else str(raw_direction).upper()
    if core in PRIMARY_CORE_STATES and direction != core:
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_direction_state_mismatch")
    if core not in PRIMARY_CORE_STATES and direction not in {None, ""}:
        raise ExternalEvidence8IReliabilityError("8i_e_nondirectional_core_cannot_carry_direction")
    return core, direction or None


def build_reliability_annotation(
    *,
    contract: Mapping[str, Any],
    phase7_contract: Mapping[str, Any],
    direction_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
    dataset_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
    phase7_explanation: Mapping[str, Any],
    aggregation_result: Mapping[str, Any],
    synthetic: bool,
) -> dict[str, Any]:
    """Join preserved 7G reliability to one 8I-C relation without outcomes."""
    validate_reliability_contract(
        contract,
        phase7_contract,
        direction_contract,
        aggregation_contract,
        dataset_contract,
        conflict_matrix,
    )
    if synthetic is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_real_reliability_annotation_not_authorized")

    phase7 = validate_reliability_explanation(phase7_explanation)
    aggregation_sha = _verify_aggregation_result(aggregation_result)
    if aggregation_result.get("synthetic") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_only_synthetic_aggregation_is_currently_authorized")

    snapshot_id = str(aggregation_result.get("snapshot_id") or "")
    symbol = str(aggregation_result.get("symbol") or "")
    if snapshot_id != str(phase7.get("source_snapshot_id") or ""):
        raise ExternalEvidence8IReliabilityError("8i_e_snapshot_identity_mismatch")
    if symbol != str(phase7.get("symbol") or ""):
        raise ExternalEvidence8IReliabilityError("8i_e_symbol_identity_mismatch")
    try:
        horizon = int(aggregation_result.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise ExternalEvidence8IReliabilityError("8i_e_horizon_required") from exc
    if horizon not in HORIZONS:
        raise ExternalEvidence8IReliabilityError("8i_e_horizon_not_in_frozen_family")

    generated_at = _parse_time(aggregation_result.get("generated_at"), "8i_e_generated_at_invalid")
    core_state, core_direction = _phase7_core_state(phase7)
    external_state = str(aggregation_result.get("external_direction_state") or "")
    expected_relation = classify_relation(
        core_state=core_state,
        external_state=external_state,
        conflict_matrix=conflict_matrix,
    )
    relation = str(aggregation_result.get("relation_state_if_core_state_supplied") or "")
    if relation != expected_relation:
        raise ExternalEvidence8IReliabilityError("8i_e_relation_mapping_mismatch")

    reliability = phase7.get("reliability")
    if not isinstance(reliability, Mapping):
        raise ExternalEvidence8IReliabilityError("8i_e_phase7_reliability_required")
    phase7_state = str(reliability.get("assessment") or "")
    primary = (
        core_state in PRIMARY_CORE_STATES
        and phase7_state in PRIMARY_PHASE7_STATES
        and relation in PRIMARY_RELATIONS
    )
    descriptive = relation in SECONDARY_RELATIONS or relation == "EXTERNAL_ONLY"
    research_role = "PRIMARY" if primary else ("SECONDARY_DESCRIPTIVE" if descriptive else "NOT_ELIGIBLE")

    annotation: dict[str, Any] = {
        "schema_version": ANNOTATION_SCHEMA,
        "phase": "8I-E",
        "state": "SYNTHETIC_RELIABILITY_RESEARCH_ANNOTATION",
        "synthetic": True,
        "snapshot_id": snapshot_id,
        "as_of": str(phase7.get("as_of") or ""),
        "symbol": symbol,
        "horizon_sessions": horizon,
        "generated_at": generated_at.isoformat(),
        "phase7_explanation_id": str(phase7.get("explanation_id") or ""),
        "aggregation_sha256": aggregation_sha,
        "phase7_reliability_state": phase7_state,
        "phase7_core_state": core_state,
        "phase7_core_direction": core_direction,
        "external_direction_state": external_state,
        "external_relation_state": relation,
        "research_role": research_role,
        "primary_research_eligible": primary,
        "research_cell": f"{phase7_state}|{relation}|{horizon}t",
        "phase7_reliability_preserved": True,
        "phase7_stance_preserved": True,
        "phase7_portfolio_action_preserved": True,
        "numeric_reliability_score": None,
        "ordinal_reliability_rank": None,
        "extended_reliability_state": None,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    annotation["annotation_sha256"] = digest(annotation)
    return annotation


def _reject_outcome_keys(row: Mapping[str, Any]) -> None:
    for key in row:
        lowered = str(key).lower()
        if any(part in lowered for part in FORBIDDEN_PRE_GATE_PARTS):
            raise ExternalEvidence8IReliabilityError(f"8i_e_pre_gate_outcome_field_forbidden:{key}")


def freeze_prospective_manifest(
    *,
    contract: Mapping[str, Any],
    annotations: Sequence[Mapping[str, Any]],
    author_identity: str,
    authored_at: object,
    synthetic: bool,
) -> dict[str, Any]:
    """Freeze annotation identities without reading any forward outcome value."""
    if contract.get("schema_version") != RELIABILITY_CONTRACT_SCHEMA:
        raise ExternalEvidence8IReliabilityError("8i_e_contract_required_for_manifest")
    if synthetic is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_real_manifest_freeze_not_authorized")
    if not str(author_identity or "").strip():
        raise ExternalEvidence8IReliabilityError("8i_e_manifest_author_required")
    authored = _parse_time(authored_at, "8i_e_manifest_authored_at_invalid")
    not_before = _parse_time(
        contract.get("prospective_evidence", {}).get("prospective_not_before"),
        "8i_e_prospective_not_before_invalid",
    )
    if authored < not_before:
        raise ExternalEvidence8IReliabilityError("8i_e_manifest_authored_before_prospective_start")

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, int]] = set()
    for raw in annotations:
        _reject_outcome_keys(raw)
        if raw.get("schema_version") != ANNOTATION_SCHEMA:
            raise ExternalEvidence8IReliabilityError("8i_e_annotation_schema_mismatch")
        _verify_digest(raw, "annotation_sha256", "8i_e_annotation_digest_mismatch")
        if raw.get("synthetic") is not True:
            raise ExternalEvidence8IReliabilityError("8i_e_only_synthetic_annotations_currently_allowed")
        generated_at = _parse_time(raw.get("generated_at"), "8i_e_annotation_generated_at_invalid")
        if generated_at < not_before:
            raise ExternalEvidence8IReliabilityError("8i_e_annotation_predates_prospective_start")
        key = (
            str(raw.get("snapshot_id") or ""),
            str(raw.get("as_of") or ""),
            str(raw.get("symbol") or ""),
            int(raw.get("horizon_sessions")),
        )
        if not all(key[:3]) or key in seen:
            raise ExternalEvidence8IReliabilityError("8i_e_duplicate_or_missing_manifest_identity")
        seen.add(key)
        rows.append(
            {
                "snapshot_id": key[0],
                "as_of": key[1],
                "symbol": key[2],
                "horizon_sessions": key[3],
                "generated_at": generated_at.isoformat(),
                "phase7_reliability_state": str(raw.get("phase7_reliability_state") or ""),
                "phase7_core_direction": raw.get("phase7_core_direction"),
                "external_relation_state": str(raw.get("external_relation_state") or ""),
                "research_role": str(raw.get("research_role") or ""),
                "annotation_sha256": str(raw.get("annotation_sha256") or ""),
                "partition": "PROSPECTIVE",
            }
        )
    rows.sort(key=lambda x: (x["as_of"], x["snapshot_id"], x["symbol"], x["horizon_sessions"]))
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA,
        "phase": "8I-E",
        "synthetic": True,
        "author_identity": str(author_identity),
        "authored_at": authored.isoformat(),
        "prospective_not_before": not_before.isoformat(),
        "partition": "PROSPECTIVE",
        "rows": rows,
        "row_count": len(rows),
        "outcomes_read_while_freezing": False,
        "outcomes_opened": False,
        "terminal_gate_passed": False,
        "one_shot_terminal_family_evaluation": True,
        "repeated_interim_significance_looks_allowed": False,
        "extended_reliability_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    manifest["manifest_sha256"] = digest(manifest)
    return manifest


def _temporal_regions(days: Sequence[pd.Timestamp], horizon: int) -> int:
    unique = sorted(set(days))
    if not unique:
        return 0
    block = 2 * int(horizon)
    regions = 0
    last: int | None = None
    for position in range(len(unique)):
        if last is None or position - last >= block:
            regions += 1
            last = position
    return regions


def terminal_family_gate(
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    maturity_rows: Sequence[Mapping[str, Any]],
    research_as_of: object,
    synthetic: bool,
) -> dict[str, Any]:
    """Use only identity and label-availability metadata to decide if outcomes may open."""
    if synthetic is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_real_terminal_gate_not_authorized")
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ExternalEvidence8IReliabilityError("8i_e_manifest_required")
    manifest_sha = _verify_digest(manifest, "manifest_sha256", "8i_e_manifest_digest_mismatch")
    if manifest.get("outcomes_opened") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_manifest_outcomes_must_remain_sealed")

    research_time = _parse_time(research_as_of, "8i_e_research_as_of_invalid")
    manifest_rows = list(manifest.get("rows") or ())
    by_key = {
        (str(r["snapshot_id"]), str(r["as_of"]), str(r["symbol"]), int(r["horizon_sessions"])): r
        for r in manifest_rows
    }
    if len(by_key) != len(manifest_rows):
        raise ExternalEvidence8IReliabilityError("8i_e_manifest_identity_not_unique")

    matured: dict[tuple[str, str, str, int], datetime] = {}
    for row in maturity_rows:
        _reject_outcome_keys({k: v for k, v in row.items() if not str(k).startswith("label_available_from_")})
        key = (
            str(row.get("snapshot_id") or ""),
            str(row.get("as_of") or ""),
            str(row.get("symbol") or ""),
            int(row.get("horizon_sessions")),
        )
        if key not in by_key or key in matured:
            raise ExternalEvidence8IReliabilityError("8i_e_maturity_identity_mismatch_or_duplicate")
        horizon = key[3]
        label_key = f"label_available_from_{horizon}t"
        extra = set(row).difference({"snapshot_id", "as_of", "symbol", "horizon_sessions", label_key})
        if extra:
            raise ExternalEvidence8IReliabilityError("8i_e_maturity_metadata_only_required")
        available = _parse_time(row.get(label_key), "8i_e_label_available_from_invalid")
        matured[key] = available
    if set(matured) != set(by_key):
        raise ExternalEvidence8IReliabilityError("8i_e_maturity_rows_must_match_manifest_exactly")

    stats = contract["statistics"]
    min_n = int(stats["minimum_snapshot_n_per_side"])
    min_regions = int(stats["minimum_temporal_support_regions_per_side"])
    statuses: list[dict[str, Any]] = []
    all_ready = True

    rows_df = pd.DataFrame(manifest_rows)
    rows_df["mature"] = [
        matured[(str(r["snapshot_id"]), str(r["as_of"]), str(r["symbol"]), int(r["horizon_sessions"]))]
        <= research_time
        for r in manifest_rows
    ]
    primary = rows_df.loc[rows_df["research_role"].eq("PRIMARY") & rows_df["mature"]].copy()

    for hypothesis in contract["primary_hypothesis_family"]["family_members"]:
        state = str(hypothesis["phase7_reliability_state"])
        horizon = int(hypothesis["horizon_sessions"])
        left_relation = str(hypothesis["left_relation"])
        right_relation = str(hypothesis["right_relation"])
        cell = primary.loc[
            primary["phase7_reliability_state"].eq(state)
            & primary["horizon_sessions"].eq(horizon)
        ].copy()
        summary: dict[str, Any] = {}
        ready = True
        for side, relation in (("left", left_relation), ("right", right_relation)):
            side_rows = cell.loc[cell["external_relation_state"].eq(relation)].copy()
            snapshot = side_rows[["snapshot_id", "as_of"]].drop_duplicates()
            dates = [pd.to_datetime(x, utc=True).normalize() for x in snapshot["as_of"].tolist()]
            n = int(len(snapshot))
            regions = _temporal_regions(dates, horizon)
            summary[f"{side}_snapshot_n"] = n
            summary[f"{side}_temporal_support_regions"] = regions
            if n < min_n or regions < min_regions:
                ready = False
        all_ready = all_ready and ready
        statuses.append({"hypothesis_id": hypothesis["hypothesis_id"], "ready": ready, **summary})

    gate: dict[str, Any] = {
        "schema_version": GATE_SCHEMA,
        "phase": "8I-E",
        "synthetic": True,
        "manifest_sha256": manifest_sha,
        "research_as_of": research_time.isoformat(),
        "family_size": len(statuses),
        "hypothesis_readiness": statuses,
        "ready_for_one_shot_terminal_family_evaluation": all_ready,
        "outcomes_open_authorized": all_ready,
        "outcomes_read": False,
        "extended_reliability_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    gate["gate_sha256"] = digest(gate)
    return gate


def _snapshot_cells(annotations: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    keys = ["snapshot_id", "as_of", "symbol", "horizon_sessions"]
    if annotations.duplicated(keys).any() or outcomes.duplicated(keys).any():
        raise ExternalEvidence8IReliabilityError("8i_e_duplicate_terminal_identity")
    merged = annotations.merge(outcomes, on=keys, how="inner", validate="one_to_one")
    if len(merged) != len(annotations) or len(merged) != len(outcomes):
        raise ExternalEvidence8IReliabilityError("8i_e_terminal_outcomes_must_match_annotations_exactly")
    peer: list[float] = []
    aligned: list[float] = []
    for row in merged.itertuples(index=False):
        horizon = int(row.horizon_sessions)
        value = float(getattr(row, f"peer_excess_{horizon}t"))
        if not math.isfinite(value):
            raise ExternalEvidence8IReliabilityError("8i_e_nonfinite_peer_excess")
        direction = str(row.phase7_core_direction)
        if direction not in PRIMARY_CORE_STATES:
            raise ExternalEvidence8IReliabilityError("8i_e_primary_terminal_row_requires_directional_core")
        peer.append(value)
        aligned.append(value if direction == "POSITIVE" else -value)
    merged["raw_peer_excess"] = peer
    merged["direction_aligned_peer_excess"] = aligned
    return (
        merged.groupby(
            ["snapshot_id", "as_of", "phase7_reliability_state", "external_relation_state", "horizon_sessions"],
            as_index=False,
            sort=True,
        )
        .agg(
            direction_aligned_peer_excess=("direction_aligned_peer_excess", "mean"),
            raw_peer_excess=("raw_peer_excess", "mean"),
            symbol_n=("symbol", "count"),
        )
    )


def _bootstrap_difference(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    horizon: int,
    reps: int,
    seed: int,
) -> dict[str, Any]:
    dates = sorted(
        set(pd.to_datetime(left["as_of"], utc=True).dt.normalize())
        | set(pd.to_datetime(right["as_of"], utc=True).dt.normalize())
    )
    if len(dates) < 2 or reps < 1:
        return {"ci_low": None, "ci_high": None, "p_value": None, "repetitions": 0}
    span = min(2 * int(horizon), len(dates))
    blocks = [[dates[(start + offset) % len(dates)] for offset in range(span)] for start in range(len(dates))]
    left_by_date: defaultdict[pd.Timestamp, list[float]] = defaultdict(list)
    right_by_date: defaultdict[pd.Timestamp, list[float]] = defaultdict(list)
    for row in left.itertuples(index=False):
        left_by_date[pd.to_datetime(row.as_of, utc=True).normalize()].append(float(row.direction_aligned_peer_excess))
    for row in right.itertuples(index=False):
        right_by_date[pd.to_datetime(row.as_of, utc=True).normalize()].append(float(row.direction_aligned_peer_excess))
    observed = float(left["direction_aligned_peer_excess"].mean() - right["direction_aligned_peer_excess"].mean())
    all_values = np.concatenate([
        left["direction_aligned_peer_excess"].to_numpy(dtype=float),
        right["direction_aligned_peer_excess"].to_numpy(dtype=float),
    ])
    grand = float(np.mean(all_values))
    centered_left = {day: [v - grand for v in vals] for day, vals in left_by_date.items()}
    centered_right = {day: [v - grand for v in vals] for day, vals in right_by_date.items()}
    rng = np.random.default_rng(seed)
    draws_per_rep = int(np.ceil(len(dates) / span))
    raw_draws: list[float] = []
    null_draws: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled = [day for index in chosen for day in blocks[index]][: len(dates)]
        l_raw = [v for day in sampled for v in left_by_date.get(day, [])]
        r_raw = [v for day in sampled for v in right_by_date.get(day, [])]
        l_null = [v for day in sampled for v in centered_left.get(day, [])]
        r_null = [v for day in sampled for v in centered_right.get(day, [])]
        if not l_raw or not r_raw or not l_null or not r_null:
            continue
        raw_draws.append(float(np.mean(l_raw) - np.mean(r_raw)))
        null_draws.append(float(np.mean(l_null) - np.mean(r_null)))
    if not raw_draws:
        return {"ci_low": None, "ci_high": None, "p_value": None, "repetitions": 0}
    raw = np.asarray(raw_draws)
    null = np.asarray(null_draws)
    low, high = np.quantile(raw, [0.025, 0.975])
    p = float((1 + np.sum(np.abs(null) >= abs(observed))) / (len(null) + 1))
    return {"ci_low": float(low), "ci_high": float(high), "p_value": min(1.0, p), "repetitions": int(len(raw_draws))}


def _holm(pairs: Sequence[tuple[str, float]], alpha: float) -> dict[str, dict[str, Any]]:
    ordered = sorted(pairs, key=lambda item: (item[1], item[0]))
    m = len(ordered)
    adjusted_running = 0.0
    adjusted: dict[str, float] = {}
    reject: dict[str, bool] = {}
    still_rejecting = True
    for rank, (hypothesis_id, p_value) in enumerate(ordered, start=1):
        factor = m - rank + 1
        adjusted_running = max(adjusted_running, min(1.0, factor * p_value))
        adjusted[hypothesis_id] = adjusted_running
        threshold = alpha / factor
        reject[hypothesis_id] = bool(still_rejecting and p_value <= threshold)
        if p_value > threshold:
            still_rejecting = False
    return {hypothesis_id: {"holm_adjusted_p": adjusted[hypothesis_id], "holm_reject": reject[hypothesis_id]} for hypothesis_id, _ in pairs}


def evaluate_synthetic_terminal_family(
    *,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    gate: Mapping[str, Any],
    outcome_rows: Sequence[Mapping[str, Any]],
    synthetic: bool,
) -> dict[str, Any]:
    """Exercise the frozen terminal analysis on synthetic data only."""
    if synthetic is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_real_outcome_evaluation_not_authorized")
    if gate.get("schema_version") != GATE_SCHEMA:
        raise ExternalEvidence8IReliabilityError("8i_e_terminal_gate_required")
    gate_sha = _verify_digest(gate, "gate_sha256", "8i_e_terminal_gate_digest_mismatch")
    manifest_sha = _verify_digest(manifest, "manifest_sha256", "8i_e_manifest_digest_mismatch")
    if str(gate.get("manifest_sha256") or "") != manifest_sha:
        raise ExternalEvidence8IReliabilityError("8i_e_gate_manifest_binding_mismatch")
    if gate.get("ready_for_one_shot_terminal_family_evaluation") is not True:
        raise ExternalEvidence8IReliabilityError("8i_e_terminal_family_not_ready")
    if gate.get("outcomes_open_authorized") is not True or gate.get("outcomes_read") is not False:
        raise ExternalEvidence8IReliabilityError("8i_e_terminal_outcome_gate_invalid")

    manifest_rows = pd.DataFrame(list(manifest.get("rows") or ()))
    primary = manifest_rows.loc[manifest_rows["research_role"].eq("PRIMARY")].copy()
    if primary.empty:
        raise ExternalEvidence8IReliabilityError("8i_e_primary_manifest_rows_required")

    required_by_h = {h: {"snapshot_id", "as_of", "symbol", "horizon_sessions", f"peer_excess_{h}t"} for h in HORIZONS}
    normalized: list[dict[str, Any]] = []
    for raw in outcome_rows:
        try:
            horizon = int(raw.get("horizon_sessions"))
        except (TypeError, ValueError) as exc:
            raise ExternalEvidence8IReliabilityError("8i_e_terminal_outcome_horizon_invalid") from exc
        required = required_by_h.get(horizon)
        if required is None or not required.issubset(raw):
            raise ExternalEvidence8IReliabilityError("8i_e_terminal_outcome_columns_missing")
        if set(raw).difference(required):
            raise ExternalEvidence8IReliabilityError("8i_e_terminal_primary_outcome_only_required")
        normalized.append(dict(raw))
    outcomes = pd.DataFrame(normalized)
    identity = ["snapshot_id", "as_of", "symbol", "horizon_sessions"]
    annotations = primary[identity + ["phase7_reliability_state", "phase7_core_direction", "external_relation_state"]].copy()
    cells = _snapshot_cells(annotations, outcomes)

    stats = contract["statistics"]
    reps = int(stats["bootstrap_repetitions"])
    seed = int(stats["bootstrap_seed"])
    alpha = float(stats["family_wise_alpha"])
    results: list[dict[str, Any]] = []
    p_values: list[tuple[str, float]] = []
    for index, hypothesis in enumerate(contract["primary_hypothesis_family"]["family_members"]):
        state = str(hypothesis["phase7_reliability_state"])
        horizon = int(hypothesis["horizon_sessions"])
        left_relation = str(hypothesis["left_relation"])
        right_relation = str(hypothesis["right_relation"])
        subset = cells.loc[cells["phase7_reliability_state"].eq(state) & cells["horizon_sessions"].eq(horizon)]
        left = subset.loc[subset["external_relation_state"].eq(left_relation)].copy()
        right = subset.loc[subset["external_relation_state"].eq(right_relation)].copy()
        if left.empty or right.empty:
            raise ExternalEvidence8IReliabilityError("8i_e_gate_ready_but_terminal_cell_empty")
        effect = float(left["direction_aligned_peer_excess"].mean() - right["direction_aligned_peer_excess"].mean())
        boot = _bootstrap_difference(left, right, horizon=horizon, reps=reps, seed=(seed + index * 1009) % (2**32 - 1))
        p = boot["p_value"]
        if p is None:
            raise ExternalEvidence8IReliabilityError("8i_e_bootstrap_failed_after_ready_gate")
        p_values.append((str(hypothesis["hypothesis_id"]), float(p)))
        results.append({
            "hypothesis_id": hypothesis["hypothesis_id"],
            "phase7_reliability_state": state,
            "horizon_sessions": horizon,
            "left_relation": left_relation,
            "right_relation": right_relation,
            "expected_effect_sign": hypothesis["expected_effect_sign"],
            "left_snapshot_n": int(len(left)),
            "right_snapshot_n": int(len(right)),
            "primary_effect": effect,
            "primary_ci_95": [boot["ci_low"], boot["ci_high"]],
            "primary_p_value_two_sided": float(p),
            "bootstrap_repetitions": int(boot["repetitions"]),
            "effect_sign": "positive" if effect > 0 else ("negative" if effect < 0 else "zero"),
            "failed_hypothesis_inverted": False,
            "automatic_promotion": False,
        })

    holm = _holm(p_values, alpha)
    for row in results:
        row.update(holm[str(row["hypothesis_id"])])
        row["evaluation_sha256"] = digest(row)

    output: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA,
        "phase": "8I-E",
        "state": "SYNTHETIC_TERMINAL_FAMILY_EVALUATED",
        "synthetic": True,
        "manifest_sha256": manifest_sha,
        "gate_sha256": gate_sha,
        "primary_metric": "direction_aligned_peer_excess",
        "family_size": len(results),
        "multiple_testing_method": "Holm",
        "family_wise_alpha": alpha,
        "results": results,
        "synthetic_tests_are_empirical_evidence": False,
        "real_empirical_completion": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    output["terminal_result_sha256"] = digest(output)
    return output
