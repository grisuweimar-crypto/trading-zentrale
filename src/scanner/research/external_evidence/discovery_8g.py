"""Phase 8G-D Discovery-only outcome opening guard.

This module controls which rows may expose Discovery outcome values. It does
not perform feature search, transform search, threshold search, cross-factor
research, Validation access, Holdout access, or production integration.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping

import pandas as pd

from scanner.research.external_evidence.research_8g import (
    ACTIVE_FACTORS,
    HORIZONS,
    validate_challenger_specs,
    validate_split_plan,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    exact_split_boundary_purge_required,
    validate_guarded_manifest,
)


DISCOVERY_PROTOCOL_SCHEMA = "external_evidence_8g_discovery_protocol_v1"


class ExternalEvidence8GDiscoveryError(ValueError):
    """Raised when Phase-8G-D Discovery access violates the frozen protocol."""


def _day(value: object) -> date:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ExternalEvidence8GDiscoveryError(f"invalid_date:{value}")
    return pd.Timestamp(parsed).date()


def validate_discovery_protocol(
    protocol: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> None:
    validate_challenger_specs(specs)
    validate_split_plan(plan, specs)
    if protocol.get("schema_version") != DISCOVERY_PROTOCOL_SCHEMA:
        raise ExternalEvidence8GDiscoveryError("unsupported_discovery_protocol")
    if protocol.get("phase") != "8G-D":
        raise ExternalEvidence8GDiscoveryError("discovery_phase_mismatch")
    if protocol.get("research_only") is not True:
        raise ExternalEvidence8GDiscoveryError("discovery_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GDiscoveryError("productive_integration_must_remain_disabled")
    if tuple(protocol.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GDiscoveryError("active_factor_family_mismatch")
    if tuple(int(x) for x in protocol.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GDiscoveryError("horizon_family_mismatch")

    access = protocol.get("outcome_access")
    if not isinstance(access, Mapping):
        raise ExternalEvidence8GDiscoveryError("outcome_access_missing")
    if access.get("allowed_split") != "DISCOVERY":
        raise ExternalEvidence8GDiscoveryError("only_discovery_split_may_open")
    for key in (
        "validation_outcomes_allowed",
        "holdout_outcomes_allowed",
        "reserved_boundary_buffer_outcomes_allowed",
    ):
        if access.get(key) is not False:
            raise ExternalEvidence8GDiscoveryError(f"outcome_seal_must_be_false:{key}")
    for key in (
        "label_value_may_be_read_only_after_maturity",
        "research_as_of_must_be_on_or_after_label_available_from",
        "exact_8g_b_binding_required_before_label_join",
        "exact_8g_c_pairing_required_before_label_join",
        "exact_split_boundary_purge_required_before_label_value_read",
    ):
        if access.get(key) is not True:
            raise ExternalEvidence8GDiscoveryError(f"discovery_gate_required:{key}")

    search = protocol.get("discovery_search_space")
    if not isinstance(search, Mapping):
        raise ExternalEvidence8GDiscoveryError("discovery_search_space_missing")
    forbidden_search_flags = (
        "feature_family_search_allowed",
        "feature_subset_search_allowed",
        "feature_sign_flip_allowed",
        "manual_direction_assignment_allowed",
        "threshold_search_allowed",
        "transform_search_allowed",
        "hyperparameter_search_allowed",
        "cross_factor_interactions_allowed",
    )
    for key in forbidden_search_flags:
        if search.get(key) is not False:
            raise ExternalEvidence8GDiscoveryError(f"frozen_search_space_reopened:{key}")

    guards = protocol.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GDiscoveryError("discovery_guards_must_remain_false")


def discovery_stream_state(
    *,
    factor_id: str,
    horizon_sessions: int,
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> Mapping[str, Any]:
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GDiscoveryError(f"factor_not_active:{factor_id}")
    if horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GDiscoveryError(f"unsupported_horizon:{horizon_sessions}")
    validate_guarded_manifest(manifest, specs, plan)
    key = f"{factor_id}_x_{horizon_sessions}t"
    stream = manifest["streams"].get(key)
    if not isinstance(stream, Mapping):
        raise ExternalEvidence8GDiscoveryError(f"stream_missing:{key}")
    assignments = list(stream.get("assignments") or ())
    discovery = [item for item in assignments if item.get("split") == "DISCOVERY"]
    usable = [item for item in discovery if item.get("usable") is True]
    return {
        "stream": key,
        "bound_assignments": len(assignments),
        "discovery_assignments": len(discovery),
        "discovery_usable_assignments": len(usable),
        "state": "WAITING_FOR_DISCOVERY_BINDING" if not discovery else "DISCOVERY_BOUND",
    }


def prepare_discovery_outcome_rows(
    *,
    paired_rows: pd.DataFrame,
    outcome_rows: pd.DataFrame,
    factor_id: str,
    horizon_sessions: int,
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    protocol: Mapping[str, Any],
    research_as_of: object,
) -> pd.DataFrame:
    """Return the only rows whose Discovery peer-excess values may be read.

    Callers must pass outcome_rows already restricted to the exact candidate
    identities. If Validation/Holdout/buffer identities are present, the
    function fails closed rather than silently filtering them away.
    """
    validate_discovery_protocol(protocol, specs, plan)
    validate_guarded_manifest(manifest, specs, plan)
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GDiscoveryError(f"factor_not_active:{factor_id}")
    if horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GDiscoveryError(f"unsupported_horizon:{horizon_sessions}")

    identity = ["snapshot_id", "as_of", "symbol", "horizon_sessions"]
    required_paired = set(identity)
    required_outcomes = set(identity) | {
        f"label_available_from_{horizon_sessions}t",
        f"peer_excess_{horizon_sessions}t",
    }
    missing_paired = sorted(required_paired.difference(paired_rows.columns))
    missing_outcomes = sorted(required_outcomes.difference(outcome_rows.columns))
    if missing_paired:
        raise ExternalEvidence8GDiscoveryError("paired_identity_missing:" + ",".join(missing_paired))
    if missing_outcomes:
        raise ExternalEvidence8GDiscoveryError("outcome_columns_missing:" + ",".join(missing_outcomes))

    if paired_rows.duplicated(identity).any():
        raise ExternalEvidence8GDiscoveryError("duplicate_paired_identity")
    if outcome_rows.duplicated(identity).any():
        raise ExternalEvidence8GDiscoveryError("duplicate_outcome_identity")

    key = f"{factor_id}_x_{horizon_sessions}t"
    stream = manifest["streams"][key]
    assignments = list(stream.get("assignments") or ())
    by_identity = {
        (str(item.get("snapshot_id")), str(item.get("as_of"))): item
        for item in assignments
    }

    paired_keys = {
        (str(row.snapshot_id), str(row.as_of), str(row.symbol), int(row.horizon_sessions))
        for row in paired_rows[identity].itertuples(index=False)
    }
    outcome_keys = {
        (str(row.snapshot_id), str(row.as_of), str(row.symbol), int(row.horizon_sessions))
        for row in outcome_rows[identity].itertuples(index=False)
    }
    if outcome_keys != paired_keys:
        raise ExternalEvidence8GDiscoveryError("outcome_rows_must_match_exact_8g_c_pairing")

    for snapshot_id, as_of, _symbol, horizon in sorted(outcome_keys):
        if horizon != horizon_sessions:
            raise ExternalEvidence8GDiscoveryError("mixed_horizon_outcome_input")
        assignment = by_identity.get((snapshot_id, as_of))
        if assignment is None:
            raise ExternalEvidence8GDiscoveryError("outcome_identity_not_bound_in_8g_b")
        if assignment.get("split") != "DISCOVERY":
            raise ExternalEvidence8GDiscoveryError("non_discovery_outcome_input_forbidden")
        if assignment.get("usable") is not True:
            raise ExternalEvidence8GDiscoveryError("reserved_boundary_buffer_outcome_forbidden")

    research_day = _day(research_as_of)
    label_field = f"label_available_from_{horizon_sessions}t"
    peer_field = f"peer_excess_{horizon_sessions}t"
    work = outcome_rows.copy()
    for value in work[label_field].tolist():
        if _day(value) > research_day:
            raise ExternalEvidence8GDiscoveryError("discovery_label_not_mature")

    # Exact boundary purge can be evaluated structurally only after the first
    # next-split binding exists. Until then no assumption about its date is made.
    next_split_dates = [
        str(item.get("as_of")) for item in assignments
        if item.get("split") == "VALIDATION"
    ]
    if next_split_dates:
        next_split_first = min(next_split_dates)
        for value in work[label_field].tolist():
            if exact_split_boundary_purge_required(
                label_available_from=str(value),
                next_split_first_as_of=next_split_first,
            ):
                raise ExternalEvidence8GDiscoveryError("exact_split_boundary_purge_required")

    if work[peer_field].isna().any():
        raise ExternalEvidence8GDiscoveryError("matured_discovery_peer_excess_missing")

    # The peer-excess value is intentionally touched only after every structural
    # gate above has passed.
    work[peer_field] = pd.to_numeric(work[peer_field], errors="raise")
    return work.sort_values(identity, kind="mergesort").reset_index(drop=True)
