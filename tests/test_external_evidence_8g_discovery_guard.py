from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from scanner.research.external_evidence.discovery_8g import (
    ExternalEvidence8GDiscoveryError,
    discovery_stream_state,
    prepare_discovery_outcome_rows,
    validate_discovery_protocol,
)
from scanner.research.external_evidence.research_8g import empty_split_manifest, slot_state


ROOT = Path(__file__).resolve().parents[1]
SPECS = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
PLAN = json.loads((ROOT / "configs" / "external_evidence_8g_split_plan_v1.json").read_text())
PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_discovery_protocol_v1.json").read_text())


def _assignment(horizon: int, ordinal: int) -> dict:
    state = slot_state(horizon, ordinal)
    day = date(2026, 9, 29) + timedelta(days=ordinal - 1)
    return {
        "ordinal": ordinal,
        "snapshot_id": f"snap-{ordinal:03d}",
        "as_of": day.isoformat(),
        "generated_at": f"{day.isoformat()}T18:00:00+00:00",
        "split": state["split"],
        "usable": state["usable"],
        "purge_reason": state["purge_reason"],
        "factor_state_sha256": f"{ordinal:064x}"[-64:],
        "active_mapping_count": 1,
        "active_mapping_ids_sha256": f"{ordinal + 1:064x}"[-64:],
    }


def _manifest_with_ordinals(ordinals: int, horizon: int = 5) -> dict:
    manifest = empty_split_manifest(SPECS, PLAN)
    key = f"rates_policy_x_{horizon}t"
    manifest["streams"][key]["assignments"] = [
        _assignment(horizon, ordinal) for ordinal in range(1, ordinals + 1)
    ]
    return manifest


def _paired(ordinal: int = 1, horizon: int = 5) -> pd.DataFrame:
    item = _assignment(horizon, ordinal)
    return pd.DataFrame([
        {
            "snapshot_id": item["snapshot_id"],
            "as_of": item["as_of"],
            "symbol": "AAA",
            "horizon_sessions": horizon,
        }
    ])


def _outcomes(
    ordinal: int = 1,
    horizon: int = 5,
    *,
    label_available_from: str = "2026-10-06",
    peer_excess: float = 0.03,
) -> pd.DataFrame:
    item = _assignment(horizon, ordinal)
    return pd.DataFrame([
        {
            "snapshot_id": item["snapshot_id"],
            "as_of": item["as_of"],
            "symbol": "AAA",
            "horizon_sessions": horizon,
            f"label_available_from_{horizon}t": label_available_from,
            f"peer_excess_{horizon}t": peer_excess,
        }
    ])


def test_discovery_protocol_keeps_validation_holdout_and_search_space_sealed() -> None:
    validate_discovery_protocol(PROTOCOL, SPECS, PLAN)
    assert PROTOCOL["outcome_access"]["allowed_split"] == "DISCOVERY"
    assert PROTOCOL["outcome_access"]["validation_outcomes_allowed"] is False
    assert PROTOCOL["outcome_access"]["holdout_outcomes_allowed"] is False
    assert all(
        PROTOCOL["discovery_search_space"][key] is False
        for key in (
            "feature_family_search_allowed",
            "feature_subset_search_allowed",
            "feature_sign_flip_allowed",
            "manual_direction_assignment_allowed",
            "threshold_search_allowed",
            "transform_search_allowed",
            "hyperparameter_search_allowed",
            "cross_factor_interactions_allowed",
        )
    )


def test_empty_realistic_manifest_reports_waiting_for_discovery_binding() -> None:
    manifest = empty_split_manifest(SPECS, PLAN)
    state = discovery_stream_state(
        factor_id="rates_policy",
        horizon_sessions=5,
        manifest=manifest,
        specs=SPECS,
        plan=PLAN,
    )
    assert state["state"] == "WAITING_FOR_DISCOVERY_BINDING"
    assert state["discovery_usable_assignments"] == 0


def test_matured_usable_discovery_outcome_can_open() -> None:
    manifest = _manifest_with_ordinals(1)
    opened = prepare_discovery_outcome_rows(
        paired_rows=_paired(1),
        outcome_rows=_outcomes(1),
        factor_id="rates_policy",
        horizon_sessions=5,
        manifest=manifest,
        specs=SPECS,
        plan=PLAN,
        protocol=PROTOCOL,
        research_as_of="2026-10-06",
    )
    assert opened["peer_excess_5t"].tolist() == pytest.approx([0.03])


def test_unmatured_discovery_label_stays_closed() -> None:
    manifest = _manifest_with_ordinals(1)
    with pytest.raises(ExternalEvidence8GDiscoveryError, match="discovery_label_not_mature"):
        prepare_discovery_outcome_rows(
            paired_rows=_paired(1),
            outcome_rows=_outcomes(1, label_available_from="2026-10-10"),
            factor_id="rates_policy",
            horizon_sessions=5,
            manifest=manifest,
            specs=SPECS,
            plan=PLAN,
            protocol=PROTOCOL,
            research_as_of="2026-10-06",
        )


def test_reserved_discovery_boundary_buffer_outcome_is_forbidden() -> None:
    manifest = _manifest_with_ordinals(36)
    with pytest.raises(
        ExternalEvidence8GDiscoveryError,
        match="reserved_boundary_buffer_outcome_forbidden",
    ):
        prepare_discovery_outcome_rows(
            paired_rows=_paired(36),
            outcome_rows=_outcomes(36, label_available_from="2026-11-20"),
            factor_id="rates_policy",
            horizon_sessions=5,
            manifest=manifest,
            specs=SPECS,
            plan=PLAN,
            protocol=PROTOCOL,
            research_as_of="2026-12-01",
        )


def test_validation_outcome_identity_is_forbidden() -> None:
    manifest = _manifest_with_ordinals(41)
    with pytest.raises(ExternalEvidence8GDiscoveryError, match="non_discovery_outcome_input_forbidden"):
        prepare_discovery_outcome_rows(
            paired_rows=_paired(41),
            outcome_rows=_outcomes(41, label_available_from="2026-12-01"),
            factor_id="rates_policy",
            horizon_sessions=5,
            manifest=manifest,
            specs=SPECS,
            plan=PLAN,
            protocol=PROTOCOL,
            research_as_of="2026-12-10",
        )


def test_exact_boundary_purge_is_applied_before_peer_excess_value_is_used() -> None:
    manifest = _manifest_with_ordinals(41)
    validation_first_as_of = _assignment(5, 41)["as_of"]
    with pytest.raises(ExternalEvidence8GDiscoveryError, match="exact_split_boundary_purge_required"):
        prepare_discovery_outcome_rows(
            paired_rows=_paired(35),
            outcome_rows=_outcomes(
                35,
                label_available_from=validation_first_as_of,
                peer_excess=999.0,
            ),
            factor_id="rates_policy",
            horizon_sessions=5,
            manifest=manifest,
            specs=SPECS,
            plan=PLAN,
            protocol=PROTOCOL,
            research_as_of="2026-12-20",
        )


def test_outcome_input_must_match_exact_8g_c_pairing() -> None:
    manifest = _manifest_with_ordinals(1)
    outcomes = _outcomes(1)
    outcomes.loc[0, "symbol"] = "OTHER"
    with pytest.raises(ExternalEvidence8GDiscoveryError, match="outcome_rows_must_match_exact_8g_c_pairing"):
        prepare_discovery_outcome_rows(
            paired_rows=_paired(1),
            outcome_rows=outcomes,
            factor_id="rates_policy",
            horizon_sessions=5,
            manifest=manifest,
            specs=SPECS,
            plan=PLAN,
            protocol=PROTOCOL,
            research_as_of="2026-10-06",
        )


def test_reopening_transform_search_fails_protocol_validation() -> None:
    changed = deepcopy(PROTOCOL)
    changed["discovery_search_space"]["transform_search_allowed"] = True
    with pytest.raises(ExternalEvidence8GDiscoveryError, match="frozen_search_space_reopened"):
        validate_discovery_protocol(changed, SPECS, PLAN)
