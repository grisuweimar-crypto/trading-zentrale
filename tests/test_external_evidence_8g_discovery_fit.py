from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanner.research.external_evidence.discovery_fit_8g import (
    discovery_final_freeze_gate,
    fit_discovery_preprocessor,
    fit_fixed_ridge,
    fit_guarded_discovery_models,
    transform_discovery_features,
)
from scanner.research.external_evidence.model_8g import resolved_baseline_columns
from scanner.research.external_evidence.research_8g import empty_split_manifest, slot_state


ROOT = Path(__file__).resolve().parents[1]
SPECS = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
PLAN = json.loads((ROOT / "configs" / "external_evidence_8g_split_plan_v1.json").read_text())
PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_discovery_protocol_v1.json").read_text())


def _assignment(ordinal: int, horizon: int = 5) -> dict:
    state = slot_state(horizon, ordinal)
    day = date(2026, 9, 28) + timedelta(days=ordinal - 1)
    return {
        "ordinal": ordinal,
        "snapshot_id": f"snap-{ordinal:03d}",
        "as_of": day.isoformat(),
        "generated_at": f"{day.isoformat()}T18:00:00+00:00",
        "split": state["split"],
        "usable": state["usable"],
        "purge_reason": state["purge_reason"],
        "factor_state_sha256": f"{ordinal:064x}"[-64:],
        "active_mapping_count": 2,
        "active_mapping_ids_sha256": f"{ordinal + 1:064x}"[-64:],
    }


def _manifest(count: int, horizon: int = 5) -> dict:
    manifest = empty_split_manifest(SPECS, PLAN)
    manifest["streams"][f"rates_policy_x_{horizon}t"]["assignments"] = [
        _assignment(i, horizon) for i in range(1, count + 1)
    ]
    return manifest


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    numeric, categorical = resolved_baseline_columns(SPECS, 5)
    rows = []
    external_levels = {"snap-001": (3.80, -0.02), "snap-002": (3.95, 0.15)}
    for snapshot_index, snapshot_id in enumerate(("snap-001", "snap-002"), start=1):
        as_of = (date(2026, 9, 27) + timedelta(days=snapshot_index)).isoformat()
        for symbol_index, symbol in enumerate(("AAA", "BBB"), start=1):
            row = {
                "snapshot_id": snapshot_id,
                "as_of": as_of,
                "generated_at": f"{as_of}T18:00:00+00:00",
                "symbol": symbol,
                "horizon_sessions": 5,
            }
            for position, column in enumerate(numeric, start=1):
                row[column] = float(snapshot_index * 100 + symbol_index * 10 + position) / 10.0
            if snapshot_id == "snap-001" and symbol == "AAA":
                row["score"] = np.nan
            categorical_values = {
                "quality_band": "R3_BACKBONE" if symbol == "AAA" else "R2_SUPPORT",
                "r_code": "R3" if symbol == "AAA" else "R2",
                "score_status": "valid",
                "trend_ok": True,
                "liquidity_ok": True,
                "cycle": 50.0,
                "currency": "USD",
            }
            for column in categorical:
                row[column] = categorical_values[column]
            rows.append(row)
    baseline = pd.DataFrame(rows)
    challenger = baseline.copy()
    challenger["external__rates_policy_level_pct"] = [
        external_levels[snapshot][0] for snapshot in challenger["snapshot_id"]
    ]
    challenger["external__rates_policy_delta_pp"] = [
        external_levels[snapshot][1] for snapshot in challenger["snapshot_id"]
    ]
    challenger["external_relationship_class"] = [
        "FINANCING_SENSITIVITY" if symbol == "AAA" else "VALUATION_DURATION"
        for symbol in challenger["symbol"]
    ]
    return baseline, challenger


def _outcomes() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "snapshot_id": "snap-001",
            "as_of": "2026-09-28",
            "symbol": "AAA",
            "horizon_sessions": 5,
            "label_available_from_5t": "2026-10-05",
            "peer_excess_5t": 0.03,
        },
        {
            "snapshot_id": "snap-001",
            "as_of": "2026-09-28",
            "symbol": "BBB",
            "horizon_sessions": 5,
            "label_available_from_5t": "2026-10-05",
            "peer_excess_5t": -0.01,
        },
        {
            "snapshot_id": "snap-002",
            "as_of": "2026-09-29",
            "symbol": "AAA",
            "horizon_sessions": 5,
            "label_available_from_5t": "2026-10-06",
            "peer_excess_5t": 0.04,
        },
        {
            "snapshot_id": "snap-002",
            "as_of": "2026-09-29",
            "symbol": "BBB",
            "horizon_sessions": 5,
            "label_available_from_5t": "2026-10-06",
            "peer_excess_5t": 0.00,
        },
    ])


def test_preprocessor_is_feature_only_deterministic_and_emits_frozen_columns() -> None:
    baseline, challenger = _frames()
    state = fit_discovery_preprocessor(
        baseline_features=baseline,
        challenger_features=challenger,
        factor_id="rates_policy",
        horizon_sessions=5,
        specs=SPECS,
        protocol=PROTOCOL,
        plan=PLAN,
    )
    assert state["outcomes_read_while_fitting_preprocessor"] is False
    assert state["numeric_ddof"] == 0
    assert state["preprocessor_sha256"]
    assert "missing::score" in state["baseline_output_columns"]
    assert "external::external__rates_policy_level_pct" in state["challenger_output_columns"]
    assert not any(name == "external_relationship_class" for name in state["challenger_output_columns"])

    baseline_x, baseline_names = transform_discovery_features(
        baseline, preprocessor=state, challenger=False
    )
    challenger_x, challenger_names = transform_discovery_features(
        challenger, preprocessor=state, challenger=True
    )
    assert baseline_x.shape[0] == challenger_x.shape[0] == 4
    assert baseline_names == state["baseline_output_columns"]
    assert challenger_names == state["challenger_output_columns"]
    assert np.isfinite(baseline_x).all()
    assert np.isfinite(challenger_x).all()


def test_zero_variance_external_feature_is_dropped_by_preregistered_rule() -> None:
    baseline, challenger = _frames()
    challenger = challenger.copy()
    challenger["external__rates_policy_delta_pp"] = 0.0
    state = fit_discovery_preprocessor(
        baseline_features=baseline,
        challenger_features=challenger,
        factor_id="rates_policy",
        horizon_sessions=5,
        specs=SPECS,
        protocol=PROTOCOL,
        plan=PLAN,
    )
    assert "external__rates_policy_delta_pp" in state["external_zero_variance_dropped"]
    assert not any(
        "external__rates_policy_delta_pp" in column
        for column in state["challenger_output_columns"]
    )


def test_fixed_ridge_is_deterministic_and_alpha_cannot_be_tuned() -> None:
    x = np.asarray([[0.0, 1.0], [1.0, 0.0], [2.0, 1.0], [3.0, 0.0]])
    y = np.asarray([0.0, 0.2, 0.5, 0.7])
    first = fit_fixed_ridge(x, y, feature_names=["a", "b"], alpha=1.0)
    second = fit_fixed_ridge(x, y, feature_names=["a", "b"], alpha=1.0)
    assert first == second
    assert first["alpha"] == 1.0
    assert first["training_n"] == 4
    with pytest.raises(Exception, match="ridge_alpha_must_remain_frozen_at_1"):
        fit_fixed_ridge(x, y, feature_names=["a", "b"], alpha=2.0)


def test_guarded_fit_fits_preprocessor_before_opening_only_matured_discovery_labels() -> None:
    baseline, challenger = _frames()
    artifact = fit_guarded_discovery_models(
        baseline_features=baseline,
        challenger_features=challenger,
        outcome_rows=_outcomes(),
        factor_id="rates_policy",
        horizon_sessions=5,
        manifest=_manifest(2),
        specs=SPECS,
        plan=PLAN,
        protocol=PROTOCOL,
        research_as_of="2026-10-06",
    )
    assert artifact["status"] == "PROVISIONAL_DISCOVERY_FIT"
    assert artifact["preprocessing_fitted_before_outcome_open"] is True
    assert artifact["feature_fit_rows"] == 4
    assert artifact["outcome_fit_rows"] == 4
    assert artifact["baseline_model"]["alpha"] == 1.0
    assert artifact["challenger_model"]["alpha"] == 1.0
    assert artifact["validation_outcomes_opened"] is False
    assert artifact["holdout_outcomes_opened"] is False
    assert artifact["validation_open_allowed"] is False


def test_discovery_freeze_gate_waits_for_complete_cohort_and_validation_boundary() -> None:
    baseline, challenger = _frames()
    artifact = fit_guarded_discovery_models(
        baseline_features=baseline,
        challenger_features=challenger,
        outcome_rows=_outcomes(),
        factor_id="rates_policy",
        horizon_sessions=5,
        manifest=_manifest(2),
        specs=SPECS,
        plan=PLAN,
        protocol=PROTOCOL,
        research_as_of="2026-10-06",
    )
    gate = discovery_final_freeze_gate(
        fit_artifact=artifact,
        manifest=_manifest(2),
        specs=SPECS,
        plan=PLAN,
    )
    assert gate["ready_for_final_discovery_model_freeze"] is False
    assert "DISCOVERY_RAW_COHORT_INCOMPLETE" in gate["reasons"]
    assert "FIRST_VALIDATION_FEATURE_BINDING_REQUIRED_FOR_EXACT_BOUNDARY" in gate["reasons"]


def test_discovery_freeze_gate_can_open_only_after_full_feature_side_boundary_exists() -> None:
    manifest = _manifest(41)
    artifact = {
        "schema_version": "external_evidence_8g_discovery_fit_v1",
        "factor_id": "rates_policy",
        "horizon_sessions": 5,
        "outcome_fit_rows": 35,
        "outcome_snapshot_ids": [f"snap-{i:03d}" for i in range(1, 36)],
        "outcome_as_of_dates": [
            (date(2026, 9, 28) + timedelta(days=i - 1)).isoformat()
            for i in range(1, 36)
        ],
    }
    gate = discovery_final_freeze_gate(
        fit_artifact=artifact,
        manifest=manifest,
        specs=SPECS,
        plan=PLAN,
    )
    assert gate["ready_for_final_discovery_model_freeze"] is True
    assert gate["state"] == "DISCOVERY_READY_FOR_FINAL_MODEL_FREEZE"
    assert gate["temporal_support_regions"] >= 2
    assert gate["validation_outcomes_opened"] is False
    assert gate["holdout_outcomes_opened"] is False


def test_protocol_implementation_details_are_frozen_before_real_outcomes() -> None:
    assert PROTOCOL["status"] == "TECHNICALLY_READY_WAITING_FOR_FIRST_ELIGIBLE_DISCOVERY_BINDING"
    assert PROTOCOL["preprocessing_implementation"]["fit_before_any_discovery_outcome_value_is_opened"] is True
    assert PROTOCOL["ridge_implementation"]["alpha"] == 1.0
    assert PROTOCOL["ridge_implementation"]["hyperparameter_search"] is False
    assert PROTOCOL["completion_gate"]["first_empirical_binding_eligible_from"].startswith("2026-09-28")
