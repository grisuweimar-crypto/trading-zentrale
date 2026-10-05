from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.research.governance.qm_j_phase1a_lag1_effectiveness import (
    Lag1EffectivenessError,
    evaluate_daily_differences,
    load_plan,
)


def _daily(n: int, difference: float, *, start: str = "2026-10-06") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "obs_date": pd.bdate_range(start, periods=n),
            "current_minus_lag1": [difference] * n,
        }
    )


def test_lag1_effectiveness_plan_is_frozen_before_eligible_partition() -> None:
    plan = load_plan()
    assert plan.eligible_observation_from == "2026-10-06"
    assert plan.horizon_sessions == 20
    assert plan.similarity_margin == 0.01
    assert plan.block_length_observation_dates == 20
    assert plan.bootstrap_repetitions == 5000
    assert plan.minimum_mature_observation_dates == 40
    assert plan.minimum_temporal_support_regions == 2


def test_lag1_effectiveness_stays_pending_until_maturity_gate() -> None:
    result = evaluate_daily_differences(_daily(39, 0.50))
    assert result["status"] == "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE"
    assert result["mature_observation_dates"] == 39
    assert result["effectiveness_verified"] is False
    assert result["promotion_released"] is False
    assert result["automatic_release_allowed"] is False


def test_lag1_effectiveness_support_requires_lower_bound_above_frozen_margin() -> None:
    result = evaluate_daily_differences(_daily(40, 0.05))
    assert result["status"] == "CAPA_EFFECTIVENESS_EVIDENCE_SUPPORTS_REVIEW"
    assert result["observed_mean_current_minus_lag1"] == pytest.approx(0.05)
    assert result["lower_95_confidence_bound_current_minus_lag1"] == pytest.approx(0.05)
    assert result["lower_95_confidence_bound_current_minus_lag1"] > 0.01
    assert result["review_required"] is True
    assert result["effectiveness_verified"] is False
    assert result["promotion_released"] is False


def test_lag1_effectiveness_is_not_established_when_freshness_edge_is_too_small() -> None:
    result = evaluate_daily_differences(_daily(40, 0.005))
    assert result["status"] == "CAPA_EFFECTIVENESS_NOT_ESTABLISHED"
    assert result["lower_95_confidence_bound_current_minus_lag1"] == pytest.approx(0.005)
    assert result["review_required"] is False
    assert result["effectiveness_verified"] is False


def test_lag1_effectiveness_ignores_pre_boundary_rows() -> None:
    prior = pd.DataFrame(
        {
            "obs_date": pd.bdate_range("2026-09-22", periods=10),
            "current_minus_lag1": [9.0] * 10,
        }
    )
    current = _daily(40, 0.02)
    result = evaluate_daily_differences(pd.concat([prior, current], ignore_index=True))
    assert result["mature_observation_dates"] == 40
    assert result["observed_mean_current_minus_lag1"] == pytest.approx(0.02)


def test_frozen_plan_mutation_is_detected(tmp_path) -> None:
    config = json.loads(
        open("configs/qm_j_phase1a_lag1_effectiveness_v1.json", encoding="utf-8").read()
    )
    config["acceptance_rule"]["inherited_similarity_margin"] = 0.02
    config_path = tmp_path / "plan.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(Lag1EffectivenessError, match="plan_changed_after_freeze"):
        load_plan(
            config_path,
            "configs/qm_j_phase1a_lag1_effectiveness_freeze_v1.json",
        )
