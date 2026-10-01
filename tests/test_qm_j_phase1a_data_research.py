from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.research.governance.qm_j_negative_controls import NegativeControlError
from scanner.research.governance.qm_j_phase1a_data_research import (
    FollowOnPlan,
    _one_way_evaluation,
    load_plan,
    run_falsification,
)


def _synthetic_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    symbols = [f"T{number:02d}" for number in range(20)]
    days = pd.bdate_range("2026-01-02", periods=90)
    price_rows: list[dict[str, object]] = []
    for j, symbol in enumerate(symbols):
        for t, day in enumerate(days):
            # Symbol- and time-dependent growth keeps forward-return ranks non-degenerate.
            close = 50.0 + j * 1.7 + (0.08 + j * 0.004) * t + 0.0008 * (j + 1) * (t**2)
            price_rows.append(
                {
                    "date": day,
                    "symbol": symbol,
                    "close": close,
                    "adj_close": close,
                }
            )

    history_rows: list[dict[str, object]] = []
    observation_days = days[::2][:36]
    for k, day in enumerate(observation_days):
        for j, symbol in enumerate(symbols):
            history_rows.append(
                {
                    "date": day,
                    "symbol": symbol,
                    "score": float((j * 7 + k * 3 + (j * k) % 11) % 101),
                    "observation_type": "observed_scanner",
                    "currency": "USD",
                    "name": symbol,
                    "sector": "test",
                }
            )
    return pd.DataFrame(history_rows), pd.DataFrame(price_rows)


def _test_plan(*, pseudo_count: int = 4) -> FollowOnPlan:
    return FollowOnPlan(
        stable_start="2026-01-01",
        min_cross_section=20,
        horizon_sessions=20,
        metric="phase1a_20t_mean_daily_spearman",
        metric_direction="HIGHER_IS_BETTER",
        similarity_margin=0.01,
        shift_by_observations=1,
        pseudo_signal_count=pseudo_count,
        pseudo_signal_quantile=0.95,
        pseudo_seed_prefix="qm-j-test-pseudo",
    )


def test_frozen_config_discloses_known_real_baseline() -> None:
    plan = load_plan()
    assert plan.prior_real_result_visibility == "KNOWN"
    assert plan.new_control_outcomes_visibility == "NONE"
    assert plan.confirmatory_promotion_use_allowed is False
    assert plan.horizon_sessions == 20
    assert plan.similarity_margin == 0.01
    assert plan.shift_by_observations == 1
    assert plan.pseudo_signal_count == 64


def test_config_cannot_pretend_real_baseline_was_unseen(tmp_path) -> None:
    source = json.loads(
        open("configs/qm_j_phase1a_data_research_controls_v1.json", encoding="utf-8").read()
    )
    source["knowledge_state"]["prior_phase1a_real_result_visibility_at_freeze"] = "NONE"
    target = tmp_path / "bad.json"
    target.write_text(json.dumps(source), encoding="utf-8")
    with pytest.raises(NegativeControlError, match="must_disclose_known_real_baseline"):
        load_plan(target)


def test_followon_evaluation_is_one_way_only() -> None:
    plan = _test_plan()
    safe = _one_way_evaluation(
        plan=plan,
        control_id="weak-control",
        real_value=0.03,
        control_value=0.005,
        comparison_context_hash_value="a" * 64,
        n_observations=100,
    )
    assert safe["triggered"] is False
    assert safe["nontrigger_is_validation"] is False
    assert safe["confirmatory_promotion_use_allowed"] is False
    assert safe["promotion_performed"] is False

    trigger = _one_way_evaluation(
        plan=plan,
        control_id="similar-control",
        real_value=0.03,
        control_value=0.025,
        comparison_context_hash_value="a" * 64,
        n_observations=100,
    )
    assert trigger["triggered"] is True
    assert trigger["status"] == "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
    assert trigger["promotion_blocked_by_qm_j"] is True
    assert trigger["capa_required"] is True


def test_harness_uses_past_shift_and_pseudo_signals_without_mutating_sources() -> None:
    history, prices = _synthetic_inputs()
    history_before = history.copy(deep=True)
    prices_before = prices.copy(deep=True)
    result = run_falsification(history, prices, plan=_test_plan())

    assert history.equals(history_before)
    assert prices.equals(prices_before)
    assert result["data_control"]["control_type"] == "SHIFTED_DATA"
    assert result["data_control"]["shift_by_observations"] == 1
    assert result["data_control"]["uses_future_rows"] is False
    assert result["data_control"]["matched_grid"] is True
    assert result["research_control"]["control_type"] == "PSEUDO_SIGNAL"
    assert result["research_control"]["control_count"] == 4
    assert result["research_control"]["outcomes_used_to_generate_signals"] is False
    assert result["research_control"]["matched_grid"] is True
    assert result["promotion_performed"] is False
    assert result["productive_integration_enabled"] is False
    assert result["execution_allowed"] is False


def test_harness_is_deterministic() -> None:
    history, prices = _synthetic_inputs()
    first = run_falsification(history, prices, plan=_test_plan())
    second = run_falsification(history, prices, plan=_test_plan())
    assert first == second
