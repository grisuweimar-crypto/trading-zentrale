from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

import scanner.research.governance.qm_j_phase1a_selection as qmj
from scanner.research.governance.qm_j_negative_controls import NegativeControlError


def _plan(*, count=20):
    return qmj.Phase1APlaceboPlan(
        stable_start="2026-04-15",
        min_cross_section=20,
        horizon_sessions=20,
        metric="phase1a_20t_mean_daily_spearman",
        metric_direction="HIGHER_IS_BETTER",
        similarity_margin=0.01,
        permutation_count=count,
        permutation_quantile=0.95,
        seed_prefix="qm-j-test",
        outcome_visibility_at_freeze="NONE",
    )


def _events(*, relation="positive", days=3, n=20):
    rows = []
    for day in range(days):
        date = pd.Timestamp("2026-06-01") + pd.Timedelta(days=day)
        for index in range(n):
            pct = index / (n - 1)
            if relation == "positive":
                ret = pct
            elif relation == "negative":
                ret = -pct
            else:
                ret = float(np.sin(index * 2.731 + day))
            rows.append({
                "obs_date": date,
                "symbol": f"S{index:02d}",
                "score_pct_full": pct,
                "return_20t": ret,
                "preserved": f"{date.date()}:{index}",
            })
    return pd.DataFrame(rows)


def test_frozen_plan_keeps_single_primary_horizon_and_no_secondary_selection():
    plan = qmj.load_plan()
    assert plan.horizon_sessions == 20
    assert plan.metric == "phase1a_20t_mean_daily_spearman"
    assert plan.permutation_count == 64
    assert plan.permutation_quantile == pytest.approx(0.95)
    assert plan.similarity_margin == pytest.approx(0.01)
    assert plan.outcome_visibility_at_freeze == "NONE"


def test_within_date_permutation_is_deterministic_preserves_marginals_and_non_score_fields():
    events = _events(relation="positive")
    original = deepcopy(events)
    a = qmj.permute_score_within_date(events, seed="frozen-seed")
    b = qmj.permute_score_within_date(events, seed="frozen-seed")
    pd.testing.assert_frame_equal(events, original)
    pd.testing.assert_series_equal(a["score_pct_full"], b["score_pct_full"])
    assert not a["score_pct_full"].equals(events["score_pct_full"])
    pd.testing.assert_frame_equal(
        a.drop(columns=["score_pct_full"]),
        events.drop(columns=["score_pct_full"]),
    )
    for day, group in events.groupby("obs_date"):
        control = a.loc[a["obs_date"].eq(day), "score_pct_full"]
        assert sorted(control.tolist()) == sorted(group["score_pct_full"].tolist())


def test_comparison_context_ignores_score_but_detects_outcome_change():
    events = _events(relation="positive")
    sample = qmj._metric_sample(events, 20, 20)
    permuted = qmj.permute_score_within_date(events, seed="seed")
    control_sample = qmj._metric_sample(permuted, 20, 20)
    assert qmj.comparison_context_hash(sample, 20) == qmj.comparison_context_hash(control_sample, 20)

    changed = control_sample.copy()
    changed.loc[0, "return_20t"] += 0.001
    assert qmj.comparison_context_hash(sample, 20) != qmj.comparison_context_hash(changed, 20)


def test_strong_real_selection_survives_permutation_control_without_promotion(monkeypatch):
    events = _events(relation="positive")
    monkeypatch.setattr(qmj, "build_events", lambda *_args, **_kwargs: events.copy())
    result = qmj.run_falsification(pd.DataFrame(), pd.DataFrame(), plan=_plan())
    evaluation = result["falsification_evaluation"]
    assert result["real_metric"]["mean_daily_spearman"] == pytest.approx(1.0)
    assert result["placebo_distribution"]["p95_higher"] < 1.0 - 0.01
    assert evaluation["status"] == "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
    assert evaluation["promotion_blocked_by_qm_j"] is False
    assert evaluation["promotion_performed"] is False
    assert result["promotion_performed"] is False


def test_bad_real_selection_is_falsified_when_placebo_is_similarly_strong(monkeypatch):
    events = _events(relation="negative")
    monkeypatch.setattr(qmj, "build_events", lambda *_args, **_kwargs: events.copy())
    result = qmj.run_falsification(pd.DataFrame(), pd.DataFrame(), plan=_plan())
    evaluation = result["falsification_evaluation"]
    assert result["real_metric"]["mean_daily_spearman"] == pytest.approx(-1.0)
    assert evaluation["status"] == "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
    assert evaluation["promotion_blocked_by_qm_j"] is True
    assert evaluation["investigation_required"] is True
    assert evaluation["capa_required"] is True
    assert evaluation["promotion_performed"] is False


def test_falsification_result_preserves_phase1a_and_source_boundaries(monkeypatch):
    events = _events(relation="independent")
    monkeypatch.setattr(qmj, "build_events", lambda *_args, **_kwargs: events.copy())
    result = qmj.run_falsification(
        pd.DataFrame(),
        pd.DataFrame(),
        plan=_plan(),
        history_source_hash="a" * 64,
        price_source_hash="b" * 64,
    )
    assert result["source"] == {"history_sha256": "a" * 64, "price_sha256": "b" * 64}
    assert result["plan"]["secondary_horizons_used_for_selection"] == []
    assert result["phase1a_logic_changed"] is False
    assert result["history_or_prices_changed"] is False
    assert result["productive_integration_enabled"] is False
    assert result["execution_allowed"] is False
    assert result["promotion_performed"] is False


def test_metric_sample_fails_closed_when_cross_section_is_too_small():
    with pytest.raises(NegativeControlError, match="no_eligible_metric_sample"):
        qmj._metric_sample(_events(n=19), 20, 20)
