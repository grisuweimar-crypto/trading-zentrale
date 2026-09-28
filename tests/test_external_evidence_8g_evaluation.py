from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from scanner.research.external_evidence.evaluation_8g import (
    ExternalEvidence8GEvaluationError,
    build_discovery_freeze_receipt,
    consume_holdout_once,
    evaluate_confirmatory_rows,
    freeze_validation_family,
    holm_adjust_family,
    new_holdout_consumption_ledger,
    promotion_candidate_state,
    validate_evaluation_protocol,
)
from scanner.research.external_evidence.discovery_fit_8g import FIT_SCHEMA


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_evaluation_protocol_v1.json").read_text())


def _fit_artifact() -> dict:
    return {
        "schema_version": FIT_SCHEMA,
        "factor_id": "rates_policy",
        "horizon_sessions": 5,
        "fit_sha256": "1" * 64,
        "preprocessor": {"preprocessor_sha256": "2" * 64},
        "baseline_model": {"model_sha256": "3" * 64},
        "challenger_model": {"model_sha256": "4" * 64},
    }


def _ready_gate(ready: bool = True) -> dict:
    return {
        "schema_version": "external_evidence_8g_discovery_freeze_gate_v1",
        "factor_id": "rates_policy",
        "horizon_sessions": 5,
        "ready_for_final_discovery_model_freeze": ready,
    }


def _result(hypothesis: str, p: float | None, effect: float | None, *, split: str = "VALIDATION", enough: bool = True) -> dict:
    return {
        "schema_version": "external_evidence_8g_confirmatory_result_v1",
        "hypothesis_id": hypothesis,
        "split": split,
        "status": "EVALUATED" if enough else "INSUFFICIENT_EVIDENCE",
        "minimum_evidence_met": enough,
        "primary_effect": effect,
        "primary_p_value_two_sided": p,
        "primary_ci_95": [0.01, 0.05] if enough else [None, None],
        "non_overlap_sensitivity": {"materially_contradicts_primary": False},
        "coverage_missingness": {"status": "COMPLETE"},
        "regime_stability": {"status": "OK"},
        "sector_stability": {"status": "OK"},
        "currency_stability": {"status": "OK"},
        "pit_integrity_pass": True,
    }


def test_evaluation_protocol_is_validation_phase_and_keeps_production_off() -> None:
    validate_evaluation_protocol(PROTOCOL)
    assert PROTOCOL["phase"] == "8G-E"
    assert PROTOCOL["next_planned_phase"] == "8G-F_HOLDOUT_OOS"
    assert PROTOCOL["split_policy"]["holdout"]["final_governance_owned_by"] == "8G-F"
    assert PROTOCOL["productive_integration_enabled"] is False
    assert PROTOCOL["automatic_promotion_enabled"] is False
    assert PROTOCOL["multiple_testing"]["family_size"] == 12
    assert PROTOCOL["cost_turnover"]["status"] == "NOT_APPLICABLE_WHILE_8G_REMAINS_PREDICTIVE_RESEARCH_ONLY"


def test_discovery_model_cannot_freeze_before_empirical_gate_is_ready() -> None:
    with pytest.raises(ExternalEvidence8GEvaluationError, match="discovery_not_ready_for_final_freeze"):
        build_discovery_freeze_receipt(_fit_artifact(), _ready_gate(False))


def test_ready_discovery_model_gets_immutable_freeze_receipt() -> None:
    receipt = build_discovery_freeze_receipt(_fit_artifact(), _ready_gate(True))
    assert receipt["hypothesis_id"] == "rates_policy_x_5t"
    assert receipt["frozen"] is True
    assert receipt["validation_refit_allowed"] is False
    assert receipt["holdout_refit_allowed"] is False
    assert len(receipt["receipt_sha256"]) == 64


def test_holm_keeps_all_12_frozen_hypotheses_and_counts_underpowered_as_p1() -> None:
    family = PROTOCOL["frozen_hypothesis_family"]
    raw = {
        family[0]: _result(family[0], 0.001, 0.05),
        family[1]: _result(family[1], 0.004, 0.04),
        family[2]: _result(family[2], None, None, enough=False),
    }
    adjusted = holm_adjust_family(raw, PROTOCOL)
    assert list(adjusted) == family
    assert adjusted[family[0]]["holm_adjusted_p"] == pytest.approx(0.012)
    assert adjusted[family[0]]["holm_positive"] is True
    assert adjusted[family[2]]["holm_adjusted_p"] == pytest.approx(1.0)
    assert adjusted[family[2]]["holm_positive"] is False
    assert all(hypothesis in adjusted for hypothesis in family)


def test_validation_family_freeze_is_required_before_holdout_and_is_hash_bound() -> None:
    raw = {hypothesis: _result(hypothesis, 0.5, 0.01) for hypothesis in PROTOCOL["frozen_hypothesis_family"]}
    receipt = freeze_validation_family(raw, PROTOCOL)
    assert receipt["frozen"] is True
    assert receipt["holdout_open_allowed"] is True
    assert receipt["holdout_spec_changes_allowed"] is False
    assert len(receipt["results_sha256"]) == 64


def test_holdout_consumption_is_one_shot_per_frozen_hypothesis() -> None:
    hypothesis = PROTOCOL["frozen_hypothesis_family"][0]
    ledger = new_holdout_consumption_ledger(PROTOCOL)
    consumed = consume_holdout_once(ledger, hypothesis_id=hypothesis, evaluation_sha256="a" * 64)
    assert consumed["streams"][hypothesis]["state"] == "CONSUMED"
    with pytest.raises(ExternalEvidence8GEvaluationError, match="holdout_may_open_once_only"):
        consume_holdout_once(consumed, hypothesis_id=hypothesis, evaluation_sha256="b" * 64)


def _opened_rows(n: int = 40) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rows = []
    context = []
    coverage = []
    start = date(2026, 10, 1)
    for i in range(n):
        market_day = start + timedelta(days=i)
        snapshot_id = f"snap-{i+1:03d}"
        as_of = market_day.isoformat()
        symbol = f"SYM{i % 5}"
        target = float((i % 7) - 3) / 100.0
        rows.append({
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "symbol": symbol,
            "horizon_sessions": 5,
            "start_market_date": market_day.isoformat(),
            "label_available_from_5t": (market_day + timedelta(days=5)).isoformat(),
            "peer_excess_5t": target,
            "baseline_prediction": target + 0.1,
            "challenger_prediction": target,
        })
        context.append({
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "symbol": symbol,
            "market_regime_stock": "bull" if i < n // 2 else "bear",
            "sector": "Technology" if i % 2 == 0 else "Industrials",
            "currency": "USD",
        })
        coverage.append({
            "paired_rows": 1,
            "excluded_unmapped_rows": 0,
            "excluded_ambiguous_mapping_rows": 0,
        })
    return pd.DataFrame(rows), pd.DataFrame(context), coverage


def test_confirmatory_metric_uses_snapshot_improvement_block_uncertainty_and_diagnostics() -> None:
    rows, context, coverage = _opened_rows()
    result = evaluate_confirmatory_rows(
        opened_rows=rows,
        factor_id="rates_policy",
        horizon_sessions=5,
        split="VALIDATION",
        protocol=PROTOCOL,
        context_rows=context,
        coverage_records=coverage,
    )
    assert result["minimum_evidence_met"] is True
    assert result["paired_n"] == 40
    assert result["temporal_support_regions"] >= 2
    assert result["primary_effect"] == pytest.approx(0.01)
    assert result["primary_ci_95"][0] == pytest.approx(0.01)
    assert result["primary_ci_95"][1] == pytest.approx(0.01)
    assert result["primary_p_value_two_sided"] < 0.01
    assert result["mean_absolute_error_improvement"] == pytest.approx(0.1)
    assert result["non_overlap_sensitivity"]["snapshot_n"] >= 2
    assert result["non_overlap_sensitivity"]["materially_contradicts_primary"] is False
    assert result["regime_stability"]["status"] == "OK"
    assert result["sector_stability"]["status"] == "OK"
    assert result["currency_stability"]["status"] == "OK"
    assert result["coverage_missingness"]["coverage_ratio"] == pytest.approx(1.0)
    assert result["model_refit"] is False


def test_underpowered_confirmatory_result_has_no_bootstrap_pvalue() -> None:
    rows, context, coverage = _opened_rows(10)
    result = evaluate_confirmatory_rows(
        opened_rows=rows,
        factor_id="rates_policy",
        horizon_sessions=5,
        split="VALIDATION",
        protocol=PROTOCOL,
        context_rows=context,
        coverage_records=coverage,
    )
    assert result["status"] == "INSUFFICIENT_EVIDENCE"
    assert result["primary_p_value_two_sided"] is None
    assert result["primary_ci_95"] == [None, None]


def test_promotion_requires_both_holm_splits_positive_and_positive_holdout_ci() -> None:
    hypothesis = "rates_policy_x_5t"
    validation = _result(hypothesis, 0.001, 0.03)
    validation["holm_positive"] = True
    validation["holm_adjusted_p"] = 0.012
    holdout = _result(hypothesis, 0.001, 0.02, split="HOLDOUT")
    holdout["holm_positive"] = True
    holdout["holm_adjusted_p"] = 0.012
    state = promotion_candidate_state(
        hypothesis_id=hypothesis,
        validation_family={hypothesis: validation},
        holdout_family={hypothesis: holdout},
    )
    assert state == "PROMOTION_CANDIDATE_FOR_8H_REVIEW"

    holdout["primary_ci_95"] = [-0.001, 0.05]
    state = promotion_candidate_state(
        hypothesis_id=hypothesis,
        validation_family={hypothesis: validation},
        holdout_family={hypothesis: holdout},
    )
    assert state == "NO_PROMOTION_EVIDENCE"
