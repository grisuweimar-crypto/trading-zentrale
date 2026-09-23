from __future__ import annotations

import pandas as pd
import pytest

from scripts.run_confidence_vnext_4 import _apply_stable_start
from scanner.domain.scoring_engine.factors.universe_csv import Universe, scale_from_universe
from scanner.domain.scoring_engine.quality.confidence import compute_confidence
from scanner.reports.confidence_vnext import (
    Phase4Config,
    _spearman_pair,
    audit_history,
    research_contract,
    testability_matrix as confidence_testability_matrix,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-02-10",
                "symbol": "AAA",
                "score": 10.0,
                "confidence": None,
                "confidence_label": None,
                "rs3m": 0.1,
                "trend200": 0.2,
                "volatility": 0.03,
                "drawdown": 0.2,
                "growth": 5.0,
                "roe": 10.0,
                "margin": 8.0,
                "debt_ratio": 30.0,
                "liquidity_risk": None,
            },
            # Earlier same-day run: keep-last semantics must not double count it.
            {
                "date": "2026-02-11",
                "symbol": "AAA",
                "score": 11.0,
                "confidence": None,
                "confidence_label": None,
                "rs3m": 0.1,
                "trend200": 0.2,
                "volatility": 0.03,
                "drawdown": 0.2,
                "growth": 5.0,
                "roe": 10.0,
                "margin": 8.0,
                "debt_ratio": 30.0,
                "liquidity_risk": None,
            },
            {
                "date": "2026-02-11",
                "symbol": "AAA",
                "score": 0.0,
                "confidence": 50.0,
                "confidence_label": "MED",
                "rs3m": 0.1,
                "trend200": 0.2,
                "volatility": 0.03,
                "drawdown": 0.2,
                "growth": 5.0,
                "roe": 10.0,
                "margin": 8.0,
                "debt_ratio": 30.0,
                "liquidity_risk": None,
            },
            {
                "date": "2026-09-22",
                "symbol": "AAA",
                "score": 40.0,
                "confidence": 55.0,
                "confidence_label": "MED",
                "rs3m": 0.2,
                "trend200": 0.3,
                "volatility": 0.02,
                "drawdown": 0.1,
                "growth": 6.0,
                "roe": 11.0,
                "margin": 9.0,
                "debt_ratio": 25.0,
                "liquidity_risk": None,
            },
            {
                "date": "2026-09-23",
                "symbol": "AAA",
                "score": 42.0,
                "confidence": 65.0,
                "confidence_label": "MED",
                "rs3m": 0.2,
                "trend200": 0.3,
                "volatility": 0.02,
                "drawdown": 0.1,
                "growth": 6.0,
                "roe": 11.0,
                "margin": 9.0,
                "debt_ratio": 25.0,
                "liquidity_risk": None,
            },
        ]
    )


def test_history_audit_deduplicates_and_keeps_ambiguous_fix_day_unknown():
    report = audit_history(_history(), Phase4Config())
    assert report["scanner_rows_after_same_day_symbol_dedup"] == 4
    assert report["aggregate_confidence"]["first_observed_non_null_date"] == "2026-02-11"

    epochs = report["aggregate_confidence"]["formula_epochs"]
    assert epochs["pre_alias_fix"]["rows"] == 2
    assert epochs["transition_unknown"]["rows"] == 1
    assert epochs["post_alias_fix"]["rows"] == 1
    assert epochs["transition_unknown"]["confidence_non_null_rows"] == 1


def test_known_2026_09_22_post_fix_run_is_classified_from_provenance():
    frame = pd.DataFrame(
        [
            {
                "date": "2026-09-22",
                "symbol": "AAA",
                "score": 40.0,
                "confidence": 60.0,
                "confidence_label": "MED",
                "run_id": "github-35753072370-1",
            }
        ]
    )
    report = audit_history(frame)
    epochs = report["aggregate_confidence"]["formula_epochs"]
    assert epochs["transition_unknown"]["rows"] == 0
    assert epochs["post_alias_fix"]["rows"] == 1
    assert epochs["post_alias_fix"]["classification_reasons"]["known_post_fix_run_id"] == 1


def test_unmarked_legacy_scanner_rows_survive_prospective_provenance_columns():
    frame = pd.DataFrame(
        [
            {
                "date": "2026-04-15",
                "symbol": "AAA",
                "score": 20.0,
                "confidence": 40.0,
                "observation_type": "",
                "data_source": "",
            },
            {
                "date": "2026-04-15",
                "symbol": "PRICE_ONLY",
                "score": None,
                "confidence": None,
                "observation_type": "",
                "data_source": "",
            },
            {
                "date": "2026-09-23",
                "symbol": "BBB",
                "score": 30.0,
                "confidence": 60.0,
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-09-23",
                "symbol": "MARKET",
                "score": 50.0,
                "confidence": 70.0,
                "observation_type": "market_data",
                "data_source": "market_data",
            },
        ]
    )
    report = audit_history(frame)
    assert report["scanner_rows_after_same_day_symbol_dedup"] == 2
    assert report["scanner_dates"] == 2
    assert report["aggregate_confidence"]["first_observed_non_null_date"] == "2026-04-15"


def test_raw_factor_presence_proxy_includes_debt_and_liquidity_and_fails_closed():
    report = audit_history(_history())
    proxy = report["raw_factor_presence_proxy"]
    assert "debt_ratio" in proxy["required_factor_fields"]
    assert "liquidity_risk" in proxy["required_factor_fields"]
    assert proxy["per_field_presence_rate"]["liquidity_risk"] == 0.0
    assert proxy["complete_raw_factor_presence_rate"] == 0.0


def test_display_label_boundary_mismatch_is_reported_not_silently_normalized():
    frame = pd.DataFrame(
        [
            {
                "date": "2026-09-18",
                "symbol": "NOW",
                "score": 40.0,
                "confidence": 50.0,
                "confidence_label": "LOW",
            }
        ]
    )
    report = audit_history(frame)
    consistency = report["aggregate_confidence"]["display_label_consistency"]
    assert consistency["compared_rows"] == 1
    assert consistency["display_score_label_mismatches"] == 1
    assert consistency["samples"][0]["label_implied_by_stored_rounded_score"] == "MED"


def test_spearman_diagnostic_uses_rank_pearson_without_scipy_dependency():
    frame = pd.DataFrame(
        {
            "confidence": [10.0, 20.0, 30.0, 40.0],
            "score": [1.0, 2.0, 3.0, 4.0],
        }
    )
    result = _spearman_pair(frame, "confidence", "score")
    assert result["N"] == 4
    assert abs(result["spearman"] - 1.0) < 1e-12


def test_runner_stable_start_filters_the_actual_audit_input(tmp_path):
    source = tmp_path / "history.csv"
    _history().to_csv(source, index=False)

    filtered_path, cleanup = _apply_stable_start(source, "2026-09-22")
    try:
        filtered = pd.read_csv(filtered_path)
        dates = pd.to_datetime(filtered["date"], errors="coerce")
        assert len(filtered) == 2
        assert dates.min() == pd.Timestamp("2026-09-22")
        assert dates.max() == pd.Timestamp("2026-09-23")
    finally:
        if cleanup is not None:
            cleanup.unlink(missing_ok=True)


def test_runner_stable_start_rejects_invalid_date(tmp_path):
    source = tmp_path / "history.csv"
    _history().to_csv(source, index=False)
    with pytest.raises(ValueError, match="invalid --stable-start"):
        _apply_stable_start(source, "not-a-date")


def test_missing_raw_value_is_neutralized_before_legacy_confidence():
    universe = Universe(df=pd.DataFrame(), dists={"Growth %": [1.0, 2.0, 3.0]})
    assert scale_from_universe(universe, "Growth %", None) == 0.5


def test_legacy_neutralized_risk_values_are_counted_as_clean():
    factors = {
        "growth": 0.5,
        "roe": 0.5,
        "margin": 0.5,
        "debt_to_equity": 0.5,
        "volatility": 0.5,
        "relative_strength": 0.5,
        "trend_200dma": 0.5,
        "max_drawdown": 0.5,
        "liquidity_risk": 0.5,
    }
    config = {
        "CONFIDENCE_WEIGHTS": {
            "coverage": 0.25,
            "confluence": 0.25,
            "risk_clean": 0.20,
            "regime_align": 0.20,
            "liquidity": 0.10,
        },
        "CONFIDENCE_CORE_FACTORS": [
            "growth",
            "roe",
            "margin",
            "debt_to_equity",
            "volatility",
            "relative_strength",
            "trend_200dma",
        ],
        "CONFIDENCE_OPPORTUNITY_FACTORS": [
            "growth",
            "roe",
            "margin",
            "relative_strength",
            "trend_200dma",
        ],
        "CONFIDENCE_RISK_FACTORS": ["volatility", "max_drawdown", "debt_to_equity"],
        "CONFIDENCE_THRESHOLDS": {"HIGH": 75, "MED": 50},
    }
    result = compute_confidence(factors, {"market_regime": "bull"}, config)

    assert result["confidence_breakdown"]["coverage"] == 1.0
    assert result["confidence_breakdown"]["risk_clean"] == 1.0
    assert result["confidence_breakdown"]["regime_align"] == 0.5
    assert result["confidence_breakdown"]["liquidity"] == 0.5
    assert result["confidence_score"] == 60.0
    assert result["confidence_label"] == "MED"


def test_vnext_contract_is_research_only_fail_closed_and_does_not_reuse_spent_holdout():
    contract = research_contract()
    semantics = contract["semantics"]
    assert semantics["research_only"] is True
    assert semantics["production_confidence_changed"] is False
    assert semantics["production_score_changed"] is False
    assert semantics["risk_weights_changed"] is False
    assert semantics["missing_evidence"] == "unknown_or_insufficient_not_neutral"
    assert contract["pillars"]["statistical_confidence"]["uncertainty"]["effective_block_length"] == "2_x_forward_horizon"
    assert contract["pillars"]["statistical_confidence"]["uncertainty"]["minimum_support_regions_for_robust_interval"] == 2
    assert contract["holdout_policy"]["phase2_phase3_holdout"] == "spent_for_prior_validation_not_for_phase4_weight_or_threshold_selection"
    assert contract["config"]["stable_start"] is None


def test_testability_does_not_claim_unarchived_components_are_backtestable():
    matrix = confidence_testability_matrix()
    assert matrix["legacy_component_breakdown"]["status"] == "not_directly_testable"
    assert matrix["statistical_confidence"]["status"] == "testable_only_as_of"
    assert matrix["model_agreement"]["status"] == "partially_testable"
