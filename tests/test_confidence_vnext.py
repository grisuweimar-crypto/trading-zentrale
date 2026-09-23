from __future__ import annotations

import pandas as pd

from scanner.domain.scoring_engine.factors.universe_csv import Universe, scale_from_universe
from scanner.domain.scoring_engine.quality.confidence import compute_confidence
from scanner.reports.confidence_vnext import (
    Phase4Config,
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
            },
        ]
    )


def test_history_audit_deduplicates_and_marks_formula_transition():
    report = audit_history(_history(), Phase4Config())
    assert report["scanner_rows_after_same_day_symbol_dedup"] == 4
    assert report["aggregate_confidence"]["first_observed_non_null_date"] == "2026-02-11"

    epochs = report["aggregate_confidence"]["formula_epochs"]
    assert epochs["pre_alias_fix"]["rows"] == 2
    assert epochs["transition_unknown"]["rows"] == 1
    assert epochs["post_alias_fix"]["rows"] == 1
    assert epochs["transition_unknown"]["confidence_non_null_rows"] == 1


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


def test_vnext_contract_is_research_only_and_fail_closed():
    contract = research_contract()
    semantics = contract["semantics"]
    assert semantics["research_only"] is True
    assert semantics["production_confidence_changed"] is False
    assert semantics["production_score_changed"] is False
    assert semantics["risk_weights_changed"] is False
    assert semantics["missing_evidence"] == "unknown_or_insufficient_not_neutral"
    assert contract["pillars"]["statistical_confidence"]["uncertainty"]["effective_block_length"] == "2_x_forward_horizon"
    assert contract["pillars"]["statistical_confidence"]["uncertainty"]["minimum_support_regions_for_robust_interval"] == 2


def test_testability_does_not_claim_unarchived_components_are_backtestable():
    matrix = confidence_testability_matrix()
    assert matrix["legacy_component_breakdown"]["status"] == "not_directly_testable"
    assert matrix["statistical_confidence"]["status"] == "testable_only_as_of"
    assert matrix["model_agreement"]["status"] == "partially_testable"
