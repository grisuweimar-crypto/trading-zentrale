from __future__ import annotations

import pandas as pd

from scanner.reports.confidence_vnext_research import (
    _agreement_state,
    _atom_prerequisites,
    _data_quality_state,
    risk_evidence,
    selection_evidence,
    timing_evidence,
)


def _phase2_selection(alpha_ci, probability_ci, *, mean=0.02, advantage=0.05, support=3):
    return {
        "horizons": {
            "5": {
                "validation_maturity": {"status": "available", "mature_target_events": 100},
                "selection": {
                    "validation": {
                        "B5": {
                            "N": 80,
                            "days": 20,
                            "symbols": 15,
                            "top_symbol_share": 0.1,
                            "mean_peer_excess": mean,
                            "probability_advantage_vs_baseline": advantage,
                            "block_bootstrap_mean_peer_excess_95": alpha_ci,
                            "block_bootstrap_probability_advantage_95": probability_ci,
                            "bootstrap_occurrence_support_region_count": support,
                        }
                    }
                },
            }
        }
    }


def test_selection_requires_robust_alpha_and_probability_intervals():
    robust = selection_evidence(_phase2_selection([0.01, 0.03], [0.01, 0.08]), 5, "B5")
    assert robust["state"] == "robust"
    assert robust["direction"] == "positive"

    immature = selection_evidence(_phase2_selection(None, None, support=1), 5, "B5")
    assert immature["state"] == "immature"
    assert immature["direction"] is None


def test_selection_point_direction_without_robust_intervals_is_not_robust():
    evidence = selection_evidence(_phase2_selection([-0.01, 0.04], [-0.02, 0.08]), 5, "B5")
    assert evidence["state"] == "directional_only"
    assert evidence["direction"] == "positive"


def test_timing_strong_validation_maps_to_one_robust_model_claim():
    horizon = {"validation_maturity": {"status": "available", "mature_target_events": 100}}
    pattern = {
        "pattern": "a & b",
        "conditions": ["a", "b"],
        "discovery_direction": "positive",
        "validation_sufficient": True,
        "joint_direction_confirmed": True,
        "strong_validation": True,
        "validation": {
            "N": 40,
            "days": 15,
            "symbols": 12,
            "top_symbol_share": 0.1,
            "bootstrap_occurrence_support_region_count": 3,
            "mean_peer_excess": 0.01,
            "probability_advantage_vs_baseline": 0.04,
            "block_bootstrap_mean_peer_excess_95": [0.002, 0.02],
            "block_bootstrap_probability_advantage_95": [0.01, 0.07],
        },
    }
    evidence = timing_evidence(pattern, horizon)
    assert evidence["state"] == "robust"
    assert evidence["direction"] == "positive"


def test_timing_missing_robust_uncertainty_fails_closed_as_immature():
    horizon = {"validation_maturity": {"status": "available", "mature_target_events": 100}}
    pattern = {
        "pattern": "a",
        "conditions": ["a"],
        "discovery_direction": "positive",
        "validation_sufficient": True,
        "joint_direction_confirmed": True,
        "strong_validation": False,
        "validation": {
            "N": 40,
            "bootstrap_occurrence_support_region_count": 1,
            "block_bootstrap_mean_peer_excess_95": None,
            "block_bootstrap_probability_advantage_95": None,
        },
    }
    evidence = timing_evidence(pattern, horizon)
    assert evidence["state"] == "immature"
    assert evidence["direction"] is None


def test_risk_is_robust_only_for_confirmed_protection_not_alpha():
    report = {
        "horizons": {
            "5": {
                "validation_maturity": {"status": "available", "protection_mature_events": 100},
                "validation": {
                    "volatility": {
                        "protection_N": 100,
                        "protection_days": 20,
                        "protection_low_risk_cutoff": 0.2,
                        "protection_high_risk_cutoff": 0.8,
                        "protection_gap_high_minus_low_adverse_excursion": 0.03,
                        "path_drawdown_gap_high_minus_low": 0.04,
                        "protection_gap_bootstrap_95": [0.01, 0.05],
                        "path_drawdown_gap_bootstrap_95": [0.02, 0.06],
                        "low_risk_alpha_advantage": -0.5,
                    }
                },
            }
        }
    }
    evidence = risk_evidence(report, 5, "volatility")
    assert evidence["state"] == "robust"
    assert evidence["direction"] == "higher_is_riskier"
    assert evidence["return_alpha_effect_kept_separate"] is True


def test_model_agreement_detects_return_conflict_and_risk_tension():
    conflict = _agreement_state(
        {"state": "robust", "direction": "positive"},
        {"state": "robust_claim", "direction": "negative"},
        {"state": "middle"},
    )
    assert conflict["state"] == "conflict"
    assert "selection_timing_return_conflict" in conflict["conflicts"]

    tension = _agreement_state(
        {"state": "robust", "direction": "positive"},
        {"state": "unknown", "direction": None},
        {"state": "elevated"},
    )
    assert tension["state"] == "conflict"
    assert "positive_return_claim_vs_elevated_downside_risk" in tension["conflicts"]


def test_low_risk_never_counts_as_positive_return_support():
    result = _agreement_state(
        {"state": "unavailable", "direction": None},
        {"state": "unknown", "direction": None},
        {"state": "low"},
    )
    assert result["state"] == "insufficient_evidence"
    assert result["low_risk_counted_as_positive_return_support"] is False
    assert result["probability_counted_as_independent_model"] is False
    assert result["regime_counted_as_model_vote"] is False


def test_data_quality_missing_evidence_is_not_neutral():
    row = pd.Series({"score": 25.0, "run_id": "run-1"})
    provenance = {"complete": False, "present": ["run_id"], "missing": ["as_of"]}
    quality = _data_quality_state(["score", "score_pct_full"], row, provenance)
    assert quality["state"] == "proxy_insufficient"
    assert quality["full_data_quality_claimed"] is False
    assert "score_pct_full" in quality["missing_fields"]


def test_timing_atom_prerequisites_distinguish_false_from_unknown():
    assert _atom_prerequisites("rs3m_d1_up") == ("rs3m_d1",)
    assert _atom_prerequisites("trend200_d10_down") == ("trend200_d10",)
    assert _atom_prerequisites("trend_cross_up") == ("trend200", "trend_prev1")
    assert _atom_prerequisites("r_upgrade") == ("r_d1",)
