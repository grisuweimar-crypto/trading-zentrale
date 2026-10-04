from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

import scanner.research.elliott_vnext.stage4_historical as stage4
from scanner.research.elliott_vnext.validation import ValidationConfig


def _prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-15",
                "symbol": "AAA",
                "close": 100.0,
                "adj_close": 100.0,
                "high": 101.0,
                "low": 99.0,
            },
            {
                "date": "2026-04-16",
                "symbol": "AAA",
                "close": 101.0,
                "adj_close": 101.0,
                "high": 102.0,
                "low": 100.0,
            },
        ]
    )


def _snapshot() -> dict:
    return {
        "symbol": "AAA",
        "as_of": "2026-04-16",
        "timeframe": "daily",
        "degree": "minor",
        "research_only": True,
        "validation_replay": {
            "mode": "prefix_only",
            "as_of": "2026-04-16",
            "partition": "legacy_development_descriptive_only",
            "price_basis": "adjusted",
            "future_rows_used": False,
            "performance_used_for_state": False,
        },
    }


def _report() -> dict:
    return {
        "schema_version": "elliott_vnext_validation_v1",
        "research_only": True,
        "automatic_promotion_allowed": False,
        "trade_decision": None,
        "order_instruction": None,
        "promotion_status": "awaiting_unspent_prospective_evidence",
        "coverage": {
            "routed_snapshots": 1,
            "structure_claims": 2,
            "projection_claims": 3,
            "route_review_claims": 4,
            "projection_outcome_rows": 15,
            "route_outcome_rows": 20,
            "cross_system_rows": 0,
            "context_alpha_rows": 0,
        },
        "structure_validation": [
            {
                "partition": "legacy_development_descriptive_only",
                "wave_stage": "wave_2_complete",
                "degree": "minor",
                "scenario_role": "primary",
                "structure_resolution": "progressed",
                "structural_fit": 1.0,
            },
            {
                "partition": "legacy_development_descriptive_only",
                "wave_stage": "wave_3_complete",
                "degree": "minor",
                "scenario_role": "alternative_1",
                "structure_resolution": "invalidated",
                "structural_fit": 0.0,
            },
        ],
        "projection_summary": [{"horizon_sessions": 5}],
        "route_summary": [{"horizon_sessions": 5}],
        "evidence_policy": {
            "legacy_data_can_support_promotion": False,
            "formal_claims_require_available_from_after_freeze": True,
            "prospective_unspent_mature_outcomes": 0,
            "prospective_unspent_resolved_structure_claims": 0,
        },
    }


def test_stage4_wraps_real_6g_without_promotion(monkeypatch) -> None:
    replay_coverage = {
        "symbols_requested": 1,
        "symbols_with_snapshots": 1,
        "snapshots": 1,
        "details": [{"symbol": "AAA", "errors": []}],
        "failures_are_missing_evidence_not_imputed": True,
        "research_only": True,
    }
    monkeypatch.setattr(
        stage4,
        "replay_universe_states",
        lambda *args, **kwargs: ([deepcopy(_snapshot())], deepcopy(replay_coverage)),
    )
    monkeypatch.setattr(
        stage4,
        "build_validation_report",
        lambda *args, **kwargs: deepcopy(_report()),
    )

    result = stage4.build_stage4_historical_validation(
        _prices(),
        price_source_sha256="b" * 64,
        source_commit="a" * 40,
        config=ValidationConfig(bootstrap_reps=0),
    )

    assert result["stage"] == "STAGE_4_HISTORICAL_VALIDATION"
    assert result["technical_stage_status"] == "COMPLETE"
    assert result["empirical_promotion_status"] == "NOT_PROMOTED"
    assert result["replay"]["guard_review"]["valid"] is True
    assert result["structure_summary"]["resolution_counts"] == {
        "invalidated": 1,
        "progressed": 1,
    }
    assert result["structure_summary"]["resolved_structural_fit_mean"] == 0.5
    assert result["boundaries"]["legacy_data_can_support_promotion"] is False
    assert result["boundaries"]["future_rows_used_for_replay"] is False
    assert result["boundaries"]["automatic_promotion_allowed"] is False
    assert result["stage4_scope"]["cross_system_incremental_value_deferred_to_stage5"] is True
    assert len(result["stage4_result_hash"]) == 64


def test_stage4_fails_closed_on_future_replay(monkeypatch) -> None:
    bad = _snapshot()
    bad["validation_replay"]["future_rows_used"] = True
    monkeypatch.setattr(
        stage4,
        "replay_universe_states",
        lambda *args, **kwargs: (
            [bad],
            {
                "symbols_requested": 1,
                "symbols_with_snapshots": 1,
                "snapshots": 1,
                "details": [{"symbol": "AAA", "errors": []}],
                "failures_are_missing_evidence_not_imputed": True,
            },
        ),
    )

    with pytest.raises(stage4.Stage4HistoricalValidationError, match="historical_replay_guard_violation"):
        stage4.build_stage4_historical_validation(
            _prices(),
            price_source_sha256="b" * 64,
            source_commit="a" * 40,
            config=ValidationConfig(bootstrap_reps=0),
        )


def test_stage4_rejects_any_automatic_promotion(monkeypatch) -> None:
    report = _report()
    report["automatic_promotion_allowed"] = True
    monkeypatch.setattr(
        stage4,
        "replay_universe_states",
        lambda *args, **kwargs: (
            [_snapshot()],
            {
                "symbols_requested": 1,
                "symbols_with_snapshots": 1,
                "snapshots": 1,
                "details": [{"symbol": "AAA", "errors": []}],
                "failures_are_missing_evidence_not_imputed": True,
            },
        ),
    )
    monkeypatch.setattr(
        stage4,
        "build_validation_report",
        lambda *args, **kwargs: deepcopy(report),
    )

    with pytest.raises(stage4.Stage4HistoricalValidationError, match="automatic_promotion_must_remain_disabled"):
        stage4.build_stage4_historical_validation(
            _prices(),
            price_source_sha256="b" * 64,
            source_commit="a" * 40,
            config=ValidationConfig(bootstrap_reps=0),
        )


def test_stage4_requires_adjusted_ohlcv_columns() -> None:
    with pytest.raises(stage4.Stage4HistoricalValidationError, match="price_history_columns_missing"):
        stage4.build_stage4_historical_validation(
            pd.DataFrame([{"date": "2026-04-15", "symbol": "AAA", "close": 100.0}]),
            price_source_sha256="b" * 64,
            source_commit="a" * 40,
            config=ValidationConfig(bootstrap_reps=0),
        )
