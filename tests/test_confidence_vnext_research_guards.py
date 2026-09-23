from __future__ import annotations

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_research import _agreement_state, _assert_pit_sources
from scanner.reports.confidence_vnext_research_guarded import (
    VOLATILITY_SCALE_BREAK_DATE,
    _assert_fail_closed_pit_sources,
    _assert_scale_audit,
    _crypto_symbols_from_latest,
    _guard_risk_data_quality,
    _guard_timing_data_quality,
    _legacy_pit_compatible_inputs,
    _mark_registry_application_guard,
    _risk_state_without_incompatible_volatility,
)


def test_volatility_scale_incompatibility_does_not_create_false_elevated_risk():
    original = {
        "state": "mixed",
        "features": [
            {"feature": "volatility", "value": 0.50, "level": "high", "low_cutoff": 0.02, "high_cutoff": 0.05},
            {"feature": "drawdown", "value": 0.10, "level": "low", "low_cutoff": 0.20, "high_cutoff": 0.55},
        ],
    }
    guarded = _risk_state_without_incompatible_volatility(original)
    assert guarded["state"] == "low"
    assert [row["feature"] for row in guarded["features"]] == ["drawdown"]
    assert guarded["excluded_features"][0]["feature"] == "volatility"
    assert guarded["excluded_features"][0]["status"] == "scale_incompatible"
    assert guarded["excluded_features"][0]["conversion_applied"] is False

    agreement = _agreement_state(
        {"state": "robust", "direction": "positive"},
        {"state": "robust_claim", "direction": "positive"},
        guarded,
    )
    assert agreement["state"] == "compatible"
    assert "positive_return_claim_vs_elevated_downside_risk" not in agreement["conflicts"]


def test_volatility_only_risk_fails_closed_as_unknown():
    guarded = _risk_state_without_incompatible_volatility(
        {
            "state": "elevated",
            "features": [
                {"feature": "volatility", "value": 0.50, "level": "high", "low_cutoff": 0.02, "high_cutoff": 0.05}
            ],
        }
    )
    assert guarded["state"] == "unknown"
    assert guarded["features"] == []


def test_risk_data_quality_excludes_incompatible_volatility_instead_of_neutralising_it():
    data_quality = {
        "risk": {
            "state": "proxy_complete",
            "required_fields": ["volatility", "drawdown"],
            "present_fields": ["volatility", "drawdown"],
            "missing_fields": [],
            "present_fraction": 1.0,
            "provenance": {"complete": True},
            "full_data_quality_claimed": False,
        }
    }
    guarded = _guard_risk_data_quality(data_quality, {"state": "low"})
    risk = guarded["risk"]
    assert risk["required_fields"] == ["drawdown"]
    assert risk["present_fields"] == ["drawdown"]
    assert risk["state"] == "proxy_complete"
    assert risk["excluded_incompatible_fields"][0]["field"] == "volatility"


def test_crypto_symbols_are_explicitly_identified_for_stock_only_evidence_scope():
    latest = pd.DataFrame(
        [
            {"date": "2026-09-23", "symbol": "AAPL", "name": "Apple", "score": 50.0, "sector": "Technology"},
            {"date": "2026-09-23", "symbol": "BTC-USD", "name": "Bitcoin", "score": 60.0, "sector": "Cryptocurrency"},
        ]
    )
    assert _crypto_symbols_from_latest(latest) == ["BTC-USD"]


def test_registry_keeps_historical_volatility_evidence_but_blocks_current_application():
    registry = {
        "horizons": {
            "5": {"risk": {"volatility": {"state": "robust"}, "drawdown": {"state": "robust"}}},
            "20": {"risk": {"volatility": {"state": "immature"}}},
        }
    }
    _mark_registry_application_guard(registry)
    vol = registry["horizons"]["5"]["risk"]["volatility"]
    assert vol["state"] == "robust"
    assert vol["current_application_status"] == "scale_incompatible"
    assert vol["current_application_conversion_applied"] is False
    assert "current_application_status" not in registry["horizons"]["5"]["risk"]["drawdown"]


def test_scale_audit_is_recorded_without_guessing_a_conversion():
    audit = {
        "features": {
            "volatility": {
                "pre_recent": {"median": 0.03065},
                "recent": {"median": 0.48651},
                "current": {"median": 0.49532},
                "recent_to_pre_median_ratio": 15.873,
                "current_to_pre_median_ratio": 16.160,
            }
        }
    }
    result = _assert_scale_audit(audit)
    assert result["status"] == "scale_incompatible"
    assert result["scale_break_date"] == VOLATILITY_SCALE_BREAK_DATE
    assert result["conversion_applied"] is False
    assert result["current_median"] == 0.49532


def _latest_with_as_of(value="2026-09-23T17:00:00Z"):
    return pd.DataFrame(
        [
            {
                "date": "2026-09-23",
                "as_of": value,
                "symbol": "AAPL",
                "name": "Apple",
                "score": 50.0,
                "sector": "Technology",
            }
        ]
    )


def test_pit_guard_rejects_missing_or_unparseable_current_as_of():
    missing = _latest_with_as_of().drop(columns=["as_of"])
    with pytest.raises(ValueError, match="current scanner as_of"):
        _assert_fail_closed_pit_sources(
            missing,
            {"source": {"as_of": "2026-09-23T16:00:00Z"}},
            {"source": {"as_of": "2026-09-23T16:00:00Z"}},
        )

    with pytest.raises(ValueError, match="current scanner as_of"):
        _assert_fail_closed_pit_sources(
            _latest_with_as_of("not-a-date"),
            {"source": {"as_of": "2026-09-23T16:00:00Z"}},
            {"source": {"as_of": "2026-09-23T16:00:00Z"}},
        )


def test_pit_guard_rejects_missing_unparseable_or_future_report_as_of():
    latest = _latest_with_as_of()
    with pytest.raises(ValueError, match="phase2 source.as_of"):
        _assert_fail_closed_pit_sources(latest, {"source": {}}, {"source": {"as_of": "2026-09-23T16:00:00Z"}})
    with pytest.raises(ValueError, match="phase3 source.as_of"):
        _assert_fail_closed_pit_sources(latest, {"source": {"as_of": "2026-09-23T16:00:00Z"}}, {"source": {"as_of": "bad"}})
    with pytest.raises(ValueError, match="PIT violation"):
        _assert_fail_closed_pit_sources(
            latest,
            {"source": {"as_of": "2026-09-23T18:00:00Z"}},
            {"source": {"as_of": "2026-09-23T16:00:00Z"}},
        )


def test_pit_guard_accepts_parseable_not_after_current_sources():
    result = _assert_fail_closed_pit_sources(
        _latest_with_as_of(),
        {"source": {"as_of": "2026-09-23T16:00:00Z"}},
        {"source": {"as_of": "2026-09-23T16:30:00Z"}},
    )
    assert result["phase2"]["not_after_current_scan"] is True
    assert result["phase3"]["not_after_current_scan"] is True


def test_legacy_pit_recheck_accepts_aware_current_and_naive_sources_after_normalization():
    latest, phase2, phase3 = _legacy_pit_compatible_inputs(
        _latest_with_as_of("2026-09-23T17:00:00Z"),
        {"source": {"as_of": "2026-09-23T16:00:00"}},
        {"source": {"as_of": "2026-09-23T16:30:00"}},
    )
    checks = _assert_pit_sources(latest, phase2, phase3)
    assert checks["phase2"]["not_after_current_scan"] is True
    assert checks["phase3"]["not_after_current_scan"] is True


def test_no_claim_timing_quality_requires_complete_provenance():
    data_quality = {
        "timing": {
            "state": "proxy_complete_no_claim",
            "full_data_quality_claimed": False,
            "unevaluable_robust_patterns": [],
        }
    }
    incomplete = _guard_timing_data_quality(
        data_quality,
        {"complete": False, "present": ["run_id"], "missing": ["as_of"]},
    )
    assert incomplete["timing"]["state"] == "proxy_insufficient"
    assert incomplete["timing"]["provenance"]["complete"] is False

    complete = _guard_timing_data_quality(
        data_quality,
        {"complete": True, "present": ["run_id", "as_of"], "missing": []},
    )
    assert complete["timing"]["state"] == "proxy_complete_no_claim"
    assert complete["timing"]["provenance"]["complete"] is True
