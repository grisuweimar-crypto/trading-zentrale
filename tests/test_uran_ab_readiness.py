from scripts.audit_uran_ab_readiness import audit
import pytest


def _base():
    daily = {"snapshot_id": "same", "as_of": "2026-10-13", "symbols": {
        "UEC": {"current": {"score": 8, "cluster_official": "Uranium"},
                "historical_matches": {"filter_id": "level_2", "conditions": {"a": 1},
                    "forward_20t": {"N": 9, "positive_count": 4, "median_return": 0.1}}},
        "UUUU": {"current": {"score": 12, "cluster_official": "Uranium"},
                 "historical_matches": {"filter_id": "level_1", "conditions": {"b": 1},
                    "forward_20t": {"N": 30, "positive_count": 17, "median_return": -0.1}}}
    }}
    w10 = {"snapshot_id": "same", "stages": {"phase6_elliott": {"status": "not_supplied"}}}
    lab = {"history_information": {"observation_count": 39359}, "laboratory_state": {"frozen_pattern_count": 0, "claim_count": 0}}
    return daily, w10, lab


def test_readiness_is_descriptive_and_no_pseudo_ab():
    result = audit(*_base())
    assert result["A_vs_B_incremental_value"] == "NOT_ESTIMABLE"
    assert result["B_outcome_test_executed"] is False
    assert result["symbols"]["UEC"]["historical_horizons"]["20"]["status"] == "INSUFFICIENT_OR_MISSING"
    assert result["symbols"]["UUUU"]["historical_horizons"]["20"]["status"] == "DESCRIPTIVE_ONLY"
    assert result["symbols"]["UEC"]["historical_filter_id"] != result["symbols"]["UUUU"]["historical_filter_id"]


def test_no_cross_snapshot():
    daily, w10, lab = _base()
    w10["snapshot_id"] = "different"
    with pytest.raises(ValueError, match="snapshot_mismatch"):
        audit(daily, w10, lab)


def test_invalid_sample_rejected():
    daily, w10, lab = _base()
    daily["symbols"]["UEC"]["historical_matches"]["forward_20t"]["positive_count"] = 25
    with pytest.raises(ValueError, match="invalid_outcomes"):
        audit(daily, w10, lab)
