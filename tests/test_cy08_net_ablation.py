"""CY-08 cost comparison: controlled synthetic *math* tests, no real PAT claims."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import pytest

import scanner.research.pattern_discovery.cycle_cy08_net_ablation as net


def samples(n=95, *, baseline="negative", shadow="conflicted", value=.02):
    traces, outcomes = [], []
    for i in range(n):
        day = (date(2026, 1, 5) + timedelta(days=i)).isoformat()
        pattern = "positive" if shadow == "conflicted" else shadow
        trace = {
            "trace_hash": f"{i+1:064x}",
            "symbol": f"S{i:03d}", "snapshot_id": f"SN-{i}",
            "observation_as_of": day + "T12:00:00Z",
            "horizon_sessions": 20,
            "target_scope": {"target_id": "peer_excess_20t_gt_0", "baseline": "LEAVE_ONE_OUT_PEER_MEDIAN", "horizon_sessions": 20},
            "active_pattern_claims": [{"claim_id": f"C-{i}", "claim_hash": "a"*64}],
            "timing_ablation": {
                "baseline_timing": {"state": baseline, "direction": baseline if baseline in ("positive", "negative") else None},
                "pattern_challenger": {"state": pattern, "direction": pattern if pattern in ("positive", "negative") else None},
                "shadow_fused_timing": {"state": shadow, "direction": shadow if shadow in ("positive", "negative") else None, "action_change_allowed": False},
            },
        }
        outcome = {
            "outcome_hash": f"{i+10000:064x}",
            "claim": {"claim_id": f"C-{i}", "claim_hash": "a"*64, "symbol": f"S{i:03d}", "capture_snapshot_id": f"SN-{i}"},
            "target": {"pattern_type": "RELATIVE_ALPHA", "target_id": "peer_excess_20t_gt_0", "baseline": "LEAVE_ONE_OUT_PEER_MEDIAN", "horizon_sessions": 20},
            "price_provenance": {"currency": "USD", "currency_conversion_performed": False, "price_kind": "ADJUSTED_CLOSE"},
            "horizon_provenance": {"start_session_date": day, "target_session_date": day},
            "outcome": {"target_value": value, "target_value_kind": "PEER_EXCESS", "peer_excess": value, "return": value},
        }
        traces.append(trace)
        outcomes.append(outcome)
    return traces, outcomes


@pytest.fixture(autouse=True)
def verified_evidence(monkeypatch):
    # Validate pure paired math with synthetic fixtures. Original L13/L8
    # hashing/PIT validators are separately exercised in upstream suites.
    monkeypatch.setattr(net, "verify_challenger_trace", lambda x: {"valid": True})
    monkeypatch.setattr(net, "verify_matured_outcome", lambda x: {"valid": True, "claim_id": x["claim"]["claim_id"]})


def calculate(traces, outcomes, **opts):
    return net.evaluate_cy08_net_shadow_ablation(
        traces, outcomes, horizon_sessions=20,
        evaluated_at="2026-12-31T12:00:00Z", **opts,
    )


def test_frozen_costs_and_no_live_authority():
    c = net.load_cy08_net_contract()
    assert c["costs_bps_roundtrip"] == [10, 20, 50]
    assert c["research_only"] is True
    assert c["execution_allowed"] is False
    tampered = deepcopy(c)
    tampered["primary_cost_bps_roundtrip"] = 0
    with pytest.raises(net.CycleCY08NetAblationError, match="unfrozen"):
        calculate([], [], contract=tampered)


def test_conflict_means_abstain_and_net_cost_savings_on_same_event():
    traces, outcomes = samples()
    r = calculate(traces, outcomes)
    assert r["status"] == "NET_INCREMENTAL_VALUE_CANDIDATE"
    assert r["sample"]["paired_n"] == 95
    assert r["sample"]["side_disagreement_n"] == 95
    assert r["net_cost_scenarios"]["20"]["mean_baseline_net"] == pytest.approx(-.022)
    assert r["net_cost_scenarios"]["20"]["mean_shadow_net"] == 0
    assert r["net_cost_scenarios"]["20"]["mean_net_difference"] == pytest.approx(.022)
    assert r["net_cost_scenarios"]["50"]["paired_block_bootstrap_95"][0] > 0
    assert r["promotion_performed"] is False
    assert r["regular_integration_approved"] is False
    assert net.verify_cy08_net_shadow_ablation(r)


def test_stress_cost_kills_weak_gross_edge():
    traces, outcomes = samples(baseline="insufficient_evidence", shadow="positive", value=.003)
    r = calculate(traces, outcomes)
    assert r["net_cost_scenarios"]["20"]["mean_net_difference"] == pytest.approx(.001)
    assert r["net_cost_scenarios"]["50"]["mean_net_difference"] == pytest.approx(-.002)
    assert r["status"] == "NO_NET_INCREMENTAL_VALUE"


def test_unmatured_and_insufficient_are_not_falsified():
    traces, outcomes = samples(n=3)
    assert calculate(traces, outcomes)["status"] == "INSUFFICIENT_SUPPORT"
    partial = calculate(traces, outcomes[:2])
    assert partial["sample"]["paired_n"] == 2
    assert partial["sample"]["immature_trace_count"] == 1
    assert partial["status"] == "INSUFFICIENT_SUPPORT"
    empty = calculate([], [])
    assert empty["status"] == "INSUFFICIENT_SUPPORT"
    assert empty["net_cost_scenarios"]["20"]["mean_net_difference"] is None


def test_duplicates_and_target_mismatch_fail_closed():
    traces, outcomes = samples(n=2)
    with pytest.raises(net.CycleCY08NetAblationError, match="duplicate_symbol_snapshot"):
        calculate(traces + [traces[0]], outcomes)
    bad = deepcopy(outcomes)
    bad[1]["target"]["baseline"] = "DIFFERENT"
    with pytest.raises(net.CycleCY08NetAblationError, match="scope_mismatch"):
        calculate(traces, bad)


def test_price_lineage_and_no_shadow_authority():
    traces, outcomes = samples(n=1)
    bad = deepcopy(outcomes)
    bad[0]["price_provenance"]["currency_conversion_performed"] = True
    with pytest.raises(net.CycleCY08NetAblationError, match="currency_or_price_invalid"):
        calculate(traces, bad)
    t = deepcopy(traces)
    t[0]["timing_ablation"]["shadow_fused_timing"]["action_change_allowed"] = True
    with pytest.raises(net.CycleCY08NetAblationError, match="action_authority_forbidden"):
        calculate(t, outcomes)


def test_early_maturation_and_fake_fusion_fail_closed():
    traces, outcomes = samples(n=1)
    late = deepcopy(outcomes)
    late[0]["horizon_provenance"]["target_session_date"] = "2027-01-01"
    with pytest.raises(net.CycleCY08NetAblationError, match="not_mature"):
        calculate(traces, late)
    fake = deepcopy(traces)
    fake[0]["timing_ablation"]["shadow_fused_timing"] = {"state": "positive", "direction": "positive", "action_change_allowed": False}
    with pytest.raises(net.CycleCY08NetAblationError, match="fused_shadow_state_mismatch"):
        calculate(fake, outcomes)


def test_output_is_hash_bound_and_tamper_detected():
    traces, outcomes = samples(n=1)
    result = calculate(traces, outcomes)
    fake = deepcopy(result)
    fake["net_cost_scenarios"]["20"]["mean_net_difference"] = 999
    with pytest.raises(net.CycleCY08NetAblationError, match="hash_mismatch"):
        net.verify_cy08_net_shadow_ablation(fake)
