from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest

from scanner.research.pattern_discovery import (
    FeatureLibrary,
    build_run_manifest,
    run_discovery_search,
)
from scanner.research.pattern_discovery.statistical_guard import (
    DiscoveryGuardError,
    _adjust_family_pvalues,
    apply_statistical_guard,
    load_guard_contract,
    verify_statistical_evidence,
    write_statistical_evidence,
)


LIBRARY_PATH = Path("configs/pattern_discovery/feature_library_v1.json")


def preregistration(
    *,
    multiple_testing=None,
    robustness_checks=None,
    minimum_raw_n=20,
    minimum_temporal_support_regions=3,
    minimum_effect_size=0.01,
    minimum_baseline_lift=0.10,
    search_total=60,
    search_per_family=60,
):
    return {
        "declared_start_at": "2026-10-06T15:00:00+00:00",
        "data_cutoff": "2026-10-06T14:59:00+00:00",
        "data_sources": [
            "artifacts/research/l4_fixture.json",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-l4-test-v1",
        "feature_library_version": "PDL-FEATURE-LIBRARY-v1",
        "allowed_transformations": [
            "raw",
            "delta_observations",
            "change_direction",
            "threshold_crossing",
            "state_transition",
            "regime_context",
        ],
        "pattern_complexity": {
            "min_atomic_conditions": 1,
            "max_atomic_conditions": 3,
        },
        "pattern_types": ["DIRECTIONAL"],
        "targets": ["return_5t_gt_0"],
        "horizons_sessions": [5],
        "baselines": {
            "return_5t_gt_0": "same_horizon_unconditional_return_baseline"
        },
        "minimum_criteria": {
            "minimum_raw_n": minimum_raw_n,
            "minimum_temporal_support_regions": minimum_temporal_support_regions,
            "minimum_effect_size": minimum_effect_size,
            "minimum_baseline_lift": minimum_baseline_lift,
        },
        "search_budget": {
            "max_tested_candidates_total": search_total,
            "max_tested_candidates_per_family": search_per_family,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": min(12, search_total),
            "max_frozen_candidates_per_horizon": min(12, search_total),
        },
        "multiple_testing": multiple_testing
        or {
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {"fdr_q": 0.05},
        },
        "statistical_primary_method":
            "cluster_aware_effect_and_probability_estimation",
        "robustness_checks": robustness_checks
        or [
            "moving_block_bootstrap",
            "symbol_concentration",
            "temporal_support",
        ],
        "exclusion_rules": [
            "missing_required_feature",
            "missing_forward_target",
        ],
        "dependency_rules": [
            "overlapping_events_not_independent",
            "duplicate_specs_rejected",
        ],
        "code_version": "l4-statistical-guard-test",
        "determinism": {
            "randomness_allowed": False,
            "seed": None,
        },
    }


def repo_fixture(tmp_path: Path):
    fixture = tmp_path / "artifacts/research/l4_fixture.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text('{"fixture":true}\n', encoding="utf-8")

    target_library = tmp_path / "configs/pattern_discovery/feature_library_v1.json"
    target_library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIBRARY_PATH, target_library)
    return tmp_path


def broad_observations():
    rows = []
    symbols = [
        ("P1", True),
        ("P2", True),
        ("P3", True),
        ("P4", True),
        ("N1", False),
        ("N2", False),
        ("N3", False),
        ("N4", False),
    ]
    for i in range(35):
        day = f"2026-07-{i + 1:02d}T12:00:00+00:00" if i < 31 else f"2026-08-{i - 30:02d}T12:00:00+00:00"
        end = f"2026-09-{(i % 20) + 1:02d}T12:00:00+00:00"
        for rank, (symbol, positive) in enumerate(symbols):
            score = (
                20.0 + i + rank * 0.01
                if positive
                else 80.0 - i - rank * 0.01
            )
            value = (
                0.04 + (rank * 0.0005)
                if positive
                else -0.03 - (rank * 0.0005)
            )
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": day,
                    "score": score,
                    "market_regime_stock": "bull" if positive else "bear",
                    "sector": "positive_sector" if positive else "negative_sector",
                    "outcomes": {
                        "return_5t_gt_0": {
                            "value": value,
                            "end_at": end,
                        }
                    },
                }
            )
    return rows


def concentrated_observations():
    rows = []
    for i in range(35):
        day = f"2026-07-{i + 1:02d}T12:00:00+00:00" if i < 31 else f"2026-08-{i - 30:02d}T12:00:00+00:00"
        end = f"2026-09-{(i % 20) + 1:02d}T12:00:00+00:00"
        rows.append(
            {
                "symbol": "ONLY",
                "as_of": day,
                "score": 10.0 + i,
                "outcomes": {
                    "return_5t_gt_0": {
                        "value": 0.05,
                        "end_at": end,
                    }
                },
            }
        )
        for symbol in ("B", "C", "D", "E"):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": day,
                    "score": 50.0 - i,
                    "outcomes": {
                        "return_5t_gt_0": {
                            "value": -0.02,
                            "end_at": end,
                        }
                    },
                }
            )
    return rows


def run_bundle(tmp_path, rows=None, **prereg_kwargs):
    root = repo_fixture(tmp_path)
    manifest = build_run_manifest(
        preregistration(**prereg_kwargs),
        repo_root=root,
    )
    library = FeatureLibrary()
    observations = rows or broad_observations()
    l3 = run_discovery_search(
        manifest,
        observations,
        repo_root=root,
        feature_library=library,
    )
    l4 = apply_statistical_guard(
        manifest,
        l3,
        observations,
        repo_root=root,
        feature_library=library,
    )
    return root, manifest, l3, l4


def test_l4_contract_is_research_only_and_explicitly_not_freeze():
    contract = load_guard_contract()
    assert contract["phase"] == "L4"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["principles"]["raw_hit_rate_never_sufficient"] is True
    assert contract["principles"]["raw_n_is_not_effective_n"] is True
    assert contract["boundaries"]["l5_candidate_freeze_performed"] is False


def test_l4_is_deterministic_and_hash_protected(tmp_path):
    root, manifest, l3, first = run_bundle(tmp_path)
    second = apply_statistical_guard(
        manifest,
        l3,
        list(reversed(broad_observations())),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    assert first == second
    verified = verify_statistical_evidence(first)
    assert verified["valid"] is True
    assert first["prospective_confirmation_used"] is False
    assert first["l5_candidate_freeze_applied"] is False


def test_all_l3_tested_candidates_enter_multiple_testing_and_negative_results_remain(tmp_path):
    _, _, l3, l4 = run_bundle(tmp_path)

    assert l4["counts"]["l3_tested_candidates"] == l3["candidate_counts"]["tested"]
    assert sum(
        item["tested_candidate_count"]
        for item in l4["family_multiple_testing"]
    ) == l3["candidate_counts"]["tested"]
    assert len(l4["candidate_evidence"]) == l3["candidate_counts"]["tested"]
    assert l4["counts"]["negative_or_rejected_results_retained"] > 0


def test_raw_n_effective_n_support_and_concentration_are_separate(tmp_path):
    _, _, _, l4 = run_bundle(tmp_path)

    records = [
        record
        for record in l4["candidate_evidence"]
        if record["discovery_evidence"]["raw_n"] > 0
    ]
    assert records
    record = max(
        records,
        key=lambda item: item["discovery_evidence"]["raw_n"],
    )
    evidence = record["discovery_evidence"]
    assert evidence["raw_n"] >= evidence["effective_n_proxy"]
    assert evidence["support_region_count"] >= 1
    assert "top_symbol_share" in evidence["concentration"]
    assert "symbol_hhi" in evidence["concentration"]


def test_broad_repeated_effect_can_reach_l5_eligibility(tmp_path):
    _, _, _, l4 = run_bundle(tmp_path)

    eligible = [
        record
        for record in l4["candidate_evidence"]
        if record["l4_gate_status"] == "ELIGIBLE_FOR_L5"
    ]
    assert eligible

    best = eligible[0]["discovery_evidence"]
    assert best["probability_advantage_lift"] >= 0.10
    assert best["support_region_count"] >= 3
    assert best["robust_uncertainty"]["aligned_effect_interval_95"][0] > 0
    assert best["robust_uncertainty"]["probability_lift_interval_95"][0] > 0
    assert best["multiple_testing"]["passed"] is True


def test_single_symbol_candidate_fails_concentration_gate(tmp_path):
    _, _, _, l4 = run_bundle(
        tmp_path,
        rows=concentrated_observations(),
        minimum_raw_n=15,
        minimum_baseline_lift=0.10,
    )

    candidates = [
        record
        for record in l4["candidate_evidence"]
        if record["l3_gate_status"] == "ELIGIBLE_FOR_L4"
        and record["discovery_evidence"]["symbol_count"] == 1
    ]
    assert candidates
    assert any(
        "MINIMUM_SYMBOL_BREADTH_NOT_MET" in record["l4_rejection_reasons"]
        or "SYMBOL_CONCENTRATION_EXCEEDED" in record["l4_rejection_reasons"]
        for record in candidates
    )
    assert all(
        record["l4_gate_status"] != "ELIGIBLE_FOR_L5"
        for record in candidates
    )


def test_multiple_testing_adjustments_cover_bh_bonferroni_and_holm():
    raw = [("A", 0.001), ("B", 0.02), ("C", 0.20)]

    bh = _adjust_family_pvalues(
        raw,
        method="BENJAMINI_HOCHBERG_FDR",
        parameters={"fdr_q": 0.05},
    )
    assert bh["A"]["passed"] is True
    assert bh["C"]["passed"] is False
    assert bh["A"]["adjusted_p_value"] <= bh["B"]["adjusted_p_value"]

    bonf = _adjust_family_pvalues(
        raw,
        method="BONFERRONI_FWER",
        parameters={"family_alpha": 0.05},
    )
    assert bonf["A"]["adjusted_p_value"] == pytest.approx(0.003)
    assert bonf["B"]["passed"] is False

    holm = _adjust_family_pvalues(
        raw,
        method="HOLM_FWER",
        parameters={"family_alpha": 0.05},
    )
    assert holm["A"]["passed"] is True
    assert holm["C"]["passed"] is False


def test_unimplemented_single_primary_and_custom_methods_fail_closed(tmp_path):
    root = repo_fixture(tmp_path)
    library = FeatureLibrary()

    for mt, message in [
        (
            {
                "primary_method": "PREDECLARED_SINGLE_PRIMARY",
                "parameters": {},
            },
            "l4_single_primary_fails_closed_without_preregistered_alpha",
        ),
        (
            {
                "primary_method": "CUSTOM_PREDECLARED",
                "parameters": {
                    "method_name": "custom",
                    "rule_hash": "abc",
                    "rationale": "test",
                },
            },
            "l4_custom_multiple_testing_not_implemented",
        ),
    ]:
        run = build_run_manifest(
            preregistration(multiple_testing=mt),
            repo_root=root,
        )
        l3 = run_discovery_search(
            run,
            broad_observations(),
            repo_root=root,
            feature_library=library,
        )
        with pytest.raises(DiscoveryGuardError, match=message):
            apply_statistical_guard(
                run,
                l3,
                broad_observations(),
                repo_root=root,
                feature_library=library,
            )


def test_unimplemented_preregistered_robustness_check_fails_closed(tmp_path):
    root = repo_fixture(tmp_path)
    run = build_run_manifest(
        preregistration(
            robustness_checks=[
                "moving_block_bootstrap",
                "future_unknown_check",
            ]
        ),
        repo_root=root,
    )
    l3 = run_discovery_search(
        run,
        broad_observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    with pytest.raises(
        DiscoveryGuardError,
        match="l4_preregistered_robustness_check_not_implemented",
    ):
        apply_statistical_guard(
            run,
            l3,
            broad_observations(),
            repo_root=root,
            feature_library=FeatureLibrary(),
        )


def test_tampered_l3_result_is_rejected_before_l4(tmp_path):
    root = repo_fixture(tmp_path)
    run = build_run_manifest(preregistration(), repo_root=root)
    l3 = run_discovery_search(
        run,
        broad_observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )
    tampered = deepcopy(l3)
    tampered["candidate_counts"]["tested"] += 1

    with pytest.raises(Exception, match="search_result_hash_mismatch"):
        apply_statistical_guard(
            run,
            tampered,
            broad_observations(),
            repo_root=root,
            feature_library=FeatureLibrary(),
        )


def test_l4_evidence_is_write_once_inside_research_namespace(tmp_path):
    root, _, _, l4 = run_bundle(tmp_path)

    target = write_statistical_evidence(root, l4)
    assert target.exists()
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert verify_statistical_evidence(saved)["evidence_hash"] == l4["evidence_hash"]

    with pytest.raises(
        DiscoveryGuardError,
        match="l4_evidence_already_exists",
    ):
        write_statistical_evidence(root, l4)
