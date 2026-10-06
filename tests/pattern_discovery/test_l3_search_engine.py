from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest

from scanner.research.pattern_discovery import (
    FeatureLibrary,
    build_run_manifest,
)
from scanner.research.pattern_discovery.search_engine import (
    DiscoverySearchError,
    load_search_contract,
    run_discovery_search,
    verify_search_result,
    write_search_result,
)


LIBRARY_PATH = Path("configs/pattern_discovery/feature_library_v1.json")


def preregistration(
    *,
    minimum_raw_n=3,
    minimum_effect_size=0.01,
    search_total=80,
    search_per_family=80,
    candidate_total=10,
    candidate_per_horizon=10,
    pattern_types=None,
):
    return {
        "declared_start_at": "2026-10-06T15:00:00+00:00",
        "data_cutoff": "2026-10-06T14:59:00+00:00",
        "data_sources": [
            "artifacts/research/l3_fixture.json",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-l3-test-v1",
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
        "pattern_types": pattern_types or ["DIRECTIONAL", "RELATIVE_ALPHA"],
        "targets": [
            "return_5t_gt_0",
            "peer_excess_5t_gt_0",
        ],
        "horizons_sessions": [5],
        "baselines": {
            "return_5t_gt_0": "same_horizon_unconditional_return_baseline",
            "peer_excess_5t_gt_0": "same_horizon_same_currency_peer_baseline",
        },
        "minimum_criteria": {
            "minimum_raw_n": minimum_raw_n,
            "minimum_temporal_support_regions": 2,
            "minimum_effect_size": minimum_effect_size,
            "minimum_baseline_lift": 0.01,
        },
        "search_budget": {
            "max_tested_candidates_total": search_total,
            "max_tested_candidates_per_family": search_per_family,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": candidate_total,
            "max_frozen_candidates_per_horizon": candidate_per_horizon,
        },
        "multiple_testing": {
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {"fdr_q": 0.05},
        },
        "statistical_primary_method": "cluster_aware_effect_and_probability_estimation",
        "robustness_checks": [
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
        "code_version": "l3-search-core-test",
        "determinism": {
            "randomness_allowed": False,
            "seed": None,
        },
    }


def repo_fixture(tmp_path: Path):
    fixture = tmp_path / "artifacts/research/l3_fixture.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text('{"fixture":true}\n', encoding="utf-8")

    target_library = tmp_path / "configs/pattern_discovery/feature_library_v1.json"
    target_library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIBRARY_PATH, target_library)
    return tmp_path


def observations():
    rows = []
    # Two symbols with opposite score paths. A creates repeatable positive
    # score-change atoms, B creates repeatable negative atoms.
    for i in range(12):
        day = f"2026-09-{i + 1:02d}T12:00:00+00:00"
        end = f"2026-09-{i + 6:02d}T12:00:00+00:00" if i < 12 else day
        rows.append(
            {
                "symbol": "A",
                "as_of": day,
                "score": 10.0 + i,
                "trend200": -0.2 + i * 0.05,
                "cycle": 20.0 + i * 6.0,
                "market_regime_stock": "bull",
                "outcomes": {
                    "return_5t_gt_0": {
                        "value": 0.04 + i * 0.001,
                        "end_at": end,
                    },
                    "peer_excess_5t_gt_0": {
                        "value": 0.03 + i * 0.001,
                        "end_at": end,
                    },
                },
            }
        )
        rows.append(
            {
                "symbol": "B",
                "as_of": day,
                "score": 30.0 - i,
                "trend200": 0.3 - i * 0.05,
                "cycle": 85.0 - i * 6.0,
                "market_regime_stock": "bear",
                "outcomes": {
                    "return_5t_gt_0": {
                        "value": -0.03 - i * 0.001,
                        "end_at": end,
                    },
                    "peer_excess_5t_gt_0": {
                        "value": -0.02 - i * 0.001,
                        "end_at": end,
                    },
                },
            }
        )
    return rows


def manifest(tmp_path: Path, **kwargs):
    root = repo_fixture(tmp_path)
    result = build_run_manifest(
        preregistration(**kwargs),
        repo_root=root,
    )
    return root, result


def test_l3_contract_is_discovery_only_and_defers_statistical_guard():
    contract = load_search_contract()

    assert contract["phase"] == "L3"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["supported_pattern_types"] == [
        "DIRECTIONAL",
        "RELATIVE_ALPHA",
    ]
    assert contract["candidate_generation"]["minimum_conditions"] == 1
    assert contract["candidate_generation"]["maximum_conditions"] == 3
    assert contract["discovery_gates"]["multiple_testing_applied_here"] is False
    assert contract["boundaries"]["l5_candidate_freeze_performed"] is False


def test_same_run_and_observations_produce_identical_search_result(tmp_path):
    root, run = manifest(tmp_path)
    library = FeatureLibrary()

    first = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=library,
    )
    second = run_discovery_search(
        run,
        list(reversed(observations())),
        repo_root=root,
        feature_library=library,
    )

    assert first == second
    assert verify_search_result(first)["valid"] is True
    assert first["confirmation_data_used"] is False
    assert first["l4_statistical_guard_applied"] is False
    assert first["l5_candidate_freeze_applied"] is False


def test_search_space_is_bounded_by_preregistered_budgets(tmp_path):
    root, run = manifest(
        tmp_path,
        search_total=6,
        search_per_family=4,
        candidate_total=6,
        candidate_per_horizon=6,
    )
    result = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    assert result["candidate_counts"]["tested"] <= 6
    assert sum(
        family["tested_candidates"] for family in result["families"]
    ) == result["candidate_counts"]["tested"]
    assert all(
        family["tested_candidates"] <= 4
        for family in result["families"]
    )
    assert result["search_space"]["condition_count_range"] == [1, 3]


def test_candidates_keep_complete_provenance_and_rejections(tmp_path):
    root, run = manifest(
        tmp_path,
        minimum_effect_size=0.5,
        search_total=20,
        search_per_family=10,
    )
    result = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    assert result["candidates"]
    assert result["candidate_counts"]["rejected_l3"] > 0
    candidate = result["candidates"][0]
    assert candidate["candidate_id"].startswith("CAND-")
    assert len(candidate["spec_hash"]) == 64
    assert candidate["family_id"].startswith("FAM-")
    assert candidate["conditions"]
    assert all("feature_version_hash" in c for c in candidate["conditions"])
    assert candidate["l3_gate_status"] in {
        "ELIGIBLE_FOR_L4",
        "REJECTED_L3",
    }
    if candidate["l3_gate_status"] == "REJECTED_L3":
        assert candidate["rejection_reasons"]


def test_candidate_ranking_is_only_within_family_and_shortlist_is_not_freeze(tmp_path):
    root, run = manifest(tmp_path, search_total=80, search_per_family=40)
    result = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    by_family = {}
    for candidate in result["candidates"]:
        if candidate["family_rank"] is not None:
            by_family.setdefault(candidate["family_id"], []).append(
                candidate["family_rank"]
            )
    assert by_family
    for ranks in by_family.values():
        assert sorted(ranks) == list(range(1, len(ranks) + 1))

    shortlisted = [
        c
        for c in result["candidates"]
        if c["shortlist_status"] == "DISCOVERY_SHORTLIST"
    ]
    assert len(shortlisted) <= 10
    assert result["l5_candidate_freeze_applied"] is False
    assert all(
        c["l3_gate_status"] == "ELIGIBLE_FOR_L4"
        for c in shortlisted
    )


def test_future_outcome_after_cutoff_fails_closed(tmp_path):
    root, run = manifest(tmp_path)
    rows = observations()
    rows[0]["outcomes"]["return_5t_gt_0"]["end_at"] = (
        "2026-10-07T12:00:00+00:00"
    )

    with pytest.raises(
        DiscoverySearchError,
        match="future_outcome_after_cutoff_forbidden",
    ):
        run_discovery_search(
            run,
            rows,
            repo_root=root,
            feature_library=FeatureLibrary(),
        )


def test_post_cutoff_observation_fails_closed(tmp_path):
    root, run = manifest(tmp_path)
    rows = observations()
    rows.append(
        {
            "symbol": "C",
            "as_of": "2026-10-07T12:00:00+00:00",
            "score": 99.0,
            "outcomes": {},
        }
    )

    with pytest.raises(
        DiscoverySearchError,
        match="observation_after_discovery_cutoff",
    ):
        run_discovery_search(
            run,
            rows,
            repo_root=root,
            feature_library=FeatureLibrary(),
        )


def test_deferred_pattern_types_fail_closed_in_l3_v1(tmp_path):
    root, run = manifest(
        tmp_path,
        pattern_types=["DIRECTIONAL", "RELATIVE_ALPHA", "RISK"],
    )

    with pytest.raises(
        DiscoverySearchError,
        match="l3_pattern_types_not_yet_supported:RISK",
    ):
        run_discovery_search(
            run,
            observations(),
            repo_root=root,
            feature_library=FeatureLibrary(),
        )


def test_tampered_search_result_is_rejected(tmp_path):
    root, run = manifest(tmp_path)
    result = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )
    tampered = deepcopy(result)
    tampered["candidate_counts"]["tested"] += 1

    with pytest.raises(
        DiscoverySearchError,
        match="search_result_hash_mismatch",
    ):
        verify_search_result(tampered)


def test_search_result_is_write_once_inside_l0_namespace(tmp_path):
    root, run = manifest(tmp_path)
    result = run_discovery_search(
        run,
        observations(),
        repo_root=root,
        feature_library=FeatureLibrary(),
    )

    target = write_search_result(root, result)
    assert target.exists()
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert verify_search_result(saved)["result_hash"] == result["result_hash"]

    with pytest.raises(
        DiscoverySearchError,
        match="search_result_already_exists",
    ):
        write_search_result(root, result)
