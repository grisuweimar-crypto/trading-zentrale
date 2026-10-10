"""CY-05 opt-in contracts: synthetic unit proofs only; no research release."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import pytest

from scanner.research.pattern_discovery.feature_library import (
    FeatureLibrary, FeatureLibraryError, load_feature_library,
)
from scanner.research.pattern_discovery.run_contract import (
    build_run_manifest, verify_run_manifest,
)
from scanner.research.pattern_discovery.search_engine import (
    _atom_state, _build_atoms, load_search_contract,
    run_discovery_search, verify_search_result, DiscoverySearchError,
)

LIB = Path("configs/pattern_discovery/feature_library_cycle_v2.json")
CONTRACT = Path("configs/pattern_discovery/l3_search_contract_cycle_v2.json")
DESIGN = Path("configs/cycle_direction/cy05_preregistered_design_v1.json")


def feature_use(transform="level_band", params=None):
    return {
        "feature_id": "scanner.cycle",
        "feature_version": "v2",
        "transformation_id": transform,
        "transformation_version": "v1",
        "parameters": {} if params is None else params,
    }


def row(cycle=20.0, day=10, symbol="AAA", **changes):
    value = {
        "symbol": symbol, "as_of": f"2026-10-{day:02d}",
        "cycle": cycle,
        "cycle_quality": "VALID",
        "cycle_research_status": "ELIGIBLE",
        "cycle_history_source": "CY03_VERIFIED_LEDGER",
        "cycle_snapshot_id": f"synthetic-{day:02d}",
        "cycle_asset_id": symbol,
        "cycle_formula": "cycle_detrended_sma20_range40_v1",
        "cycle_currency": "USD",
        "cycle_listing_symbol": symbol,
        "cycle_price_symbol": symbol,
        "cycle_lag_1obs": "RESEARCH_ELIGIBLE",
        "cycle_lag_5obs": "RESEARCH_ELIGIBLE",
        "cycle_lag_10obs": "RESEARCH_ELIGIBLE",
    }
    value.update(changes)
    return value


def test_cycle_v2_opt_in_does_not_mutate_default_l2_l3():
    old = FeatureLibrary()
    new = FeatureLibrary(LIB)
    assert old.version == "PDL-FEATURE-LIBRARY-v1"
    assert new.version == "PDL-FEATURE-LIBRARY-CYCLE-v2"
    assert len(new.features) == len(old.features) == 18
    assert len(new.transformations) == len(old.transformations) + 1
    assert new.get_feature("scanner.cycle", "v2")["feature_version"] == "v2"
    with pytest.raises(FeatureLibraryError, match="feature_version_not_registered"):
        old.validate_feature_use(feature_use())
    validated = new.validate_feature_use(feature_use())
    assert validated["transformation_id"] == "level_band"
    assert "level_band" not in load_search_contract()["atom_generation"]["included_transformations"]
    assert "level_band" in load_search_contract(CONTRACT)["atom_generation"]["included_transformations"]


@pytest.mark.parametrize(
    "cycle,expected",
    [
        (0, "LEVEL_LT_25"),
        (24.9999, "LEVEL_LT_25"),
        (25, "LEVEL_25_LT_50"),
        (49.999, "LEVEL_25_LT_50"),
        (50, "LEVEL_50_LT_75"),
        (74.999, "LEVEL_50_LT_75"),
        (75, "LEVEL_GTE_75"),
        (100, "LEVEL_GTE_75"),
    ],
)
def test_exact_level_boundaries(cycle, expected):
    spec = FeatureLibrary(LIB).validate_feature_use(feature_use())
    state, reason = _atom_state(spec, row(cycle), symbol_history=[], position=0)
    assert (state, reason) == (expected, None)


@pytest.mark.parametrize("cycle", [None, float("nan"), float("inf"), -1, 100.001, True])
def test_invalid_cycle_level_never_emits_atom(cycle):
    spec = FeatureLibrary(LIB).validate_feature_use(feature_use())
    state, reason = _atom_state(spec, row(cycle), symbol_history=[], position=0)
    assert state is None
    assert reason is not None


@pytest.mark.parametrize(
    "overrides,refusal",
    [
        ({"cycle_research_status": "BLOCKED_EXTERNAL_VERIFICATION_269"}, "CYCLE_RESEARCH_NOT_RELEASED"),
        ({"cycle_history_source": "LEGACY_HISTORY"}, "CYCLE_VERIFIED_LEDGER_REQUIRED"),
        ({"cycle_quality": "STALE"}, "CYCLE_QUALITY_NOT_VALID"),
        ({"cycle_snapshot_id": ""}, "CYCLE_LINEAGE_MISSING"),
        ({"cycle_asset_id": "OTHER"}, "CYCLE_ASSET_ID_MISMATCH"),
        ({"cycle": 101.0}, "CYCLE_VALUE_OUT_OF_RANGE"),
    ],
)
def test_cycle_v2_research_gate_fails_closed(overrides, refusal):
    result = FeatureLibrary(LIB).pit_availability(
        feature_use(), row(**overrides), data_cutoff="2026-10-10"
    )
    assert not result["available"]
    assert result["status"] == refusal


def test_lags_1_5_10_demand_approved_archive_chain_and_lineage():
    library = FeatureLibrary(LIB)
    current = row(60, day=11)
    history = [row(10 + i * 5, day=i + 1) for i in range(10)]
    for lag in [1, 5, 10]:
        use = feature_use("change_direction", {"lag_observations": lag})
        accepted = library.pit_availability(
            use, current, history=history, data_cutoff="2026-10-11"
        )
        assert accepted["available"] is True

        denied = library.pit_availability(
            use, {**current, f"cycle_lag_{lag}obs": "PROVISIONAL_CHAIN"},
            history=history, data_cutoff="2026-10-11",
        )
        assert not denied["available"]
        assert denied["status"] == "CYCLE_LAG_NOT_RESEARCH_ELIGIBLE"

    mismatch = deepcopy(history)
    mismatch[5]["cycle_currency"] = "EUR"
    denied = library.pit_availability(
        feature_use("change_direction", {"lag_observations": 10}),
        current, history=mismatch, data_cutoff="2026-10-11",
    )
    assert denied["status"] == "CYCLE_LAG_LINEAGE_MISMATCH"
    no_prior = library.pit_availability(
        feature_use("change_direction", {"lag_observations": 10}),
        current, history=history[:2], data_cutoff="2026-10-11",
    )
    assert no_prior["status"] == "INSUFFICIENT_PRIOR_OBSERVATIONS"


def test_cycle_level_and_direction_are_combined_only_when_release_present():
    library = FeatureLibrary(LIB)
    contract = load_search_contract(CONTRACT)
    prereg = {"allowed_transformations": ["level_band", "change_direction", "threshold_crossing"]}
    observations = [row(20 + i * 6, day=i + 1) for i in range(11)]
    atoms, _ = _build_atoms(
        library, prereg, contract, observations,
        datetime(2026, 10, 12, tzinfo=timezone.utc),
    )
    states = {(a["transformation_id"], a["state"]) for a in atoms}
    assert ("level_band", "LEVEL_LT_25") in states
    assert ("level_band", "LEVEL_25_LT_50") in states
    assert ("level_band", "LEVEL_50_LT_75") in states
    assert ("level_band", "LEVEL_GTE_75") in states
    assert ("change_direction", "UP") in states
    assert ("threshold_crossing", "CROSS_UP") in states
    blocked = [dict(r, cycle_research_status="BLOCKED_EXTERNAL_VERIFICATION_269") for r in observations]
    blocked_atoms, _ = _build_atoms(
        library, prereg, contract, blocked,
        datetime(2026, 10, 12, tzinfo=timezone.utc),
    )
    assert not blocked_atoms


def test_level_transformation_is_fixed_and_observation_pit_is_enforced(tmp_path):
    payload = load_feature_library(LIB)
    tampered = deepcopy(payload)
    next(f for f in tampered["features"] if f["feature_id"] == "scanner.cycle")[
        "level_band_boundaries"
    ] = [20, 50, 80]
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(FeatureLibraryError, match="cycle_level_bands_not_preregistered"):
        FeatureLibrary(path).validate_feature_use(feature_use())

    late = FeatureLibrary(LIB).pit_availability(
        feature_use(), row(60, day=11), data_cutoff="2026-10-10"
    )
    assert late["status"] == "OBSERVATION_AFTER_CUTOFF"


def test_l1_frozen_manifest_binds_opt_in_cycle_v2_library_bytes(tmp_path):
    """Only a synthetic L1 identity proof, never a market research result."""
    copied = tmp_path / LIB
    copied.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIB, copied)
    copied_contract = tmp_path / CONTRACT
    copied_contract.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(CONTRACT, copied_contract)
    copied_design = tmp_path / DESIGN
    copied_design.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(DESIGN, copied_design)
    fixture = tmp_path / "artifacts/research/cy05_fixture.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    # Unit-only input: artificial observations and fully matured outcomes.
    synthetic_cycles = [10, 12, 14, 16, 18, 20, 22, 24, 10, 12, 14]
    observations = [
        {**row(value, day=i + 1), "outcomes": {
            "return_5t_gt_0": {"value": 0.02, "end_at": "2026-10-20T12:00:00+00:00"},
            "peer_excess_5t_gt_0": {"value": 0.01, "end_at": "2026-10-20T12:00:00+00:00"},
        }}
        for i, value in enumerate(synthetic_cycles)
    ]
    fixture.write_text(json.dumps(observations, sort_keys=True) + "\n", encoding="utf-8")
    prereg = {
        "declared_start_at": "2026-11-01T00:00:00+00:00",
        "data_cutoff": "2026-10-31T23:59:00+00:00",
        "data_sources": [str(LIB), str(CONTRACT), str(DESIGN), "artifacts/research/cy05_fixture.json"],
        "pit_rules": ["no_future_features", "missing_remains_missing", "no_retrofit_of_modern_features"],
        "universe_version": "synthetic-cy05-v1",
        "feature_library_version": "PDL-FEATURE-LIBRARY-CYCLE-v2",
        "allowed_transformations": ["level_band", "change_direction", "threshold_crossing"],
        "pattern_complexity": {"min_atomic_conditions": 1, "max_atomic_conditions": 3},
        "pattern_types": ["DIRECTIONAL", "RELATIVE_ALPHA"],
        "targets": ["return_5t_gt_0", "peer_excess_5t_gt_0"],
        "horizons_sessions": [5],
        "baselines": {
            "return_5t_gt_0": "same_horizon_unconditional_return_baseline",
            "peer_excess_5t_gt_0": "same_horizon_same_currency_peer_baseline",
        },
        "minimum_criteria": {
            "minimum_raw_n": 3,
            "minimum_temporal_support_regions": 2,
            "minimum_effect_size": 0.01,
            "minimum_baseline_lift": 0.01,
        },
        "search_budget": {
            "max_tested_candidates_total": 400,
            "max_tested_candidates_per_family": 200,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": 5,
            "max_frozen_candidates_per_horizon": 5,
        },
        "multiple_testing": {
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {"fdr_q": 0.05},
        },
        "statistical_primary_method": "cluster_aware_effect_and_probability_estimation",
        "robustness_checks": ["moving_block_bootstrap", "symbol_concentration", "temporal_support"],
        "exclusion_rules": ["missing_required_feature", "missing_forward_target"],
        "dependency_rules": ["overlapping_events_not_independent", "duplicate_specs_rejected"],
        "code_version": "cy05-synthetic-contract-only",
        "determinism": {"randomness_allowed": False, "seed": None},
    }
    first = build_run_manifest(prereg, repo_root=tmp_path)
    second = build_run_manifest(prereg, repo_root=tmp_path)
    assert first == second
    assert verify_run_manifest(first)["valid"] is True
    assert FeatureLibrary(LIB).validate_run_binding(first, repo_root=tmp_path)["valid"]
    cycle_library = FeatureLibrary(LIB)
    cy05_contract = load_search_contract(CONTRACT)
    input_rows = json.loads(fixture.read_text(encoding="utf-8"))
    first_result = run_discovery_search(
        first, input_rows, repo_root=tmp_path,
        feature_library=cycle_library, contract=cy05_contract,
    )
    with pytest.raises(DiscoverySearchError, match="cy05_l2_l3_variant_binding_mismatch"):
        run_discovery_search(
            first, input_rows, repo_root=tmp_path,
            feature_library=cycle_library,
        )
    repeat_result = run_discovery_search(
        first, list(reversed(input_rows)), repo_root=tmp_path,
        feature_library=cycle_library, contract=cy05_contract,
    )
    assert first_result == repeat_result
    assert verify_search_result(first_result)["valid"]
    assert first_result["feature_library_version"] == "PDL-FEATURE-LIBRARY-CYCLE-v2"
    combinations = [
        {(c["transformation_id"], c["state"]) for c in candidate["conditions"]}
        for candidate in first_result["candidates"]
        if len(candidate["conditions"]) == 2
    ]
    assert any(
        ("level_band", "LEVEL_LT_25") in states and ("change_direction", "UP") in states
        for states in combinations
    )
    assert first_result["confirmation_data_used"] is False
    assert first_result["l5_candidate_freeze_applied"] is False
    # Existing frozen L1 fingerprints forbid any post-registration L3
    # search-policy replacement, including contract-variant substitutions.
    altered_contract = deepcopy(cy05_contract)
    altered_contract["atom_generation"]["included_transformations"] = [
        "level_band"
    ]
    with pytest.raises(DiscoverySearchError, match="cy05_l3_contract_parameter_mismatch"):
        run_discovery_search(
            first, input_rows, repo_root=tmp_path,
            feature_library=cycle_library, contract=altered_contract,
        )
    copied_contract.write_text(copied_contract.read_text(encoding="utf-8") + " ",
                               encoding="utf-8")
    with pytest.raises(DiscoverySearchError, match="cy05_l3_contract_bytes_not_frozen"):
        run_discovery_search(
            first, input_rows, repo_root=tmp_path,
            feature_library=cycle_library, contract=cy05_contract,
        )

    # Restore the L3 policy and verify that altering just the frozen A-D
    # design document (without changing its L1 hash) stops the research run.
    shutil.copyfile(CONTRACT, copied_contract)
    copied_design.write_text(copied_design.read_text(encoding="utf-8") + " ",
                             encoding="utf-8")
    with pytest.raises(DiscoverySearchError, match="cy05_l1_frozen_inputs_changed"):
        run_discovery_search(
            first, input_rows, repo_root=tmp_path,
            feature_library=cycle_library, contract=cy05_contract,
        )
    shutil.copyfile(DESIGN, copied_design)

    copied.write_text(copied.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(FeatureLibraryError, match="run_repo_feature_library_bytes_mismatch"):
        FeatureLibrary(LIB).validate_run_binding(first, repo_root=tmp_path)


def test_cy05_methodology_is_preregistered_without_fake_l1_release():
    design = json.loads(DESIGN.read_text(encoding="utf-8"))
    assert design["schema_version"] == "cycle_direction_cy05_research_design_v1"
    assert design["preregistration_state"] == "METHODOLOGY_FROZEN_BEFORE_OUTCOME_INSPECTION"
    assert design["research_only"] is True
    assert design["productive_integration_enabled"] is False
    assert design["execution_allowed"] is False
    assert design["l2_l3_contracts"]["cycle_level_thresholds"] == [25, 50, 75]
    assert design["data_policy"]["allowed_lag_observations"] == [1, 5, 10]
    assert design["data_policy"]["forward_targets_sessions"] == [5, 20, 40, 60]
    arms = design["registered_comparison"]
    assert set(arms) >= {"A", "B", "C", "D", "primary_contrast", "primary_target"}
    assert arms["primary_contrast"] == "C_minus_B"
    assert arms["primary_target"] == "peer_excess_20t_gt_0"
    assert arms["primary_condition"] == {
        "cycle_level_band": "LEVEL_LT_25",
        "cycle_change_direction": "UP",
        "lag_observations": 5,
    }
    policy = design["execution_policy"]
    assert policy["multiple_testing_method"] == "BENJAMINI_HOCHBERG_FDR"
    assert policy["fdr_q"] == 0.05
    assert policy["turnover_cost_roundtrip_bps"] == 20
    assert policy["costs_sensitivity_bps"] == [10, 50]
    assert policy["l1_manifest_required_before_outcome_run"] is True
    assert design["evidence_status"]["frozen_l1_run_manifest_created"] is False
    assert design["evidence_status"]["empirical_results_computed"] is False
