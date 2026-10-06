import csv
from pathlib import Path
import shutil

import pytest

from scanner.research.pattern_discovery import (
    FeatureLibrary,
    FeatureLibraryError,
    build_run_manifest,
    load_feature_library,
)


LIBRARY_PATH = Path("configs/pattern_discovery/feature_library_v1.json")


def use(feature_id, transformation_id="raw", parameters=None):
    return {
        "feature_id": feature_id,
        "feature_version": "v1",
        "transformation_id": transformation_id,
        "transformation_version": "v1",
        "parameters": {} if parameters is None else parameters,
    }


def test_l2_contract_is_research_only_versioned_and_explicit():
    payload = load_feature_library()
    assert payload["phase"] == "L2"
    assert payload["research_only"] is True
    assert payload["productive_integration_enabled"] is False
    assert payload["execution_allowed"] is False
    assert payload["feature_library_version"] == "PDL-FEATURE-LIBRARY-v1"
    assert payload["principles"]["only_registered_features_are_usable"] is True
    assert payload["principles"]["only_registered_transformations_are_usable"] is True
    assert payload["principles"]["modern_features_may_not_be_retrofit_into_history"] is True


def test_library_has_unique_feature_versions_and_deterministic_hashes():
    library = FeatureLibrary()
    integrity = library.verify_integrity()

    assert integrity["valid"] is True
    assert integrity["feature_version_count"] == 18
    assert integrity["transformation_version_count"] == 6
    assert len(integrity["feature_library_hash"]) == 64

    features = [
        (item["feature_id"], item["feature_version"])
        for item in library.payload["features"]
    ]
    assert len(features) == len(set(features))
    assert "elliott_vnext" not in {
        item["feature_id"] for item in library.payload["features"]
    }


def test_registered_source_fields_exist_in_current_scanner_schema():
    library = FeatureLibrary()
    with Path("artifacts/research/latest_scanner.csv").open(
        "r", encoding="utf-8", newline=""
    ) as handle:
        columns = next(csv.reader(handle))
    result = library.validate_source_columns(columns)
    assert result["valid"] is True


def test_unknown_feature_or_transformation_fails_closed():
    library = FeatureLibrary()
    with pytest.raises(
        FeatureLibraryError,
        match="feature_version_not_registered:scanner.unknown::v1",
    ):
        library.validate_feature_use(use("scanner.unknown"))

    with pytest.raises(
        FeatureLibraryError,
        match="transformation_version_not_registered:magic_transform::v1",
    ):
        library.validate_feature_use(use("scanner.score", "magic_transform"))


def test_unregistered_feature_transformation_combination_fails_closed():
    library = FeatureLibrary()
    with pytest.raises(
        FeatureLibraryError,
        match="transformation_not_allowed_for_feature:segment.sector:delta_observations",
    ):
        library.validate_feature_use(
            use("segment.sector", "delta_observations", {"lag_observations": 1})
        )


def test_deltas_crossings_transitions_and_regime_context_are_predeclared():
    library = FeatureLibrary()

    delta = library.validate_feature_use(
        use("scanner.score", "delta_observations", {"lag_observations": 5})
    )
    assert delta["parameters"] == {"lag_observations": 5}

    crossing = library.validate_feature_use(
        use("scanner.trend200", "threshold_crossing", {"threshold": 0})
    )
    assert crossing["parameters"] == {"threshold": 0.0}

    transition = library.validate_feature_use(
        use("scanner.r_code", "state_transition")
    )
    assert transition["transformation_id"] == "state_transition"

    regime = library.validate_feature_use(
        use("market.regime_stock", "regime_context")
    )
    assert regime["transformation_id"] == "regime_context"


def test_posthoc_threshold_tuning_is_rejected():
    library = FeatureLibrary()
    with pytest.raises(
        FeatureLibraryError,
        match="threshold_not_registered_for_feature:scanner.cycle:63.0",
    ):
        library.validate_feature_use(
            use("scanner.cycle", "threshold_crossing", {"threshold": 63})
        )


def test_historical_nonavailability_remains_visible():
    library = FeatureLibrary()
    feature_use = use("scanner.r_code")

    absent = library.pit_availability(
        feature_use,
        {"symbol": "AAA", "as_of": "2026-04-15", "score": 50},
        data_cutoff="2026-04-15",
    )
    assert absent == {
        "feature_id": "scanner.r_code",
        "feature_version": "v1",
        "transformation_id": "raw",
        "transformation_version": "v1",
        "available": False,
        "status": "SOURCE_FIELD_NOT_RECORDED",
        "available_from": None,
    }

    missing = library.pit_availability(
        feature_use,
        {"symbol": "AAA", "as_of": "2026-04-16", "r_code": None},
        data_cutoff="2026-04-16",
    )
    assert missing["available"] is False
    assert missing["status"] == "SOURCE_VALUE_MISSING"


def test_future_observation_cannot_leak_across_cutoff():
    library = FeatureLibrary()
    result = library.pit_availability(
        use("scanner.score"),
        {"symbol": "AAA", "as_of": "2026-04-16", "score": 55.0},
        data_cutoff="2026-04-15",
    )
    assert result["available"] is False
    assert result["status"] == "OBSERVATION_AFTER_CUTOFF"


def test_delta_requires_real_prior_point_in_time_observations():
    library = FeatureLibrary()
    feature_use = use(
        "scanner.score", "delta_observations", {"lag_observations": 5}
    )
    current = {"symbol": "AAA", "as_of": "2026-04-20", "score": 55.0}
    short_history = [
        {"symbol": "AAA", "as_of": "2026-04-19", "score": 54.0},
        {"symbol": "AAA", "as_of": "2026-04-18", "score": 53.0},
    ]
    unavailable = library.pit_availability(
        feature_use,
        current,
        data_cutoff="2026-04-20",
        history=short_history,
    )
    assert unavailable["available"] is False
    assert unavailable["status"] == "INSUFFICIENT_PRIOR_OBSERVATIONS"

    history = [
        {"symbol": "AAA", "as_of": "2026-04-19", "score": 54.0},
        {"symbol": "AAA", "as_of": "2026-04-18", "score": 53.0},
        {"symbol": "AAA", "as_of": "2026-04-17", "score": 52.0},
        {"symbol": "AAA", "as_of": "2026-04-16", "score": 51.0},
        {"symbol": "AAA", "as_of": "2026-04-15", "score": 50.0},
        {"symbol": "BBB", "as_of": "2026-04-14", "score": 99.0},
    ]
    available = library.pit_availability(
        feature_use,
        current,
        data_cutoff="2026-04-20",
        history=history,
    )
    assert available["available"] is True
    assert available["status"] == "AVAILABLE"
    assert available["available_from"].startswith("2026-04-20T00:00:00")


def full_preregistration(feature_library_version):
    return {
        "declared_start_at": "2026-10-06T15:00:00+00:00",
        "data_cutoff": "2026-10-06T14:59:00+00:00",
        "data_sources": [
            "artifacts/research/history_metadata.json",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-test-v1",
        "feature_library_version": feature_library_version,
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
        "pattern_types": ["DIRECTIONAL", "RELATIVE_ALPHA"],
        "targets": ["peer_excess_5t_gt_0"],
        "horizons_sessions": [5],
        "baselines": {
            "peer_excess_5t_gt_0": "same_horizon_same_regime_peer_baseline"
        },
        "minimum_criteria": {
            "minimum_raw_n": 40,
            "minimum_temporal_support_regions": 3,
            "minimum_effect_size": 0.02,
            "minimum_baseline_lift": 0.05,
        },
        "search_budget": {
            "max_tested_candidates_total": 5000,
            "max_tested_candidates_per_family": 1000,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": 40,
            "max_frozen_candidates_per_horizon": 15,
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
        "exclusion_rules": ["missing_required_feature", "missing_forward_target"],
        "dependency_rules": [
            "overlapping_events_not_independent",
            "duplicate_specs_rejected",
        ],
        "code_version": "commit-test-l2",
        "determinism": {"randomness_allowed": False, "seed": None},
    }


def test_l1_run_binding_requires_exact_feature_library_version_and_bytes(tmp_path):
    library = FeatureLibrary()

    metadata = tmp_path / "artifacts/research/history_metadata.json"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text('{"as_of":"2026-10-06"}\n', encoding="utf-8")

    copied_library = tmp_path / "configs/pattern_discovery/feature_library_v1.json"
    copied_library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIBRARY_PATH, copied_library)

    manifest = build_run_manifest(
        full_preregistration(library.version),
        repo_root=tmp_path,
    )
    result = library.validate_run_binding(manifest, repo_root=tmp_path)
    assert result["valid"] is True
    assert result["feature_library_version"] == library.version

    wrong = build_run_manifest(
        full_preregistration("PDL-FEATURE-LIBRARY-v999"),
        repo_root=tmp_path,
    )
    with pytest.raises(
        FeatureLibraryError,
        match="run_feature_library_version_mismatch",
    ):
        library.validate_run_binding(wrong, repo_root=tmp_path)


def test_run_binding_fails_if_feature_library_was_not_fingerprinted(tmp_path):
    library = FeatureLibrary()
    metadata = tmp_path / "artifacts/research/history_metadata.json"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text('{"as_of":"2026-10-06"}\n', encoding="utf-8")

    raw = full_preregistration(library.version)
    raw["data_sources"] = ["artifacts/research/history_metadata.json"]
    manifest = build_run_manifest(raw, repo_root=tmp_path)

    with pytest.raises(
        FeatureLibraryError,
        match="run_must_fingerprint_feature_library_source",
    ):
        library.validate_run_binding(manifest, repo_root=tmp_path)


def test_l2_does_not_claim_search_or_productive_execution():
    payload = load_feature_library()
    boundaries = payload["boundaries"]
    assert boundaries["feature_values_reconstructed_here"] is False
    assert boundaries["pattern_conditions_generated_here"] is False
    assert boundaries["pattern_search_executed_here"] is False
    assert boundaries["outcomes_inspected_here"] is False
    assert boundaries["productive_scanner_or_decision_semantics_changed"] is False
