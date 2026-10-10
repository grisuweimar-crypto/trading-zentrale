"""CY-05 opt-in contracts: synthetic unit proofs only; no research release."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from scanner.research.pattern_discovery.feature_library import (
    FeatureLibrary, FeatureLibraryError, load_feature_library,
)
from scanner.research.pattern_discovery.search_engine import (
    _atom_state, _build_atoms, load_search_contract,
)

LIB = Path("configs/pattern_discovery/feature_library_cycle_v2.json")
CONTRACT = Path("configs/pattern_discovery/l3_search_contract_cycle_v2.json")


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
