from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest

from scanner.research.external_evidence.model_8g import (
    ExternalEvidence8GCError,
    build_paired_feature_frames,
    resolved_baseline_columns,
    validate_model_contract,
)
from scanner.research.external_evidence.research_8g import empty_split_manifest
from scanner.research.external_evidence.research_8g_binding_guard import (
    bind_snapshot_to_manifest_guarded,
)


ROOT = Path(__file__).resolve().parents[1]
SPECS_PATH = ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json"
PLAN_PATH = ROOT / "configs" / "external_evidence_8g_split_plan_v1.json"
MODEL_PATH = ROOT / "configs" / "external_evidence_8g_model_construction_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _snapshot() -> dict:
    return {
        "schema_version": "research_views_v1",
        "snapshot_id": "snap-8gc-001",
        "as_of": "2026-09-29",
        "generated_at": "2026-09-29T18:00:00+00:00",
        "views_generated_at": "2026-09-29T18:00:00+00:00",
        "latest_run_complete": True,
    }


def _ledger() -> dict:
    valid_from = "2026-09-28T12:00:00+00:00"
    return {
        "schema_version": "external_evidence_8f_macro_ledger_v1",
        "observations": [
            {
                "factor_id": "rates_policy",
                "source_id": "FED_H15",
                "series_id": "RIFSPFF_N.D",
                "observation_date": "2026-09-27",
                "revision_id": "rates-20260927-r1",
                "value": 3.90,
                "status": "KNOWN",
                "valid_from": valid_from,
                "source_record_sha256": "1" * 64,
            },
            {
                "factor_id": "rates_policy",
                "source_id": "FED_H15",
                "series_id": "RIFSPFF_N.D",
                "observation_date": "2026-09-28",
                "revision_id": "rates-20260928-r1",
                "value": 3.88,
                "status": "KNOWN",
                "valid_from": valid_from,
                "source_record_sha256": "2" * 64,
            },
        ],
    }


def _mapping(
    mapping_id: str,
    symbol: str,
    relationship_class: str,
    *,
    factor_id: str = "rates_policy",
) -> dict:
    return {
        "mapping_id": mapping_id,
        "subject_id": symbol,
        "factor_id": factor_id,
        "relationship_class": relationship_class,
        "human_reviewed": True,
        "review_status": "ACTIVE",
        "valid_from": "2026-09-28T10:00:00+00:00",
        "valid_to": None,
    }


def _exposure_map() -> dict:
    return {
        "schema_version": "external_evidence_8f_exposure_map_v1",
        "mappings": [
            _mapping("MAP:AAA:rates_policy:1", "AAA", "FINANCING_SENSITIVITY"),
            _mapping("MAP:BBB:rates_policy:1", "BBB", "FINANCING_SENSITIVITY"),
            _mapping("MAP:DDD:rates_policy:1", "DDD", "FINANCING_SENSITIVITY"),
            _mapping("MAP:DDD:rates_policy:2", "DDD", "OTHER_DOCUMENTED"),
        ],
    }


def _prepared_frame(specs: dict, *, horizon: int = 5) -> pd.DataFrame:
    numeric, categorical = resolved_baseline_columns(specs, horizon)
    rows = []
    for index, symbol in enumerate(("AAA", "BBB", "CCC", "DDD"), start=1):
        row = {
            "snapshot_id": "snap-8gc-001",
            "as_of": "2026-09-29",
            "generated_at": "2026-09-29T18:00:00+00:00",
            "symbol": symbol,
        }
        for position, column in enumerate(numeric, start=1):
            row[column] = float(index * 10 + position) / 10.0
        categorical_values = {
            "quality_band": "R3_BACKBONE",
            "r_code": "R3",
            "score_status": "valid",
            "trend_ok": True,
            "liquidity_ok": True,
            "cycle": 50.0,
            "currency": "USD",
        }
        for column in categorical:
            row[column] = categorical_values[column]
        rows.append(row)
    return pd.DataFrame(rows)


def _bound_manifest(specs: dict, plan: dict, exposure_map: dict) -> dict:
    manifest = empty_split_manifest(specs, plan)
    bound, audit = bind_snapshot_to_manifest_guarded(
        manifest=manifest,
        snapshot_metadata=_snapshot(),
        macro_ledger=_ledger(),
        exposure_map=exposure_map,
        specs=specs,
        plan=plan,
    )
    assert any(item["stream"] == "rates_policy_x_5t" for item in audit["bound"])
    return bound


def _inputs() -> tuple[dict, dict, dict, dict, pd.DataFrame, dict]:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    model = _load(MODEL_PATH)
    exposure = _exposure_map()
    frame = _prepared_frame(specs)
    manifest = _bound_manifest(specs, plan, exposure)
    return specs, plan, model, exposure, frame, manifest


def test_8g_c_contract_freezes_construction_without_fit() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    model = _load(MODEL_PATH)
    validate_model_contract(model, specs, plan)
    assert model["phase"] == "8G-C"
    assert model["active_factor_ids"] == ["rates_policy", "yield_curve", "fx"]
    assert model["preprocessing_state"]["status"] == "NOT_FIT_IN_8G_C"
    assert model["estimator_state"]["status"] == "NOT_FIT_IN_8G_C"
    assert model["estimator_state"]["fit_allowed_in_8g_c"] is False
    assert all(value is False for value in model["guards"].values())


def test_builds_identical_baseline_rows_plus_exactly_one_factor_family() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    baseline, challenger, metadata = build_paired_feature_frames(
        prepared_baseline_frame=frame,
        snapshot_metadata=_snapshot(),
        factor_id="rates_policy",
        horizon_sessions=5,
        macro_ledger=_ledger(),
        exposure_map=exposure,
        specs=specs,
        plan=plan,
        manifest=manifest,
        model_contract=model,
    )

    assert list(baseline["symbol"]) == ["AAA", "BBB"]
    assert list(challenger["symbol"]) == ["AAA", "BBB"]
    assert not any(column.startswith("external") for column in baseline.columns)
    assert {
        "external__rates_policy_level_pct",
        "external__rates_policy_delta_pp",
        "external_relationship_class",
    }.issubset(challenger.columns)
    assert not any("yield_curve" in column or column.startswith("external__fx_") for column in challenger.columns)
    assert challenger["external__rates_policy_level_pct"].tolist() == pytest.approx([3.88, 3.88])
    assert challenger["external__rates_policy_delta_pp"].tolist() == pytest.approx([-0.02, -0.02])
    assert set(challenger["external_relationship_class"]) == {"FINANCING_SENSITIVITY"}

    numeric, categorical = resolved_baseline_columns(specs, 5)
    for column in numeric + categorical:
        assert baseline[column].equals(challenger[column])

    assert metadata["paired_rows"] == 2
    assert metadata["excluded_unmapped_rows"] == 1
    assert metadata["excluded_ambiguous_mapping_rows"] == 1
    assert metadata["split"] == "DISCOVERY"
    assert metadata["slot_ordinal"] == 1
    assert metadata["outcomes_read"] is False
    assert metadata["preprocessing_fit"] is False
    assert metadata["estimator_fit"] is False


def test_outcome_or_forward_label_columns_fail_closed() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    contaminated = frame.copy()
    contaminated["peer_excess_5t"] = 0.25
    with pytest.raises(ExternalEvidence8GCError, match="outcome_or_forward_label_columns_forbidden"):
        build_paired_feature_frames(
            prepared_baseline_frame=contaminated,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )


def test_exact_snapshot_identity_is_required_not_date_only_join() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    wrong = frame.copy()
    wrong.loc[0, "snapshot_id"] = "another-snapshot"
    with pytest.raises(ExternalEvidence8GCError, match="prepared_frame_snapshot_id_mismatch"):
        build_paired_feature_frames(
            prepared_baseline_frame=wrong,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )


def test_8g_b_factor_state_hash_is_reverified_before_construction() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    tampered = deepcopy(manifest)
    tampered["streams"]["rates_policy_x_5t"]["assignments"][0]["factor_state_sha256"] = "0" * 64
    with pytest.raises(ExternalEvidence8GCError, match="8g_b_factor_state_hash_mismatch"):
        build_paired_feature_frames(
            prepared_baseline_frame=frame,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=tampered,
            model_contract=model,
        )


def test_8g_b_mapping_hash_is_reverified_before_construction() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    changed_exposure = deepcopy(exposure)
    changed_exposure["mappings"].append(
        _mapping("MAP:EEE:rates_policy:1", "EEE", "FINANCING_SENSITIVITY")
    )
    with pytest.raises(ExternalEvidence8GCError, match="8g_b_mapping_hash_mismatch"):
        build_paired_feature_frames(
            prepared_baseline_frame=frame,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=changed_exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )


def test_wrong_source_for_frozen_series_fails_closed() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    ledger = _ledger()
    ledger["observations"][0]["source_id"] = "NOT_FED_H15"
    with pytest.raises(ExternalEvidence8GCError, match="frozen_source_mismatch"):
        build_paired_feature_frames(
            prepared_baseline_frame=frame,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=ledger,
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )


def test_entirely_missing_required_baseline_numeric_feature_fails_closed() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    frame = frame.copy()
    frame["score"] = pd.NA
    with pytest.raises(ExternalEvidence8GCError, match="entirely_missing_required_numeric_feature:score"):
        build_paired_feature_frames(
            prepared_baseline_frame=frame,
            snapshot_metadata=_snapshot(),
            factor_id="rates_policy",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )


def test_factor_not_frozen_active_confirmatory_is_rejected() -> None:
    specs, plan, model, exposure, frame, manifest = _inputs()
    with pytest.raises(ExternalEvidence8GCError, match="factor_not_active_confirmatory:oil"):
        build_paired_feature_frames(
            prepared_baseline_frame=frame,
            snapshot_metadata=_snapshot(),
            factor_id="oil",
            horizon_sessions=5,
            macro_ledger=_ledger(),
            exposure_map=exposure,
            specs=specs,
            plan=plan,
            manifest=manifest,
            model_contract=model,
        )
