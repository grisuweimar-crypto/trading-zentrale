from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.research_8g import (
    ExternalEvidence8GError,
    bind_snapshot_to_manifest,
    empty_split_manifest,
    factor_snapshot_eligibility,
    slot_state,
    validate_challenger_specs,
    validate_split_manifest,
    validate_split_plan,
)


ROOT = Path(__file__).resolve().parents[1]
SPECS_PATH = ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json"
PLAN_PATH = ROOT / "configs" / "external_evidence_8g_split_plan_v1.json"
MANIFEST_PATH = ROOT / "artifacts" / "research" / "external_evidence_8g_split_manifest_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(
    *,
    factor: str,
    source: str,
    series: str,
    day: str,
    value: float,
    valid_from: str,
) -> dict:
    return {
        "factor_id": factor,
        "source_id": source,
        "series_id": series,
        "observation_date": day,
        "revision_id": f"{series}:{day}:r1",
        "value": value,
        "status": "KNOWN",
        "valid_from": valid_from,
        "source_record_sha256": (series + day).encode("utf-8").hex()[:64].ljust(64, "0"),
    }


def _ledger() -> dict:
    available = "2026-09-28T12:00:00+00:00"
    return {
        "schema_version": "external_evidence_8f_macro_ledger_v1",
        "observations": [
            _row(factor="rates_policy", source="FED_H15", series="RIFSPFF_N.D", day="2026-09-27", value=3.90, valid_from=available),
            _row(factor="rates_policy", source="FED_H15", series="RIFSPFF_N.D", day="2026-09-28", value=3.88, valid_from=available),
            _row(factor="yield_curve", source="FED_H15", series="RIFLGFCY02_N.B", day="2026-09-27", value=4.80, valid_from=available),
            _row(factor="yield_curve", source="FED_H15", series="RIFLGFCY10_N.B", day="2026-09-27", value=5.00, valid_from=available),
            _row(factor="yield_curve", source="FED_H15", series="RIFLGFCY02_N.B", day="2026-09-28", value=4.85, valid_from=available),
            _row(factor="yield_curve", source="FED_H15", series="RIFLGFCY10_N.B", day="2026-09-28", value=5.10, valid_from=available),
            _row(factor="fx", source="ECB_EXR", series="EXR.D.USD.EUR.SP00.A", day="2026-09-27", value=1.14, valid_from=available),
            _row(factor="fx", source="ECB_EXR", series="EXR.D.USD.EUR.SP00.A", day="2026-09-28", value=1.15, valid_from=available),
        ],
    }


def _exposure_map() -> dict:
    valid_from = "2026-09-28T10:00:00+00:00"
    return {
        "schema_version": "external_evidence_8f_exposure_map_v1",
        "mappings": [
            {
                "mapping_id": "MAP:AAA:rates_policy:1",
                "subject_id": "AAA",
                "factor_id": "rates_policy",
                "relationship_class": "FINANCING_SENSITIVITY",
                "human_reviewed": True,
                "review_status": "ACTIVE",
                "valid_from": valid_from,
                "valid_to": None,
            },
            {
                "mapping_id": "MAP:BBB:yield_curve:1",
                "subject_id": "BBB",
                "factor_id": "yield_curve",
                "relationship_class": "FINANCING_SENSITIVITY",
                "human_reviewed": True,
                "review_status": "ACTIVE",
                "valid_from": valid_from,
                "valid_to": None,
            },
            {
                "mapping_id": "MAP:CCC:fx:1",
                "subject_id": "CCC",
                "factor_id": "fx",
                "relationship_class": "CURRENCY_TRANSLATION",
                "human_reviewed": True,
                "review_status": "ACTIVE",
                "valid_from": valid_from,
                "valid_to": None,
            },
        ],
    }


def _snapshot(snapshot_id: str = "snap-001", generated_at: str = "2026-09-29T18:00:00+00:00") -> dict:
    return {
        "schema_version": "research_views_v1",
        "snapshot_id": snapshot_id,
        "generated_at": generated_at,
        "views_generated_at": generated_at,
        "latest_run_complete": True,
    }


def test_8g_b_configs_and_seed_manifest_validate() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = _load(MANIFEST_PATH)

    validate_challenger_specs(specs)
    validate_split_plan(plan, specs)
    validate_split_manifest(manifest, specs, plan)

    assert specs["active_confirmatory_factor_ids"] == ["rates_policy", "yield_curve", "fx"]
    assert specs["outcome_values_read_while_defining_specs"] is False
    assert specs["challenger_construction"]["exactly_one_external_factor_family"] is True
    assert specs["challenger_construction"]["cross_factor_interactions_allowed"] is False
    assert specs["challenger_construction"]["feature_sign_flip_allowed"] is False
    assert specs["estimator"]["alpha"] == 1.0
    assert specs["estimator"]["hyperparameter_tuning_allowed"] is False
    assert manifest["outcomes_read"] is False
    assert all(not stream["assignments"] for stream in manifest["streams"].values())


def test_slot_boundaries_implement_50_25_25_with_horizon_purges() -> None:
    assert slot_state(5, 1) == {"split": "DISCOVERY", "usable": True, "purge_reason": None}
    assert slot_state(5, 35)["usable"] is True
    assert slot_state(5, 36) == {
        "split": "DISCOVERY",
        "usable": False,
        "purge_reason": "FORWARD_WINDOW_BOUNDARY_PURGE",
    }
    assert slot_state(5, 40)["usable"] is False
    assert slot_state(5, 41) == {"split": "VALIDATION", "usable": True, "purge_reason": None}
    assert slot_state(5, 55)["usable"] is True
    assert slot_state(5, 56)["usable"] is False
    assert slot_state(5, 60)["usable"] is False
    assert slot_state(5, 61) == {"split": "HOLDOUT", "usable": True, "purge_reason": None}
    assert slot_state(5, 80)["split"] == "HOLDOUT"
    with pytest.raises(ExternalEvidence8GError):
        slot_state(5, 81)


def test_first_eligible_snapshot_binds_all_active_factor_horizon_streams_once() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = empty_split_manifest(specs, plan)

    updated, audit = bind_snapshot_to_manifest(
        manifest=manifest,
        snapshot_metadata=_snapshot(),
        macro_ledger=_ledger(),
        exposure_map=_exposure_map(),
        specs=specs,
        plan=plan,
    )

    assert len(audit["bound"]) == 12
    assert not audit["skipped"]
    assert audit["outcomes_read"] is False
    for stream in updated["streams"].values():
        assert len(stream["assignments"]) == 1
        assignment = stream["assignments"][0]
        assert assignment["ordinal"] == 1
        assert assignment["split"] == "DISCOVERY"
        assert assignment["usable"] is True
        assert len(assignment["factor_state_sha256"]) == 64
        assert assignment["active_mapping_count"] == 1

    second, second_audit = bind_snapshot_to_manifest(
        manifest=updated,
        snapshot_metadata=_snapshot(),
        macro_ledger=_ledger(),
        exposure_map=_exposure_map(),
        specs=specs,
        plan=plan,
    )
    assert not second_audit["bound"]
    assert len(second_audit["skipped"]) == 12
    assert all(item["reason"] == "IDEMPOTENT_ALREADY_BOUND" for item in second_audit["skipped"])
    assert second == updated


def test_factor_features_use_only_rows_knowable_by_snapshot() -> None:
    specs = _load(SPECS_PATH)
    ledger = _ledger()
    ledger["observations"].append(
        _row(
            factor="rates_policy",
            source="FED_H15",
            series="RIFSPFF_N.D",
            day="2026-09-29",
            value=9.99,
            valid_from="2026-09-30T12:00:00+00:00",
        )
    )
    result = factor_snapshot_eligibility(
        factor_id="rates_policy",
        snapshot_metadata=_snapshot(),
        macro_ledger=ledger,
        exposure_map=_exposure_map(),
        specs=specs,
    )
    assert result["eligible"] is True
    assert result["feature_values"]["rates_policy_level_pct"] == pytest.approx(3.88)
    assert result["feature_values"]["rates_policy_delta_pp"] == pytest.approx(-0.02)


def test_snapshot_before_frozen_prospective_start_consumes_no_slot() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = empty_split_manifest(specs, plan)
    updated, audit = bind_snapshot_to_manifest(
        manifest=manifest,
        snapshot_metadata=_snapshot(generated_at="2026-09-28T23:59:59+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_exposure_map(),
        specs=specs,
        plan=plan,
    )
    assert not audit["bound"]
    assert len(audit["skipped"]) == 12
    assert all(item["reason"] == "BEFORE_PROSPECTIVE_COHORT_START" for item in audit["skipped"])
    assert updated == manifest


def test_stale_factor_state_does_not_consume_slots() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = empty_split_manifest(specs, plan)
    updated, audit = bind_snapshot_to_manifest(
        manifest=manifest,
        snapshot_metadata=_snapshot(generated_at="2026-10-10T18:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_exposure_map(),
        specs=specs,
        plan=plan,
    )
    assert not audit["bound"]
    assert len(audit["skipped"]) == 12
    assert all(item["reason"] == "PIT_INELIGIBLE_STALE" for item in audit["skipped"])
    assert updated == manifest


def test_outcome_fields_are_rejected_before_split_binding() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = empty_split_manifest(specs, plan)
    contaminated = _snapshot()
    contaminated["peer_excess_5t"] = 0.123
    with pytest.raises(ExternalEvidence8GError, match="outcome_key_forbidden"):
        bind_snapshot_to_manifest(
            manifest=manifest,
            snapshot_metadata=contaminated,
            macro_ledger=_ledger(),
            exposure_map=_exposure_map(),
            specs=specs,
            plan=plan,
        )


def test_out_of_order_snapshot_binding_fails_closed() -> None:
    specs = _load(SPECS_PATH)
    plan = _load(PLAN_PATH)
    manifest = empty_split_manifest(specs, plan)
    later, _ = bind_snapshot_to_manifest(
        manifest=manifest,
        snapshot_metadata=_snapshot("snap-later", "2026-09-30T18:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_exposure_map(),
        specs=specs,
        plan=plan,
    )
    with pytest.raises(ExternalEvidence8GError, match="out_of_order_snapshot_binding"):
        bind_snapshot_to_manifest(
            manifest=later,
            snapshot_metadata=_snapshot("snap-earlier", "2026-09-29T18:00:00+00:00"),
            macro_ledger=_ledger(),
            exposure_map=_exposure_map(),
            specs=specs,
            plan=plan,
        )
