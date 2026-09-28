from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.research_8g import (
    ExternalEvidence8GError,
    empty_split_manifest,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    bind_snapshot_to_manifest_guarded,
    exact_split_boundary_purge_required,
    validate_guarded_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
SPECS = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
PLAN = json.loads((ROOT / "configs" / "external_evidence_8g_split_plan_v1.json").read_text())


def _row(factor: str, source: str, series: str, day: str, value: float) -> dict:
    return {
        "factor_id": factor,
        "source_id": source,
        "series_id": series,
        "observation_date": day,
        "revision_id": f"{series}:{day}:r1",
        "value": value,
        "status": "KNOWN",
        "valid_from": "2026-09-28T12:00:00+00:00",
        "source_record_sha256": "a" * 64,
    }


def _ledger() -> dict:
    return {
        "schema_version": "external_evidence_8f_macro_ledger_v1",
        "observations": [
            _row("rates_policy", "FED_H15", "RIFSPFF_N.D", "2026-09-27", 3.90),
            _row("rates_policy", "FED_H15", "RIFSPFF_N.D", "2026-09-28", 3.88),
            _row("yield_curve", "FED_H15", "RIFLGFCY02_N.B", "2026-09-27", 4.80),
            _row("yield_curve", "FED_H15", "RIFLGFCY10_N.B", "2026-09-27", 5.00),
            _row("yield_curve", "FED_H15", "RIFLGFCY02_N.B", "2026-09-28", 4.85),
            _row("yield_curve", "FED_H15", "RIFLGFCY10_N.B", "2026-09-28", 5.10),
            _row("fx", "ECB_EXR", "EXR.D.USD.EUR.SP00.A", "2026-09-27", 1.14),
            _row("fx", "ECB_EXR", "EXR.D.USD.EUR.SP00.A", "2026-09-28", 1.15),
        ],
    }


def _map() -> dict:
    return {
        "mappings": [
            {"mapping_id": "M1", "subject_id": "AAA", "factor_id": "rates_policy", "relationship_class": "FINANCING_SENSITIVITY", "human_reviewed": True, "review_status": "ACTIVE", "valid_from": "2026-09-28T10:00:00+00:00", "valid_to": None},
            {"mapping_id": "M2", "subject_id": "BBB", "factor_id": "yield_curve", "relationship_class": "FINANCING_SENSITIVITY", "human_reviewed": True, "review_status": "ACTIVE", "valid_from": "2026-09-28T10:00:00+00:00", "valid_to": None},
            {"mapping_id": "M3", "subject_id": "CCC", "factor_id": "fx", "relationship_class": "CURRENCY_TRANSLATION", "human_reviewed": True, "review_status": "ACTIVE", "valid_from": "2026-09-28T10:00:00+00:00", "valid_to": None},
        ]
    }


def _snapshot(snapshot_id: str, as_of: str, generated_at: str) -> dict:
    return {
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "generated_at": generated_at,
        "views_generated_at": generated_at,
        "latest_run_complete": True,
    }


def test_plan_freezes_one_sampling_slot_per_as_of_day() -> None:
    assert PLAN["sampling_identity"] == "factor_id+horizon_sessions+as_of"
    assert PLAN["same_as_of_date_may_bind_once_per_factor_horizon"] is True
    assert PLAN["first_eligible_snapshot_of_as_of_date_wins"] is True
    assert PLAN["guards"]["same_day_repeat_can_consume_additional_slot"] is False


def test_manual_rerun_same_day_does_not_inflate_sample() -> None:
    manifest = empty_split_manifest(SPECS, PLAN)
    first, audit1 = bind_snapshot_to_manifest_guarded(
        manifest=manifest,
        snapshot_metadata=_snapshot("snap-1", "2026-09-29", "2026-09-29T17:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_map(),
        specs=SPECS,
        plan=PLAN,
    )
    assert len(audit1["bound"]) == 12
    assert all(item["as_of"] == "2026-09-29" for stream in first["streams"].values() for item in stream["assignments"])

    second, audit2 = bind_snapshot_to_manifest_guarded(
        manifest=first,
        snapshot_metadata=_snapshot("snap-2", "2026-09-29", "2026-09-29T19:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_map(),
        specs=SPECS,
        plan=PLAN,
    )
    assert not audit2["bound"]
    assert len(audit2["skipped"]) == 12
    assert all(item["reason"] == "AS_OF_DATE_ALREADY_BOUND" for item in audit2["skipped"])
    assert second == first


def test_new_as_of_day_consumes_next_slot() -> None:
    manifest = empty_split_manifest(SPECS, PLAN)
    first, _ = bind_snapshot_to_manifest_guarded(
        manifest=manifest,
        snapshot_metadata=_snapshot("snap-1", "2026-09-29", "2026-09-29T17:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_map(),
        specs=SPECS,
        plan=PLAN,
    )
    second, audit = bind_snapshot_to_manifest_guarded(
        manifest=first,
        snapshot_metadata=_snapshot("snap-2", "2026-09-30", "2026-09-30T17:00:00+00:00"),
        macro_ledger=_ledger(),
        exposure_map=_map(),
        specs=SPECS,
        plan=PLAN,
    )
    assert len(audit["bound"]) == 12
    assert all(stream["assignments"][-1]["ordinal"] == 2 for stream in second["streams"].values())
    validate_guarded_manifest(second, SPECS, PLAN)


def test_frozen_series_rejects_wrong_source_id() -> None:
    manifest = empty_split_manifest(SPECS, PLAN)
    ledger = _ledger()
    ledger["observations"].append(
        _row("rates_policy", "WRONG_SOURCE", "RIFSPFF_N.D", "2026-09-28", 99.0)
    )
    with pytest.raises(ExternalEvidence8GError, match="frozen_source_mismatch"):
        bind_snapshot_to_manifest_guarded(
            manifest=manifest,
            snapshot_metadata=_snapshot("snap-1", "2026-09-29", "2026-09-29T17:00:00+00:00"),
            macro_ledger=ledger,
            exposure_map=_map(),
            specs=SPECS,
            plan=PLAN,
        )


def test_as_of_is_mandatory_sampling_identity() -> None:
    manifest = empty_split_manifest(SPECS, PLAN)
    snapshot = _snapshot("snap-1", "2026-09-29", "2026-09-29T17:00:00+00:00")
    snapshot.pop("as_of")
    with pytest.raises(ExternalEvidence8GError, match="snapshot_as_of_required"):
        bind_snapshot_to_manifest_guarded(
            manifest=manifest,
            snapshot_metadata=snapshot,
            macro_ledger=_ledger(),
            exposure_map=_map(),
            specs=SPECS,
            plan=PLAN,
        )


def test_exact_boundary_purge_uses_structural_label_date_not_outcome_value() -> None:
    assert exact_split_boundary_purge_required(
        label_available_from="2026-10-20",
        next_split_first_as_of="2026-10-20",
    ) is True
    assert exact_split_boundary_purge_required(
        label_available_from="2026-10-19",
        next_split_first_as_of="2026-10-20",
    ) is False
    block = PLAN["exact_split_boundary_purge"]
    assert block["required"] is True
    assert block["uses_outcome_value"] is False
    assert block["reserved_tail_slots_substitute_for_exact_purge"] is False
