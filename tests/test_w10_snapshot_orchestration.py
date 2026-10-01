from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from scanner.research.decision_layer.w10_orchestration import (
    W10OrchestrationError,
    begin_manifest,
    freeze_pre_7a,
    record_preexisting_artifact,
    record_runtime_artifact,
    resolve_phase6,
    seal_final_7a,
    validate_sealed_manifest,
)


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc)


def _daily() -> dict[str, object]:
    return {
        "snapshot_id": "snapshot-x",
        "as_of": "2026-10-01",
        "generated_at": "2026-10-01T17:00:00+00:00",
    }


def _json(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _collect_upstream(tmp_path: Path):
    manifest = begin_manifest(_daily(), now=_dt(17, 1))

    phase2 = _json(
        tmp_path / "p2.json",
        {"source": {"snapshot_id": "snapshot-x"}},
    )
    manifest = record_preexisting_artifact(
        manifest,
        stage="phase2_probability",
        artifact_path=phase2,
        source_commit="a" * 40,
        source_available_from="2026-10-01T16:59:00+00:00",
        require_snapshot_match=True,
    )

    phase3 = _json(tmp_path / "p3.json", {"phase": "3"})
    manifest = record_runtime_artifact(
        manifest,
        stage="phase3_risk",
        artifact_path=phase3,
        now=_dt(17, 2),
    )

    phase4 = _json(
        tmp_path / "p4.json",
        {"snapshot_id": "snapshot-x", "phase": "4"},
    )
    manifest = record_runtime_artifact(
        manifest,
        stage="phase4_confidence",
        artifact_path=phase4,
        now=_dt(17, 3),
    )

    phase5 = _json(
        tmp_path / "p5.json",
        {"phase": "5e", "snapshot_id": "historical-governance-sample"},
    )
    manifest = record_preexisting_artifact(
        manifest,
        stage="phase5_governance",
        artifact_path=phase5,
        source_commit="b" * 40,
        source_available_from="2026-09-30T20:00:00+00:00",
        require_snapshot_match=False,
    )

    manifest = resolve_phase6(manifest, artifact_path=None, now=_dt(17, 4))
    return manifest


def test_w10_requires_all_upstream_stages_before_7a_freeze(tmp_path: Path):
    manifest = begin_manifest(_daily(), now=_dt(17, 1))
    with pytest.raises(W10OrchestrationError, match="upstream_stage_unresolved"):
        freeze_pre_7a(manifest, now=_dt(17, 5))


def test_w10_phase6_absence_is_explicit_not_neutral(tmp_path: Path):
    manifest = _collect_upstream(tmp_path)
    phase6 = manifest["stages"]["phase6_elliott"]
    assert phase6["status"] == "not_supplied"
    assert phase6["decision_effect"] == "none"
    assert phase6["missing_is_neutral_evidence"] is False
    assert manifest["guards"]["phase6_missing_is_neutral_evidence"] is False
    assert (
        manifest["stages"]["phase5_governance"]["snapshot_identity_state"]
        == "not_market_snapshot_evidence"
    )


def test_w10_runtime_availability_cannot_be_backdated_by_caller(tmp_path: Path):
    manifest = begin_manifest(_daily(), now=_dt(17, 1))
    artifact = _json(tmp_path / "p3.json", {"phase": "3"})
    manifest = record_runtime_artifact(
        manifest,
        stage="phase3_risk",
        artifact_path=artifact,
        now=_dt(17, 2),
    )
    assert (
        manifest["stages"]["phase3_risk"]["available_from"]
        == "2026-10-01T17:02:00+00:00"
    )
    assert (
        manifest["stages"]["phase3_risk"]["availability_source"]
        == "w10_runtime_recorded_after_stage"
    )


def test_w10_rejects_future_phase6_source(tmp_path: Path):
    manifest = _collect_upstream(tmp_path)
    del manifest["stages"]["phase6_elliott"]
    source = _json(
        tmp_path / "phase6.json",
        {
            "schema_version": "decision_elliott_6h_source_v1",
            "source_commit": "c" * 40,
            "available_from": "2026-10-01T18:00:00+00:00",
            "outputs": [],
        },
    )
    with pytest.raises(W10OrchestrationError, match="future_phase6_source"):
        resolve_phase6(manifest, artifact_path=source, now=_dt(17, 4))


def test_w10_final_7a_must_follow_freeze_and_match_snapshot(tmp_path: Path):
    manifest = freeze_pre_7a(_collect_upstream(tmp_path), now=_dt(17, 5))
    packet_set = _json(
        tmp_path / "packets.json",
        {
            "snapshot_id": "snapshot-x",
            "as_of": "2026-10-01T17:06:00+00:00",
            "packets": [{"symbol": "TEST"}],
        },
    )
    archive = tmp_path / "archive.jsonl"
    archive.write_text('{"symbol":"TEST"}\n', encoding="utf-8")

    sealed = seal_final_7a(
        manifest,
        packet_set_path=packet_set,
        archive_path=archive,
        now=_dt(17, 7),
    )
    validated = validate_sealed_manifest(
        sealed, expected_snapshot_id="snapshot-x"
    )
    assert validated["status"] == "sealed"
    assert validated["stages"]["final_7a"]["snapshot_identity_state"] == "verified"
    assert validated["downstream_contract"]["chain"] == ["7D", "7E", "7F", "7G", "7H"]
    assert validated["downstream_contract"]["private_position_data_persisted"] is False


def test_w10_refuses_7a_built_before_freeze(tmp_path: Path):
    manifest = freeze_pre_7a(_collect_upstream(tmp_path), now=_dt(17, 5))
    packet_set = _json(
        tmp_path / "packets.json",
        {
            "snapshot_id": "snapshot-x",
            "as_of": "2026-10-01T17:04:59+00:00",
            "packets": [{"symbol": "TEST"}],
        },
    )
    archive = tmp_path / "archive.jsonl"
    archive.write_text("{}\n", encoding="utf-8")
    with pytest.raises(W10OrchestrationError, match="final_7a_predates_upstream_freeze"):
        seal_final_7a(
            manifest,
            packet_set_path=packet_set,
            archive_path=archive,
            now=_dt(17, 7),
        )


def test_w10_refuses_upstream_mutation_after_freeze(tmp_path: Path):
    manifest = freeze_pre_7a(_collect_upstream(tmp_path), now=_dt(17, 5))
    later = _json(tmp_path / "later.json", {"phase": "late"})
    with pytest.raises(W10OrchestrationError, match="upstream_manifest_already_frozen"):
        record_runtime_artifact(
            manifest,
            stage="late_evidence",
            artifact_path=later,
            now=_dt(17, 6),
        )
