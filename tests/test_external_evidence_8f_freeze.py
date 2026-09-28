from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FREEZE = ROOT / "configs/external_evidence_8f_freeze_v1.json"
COMPLETION = ROOT / "configs/external_evidence_8f_completion_v1.json"
MACRO = ROOT / "configs/external_evidence_8f_macro_exposure_v1.json"
FREEZE_DIR = ROOT / "artifacts/research/external_evidence_8f_freeze_2026-09-28"


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_phase8f_freeze_contract_is_complete_and_outcome_blind():
    freeze = _load(FREEZE)
    completion = _load(COMPLETION)
    macro = _load(MACRO)

    assert freeze["schema_version"] == "external_evidence_8f_freeze_v1"
    assert freeze["status"] == "PHASE_8F_SOURCE_AND_EXPOSURE_LAYER_FROZEN_OUTCOME_RESEARCH_PENDING_8G"
    assert freeze["completion"] == {
        "status": "PASS_8F_COMPLETION",
        "freeze_allowed": True,
        "active_reviewed_mapping_count": 195,
        "explicit_unmapped_subject_count": 12,
        "domain_subject_count": 207,
        "domain_accounted_count": 207,
        "unaccounted_subject_count": 0,
        "knowable_ledger_row_count": 25,
        "ledger_series_count": 4,
        "observed_factor_ids": ["fx", "rates_policy", "yield_curve"],
    }
    assert freeze["research_domain"]["source_blob_sha"] == "2c6efec912ea7478096f77dc8f88d7eed2a0581b"
    assert freeze["research_domain"]["accounted_subject_count"] == 207
    assert freeze["source_live_run"]["workflow_run_id"] == 36381751961
    assert freeze["source_live_run"]["head_sha"] == "10f37e7d28e533d1ec9599ab75edc5beb0e82e6f"
    assert freeze["next_phase"] == "8G_incremental_evidence_research"

    assert all(value is False for value in freeze["guards"].values())
    assert freeze["scope_decisions"]["eia_status_at_freeze"] == "SKIPPED_NO_API_KEY"
    assert freeze["scope_decisions"]["eia_missing_blocks_freeze"] is False
    assert freeze["scope_decisions"]["deferred_factors_may_be_treated_as_known"] is False
    assert freeze["scope_decisions"]["unobserved_adapters_may_be_treated_as_known"] is False

    for factor_id in ("gold", "silver", "copper"):
        assert "DEFERRED" in freeze["factor_states"][factor_id]
        assert "DEFERRED" in next(
            item["status"] for item in macro["factor_catalog"] if item["factor_id"] == factor_id
        )

    assert completion["status"] == "PHASE_8F_FROZEN_COMPLETION_GATE_PASSED"
    assert completion["freeze_allowed"] is True
    assert completion["freeze_artifact"] == "configs/external_evidence_8f_freeze_v1.json"
    assert macro["status"] == freeze["status"]
    assert macro["next_gates"] == ["8G_INCREMENTAL_OUTCOME_RESEARCH_ONLY_AFTER_8F_FREEZE"]


def test_persisted_freeze_metadata_is_hash_bound_to_the_passing_live_run():
    freeze = _load(FREEZE)
    gate_path = FREEZE_DIR / "external_evidence_8f_completion_gate.json"
    collection_path = FREEZE_DIR / "external_evidence_8f_collection_latest.json"

    assert _sha256(gate_path) == freeze["frozen_artifact_hashes"]["completion_gate_sha256"]
    assert _sha256(collection_path) == freeze["frozen_artifact_hashes"]["collection_metadata_sha256"]

    gate = _load(gate_path)
    collection = _load(collection_path)
    assert gate["status"] == "PASS_8F_COMPLETION"
    assert gate["freeze_allowed"] is True
    assert gate["blockers"] == []
    assert gate["metrics"]["active_reviewed_mapping_count"] == 195
    assert gate["metrics"]["domain_accounted_count"] == 207
    assert gate["metrics"]["knowable_ledger_row_count"] == 25
    assert gate["metrics"]["ledger_series_count"] == 4

    assert collection["status"] == "COLLECTED_OUTCOME_BLIND_PIT_SNAPSHOT"
    assert collection["ledger_knowable_row_count"] == 25
    assert collection["eia_status"] == "SKIPPED_NO_API_KEY"
    raw_hashes = {row["source_id"]: row["sha256"] for row in collection["raw_snapshots"]}
    assert raw_hashes["federal_reserve_board_h15"] == freeze["frozen_artifact_hashes"]["fed_h15_raw_sha256"]
    assert raw_hashes["ecb_data_portal"] == freeze["frozen_artifact_hashes"]["ecb_fx_raw_sha256"]

    # The full ledger/domain audit were produced by the cited immutable CI run and are
    # cryptographically bound here; the passing gate above records the exact metrics.
    assert freeze["frozen_artifact_hashes"]["macro_ledger_sha256"] == "407a9d86bcaad5d7ed131124d4e7a6954779e02ccb7c0773632d37d29f9310a0"
    assert freeze["frozen_artifact_hashes"]["exposure_domain_audit_sha256"] == "4e52ffe104ae1ad9c344ef3ed5b31a80b4ac65b6b0980e214ea048e0c34fba95"
