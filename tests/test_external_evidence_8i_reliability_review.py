from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.reliability_review_8i import (
    ExternalEvidence8IReviewError,
    build_real_review_status,
    digest,
)


ROOT = Path(__file__).resolve().parents[1]


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict, dict]:
    return (
        _load("configs/external_evidence_8i_reliability_extension_research_v1.json"),
        _load("configs/external_evidence_8i_promotion_provenance_binding_v1.json"),
        _load("artifacts/research/external_evidence_8g_split_manifest_v1.json"),
        _load("artifacts/research/external_evidence_8g_holdout_consumption_v1.json"),
        _load("configs/external_evidence_source_identity_correction_v1.json"),
    )


def _run(at: str, *, reliability=None, binding=None, manifest=None, ledger=None, correction=None):
    base = _inputs()
    return build_real_review_status(
        reliability_contract=base[0] if reliability is None else reliability,
        binding_contract=base[1] if binding is None else binding,
        split_manifest=base[2] if manifest is None else manifest,
        holdout_ledger=base[3] if ledger is None else ledger,
        source_identity_correction=base[4] if correction is None else correction,
        research_as_of=at,
    )


def test_current_real_review_starts_but_waits_for_frozen_prospective_start() -> None:
    result = _run("2026-09-28T15:25:00+00:00")
    assert result["state"] == "STARTED_WAITING_FOR_PROSPECTIVE_START"
    assert result["evaluation_started"] is True
    assert result["review_started"] is True
    assert result["before_prospective_start"] is True
    assert result["upstream_8g"]["prospective_assignment_count"] == 0
    assert result["upstream_8g"]["holdout_evaluation_count"] == 0
    assert result["upstream_8g"]["promotion_present"] is False
    assert result["upstream_8h"]["promotion_present"] is False
    assert result["8i_binding"]["bound_component_count"] == 0
    assert "BEFORE_8I_E_PROSPECTIVE_START" in result["blockers"]
    assert "NO_8G_PROSPECTIVE_ASSIGNMENTS" in result["blockers"]
    assert "NO_8G_HOLDOUT_EVALUATIONS" in result["blockers"]
    assert "NO_UPSTREAM_8G_OR_8H_PROMOTIONS" in result["blockers"]
    assert "NO_8I_BOUND_EXTERNAL_COMPONENTS" in result["blockers"]
    assert result["real_outcomes_opened"] is False
    assert result["real_outcome_values_read_by_review"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"


def test_after_prospective_start_zero_assignment_is_next_actual_blocker() -> None:
    result = _run("2026-09-29T00:00:01+00:00")
    assert result["state"] == "STARTED_WAITING_FOR_FIRST_8G_PROSPECTIVE_ASSIGNMENT"
    assert result["before_prospective_start"] is False
    assert "BEFORE_8I_E_PROSPECTIVE_START" not in result["blockers"]
    assert "NO_8G_PROSPECTIVE_ASSIGNMENTS" in result["blockers"]
    assert result["real_outcomes_opened"] is False


def test_source_identity_correction_is_exact_outcome_blind_and_not_retroactive() -> None:
    correction = _inputs()[4]
    assert correction["state"] == "RESOLVED_OUTCOME_BLIND_VERSIONED"
    assert correction["alias_to_canonical"] == {
        "FED_H15": "federal_reserve_board_h15",
        "ECB_EXR": "ecb_data_portal",
    }
    assert correction["outcomes_read"] is False
    assert correction["retroactive_evidence_rewrite"] is False
    unsigned = copy.deepcopy(correction)
    recorded = unsigned.pop("correction_sha256")
    assert digest(unsigned) == recorded
    result = _run("2026-09-28T15:25:00+00:00")
    assert result["source_identity_correction"]["state"] == "RESOLVED_OUTCOME_BLIND_VERSIONED_RECEIPT_AVAILABLE"
    assert result["source_identity_correction"]["correction_sha256"] == recorded


def test_correction_tamper_fails_closed() -> None:
    correction = copy.deepcopy(_inputs()[4])
    correction["alias_to_canonical"]["FED_H15"] = "wrong_source"
    with pytest.raises(ExternalEvidence8IReviewError, match="alias_map_mismatch|digest_mismatch"):
        _run("2026-09-28T15:25:00+00:00", correction=correction)


def test_review_rejects_any_outcome_injection_before_terminal_gate() -> None:
    manifest = copy.deepcopy(_inputs()[2])
    manifest["peer_excess_5t"] = 0.25
    with pytest.raises(ExternalEvidence8IReviewError, match="outcome_field_forbidden"):
        _run("2026-09-28T15:25:00+00:00", manifest=manifest)

    ledger = copy.deepcopy(_inputs()[3])
    ledger["observed_outcome"] = 1.0
    with pytest.raises(ExternalEvidence8IReviewError, match="outcome_field_forbidden"):
        _run("2026-09-28T15:25:00+00:00", ledger=ledger)


def test_assignment_progress_changes_state_without_opening_outcomes() -> None:
    manifest = copy.deepcopy(_inputs()[2])
    one = next(iter(manifest["streams"].values()))
    one["assignments"] = [{
        "snapshot_id": "synthetic-metadata-only",
        "as_of": "2026-09-29T17:00:00+00:00",
        "split": "DISCOVERY",
        "usable": True,
    }]
    result = _run("2026-09-30T00:00:00+00:00", manifest=manifest)
    assert result["upstream_8g"]["prospective_assignment_count"] == 1
    assert result["state"] == "STARTED_COLLECTING_UPSTREAM_8G_EVIDENCE"
    assert result["real_outcomes_opened"] is False
    assert result["terminal_outcome_gate_ready"] is False


def test_review_never_enables_reliability_stance_action_or_orders() -> None:
    result = _run("2026-09-29T00:00:01+00:00")
    assert result["phase7_reliability_preserved"] is True
    assert result["extended_reliability_enabled"] is False
    assert result["extended_stance_enabled"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False
    assert result["empirical_review_complete"] is False
