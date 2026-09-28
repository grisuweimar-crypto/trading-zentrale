from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.evaluation_8g import (
    EVALUATION_RESULT_SCHEMA,
    freeze_validation_family,
    holm_adjust_family,
    new_holdout_consumption_ledger,
)
from scanner.research.external_evidence.holdout_8g import (
    ExternalEvidence8GHoldoutError,
    authorize_holdout_open,
    consume_evaluated_holdout,
    finalize_holdout_family,
    freeze_holdout_candidates,
    validate_holdout_protocol,
    validate_holdout_seed_ledger,
)


ROOT = Path(__file__).resolve().parents[1]
EVAL_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_evaluation_protocol_v1.json").read_text())
HOLDOUT_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_holdout_protocol_v1.json").read_text())
SEED_LEDGER = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8g_holdout_consumption_v1.json").read_text())


def _result(
    hypothesis: str,
    *,
    split: str,
    p: float | None,
    effect: float | None,
    enough: bool = True,
    ci_low: float | None = 0.01,
    ci_high: float | None = 0.05,
) -> dict:
    return {
        "schema_version": EVALUATION_RESULT_SCHEMA,
        "phase": "8G-E",
        "hypothesis_id": hypothesis,
        "split": split,
        "status": "EVALUATED" if enough else "INSUFFICIENT_EVIDENCE",
        "minimum_evidence_met": enough,
        "primary_effect": effect,
        "primary_p_value_two_sided": p,
        "primary_ci_95": [ci_low, ci_high] if enough else [None, None],
        "non_overlap_sensitivity": {"materially_contradicts_primary": False},
        "coverage_missingness": {"status": "COMPLETE"},
        "regime_stability": {"status": "OK"},
        "sector_stability": {"status": "OK"},
        "currency_stability": {"status": "OK"},
        "pit_integrity_pass": True,
        "model_refit": False,
        "evaluation_sha256": (hypothesis + split).encode("utf-8").hex()[:64].ljust(64, "0"),
    }


def _validation_raw() -> dict[str, dict]:
    family = EVAL_PROTOCOL["frozen_hypothesis_family"]
    raw = {hypothesis: _result(hypothesis, split="VALIDATION", p=0.5, effect=0.01) for hypothesis in family}
    # Only the first hypothesis survives the frozen 12-member Holm family.
    raw[family[0]] = _result(family[0], split="VALIDATION", p=0.001, effect=0.05)
    return raw


def _validation_freeze(raw: dict[str, dict]) -> tuple[dict[str, dict], dict]:
    adjusted = holm_adjust_family(raw, EVAL_PROTOCOL)
    receipt = freeze_validation_family(raw, EVAL_PROTOCOL)
    return adjusted, receipt


def test_holdout_protocol_restores_8g_f_boundary_and_keeps_production_off() -> None:
    validate_holdout_protocol(HOLDOUT_PROTOCOL, EVAL_PROTOCOL)
    validate_holdout_seed_ledger(SEED_LEDGER, HOLDOUT_PROTOCOL)
    assert HOLDOUT_PROTOCOL["phase"] == "8G-F"
    assert HOLDOUT_PROTOCOL["completion_gate"]["next_subphase"] == "8G-G_PROSPECTIVE_CONFIRMATION"
    assert HOLDOUT_PROTOCOL["multiple_testing"]["family_size"] == 12
    assert HOLDOUT_PROTOCOL["productive_integration_enabled"] is False
    assert HOLDOUT_PROTOCOL["automatic_promotion_enabled"] is False
    assert SEED_LEDGER["phase"] == "8G-F"
    assert all(stream["state"] == "SEALED" for stream in SEED_LEDGER["streams"].values())


def test_candidate_set_is_frozen_from_validation_only() -> None:
    raw = _validation_raw()
    adjusted, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    family = EVAL_PROTOCOL["frozen_hypothesis_family"]
    assert authorization["candidate_hypotheses"] == [family[0]]
    assert authorization["hypothesis_states"][family[0]] == "HOLDOUT_CANDIDATE"
    assert authorization["hypothesis_states"][family[1]] == "NOT_SELECTED_FROM_VALIDATION"
    assert authorization["candidate_set_changes_allowed"] is False
    assert authorization["holdout_outcomes_read_while_freezing_candidates"] is False
    assert authorization["validation_results_sha256"]
    assert adjusted[family[0]]["holm_positive"] is True


def test_validation_hash_mismatch_fails_before_holdout_authorization() -> None:
    raw = _validation_raw()
    _adjusted, receipt = _validation_freeze(raw)
    tampered = dict(raw)
    first = EVAL_PROTOCOL["frozen_hypothesis_family"][0]
    tampered[first] = dict(tampered[first])
    tampered[first]["primary_effect"] = 999.0
    with pytest.raises(ExternalEvidence8GHoldoutError, match="validation_results_hash_mismatch"):
        freeze_holdout_candidates(
            validation_results=tampered,
            validation_receipt=receipt,
            protocol=HOLDOUT_PROTOCOL,
            evaluation_protocol=EVAL_PROTOCOL,
        )


def test_non_candidate_cannot_open_holdout() -> None:
    raw = _validation_raw()
    _adjusted, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    family = EVAL_PROTOCOL["frozen_hypothesis_family"]
    with pytest.raises(ExternalEvidence8GHoldoutError, match="non_candidate_holdout_access_forbidden"):
        authorize_holdout_open(
            hypothesis_id=family[1],
            candidate_authorization=authorization,
            ledger=SEED_LEDGER,
            protocol=HOLDOUT_PROTOCOL,
            evaluation_protocol=EVAL_PROTOCOL,
        )


def test_candidate_gets_one_shot_token_and_second_open_is_forbidden_after_consumption() -> None:
    raw = _validation_raw()
    _adjusted, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    hypothesis = authorization["candidate_hypotheses"][0]
    token = authorize_holdout_open(
        hypothesis_id=hypothesis,
        candidate_authorization=authorization,
        ledger=SEED_LEDGER,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    holdout = _result(hypothesis, split="HOLDOUT", p=0.001, effect=0.04)
    updated, consume_receipt = consume_evaluated_holdout(
        ledger=SEED_LEDGER,
        holdout_result=holdout,
        open_token=token,
        candidate_authorization=authorization,
        protocol=HOLDOUT_PROTOCOL,
    )
    assert updated["streams"][hypothesis]["state"] == "CONSUMED"
    assert consume_receipt["consumed_once"] is True
    with pytest.raises(ExternalEvidence8GHoldoutError, match="holdout_may_open_once_only"):
        authorize_holdout_open(
            hypothesis_id=hypothesis,
            candidate_authorization=authorization,
            ledger=updated,
            protocol=HOLDOUT_PROTOCOL,
            evaluation_protocol=EVAL_PROTOCOL,
        )


def test_holdout_holm_keeps_full_12_member_family_even_with_one_candidate() -> None:
    raw = _validation_raw()
    _adjusted_validation, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    hypothesis = authorization["candidate_hypotheses"][0]
    holdout = _result(hypothesis, split="HOLDOUT", p=0.001, effect=0.04)
    adjusted_holdout = holm_adjust_family({hypothesis: holdout}, EVAL_PROTOCOL)
    family = EVAL_PROTOCOL["frozen_hypothesis_family"]
    assert list(adjusted_holdout) == family
    assert adjusted_holdout[hypothesis]["holm_adjusted_p"] == pytest.approx(0.012)
    assert adjusted_holdout[hypothesis]["holm_positive"] is True
    assert adjusted_holdout[family[1]]["holm_adjusted_p"] == pytest.approx(1.0)


def test_finalize_holdout_family_confirms_candidate_and_keeps_non_candidates_sealed() -> None:
    raw = _validation_raw()
    _adjusted_validation, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    hypothesis = authorization["candidate_hypotheses"][0]
    token = authorize_holdout_open(
        hypothesis_id=hypothesis,
        candidate_authorization=authorization,
        ledger=SEED_LEDGER,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    holdout = _result(hypothesis, split="HOLDOUT", p=0.001, effect=0.04)
    adjusted_holdout = holm_adjust_family({hypothesis: holdout}, EVAL_PROTOCOL)[hypothesis]
    adjusted_holdout["evaluation_sha256"] = holdout["evaluation_sha256"]
    updated, _consume_receipt = consume_evaluated_holdout(
        ledger=SEED_LEDGER,
        holdout_result=adjusted_holdout,
        open_token=token,
        candidate_authorization=authorization,
        protocol=HOLDOUT_PROTOCOL,
    )
    completion = finalize_holdout_family(
        validation_results=raw,
        validation_receipt=receipt,
        holdout_results={hypothesis: adjusted_holdout},
        candidate_authorization=authorization,
        ledger=updated,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    assert completion["state"] == "HOLDOUT_COMPLETE"
    assert completion["empirically_complete"] is True
    assert completion["hypothesis_states"][hypothesis] == "HOLDOUT_CONFIRMED"
    assert completion["holm_family_size"] == 12
    assert completion["non_candidate_holdout_streams_remain_sealed"] is True
    assert completion["next_subphase"] == "8G-G_PROSPECTIVE_CONFIRMATION"


def test_empty_validation_candidate_set_is_valid_holdout_completion_without_opening_any_holdout() -> None:
    family = EVAL_PROTOCOL["frozen_hypothesis_family"]
    raw = {hypothesis: _result(hypothesis, split="VALIDATION", p=0.5, effect=0.01) for hypothesis in family}
    _adjusted, receipt = _validation_freeze(raw)
    authorization = freeze_holdout_candidates(
        validation_results=raw,
        validation_receipt=receipt,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    assert authorization["candidate_hypotheses"] == []
    completion = finalize_holdout_family(
        validation_results=raw,
        validation_receipt=receipt,
        holdout_results={},
        candidate_authorization=authorization,
        ledger=SEED_LEDGER,
        protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    assert completion["state"] == "NO_VALIDATION_CANDIDATES"
    assert completion["empirically_complete"] is True
    assert completion["non_candidate_holdout_streams_remain_sealed"] is True
