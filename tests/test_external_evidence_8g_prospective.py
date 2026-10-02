from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.evaluation_8g import EVALUATION_RESULT_SCHEMA
from scanner.research.external_evidence.holdout_8g import HOLDOUT_COMPLETION_SCHEMA
from scanner.research.external_evidence.prospective_8g import (
    ExternalEvidence8GProspectiveError,
    authorize_terminal_evaluation,
    consume_terminal_result,
    finalize_prospective_family,
    freeze_prospective_authorization,
    new_prospective_consumption_ledger,
    validate_prospective_protocol,
)


ROOT = Path(__file__).resolve().parents[1]
EVAL_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_evaluation_protocol_v1.json").read_text())
HOLDOUT_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_holdout_protocol_v1.json").read_text())
PROSPECTIVE_PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_prospective_protocol_v1.json").read_text())


def _holdout_completion(*, confirmed_first: bool = True) -> dict:
    family = PROSPECTIVE_PROTOCOL["frozen_hypothesis_family"]
    states = {h: "NOT_SELECTED_FROM_VALIDATION" for h in family}
    if confirmed_first:
        states[family[0]] = "HOLDOUT_CONFIRMED"
    return {
        "schema_version": HOLDOUT_COMPLETION_SCHEMA,
        "phase": "8G-F",
        "state": "HOLDOUT_COMPLETE",
        "empirically_complete": True,
        "candidate_hypotheses": [family[0]] if confirmed_first else [],
        "pending_candidates": [],
        "hypothesis_states": states,
        "next_subphase": "8G-G_PROSPECTIVE_CONFIRMATION",
        "completion_sha256": "a" * 64,
    }


def _authorization(completion: dict | None = None) -> dict:
    return freeze_prospective_authorization(
        holdout_completion=completion or _holdout_completion(),
        holdout_cutoff_as_of="2026-12-31T23:59:59Z",
        protocol=PROSPECTIVE_PROTOCOL,
        holdout_protocol=HOLDOUT_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )


def _summary(*, n: int = 30, regions: int = 2, first: str = "2027-01-02") -> dict:
    return {
        "split": "PROSPECTIVE",
        "paired_n": n,
        "temporal_support_regions": regions,
        "first_as_of": first,
        "last_as_of": "2027-03-01",
    }


def _result(hypothesis: str, *, p: float = 0.001, effect: float = 0.04, enough: bool = True) -> dict:
    return {
        "schema_version": EVALUATION_RESULT_SCHEMA,
        "phase": "8G-G",
        "hypothesis_id": hypothesis,
        "split": "PROSPECTIVE",
        "status": "EVALUATED" if enough else "INSUFFICIENT_EVIDENCE",
        "minimum_evidence_met": enough,
        "primary_effect": effect if enough else None,
        "primary_p_value_two_sided": p if enough else None,
        "primary_ci_95": [0.01, 0.07] if enough else [None, None],
        "non_overlap_sensitivity": {"materially_contradicts_primary": False},
        "coverage_missingness": {"status": "COMPLETE"},
        "regime_stability": {"status": "OK"},
        "sector_stability": {"status": "OK"},
        "currency_stability": {"status": "OK"},
        "pit_integrity_pass": True,
        "model_refit": False,
        "evaluation_sha256": (hypothesis + "PROSPECTIVE").encode("utf-8").hex()[:64].ljust(64, "0"),
    }


def test_protocol_keeps_8g_g_research_only_and_full_family() -> None:
    validate_prospective_protocol(PROSPECTIVE_PROTOCOL, HOLDOUT_PROTOCOL, EVAL_PROTOCOL)
    assert PROSPECTIVE_PROTOCOL["phase"] == "8G-G"
    assert PROSPECTIVE_PROTOCOL["multiple_testing"]["family_size"] == 12
    assert PROSPECTIVE_PROTOCOL["productive_integration_enabled"] is False
    assert PROSPECTIVE_PROTOCOL["automatic_promotion_enabled"] is False
    assert PROSPECTIVE_PROTOCOL["completion_gate"]["next_subphase"] == "8G-H_PROMOTION_REVIEW"


def test_only_holdout_confirmed_hypothesis_becomes_prospective_eligible() -> None:
    authorization = _authorization()
    family = PROSPECTIVE_PROTOCOL["frozen_hypothesis_family"]
    assert authorization["eligible_hypotheses"] == [family[0]]
    assert authorization["hypothesis_states"][family[0]] == "PROSPECTIVE_ELIGIBLE"
    assert authorization["hypothesis_states"][family[1]] == "NOT_ELIGIBLE_FROM_HOLDOUT"
    assert authorization["candidate_set_changes_allowed"] is False
    assert authorization["prospective_outcomes_read_while_freezing"] is False


def test_incomplete_8g_f_cannot_authorize_8g_g() -> None:
    completion = _holdout_completion()
    completion["empirically_complete"] = False
    with pytest.raises(ExternalEvidence8GProspectiveError, match="8g_f_must_be_empirically_complete"):
        _authorization(completion)


def test_non_eligible_stream_remains_sealed_and_cannot_open() -> None:
    authorization = _authorization()
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    family = PROSPECTIVE_PROTOCOL["frozen_hypothesis_family"]
    assert ledger["streams"][family[0]]["state"] == "ARMED"
    assert ledger["streams"][family[1]]["state"] == "SEALED"
    with pytest.raises(ExternalEvidence8GProspectiveError, match="non_eligible_prospective_access_forbidden"):
        authorize_terminal_evaluation(
            hypothesis_id=family[1],
            evidence_summary=_summary(),
            authorization=authorization,
            ledger=ledger,
            protocol=PROSPECTIVE_PROTOCOL,
        )


def test_terminal_trigger_is_post_holdout_and_outcome_blind() -> None:
    authorization = _authorization()
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    hypothesis = authorization["eligible_hypotheses"][0]
    with pytest.raises(ExternalEvidence8GProspectiveError, match="strictly_post_holdout"):
        authorize_terminal_evaluation(
            hypothesis_id=hypothesis,
            evidence_summary=_summary(first="2026-12-31"),
            authorization=authorization,
            ledger=ledger,
            protocol=PROSPECTIVE_PROTOCOL,
        )
    contaminated = _summary()
    contaminated["primary_effect"] = 999.0
    with pytest.raises(ExternalEvidence8GProspectiveError, match="must_not_inspect_outcome_values"):
        authorize_terminal_evaluation(
            hypothesis_id=hypothesis,
            evidence_summary=contaminated,
            authorization=authorization,
            ledger=ledger,
            protocol=PROSPECTIVE_PROTOCOL,
        )


def test_terminal_trigger_requires_fixed_minimum_evidence() -> None:
    authorization = _authorization()
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    hypothesis = authorization["eligible_hypotheses"][0]
    with pytest.raises(ExternalEvidence8GProspectiveError, match="minimum_evidence_not_met"):
        authorize_terminal_evaluation(
            hypothesis_id=hypothesis,
            evidence_summary=_summary(n=29),
            authorization=authorization,
            ledger=ledger,
            protocol=PROSPECTIVE_PROTOCOL,
        )
    with pytest.raises(ExternalEvidence8GProspectiveError, match="temporal_support_not_met"):
        authorize_terminal_evaluation(
            hypothesis_id=hypothesis,
            evidence_summary=_summary(regions=1),
            authorization=authorization,
            ledger=ledger,
            protocol=PROSPECTIVE_PROTOCOL,
        )


def test_terminal_evaluation_consumes_once_and_cannot_be_reopened() -> None:
    authorization = _authorization()
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    hypothesis = authorization["eligible_hypotheses"][0]
    token = authorize_terminal_evaluation(
        hypothesis_id=hypothesis,
        evidence_summary=_summary(),
        authorization=authorization,
        ledger=ledger,
        protocol=PROSPECTIVE_PROTOCOL,
    )
    result = _result(hypothesis)
    updated, receipt = consume_terminal_result(
        ledger=ledger,
        result=result,
        token=token,
        authorization=authorization,
        protocol=PROSPECTIVE_PROTOCOL,
    )
    assert updated["streams"][hypothesis]["state"] == "CONSUMED"
    assert receipt["consumed_once"] is True
    with pytest.raises(ExternalEvidence8GProspectiveError, match="may_run_once_only"):
        authorize_terminal_evaluation(
            hypothesis_id=hypothesis,
            evidence_summary=_summary(),
            authorization=authorization,
            ledger=updated,
            protocol=PROSPECTIVE_PROTOCOL,
        )


def test_finalize_keeps_full_holm_family_and_requires_positive_prospective_confirmation() -> None:
    completion = _holdout_completion()
    authorization = _authorization(completion)
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    hypothesis = authorization["eligible_hypotheses"][0]
    token = authorize_terminal_evaluation(
        hypothesis_id=hypothesis,
        evidence_summary=_summary(),
        authorization=authorization,
        ledger=ledger,
        protocol=PROSPECTIVE_PROTOCOL,
    )
    result = _result(hypothesis)
    updated, _receipt = consume_terminal_result(
        ledger=ledger,
        result=result,
        token=token,
        authorization=authorization,
        protocol=PROSPECTIVE_PROTOCOL,
    )
    final = finalize_prospective_family(
        prospective_results={hypothesis: result},
        holdout_completion=completion,
        authorization=authorization,
        ledger=updated,
        protocol=PROSPECTIVE_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    family = PROSPECTIVE_PROTOCOL["frozen_hypothesis_family"]
    assert final["holm_family_size"] == 12
    assert final["hypothesis_states"][hypothesis] == "PROSPECTIVE_CONFIRMED"
    assert final["hypothesis_states"][family[1]] == "NOT_ELIGIBLE_FROM_HOLDOUT"
    assert final["state"] == "PROSPECTIVE_COMPLETE"
    assert final["automatic_promotion_authorized"] is False
    assert final["next_subphase"] == "8G-H_PROMOTION_REVIEW"


def test_empty_holdout_confirmed_set_is_valid_8g_g_completion() -> None:
    completion = _holdout_completion(confirmed_first=False)
    authorization = _authorization(completion)
    ledger = new_prospective_consumption_ledger(authorization, PROSPECTIVE_PROTOCOL)
    final = finalize_prospective_family(
        prospective_results={},
        holdout_completion=completion,
        authorization=authorization,
        ledger=ledger,
        protocol=PROSPECTIVE_PROTOCOL,
        evaluation_protocol=EVAL_PROTOCOL,
    )
    assert final["empirically_complete"] is True
    assert final["state"] == "NO_HOLDOUT_CONFIRMED_CANDIDATES"
