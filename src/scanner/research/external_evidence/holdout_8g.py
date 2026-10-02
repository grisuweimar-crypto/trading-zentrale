"""Phase 8G-F one-shot Holdout/OOS governance.

Only candidates frozen from the complete Validation family may consume Holdout.
Candidate selection is completed before any Holdout value is opened. The full
12-hypothesis family remains intact for Holm adjustment; non-candidates enter
Holdout multiplicity as p=1 and their Holdout streams remain sealed.
"""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Mapping

from scanner.research.external_evidence.evaluation_8g import (
    EVALUATION_RESULT_SCHEMA,
    HOLDOUT_LEDGER_SCHEMA,
    VALIDATION_FAMILY_SCHEMA,
    consume_holdout_once,
    holm_adjust_family,
    validate_evaluation_protocol,
)
from scanner.research.external_evidence.research_8g import ACTIVE_FACTORS, HORIZONS


HOLDOUT_PROTOCOL_SCHEMA = "external_evidence_8g_holdout_protocol_v1"
HOLDOUT_AUTHORIZATION_SCHEMA = "external_evidence_8g_holdout_authorization_v1"
HOLDOUT_TOKEN_SCHEMA = "external_evidence_8g_holdout_open_token_v1"
HOLDOUT_COMPLETION_SCHEMA = "external_evidence_8g_holdout_completion_v1"


class ExternalEvidence8GHoldoutError(ValueError):
    """Raised when Phase-8G-F Holdout/OOS governance is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def validate_holdout_protocol(
    protocol: Mapping[str, Any], evaluation_protocol: Mapping[str, Any]
) -> None:
    validate_evaluation_protocol(evaluation_protocol)
    if protocol.get("schema_version") != HOLDOUT_PROTOCOL_SCHEMA:
        raise ExternalEvidence8GHoldoutError("unsupported_holdout_protocol")
    if protocol.get("phase") != "8G-F":
        raise ExternalEvidence8GHoldoutError("holdout_phase_mismatch")
    if protocol.get("research_only") is not True:
        raise ExternalEvidence8GHoldoutError("8g_f_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GHoldoutError("production_integration_must_remain_disabled")
    if tuple(protocol.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GHoldoutError("active_factor_family_mismatch")
    if tuple(int(x) for x in protocol.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GHoldoutError("horizon_family_mismatch")
    family = list(protocol.get("frozen_hypothesis_family") or ())
    if family != list(evaluation_protocol.get("frozen_hypothesis_family") or ()):
        raise ExternalEvidence8GHoldoutError("holdout_family_must_match_8g_e")
    if int(protocol["multiple_testing"]["family_size"]) != len(family):
        raise ExternalEvidence8GHoldoutError("holdout_holm_family_size_mismatch")
    if protocol["multiple_testing"].get("method") != "Holm":
        raise ExternalEvidence8GHoldoutError("holdout_holm_required")
    if float(protocol["multiple_testing"].get("family_wise_alpha")) != 0.05:
        raise ExternalEvidence8GHoldoutError("holdout_family_wise_alpha_must_be_0_05")
    if protocol["completion_gate"].get("next_subphase") != "8G-G_PROSPECTIVE_CONFIRMATION":
        raise ExternalEvidence8GHoldoutError("roadmap_after_8g_f_must_be_8g_g")
    guards = protocol.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GHoldoutError("8g_f_definition_guards_must_remain_false")


def validate_holdout_seed_ledger(
    ledger: Mapping[str, Any], protocol: Mapping[str, Any]
) -> None:
    if ledger.get("schema_version") != HOLDOUT_LEDGER_SCHEMA:
        raise ExternalEvidence8GHoldoutError("unsupported_holdout_ledger")
    if ledger.get("phase") != "8G-F":
        raise ExternalEvidence8GHoldoutError("holdout_ledger_phase_mismatch")
    if ledger.get("append_only") is not True:
        raise ExternalEvidence8GHoldoutError("holdout_ledger_must_be_append_only")
    streams = ledger.get("streams")
    if not isinstance(streams, Mapping):
        raise ExternalEvidence8GHoldoutError("holdout_ledger_streams_missing")
    family = list(protocol["frozen_hypothesis_family"])
    if list(streams.keys()) != family:
        raise ExternalEvidence8GHoldoutError("holdout_ledger_family_mismatch")
    for hypothesis, stream in streams.items():
        if not isinstance(stream, Mapping):
            raise ExternalEvidence8GHoldoutError(f"holdout_ledger_stream_invalid:{hypothesis}")
        if stream.get("state") not in {"SEALED", "CONSUMED"}:
            raise ExternalEvidence8GHoldoutError(f"holdout_ledger_state_invalid:{hypothesis}")
        if stream.get("state") == "SEALED" and stream.get("evaluation_sha256") is not None:
            raise ExternalEvidence8GHoldoutError(f"sealed_stream_has_evaluation:{hypothesis}")
        if stream.get("state") == "CONSUMED" and not stream.get("evaluation_sha256"):
            raise ExternalEvidence8GHoldoutError(f"consumed_stream_missing_evaluation:{hypothesis}")


def _validated_validation_family(
    validation_results: Mapping[str, Mapping[str, Any]],
    validation_receipt: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    validate_evaluation_protocol(evaluation_protocol)
    if validation_receipt.get("schema_version") != VALIDATION_FAMILY_SCHEMA:
        raise ExternalEvidence8GHoldoutError("frozen_validation_family_receipt_required")
    if validation_receipt.get("frozen") is not True:
        raise ExternalEvidence8GHoldoutError("validation_family_not_frozen")
    if validation_receipt.get("holdout_open_allowed") is not True:
        raise ExternalEvidence8GHoldoutError("validation_receipt_does_not_allow_holdout")
    if validation_receipt.get("holdout_spec_changes_allowed") is not False:
        raise ExternalEvidence8GHoldoutError("holdout_spec_changes_must_be_forbidden")
    family = list(evaluation_protocol["frozen_hypothesis_family"])
    if list(validation_receipt.get("family_members") or ()) != family:
        raise ExternalEvidence8GHoldoutError("validation_receipt_family_mismatch")
    adjusted = holm_adjust_family(validation_results, evaluation_protocol)
    if _digest(adjusted) != str(validation_receipt.get("results_sha256")):
        raise ExternalEvidence8GHoldoutError("validation_results_hash_mismatch")
    if _digest(evaluation_protocol) != str(validation_receipt.get("evaluation_protocol_sha256")):
        raise ExternalEvidence8GHoldoutError("validation_evaluation_protocol_hash_mismatch")
    for hypothesis, result in adjusted.items():
        if "split" in result and str(result.get("split")).upper() != "VALIDATION":
            raise ExternalEvidence8GHoldoutError(
                f"validation_family_contains_non_validation_result:{hypothesis}"
            )
    return adjusted


def freeze_holdout_candidates(
    *,
    validation_results: Mapping[str, Mapping[str, Any]],
    validation_receipt: Mapping[str, Any],
    protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze the Holdout candidate set using Validation only."""
    validate_holdout_protocol(protocol, evaluation_protocol)
    adjusted = _validated_validation_family(
        validation_results, validation_receipt, evaluation_protocol
    )
    family = list(protocol["frozen_hypothesis_family"])
    candidates: list[str] = []
    states: dict[str, str] = {}
    for hypothesis in family:
        result = adjusted[hypothesis]
        selected = bool(
            result.get("minimum_evidence_met") is True
            and result.get("primary_effect") is not None
            and float(result["primary_effect"]) > 0
            and result.get("holm_positive") is True
        )
        states[hypothesis] = "HOLDOUT_CANDIDATE" if selected else "NOT_SELECTED_FROM_VALIDATION"
        if selected:
            candidates.append(hypothesis)
    authorization: dict[str, Any] = {
        "schema_version": HOLDOUT_AUTHORIZATION_SCHEMA,
        "phase": "8G-F",
        "frozen": True,
        "validation_receipt_sha256": str(validation_receipt.get("receipt_sha256") or ""),
        "validation_results_sha256": _digest(adjusted),
        "evaluation_protocol_sha256": _digest(evaluation_protocol),
        "holdout_protocol_sha256": _digest(protocol),
        "family_members": family,
        "candidate_hypotheses": candidates,
        "hypothesis_states": states,
        "candidate_set_changes_allowed": False,
        "holdout_outcomes_read_while_freezing_candidates": False,
        "state": "NO_VALIDATION_CANDIDATES" if not candidates else "HOLDOUT_CANDIDATES_FROZEN",
    }
    if not authorization["validation_receipt_sha256"]:
        raise ExternalEvidence8GHoldoutError("validation_receipt_hash_missing")
    authorization["authorization_sha256"] = _digest(authorization)
    return authorization


def authorize_holdout_open(
    *,
    hypothesis_id: str,
    candidate_authorization: Mapping[str, Any],
    ledger: Mapping[str, Any],
    protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Issue a token before any Holdout outcome values may be read."""
    validate_holdout_protocol(protocol, evaluation_protocol)
    validate_holdout_seed_ledger(ledger, protocol)
    if candidate_authorization.get("schema_version") != HOLDOUT_AUTHORIZATION_SCHEMA:
        raise ExternalEvidence8GHoldoutError("frozen_holdout_candidate_authorization_required")
    if candidate_authorization.get("frozen") is not True:
        raise ExternalEvidence8GHoldoutError("holdout_candidate_authorization_not_frozen")
    if str(candidate_authorization.get("holdout_protocol_sha256")) != _digest(protocol):
        raise ExternalEvidence8GHoldoutError("holdout_protocol_hash_mismatch")
    if str(candidate_authorization.get("evaluation_protocol_sha256")) != _digest(evaluation_protocol):
        raise ExternalEvidence8GHoldoutError("evaluation_protocol_hash_mismatch")
    if hypothesis_id not in set(candidate_authorization.get("candidate_hypotheses") or ()):
        raise ExternalEvidence8GHoldoutError("non_candidate_holdout_access_forbidden")
    stream = ledger["streams"].get(hypothesis_id)
    if not isinstance(stream, Mapping):
        raise ExternalEvidence8GHoldoutError("holdout_hypothesis_not_registered")
    if stream.get("state") != "SEALED":
        raise ExternalEvidence8GHoldoutError("holdout_may_open_once_only")
    token: dict[str, Any] = {
        "schema_version": HOLDOUT_TOKEN_SCHEMA,
        "phase": "8G-F",
        "hypothesis_id": hypothesis_id,
        "authorization_sha256": str(candidate_authorization.get("authorization_sha256")),
        "ledger_before_sha256": _digest(ledger),
        "one_shot": True,
        "model_refit_allowed": False,
        "feature_change_allowed": False,
        "threshold_change_allowed": False,
        "horizon_reselection_allowed": False,
        "holdout_values_read_before_token": False,
    }
    token["token_sha256"] = _digest(token)
    return token


def consume_evaluated_holdout(
    *,
    ledger: Mapping[str, Any],
    holdout_result: Mapping[str, Any],
    open_token: Mapping[str, Any],
    candidate_authorization: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Consume one already-authorized Holdout result exactly once."""
    validate_holdout_seed_ledger(ledger, protocol)
    if open_token.get("schema_version") != HOLDOUT_TOKEN_SCHEMA:
        raise ExternalEvidence8GHoldoutError("holdout_open_token_required")
    hypothesis = str(open_token.get("hypothesis_id") or "")
    if hypothesis != str(holdout_result.get("hypothesis_id") or ""):
        raise ExternalEvidence8GHoldoutError("holdout_result_token_identity_mismatch")
    if hypothesis not in set(candidate_authorization.get("candidate_hypotheses") or ()):
        raise ExternalEvidence8GHoldoutError("holdout_result_not_authorized_candidate")
    if str(open_token.get("authorization_sha256")) != str(candidate_authorization.get("authorization_sha256")):
        raise ExternalEvidence8GHoldoutError("holdout_token_authorization_hash_mismatch")
    if str(open_token.get("ledger_before_sha256")) != _digest(ledger):
        raise ExternalEvidence8GHoldoutError("holdout_token_ledger_hash_mismatch")
    if holdout_result.get("schema_version") != EVALUATION_RESULT_SCHEMA:
        raise ExternalEvidence8GHoldoutError("unsupported_holdout_result")
    if str(holdout_result.get("split")).upper() != "HOLDOUT":
        raise ExternalEvidence8GHoldoutError("holdout_result_split_required")
    if holdout_result.get("model_refit") is not False:
        raise ExternalEvidence8GHoldoutError("holdout_model_refit_forbidden")
    evaluation_sha = str(holdout_result.get("evaluation_sha256") or "")
    if not evaluation_sha:
        raise ExternalEvidence8GHoldoutError("holdout_evaluation_hash_missing")
    updated = consume_holdout_once(
        ledger, hypothesis_id=hypothesis, evaluation_sha256=evaluation_sha
    )
    receipt: dict[str, Any] = {
        "schema_version": "external_evidence_8g_holdout_consumption_receipt_v1",
        "phase": "8G-F",
        "hypothesis_id": hypothesis,
        "token_sha256": str(open_token.get("token_sha256")),
        "evaluation_sha256": evaluation_sha,
        "ledger_before_sha256": _digest(ledger),
        "ledger_after_sha256": _digest(updated),
        "consumed_once": True,
    }
    receipt["receipt_sha256"] = _digest(receipt)
    return updated, receipt


def _candidate_confirmation_state(
    validation: Mapping[str, Any], holdout: Mapping[str, Any]
) -> str:
    if holdout.get("minimum_evidence_met") is not True:
        return "INSUFFICIENT_EVIDENCE"
    ci = holdout.get("primary_ci_95") or [None, None]
    ci_positive = ci[0] is not None and float(ci[0]) > 0
    direction_agrees = bool(
        validation.get("primary_effect") is not None
        and holdout.get("primary_effect") is not None
        and float(validation["primary_effect"]) > 0
        and float(holdout["primary_effect"]) > 0
    )
    non_overlap_ok = not bool(
        (holdout.get("non_overlap_sensitivity") or {}).get("materially_contradicts_primary")
    )
    coverage_ok = (holdout.get("coverage_missingness") or {}).get("status") == "COMPLETE"
    diagnostics_ok = all(
        (holdout.get(key) or {}).get("status") in {"OK", "DIAGNOSTIC_CONTEXT_INCOMPLETE"}
        for key in ("regime_stability", "sector_stability", "currency_stability")
    )
    pit_ok = holdout.get("pit_integrity_pass") is True
    confirmed = bool(validation.get("holm_positive")) and bool(holdout.get("holm_positive"))
    if confirmed and ci_positive and direction_agrees and non_overlap_ok and coverage_ok and diagnostics_ok and pit_ok:
        return "HOLDOUT_CONFIRMED"
    return "HOLDOUT_NOT_CONFIRMED"


def finalize_holdout_family(
    *,
    validation_results: Mapping[str, Mapping[str, Any]],
    validation_receipt: Mapping[str, Any],
    holdout_results: Mapping[str, Mapping[str, Any]],
    candidate_authorization: Mapping[str, Any],
    ledger: Mapping[str, Any],
    protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Finalize 8G-F while retaining the full 12-member Holm family."""
    validate_holdout_protocol(protocol, evaluation_protocol)
    validate_holdout_seed_ledger(ledger, protocol)
    validation = _validated_validation_family(
        validation_results, validation_receipt, evaluation_protocol
    )
    if candidate_authorization.get("schema_version") != HOLDOUT_AUTHORIZATION_SCHEMA:
        raise ExternalEvidence8GHoldoutError("frozen_holdout_candidate_authorization_required")
    if str(candidate_authorization.get("validation_results_sha256")) != _digest(validation):
        raise ExternalEvidence8GHoldoutError("holdout_authorization_validation_hash_mismatch")
    candidates = list(candidate_authorization.get("candidate_hypotheses") or ())
    candidate_set = set(candidates)
    unexpected = sorted(set(holdout_results).difference(candidate_set))
    if unexpected:
        raise ExternalEvidence8GHoldoutError(
            "non_candidate_holdout_results_forbidden:" + ",".join(unexpected)
        )
    for hypothesis, result in holdout_results.items():
        if str(result.get("split")).upper() != "HOLDOUT":
            raise ExternalEvidence8GHoldoutError(f"non_holdout_result_in_8g_f:{hypothesis}")
        stream = ledger["streams"].get(hypothesis) or {}
        if stream.get("state") != "CONSUMED":
            raise ExternalEvidence8GHoldoutError(f"holdout_result_without_consumption:{hypothesis}")
        if str(stream.get("evaluation_sha256")) != str(result.get("evaluation_sha256")):
            raise ExternalEvidence8GHoldoutError(f"holdout_ledger_result_hash_mismatch:{hypothesis}")

    holdout_family = holm_adjust_family(holdout_results, evaluation_protocol)
    states: dict[str, str] = {}
    for hypothesis in protocol["frozen_hypothesis_family"]:
        if hypothesis not in candidate_set:
            states[hypothesis] = "NOT_SELECTED_FROM_VALIDATION"
        elif hypothesis not in holdout_results:
            states[hypothesis] = "HOLDOUT_PENDING"
        else:
            states[hypothesis] = _candidate_confirmation_state(
                validation[hypothesis], holdout_family[hypothesis]
            )

    pending = [hypothesis for hypothesis in candidates if states[hypothesis] == "HOLDOUT_PENDING"]
    complete = not pending
    if not candidates:
        overall = "NO_VALIDATION_CANDIDATES"
    elif pending:
        overall = "HOLDOUT_INCOMPLETE"
    else:
        overall = "HOLDOUT_COMPLETE"
    completion: dict[str, Any] = {
        "schema_version": HOLDOUT_COMPLETION_SCHEMA,
        "phase": "8G-F",
        "state": overall,
        "empirically_complete": complete,
        "candidate_hypotheses": candidates,
        "pending_candidates": pending,
        "hypothesis_states": states,
        "validation_results_sha256": _digest(validation),
        "holdout_family_sha256": _digest(holdout_family),
        "ledger_sha256": _digest(ledger),
        "holm_family_size": len(protocol["frozen_hypothesis_family"]),
        "non_candidate_holdout_streams_remain_sealed": all(
            ledger["streams"][hypothesis]["state"] == "SEALED"
            for hypothesis in protocol["frozen_hypothesis_family"]
            if hypothesis not in candidate_set
        ),
        "next_subphase": "8G-G_PROSPECTIVE_CONFIRMATION",
    }
    completion["completion_sha256"] = _digest(completion)
    return completion
