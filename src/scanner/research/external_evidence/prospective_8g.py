"""Phase 8G-G prospective post-Holdout confirmation governance.

Only hypotheses confirmed in 8G-F may enter a new post-Holdout shadow stream.
The candidate set, model, features, thresholds, horizons and variants remain frozen.
Each eligible hypothesis receives one outcome-independent terminal trigger and one
terminal prospective evaluation. The full 12-member Holm family is retained.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Mapping

from scanner.research.external_evidence.evaluation_8g import (
    EVALUATION_RESULT_SCHEMA,
    holm_adjust_family,
    validate_evaluation_protocol,
)
from scanner.research.external_evidence.holdout_8g import (
    HOLDOUT_COMPLETION_SCHEMA,
    validate_holdout_protocol,
)
from scanner.research.external_evidence.research_8g import ACTIVE_FACTORS, HORIZONS


PROSPECTIVE_PROTOCOL_SCHEMA = "external_evidence_8g_prospective_protocol_v1"
PROSPECTIVE_AUTHORIZATION_SCHEMA = "external_evidence_8g_prospective_authorization_v1"
PROSPECTIVE_LEDGER_SCHEMA = "external_evidence_8g_prospective_consumption_ledger_v1"
PROSPECTIVE_TOKEN_SCHEMA = "external_evidence_8g_prospective_terminal_token_v1"
PROSPECTIVE_COMPLETION_SCHEMA = "external_evidence_8g_prospective_completion_v1"


class ExternalEvidence8GProspectiveError(ValueError):
    """Raised when Phase-8G-G prospective governance is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _utc(value: object) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ExternalEvidence8GProspectiveError("prospective_timestamp_missing")
    if len(text) == 10:
        text += "T00:00:00+00:00"
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8GProspectiveError("prospective_timestamp_invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_prospective_protocol(
    protocol: Mapping[str, Any],
    holdout_protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> None:
    validate_evaluation_protocol(evaluation_protocol)
    validate_holdout_protocol(holdout_protocol, evaluation_protocol)
    if protocol.get("schema_version") != PROSPECTIVE_PROTOCOL_SCHEMA:
        raise ExternalEvidence8GProspectiveError("unsupported_prospective_protocol")
    if protocol.get("phase") != "8G-G":
        raise ExternalEvidence8GProspectiveError("prospective_phase_mismatch")
    if protocol.get("research_only") is not True:
        raise ExternalEvidence8GProspectiveError("8g_g_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GProspectiveError("production_integration_must_remain_disabled")
    if protocol.get("automatic_promotion_enabled") is not False:
        raise ExternalEvidence8GProspectiveError("automatic_promotion_must_remain_disabled")
    if tuple(protocol.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GProspectiveError("active_factor_family_mismatch")
    if tuple(int(x) for x in protocol.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GProspectiveError("horizon_family_mismatch")
    family = list(protocol.get("frozen_hypothesis_family") or ())
    if family != list(holdout_protocol.get("frozen_hypothesis_family") or ()):
        raise ExternalEvidence8GProspectiveError("prospective_family_must_match_8g_f")
    if family != list(evaluation_protocol.get("frozen_hypothesis_family") or ()):
        raise ExternalEvidence8GProspectiveError("prospective_family_must_match_8g_e")
    if int(protocol["multiple_testing"]["family_size"]) != len(family):
        raise ExternalEvidence8GProspectiveError("prospective_holm_family_size_mismatch")
    if protocol["multiple_testing"].get("method") != "Holm":
        raise ExternalEvidence8GProspectiveError("prospective_holm_required")
    if float(protocol["multiple_testing"].get("family_wise_alpha")) != 0.05:
        raise ExternalEvidence8GProspectiveError("prospective_family_wise_alpha_must_be_0_05")
    trigger = protocol.get("terminal_evaluation_trigger") or {}
    if int(trigger.get("minimum_paired_n", 0)) != 30:
        raise ExternalEvidence8GProspectiveError("prospective_minimum_paired_n_must_remain_30")
    if int(trigger.get("minimum_temporal_support_regions", 0)) != 2:
        raise ExternalEvidence8GProspectiveError("prospective_temporal_regions_must_remain_2")
    if trigger.get("trigger_must_be_outcome_independent") is not True:
        raise ExternalEvidence8GProspectiveError("prospective_trigger_must_be_outcome_independent")
    if trigger.get("repeated_interim_significance_looks_allowed") is not False:
        raise ExternalEvidence8GProspectiveError("prospective_repeated_significance_looks_forbidden")
    if protocol["completion_gate"].get("next_subphase") != "8G-H_PROMOTION_REVIEW":
        raise ExternalEvidence8GProspectiveError("roadmap_after_8g_g_must_be_8g_h")
    guards = protocol.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GProspectiveError("8g_g_definition_guards_must_remain_false")


def freeze_prospective_authorization(
    *,
    holdout_completion: Mapping[str, Any],
    holdout_cutoff_as_of: str,
    protocol: Mapping[str, Any],
    holdout_protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze the 8G-G eligible set from completed 8G-F only."""
    validate_prospective_protocol(protocol, holdout_protocol, evaluation_protocol)
    if holdout_completion.get("schema_version") != HOLDOUT_COMPLETION_SCHEMA:
        raise ExternalEvidence8GProspectiveError("8g_f_completion_receipt_required")
    if holdout_completion.get("empirically_complete") is not True:
        raise ExternalEvidence8GProspectiveError("8g_f_must_be_empirically_complete")
    if holdout_completion.get("next_subphase") != "8G-G_PROSPECTIVE_CONFIRMATION":
        raise ExternalEvidence8GProspectiveError("8g_f_completion_does_not_authorize_8g_g")
    family = list(protocol["frozen_hypothesis_family"])
    states = holdout_completion.get("hypothesis_states")
    if not isinstance(states, Mapping) or set(states) != set(family):
        raise ExternalEvidence8GProspectiveError("8g_f_hypothesis_states_incomplete")
    cutoff = _utc(holdout_cutoff_as_of)
    eligible = [h for h in family if states[h] == "HOLDOUT_CONFIRMED"]
    authorization: dict[str, Any] = {
        "schema_version": PROSPECTIVE_AUTHORIZATION_SCHEMA,
        "phase": "8G-G",
        "frozen": True,
        "holdout_completion_sha256": str(holdout_completion.get("completion_sha256") or _digest(holdout_completion)),
        "holdout_cutoff_as_of": cutoff.isoformat(),
        "family_members": family,
        "eligible_hypotheses": eligible,
        "hypothesis_states": {
            h: ("PROSPECTIVE_ELIGIBLE" if h in eligible else "NOT_ELIGIBLE_FROM_HOLDOUT")
            for h in family
        },
        "candidate_set_changes_allowed": False,
        "model_refit_allowed": False,
        "feature_change_allowed": False,
        "threshold_change_allowed": False,
        "horizon_reselection_allowed": False,
        "prospective_outcomes_read_while_freezing": False,
    }
    authorization["authorization_sha256"] = _digest(authorization)
    return authorization


def new_prospective_consumption_ledger(
    authorization: Mapping[str, Any], protocol: Mapping[str, Any]
) -> dict[str, Any]:
    if authorization.get("schema_version") != PROSPECTIVE_AUTHORIZATION_SCHEMA:
        raise ExternalEvidence8GProspectiveError("frozen_prospective_authorization_required")
    family = list(protocol["frozen_hypothesis_family"])
    eligible = set(authorization.get("eligible_hypotheses") or ())
    return {
        "schema_version": PROSPECTIVE_LEDGER_SCHEMA,
        "phase": "8G-G",
        "append_only": True,
        "authorization_sha256": str(authorization.get("authorization_sha256")),
        "streams": {
            h: {
                "state": "ARMED" if h in eligible else "SEALED",
                "terminal_evaluation_sha256": None,
            }
            for h in family
        },
    }


def validate_prospective_ledger(
    ledger: Mapping[str, Any], authorization: Mapping[str, Any], protocol: Mapping[str, Any]
) -> None:
    if ledger.get("schema_version") != PROSPECTIVE_LEDGER_SCHEMA:
        raise ExternalEvidence8GProspectiveError("unsupported_prospective_ledger")
    if ledger.get("append_only") is not True:
        raise ExternalEvidence8GProspectiveError("prospective_ledger_must_be_append_only")
    if str(ledger.get("authorization_sha256")) != str(authorization.get("authorization_sha256")):
        raise ExternalEvidence8GProspectiveError("prospective_ledger_authorization_hash_mismatch")
    streams = ledger.get("streams")
    family = list(protocol["frozen_hypothesis_family"])
    if not isinstance(streams, Mapping) or list(streams.keys()) != family:
        raise ExternalEvidence8GProspectiveError("prospective_ledger_family_mismatch")
    eligible = set(authorization.get("eligible_hypotheses") or ())
    for hypothesis in family:
        stream = streams[hypothesis]
        state = stream.get("state")
        if hypothesis not in eligible and state != "SEALED":
            raise ExternalEvidence8GProspectiveError("non_eligible_prospective_stream_must_remain_sealed")
        if hypothesis in eligible and state not in {"ARMED", "CONSUMED"}:
            raise ExternalEvidence8GProspectiveError("eligible_prospective_stream_state_invalid")
        if state == "CONSUMED" and not stream.get("terminal_evaluation_sha256"):
            raise ExternalEvidence8GProspectiveError("consumed_prospective_stream_missing_hash")


def authorize_terminal_evaluation(
    *,
    hypothesis_id: str,
    evidence_summary: Mapping[str, Any],
    authorization: Mapping[str, Any],
    ledger: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Issue the single terminal token using metadata only, before outcome inspection."""
    validate_prospective_ledger(ledger, authorization, protocol)
    if authorization.get("frozen") is not True:
        raise ExternalEvidence8GProspectiveError("prospective_authorization_not_frozen")
    eligible = set(authorization.get("eligible_hypotheses") or ())
    if hypothesis_id not in eligible:
        raise ExternalEvidence8GProspectiveError("non_eligible_prospective_access_forbidden")
    stream = ledger["streams"][hypothesis_id]
    if stream.get("state") != "ARMED":
        raise ExternalEvidence8GProspectiveError("prospective_terminal_evaluation_may_run_once_only")
    forbidden = {
        "primary_effect", "primary_p_value_two_sided", "primary_ci_95", "holm_positive",
        "peer_excess", "loss_improvement", "outcome", "label_value",
    }
    if forbidden.intersection(evidence_summary):
        raise ExternalEvidence8GProspectiveError("terminal_trigger_must_not_inspect_outcome_values")
    if str(evidence_summary.get("split") or "").upper() != "PROSPECTIVE":
        raise ExternalEvidence8GProspectiveError("prospective_split_required")
    n = int(evidence_summary.get("paired_n") or 0)
    regions = int(evidence_summary.get("temporal_support_regions") or 0)
    trigger = protocol["terminal_evaluation_trigger"]
    if n < int(trigger["minimum_paired_n"]):
        raise ExternalEvidence8GProspectiveError("prospective_minimum_evidence_not_met")
    if regions < int(trigger["minimum_temporal_support_regions"]):
        raise ExternalEvidence8GProspectiveError("prospective_temporal_support_not_met")
    first_as_of = _utc(evidence_summary.get("first_as_of"))
    last_as_of = _utc(evidence_summary.get("last_as_of"))
    cutoff = _utc(authorization.get("holdout_cutoff_as_of"))
    if first_as_of <= cutoff:
        raise ExternalEvidence8GProspectiveError("prospective_rows_must_be_strictly_post_holdout")
    if last_as_of < first_as_of:
        raise ExternalEvidence8GProspectiveError("prospective_evidence_window_invalid")
    token: dict[str, Any] = {
        "schema_version": PROSPECTIVE_TOKEN_SCHEMA,
        "phase": "8G-G",
        "hypothesis_id": hypothesis_id,
        "authorization_sha256": str(authorization.get("authorization_sha256")),
        "ledger_before_sha256": _digest(ledger),
        "evidence_summary_sha256": _digest(evidence_summary),
        "one_shot": True,
        "outcomes_read_before_token": False,
        "model_refit_allowed": False,
        "feature_change_allowed": False,
        "threshold_change_allowed": False,
        "horizon_reselection_allowed": False,
    }
    token["token_sha256"] = _digest(token)
    return token


def consume_terminal_result(
    *,
    ledger: Mapping[str, Any],
    result: Mapping[str, Any],
    token: Mapping[str, Any],
    authorization: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_prospective_ledger(ledger, authorization, protocol)
    if token.get("schema_version") != PROSPECTIVE_TOKEN_SCHEMA:
        raise ExternalEvidence8GProspectiveError("prospective_terminal_token_required")
    hypothesis = str(token.get("hypothesis_id") or "")
    if hypothesis != str(result.get("hypothesis_id") or ""):
        raise ExternalEvidence8GProspectiveError("prospective_result_token_identity_mismatch")
    if str(token.get("authorization_sha256")) != str(authorization.get("authorization_sha256")):
        raise ExternalEvidence8GProspectiveError("prospective_token_authorization_hash_mismatch")
    if str(token.get("ledger_before_sha256")) != _digest(ledger):
        raise ExternalEvidence8GProspectiveError("prospective_token_ledger_hash_mismatch")
    if result.get("schema_version") != EVALUATION_RESULT_SCHEMA:
        raise ExternalEvidence8GProspectiveError("unsupported_prospective_result")
    if str(result.get("split") or "").upper() != "PROSPECTIVE":
        raise ExternalEvidence8GProspectiveError("prospective_result_split_required")
    if result.get("model_refit") is not False:
        raise ExternalEvidence8GProspectiveError("prospective_model_refit_forbidden")
    evaluation_sha = str(result.get("evaluation_sha256") or "")
    if not evaluation_sha:
        raise ExternalEvidence8GProspectiveError("prospective_evaluation_hash_missing")
    updated = deepcopy(dict(ledger))
    updated["streams"] = deepcopy(dict(ledger["streams"]))
    stream = deepcopy(dict(updated["streams"][hypothesis]))
    if stream.get("state") != "ARMED":
        raise ExternalEvidence8GProspectiveError("prospective_terminal_evaluation_may_run_once_only")
    stream["state"] = "CONSUMED"
    stream["terminal_evaluation_sha256"] = evaluation_sha
    updated["streams"][hypothesis] = stream
    receipt: dict[str, Any] = {
        "schema_version": "external_evidence_8g_prospective_consumption_receipt_v1",
        "phase": "8G-G",
        "hypothesis_id": hypothesis,
        "token_sha256": str(token.get("token_sha256")),
        "evaluation_sha256": evaluation_sha,
        "ledger_before_sha256": _digest(ledger),
        "ledger_after_sha256": _digest(updated),
        "consumed_once": True,
    }
    receipt["receipt_sha256"] = _digest(receipt)
    return updated, receipt


def finalize_prospective_family(
    *,
    prospective_results: Mapping[str, Mapping[str, Any]],
    holdout_completion: Mapping[str, Any],
    authorization: Mapping[str, Any],
    ledger: Mapping[str, Any],
    protocol: Mapping[str, Any],
    evaluation_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Finalize the frozen 8G-G family without shrinking multiplicity."""
    validate_prospective_ledger(ledger, authorization, protocol)
    if holdout_completion.get("schema_version") != HOLDOUT_COMPLETION_SCHEMA:
        raise ExternalEvidence8GProspectiveError("8g_f_completion_receipt_required")
    eligible = set(authorization.get("eligible_hypotheses") or ())
    unexpected = sorted(set(prospective_results).difference(eligible))
    if unexpected:
        raise ExternalEvidence8GProspectiveError(
            "non_eligible_prospective_results_forbidden:" + ",".join(unexpected)
        )
    for hypothesis, result in prospective_results.items():
        if str(result.get("split") or "").upper() != "PROSPECTIVE":
            raise ExternalEvidence8GProspectiveError(f"non_prospective_result_in_8g_g:{hypothesis}")
        stream = ledger["streams"][hypothesis]
        if stream.get("state") != "CONSUMED":
            raise ExternalEvidence8GProspectiveError(f"prospective_result_without_consumption:{hypothesis}")
        if str(stream.get("terminal_evaluation_sha256")) != str(result.get("evaluation_sha256")):
            raise ExternalEvidence8GProspectiveError(f"prospective_ledger_result_hash_mismatch:{hypothesis}")

    adjusted = holm_adjust_family(prospective_results, evaluation_protocol)
    family = list(protocol["frozen_hypothesis_family"])
    holdout_states = holdout_completion["hypothesis_states"]
    states: dict[str, str] = {}
    for hypothesis in family:
        if hypothesis not in eligible:
            states[hypothesis] = "NOT_ELIGIBLE_FROM_HOLDOUT"
            continue
        if hypothesis not in prospective_results:
            states[hypothesis] = "PROSPECTIVE_PENDING"
            continue
        result = adjusted[hypothesis]
        if result.get("minimum_evidence_met") is not True:
            states[hypothesis] = "INSUFFICIENT_EVIDENCE"
            continue
        ci = result.get("primary_ci_95") or [None, None]
        ci_positive = ci[0] is not None and float(ci[0]) > 0
        effect_positive = result.get("primary_effect") is not None and float(result["primary_effect"]) > 0
        non_overlap_ok = not bool((result.get("non_overlap_sensitivity") or {}).get("materially_contradicts_primary"))
        coverage_ok = (result.get("coverage_missingness") or {}).get("status") == "COMPLETE"
        diagnostics_ok = all(
            (result.get(key) or {}).get("status") in {"OK", "DIAGNOSTIC_CONTEXT_INCOMPLETE"}
            for key in ("regime_stability", "sector_stability", "currency_stability")
        )
        confirmed = bool(
            holdout_states.get(hypothesis) == "HOLDOUT_CONFIRMED"
            and effect_positive
            and result.get("holm_positive") is True
            and ci_positive
            and non_overlap_ok
            and coverage_ok
            and diagnostics_ok
            and result.get("pit_integrity_pass") is True
        )
        states[hypothesis] = "PROSPECTIVE_CONFIRMED" if confirmed else "PROSPECTIVE_NOT_CONFIRMED"

    pending = [h for h in authorization.get("eligible_hypotheses") or () if states[h] == "PROSPECTIVE_PENDING"]
    complete = not pending
    if not eligible:
        overall = "NO_HOLDOUT_CONFIRMED_CANDIDATES"
    elif pending:
        overall = "PROSPECTIVE_INCOMPLETE"
    else:
        overall = "PROSPECTIVE_COMPLETE"
    completion: dict[str, Any] = {
        "schema_version": PROSPECTIVE_COMPLETION_SCHEMA,
        "phase": "8G-G",
        "state": overall,
        "empirically_complete": complete,
        "eligible_hypotheses": list(authorization.get("eligible_hypotheses") or ()),
        "pending_hypotheses": pending,
        "hypothesis_states": states,
        "holm_family_size": len(family),
        "prospective_family_sha256": _digest(adjusted),
        "ledger_sha256": _digest(ledger),
        "automatic_promotion_authorized": False,
        "next_subphase": "8G-H_PROMOTION_REVIEW",
    }
    completion["completion_sha256"] = _digest(completion)
    return completion
