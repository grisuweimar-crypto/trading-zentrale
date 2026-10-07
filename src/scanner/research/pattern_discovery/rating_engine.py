"""Phase L10 Pattern Rating Engine.

L10 is a research-only lifecycle interpretation layer over immutable L5 pattern
versions and unchanged, hash-verified L9 confirmation/falsification reports.
It does not recompute confirmation statistics, rewrite L9 result classes, use
Discovery evidence as confirmation, promote patterns, or create productive
Decision/Portfolio/Execution authority.

Ratings are lifecycle gates, not a mathematical performance score:
D = Discovery, C = Challenger, B = Confirmed, A = Robust,
U = Unresolved/Insufficient, F = Falsified/Retired.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .confirmation_engine import verify_confirmation_look


SCHEMA_VERSION = "pattern_discovery_l10_rating_engine_v1"
HISTORY_SCHEMA_VERSION = "pattern_discovery_l10_rating_history_v1"
TRANSITION_SCHEMA_VERSION = "pattern_discovery_l10_rating_transition_v1"
REGISTRY_EVENT_SCHEMA_VERSION = "pattern_discovery_l10_rating_registry_event_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l10_rating_engine_v1.json"
)


class RatingEngineError(ValueError):
    """Raised when an L10 rating invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise RatingEngineError(f"value_required:{field}")
    return result


def _sha256_text(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if len(text) != 64 or any(
        ch not in "0123456789abcdef" for ch in text
    ):
        raise RatingEngineError(f"sha256_required:{field}")
    return text


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RatingEngineError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise RatingEngineError(f"timezone_required:{field}")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_rating_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RatingEngineError(
            f"rating_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise RatingEngineError("rating_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise RatingEngineError("rating_contract_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise RatingEngineError("rating_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise RatingEngineError("rating_execution_forbidden")
    ratings = payload.get("ratings")
    if (
        not isinstance(ratings, Mapping)
        or set(ratings) != {"D", "C", "B", "A", "U", "F"}
    ):
        raise RatingEngineError("rating_state_set_invalid")
    if (
        payload.get("principles", {}).get(
            "rating_is_not_performance_score"
        )
        is not True
    ):
        raise RatingEngineError(
            "rating_score_separation_guard_missing"
        )
    return payload


def rating_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    return _hash(value)


def _verify_frozen_pattern(
    pattern: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(pattern, Mapping):
        raise RatingEngineError("frozen_pattern_must_be_object")
    if (
        pattern.get("schema_version")
        != "pattern_discovery_l5_frozen_pattern_v1"
    ):
        raise RatingEngineError("frozen_pattern_schema_invalid")
    if pattern.get("research_only") is not True:
        raise RatingEngineError(
            "frozen_pattern_research_only_guard_missing"
        )
    if pattern.get("productive_integration_enabled") is not False:
        raise RatingEngineError(
            "frozen_pattern_productive_integration_forbidden"
        )
    if pattern.get("execution_allowed") is not False:
        raise RatingEngineError(
            "frozen_pattern_execution_forbidden"
        )
    if pattern.get("rating") is not None:
        raise RatingEngineError(
            "l5_frozen_pattern_rating_must_remain_unassigned"
        )
    if pattern.get("confirmation_data_used") is not False:
        raise RatingEngineError(
            "l5_discovery_confirmation_boundary_invalid"
        )
    pattern_id = _text(pattern.get("pattern_id"), "pattern_id")
    version = _text(
        pattern.get("pattern_version"), "pattern_version"
    )
    spec_hash = _sha256_text(
        pattern.get("pattern_spec_hash"), "pattern_spec_hash"
    )
    stored = _sha256_text(
        pattern.get("frozen_record_hash"), "frozen_record_hash"
    )
    body = dict(pattern)
    body.pop("frozen_record_hash", None)
    if _hash(body) != stored:
        raise RatingEngineError("frozen_pattern_hash_mismatch")
    freeze_at = _timestamp(
        pattern.get("freeze_timestamp"), "freeze_timestamp"
    )
    PatternDiscoveryBoundary().assert_research_payload(pattern)
    return {
        "pattern_id": pattern_id,
        "pattern_version": version,
        "pattern_spec_hash": spec_hash,
        "frozen_record_hash": stored,
        "freeze_timestamp": freeze_at,
    }


def _pattern_result(
    report: Mapping[str, Any],
    identity: Mapping[str, str],
) -> Mapping[str, Any] | None:
    matches = [
        row
        for row in (report.get("pattern_results") or [])
        if row.get("pattern_id") == identity["pattern_id"]
        and row.get("pattern_version")
        == identity["pattern_version"]
    ]
    if len(matches) > 1:
        raise RatingEngineError(
            "duplicate_pattern_result_in_l9_look"
        )
    if not matches:
        return None
    row = matches[0]
    if row.get("pattern_spec_hash") != identity["pattern_spec_hash"]:
        raise RatingEngineError(
            "l9_pattern_spec_hash_mismatch_l5"
        )
    return row


def _unresolved_applies(
    report: Mapping[str, Any],
    identity: Mapping[str, str],
) -> bool:
    readiness = report.get("family_readiness") or {}
    if not isinstance(readiness, Mapping):
        return False
    matches = [
        row
        for row in readiness.values()
        if isinstance(row, Mapping)
        and row.get("pattern_id") == identity["pattern_id"]
        and row.get("pattern_version")
        == identity["pattern_version"]
    ]
    if len(matches) > 1:
        raise RatingEngineError(
            "duplicate_pattern_readiness_in_l9_look"
        )
    return bool(matches)


def _selected_metrics(
    result: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = result.get("prospective_evidence") or {}
    if not isinstance(evidence, Mapping):
        raise RatingEngineError(
            "l9_prospective_evidence_missing"
        )
    return {
        "raw_n": evidence.get("raw_n"),
        "effective_n": evidence.get("effective_n"),
        "symbol_count": evidence.get("symbol_count"),
        "support_region_count": evidence.get(
            "support_region_count"
        ),
        "direction_probability": evidence.get(
            "direction_probability"
        ),
        "baseline_probability": evidence.get(
            "baseline_probability"
        ),
        "probability_advantage_lift": evidence.get(
            "probability_advantage_lift"
        ),
        "mean_aligned_outcome": evidence.get(
            "mean_aligned_outcome"
        ),
        "effect_size_vs_baseline": evidence.get(
            "effect_size_vs_baseline"
        ),
        "robust_uncertainty": evidence.get(
            "robust_uncertainty"
        ),
        "concentration": evidence.get("concentration"),
        "temporal_stability": evidence.get(
            "temporal_stability"
        ),
        "regime_diagnostics": evidence.get(
            "regime_diagnostics"
        ),
        "confirmation_period": evidence.get(
            "confirmation_period"
        ),
    }


def _initial_snapshot(
    pattern: Mapping[str, Any],
    identity: Mapping[str, str],
) -> dict[str, Any]:
    snapshot = {
        "source_type": "L5_FROZEN_PATTERN",
        "source_phase": "L5",
        "pattern_id": identity["pattern_id"],
        "pattern_version": identity["pattern_version"],
        "pattern_spec_hash": identity["pattern_spec_hash"],
        "frozen_record_hash": identity["frozen_record_hash"],
        "freeze_timestamp": identity["freeze_timestamp"],
        "discovery_run_id": pattern.get("discovery_run_id"),
        "l4_evidence_hash": pattern.get("l4_evidence_hash"),
        "discovery_evidence_used_as_confirmation": False,
    }
    snapshot["evidence_snapshot_hash"] = _hash(snapshot)
    return snapshot


def _l9_snapshot(
    report: Mapping[str, Any],
    result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    governance = report.get("qm_governance") or {}
    next_look = governance.get("next_look") or {}
    base: dict[str, Any] = {
        "source_type": "L9_CONFIRMATION_LOOK",
        "source_phase": "L9",
        "confirmation_look_id": report.get(
            "confirmation_look_id"
        ),
        "look_hash": report.get("look_hash"),
        "confirmation_evidence_hash": report.get(
            "confirmation_evidence_hash"
        ),
        "look_status": report.get("look_status"),
        "evaluated_at": report.get("evaluated_at"),
        "monitoring_plan_id": governance.get(
            "monitoring_plan_id"
        ),
        "monitoring_plan_version": governance.get(
            "monitoring_plan_version"
        ),
        "look_id": (
            governance.get("look_id")
            or next_look.get("look_id")
        ),
        "is_final_look": governance.get("is_final_look"),
        "discovery_evidence_used_as_confirmation": False,
    }
    if result is not None:
        base.update(
            {
                "pattern_id": result.get("pattern_id"),
                "pattern_version": result.get(
                    "pattern_version"
                ),
                "pattern_spec_hash": result.get(
                    "pattern_spec_hash"
                ),
                "result_class": result.get("result_class"),
                "result_reasons": list(
                    result.get("result_reasons") or []
                ),
                "multiple_testing": result.get(
                    "multiple_testing"
                ),
                "prospective_metrics": _selected_metrics(
                    result
                ),
            }
        )
    base["evidence_snapshot_hash"] = _hash(base)
    return base


def _retirement_snapshot(
    identity: Mapping[str, str],
    retirement: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot = {
        "source_type": "FORMAL_RETIREMENT",
        "source_phase": "L10",
        "pattern_id": identity["pattern_id"],
        "pattern_version": identity["pattern_version"],
        "pattern_spec_hash": identity["pattern_spec_hash"],
        "observed_at": _timestamp(
            retirement.get("observed_at"),
            "retirement.observed_at",
        ),
        "retirement_reason": _text(
            retirement.get("reason_code"),
            "retirement.reason_code",
        ),
        "evidence_hash": _sha256_text(
            retirement.get("evidence_hash"),
            "retirement.evidence_hash",
        ),
        "discovery_evidence_used_as_confirmation": False,
    }
    snapshot["evidence_snapshot_hash"] = _hash(snapshot)
    return snapshot


def _positive_challenger_gate(
    result: Mapping[str, Any],
) -> bool:
    if result.get("result_class") != "INCONCLUSIVE":
        return False
    evidence = result.get("prospective_evidence") or {}
    aligned = evidence.get("mean_aligned_outcome")
    lift = evidence.get("probability_advantage_lift")
    if aligned is None or lift is None:
        return False
    return float(aligned) > 0.0 and float(lift) > 0.0


def _supported_final_epochs(
    reports: Sequence[Mapping[str, Any]],
    identity: Mapping[str, str],
) -> set[str]:
    epochs: set[str] = set()
    for report in reports:
        if report.get("look_status") != "EVALUATED":
            continue
        result = _pattern_result(report, identity)
        if (
            result is None
            or result.get("result_class") != "SUPPORTED"
        ):
            continue
        governance = report.get("qm_governance") or {}
        if governance.get("is_final_look") is not True:
            continue
        plan = _text(
            governance.get("monitoring_plan_id"),
            "monitoring_plan_id",
        )
        version = _text(
            governance.get("monitoring_plan_version"),
            "monitoring_plan_version",
        )
        epochs.add(f"{plan}::{version}")
    return epochs


def _derive_next_rating(
    current: str,
    report: Mapping[str, Any],
    result: Mapping[str, Any] | None,
    identity: Mapping[str, str],
    processed_reports: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> tuple[str, list[str]]:
    if (
        current == "F"
        and contract["lifecycle"][
            "f_is_terminal_for_pattern_version"
        ]
    ):
        return "F", ["F_PATTERN_VERSION_IS_TERMINAL"]

    if report.get("look_status") == "UNRESOLVED_NOT_DUE":
        if current in {"D", "U"}:
            return "U", [
                "CONFIRMATION_NOT_DUE_OR_INSUFFICIENT_MATURITY"
            ]
        return current, [
            "NOT_DUE_DOES_NOT_ERASE_ESTABLISHED_RATING"
        ]

    if result is None:
        return current, [
            "L9_LOOK_NOT_APPLICABLE_TO_PATTERN"
        ]

    result_class = _text(
        result.get("result_class"), "result_class"
    )
    if result_class == "SUPPORTED":
        epochs = _supported_final_epochs(
            processed_reports,
            identity,
        )
        minimum = int(
            contract["a_rating"][
                "minimum_distinct_supported_final_monitoring_epochs"
            ]
        )
        if len(epochs) >= minimum:
            return "A", [
                "REPEATED_PROSPECTIVE_CONFIRMATION_ACROSS_DISTINCT_EPOCHS"
            ]
        return "B", ["L9_CONFIRMATION_GATES_PASSED"]

    if result_class == "FALSIFIED":
        return "F", ["L9_ROBUST_FALSIFICATION"]

    if result_class == "NEGATIVE_NOT_CONFIRMED":
        return "F", ["L9_FINAL_EFFECT_NOT_REPLICATED"]

    if result_class == "INCONCLUSIVE":
        if _positive_challenger_gate(result):
            if current == "A":
                return "B", [
                    "ROBUSTNESS_WEAKENED_BY_NEW_INCONCLUSIVE_POSITIVE_EVIDENCE"
                ]
            if current == "B":
                return "C", [
                    "CONFIRMATION_STRENGTH_WEAKENED_TO_CHALLENGER"
                ]
            return "C", [
                "FIRST_POSITIVE_PROSPECTIVE_EVIDENCE_WITH_GATES_UNMET"
            ]
        return "U", [
            "PROSPECTIVE_EVIDENCE_INCONCLUSIVE_OR_INSUFFICIENT"
        ]

    if result_class == "UNRESOLVED_NOT_DUE":
        return "U", [
            "CONFIRMATION_NOT_DUE_OR_INSUFFICIENT_MATURITY"
        ]

    raise RatingEngineError(
        f"unsupported_l9_result_class:{result_class}"
    )


def _transition_allowed(
    previous: str | None,
    current: str,
    contract: Mapping[str, Any],
) -> bool:
    key = "INITIAL" if previous is None else previous
    allowed = contract["lifecycle"][
        "allowed_status_changes"
    ].get(key, [])
    return current in set(allowed)


def _make_transition(
    *,
    identity: Mapping[str, str],
    previous_rating: str | None,
    new_rating: str,
    observed_at: str,
    reason_codes: Sequence[str],
    evidence_snapshot: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    if not _transition_allowed(
        previous_rating,
        new_rating,
        contract,
    ):
        raise RatingEngineError(
            "rating_transition_forbidden:"
            f"{previous_rating or 'INITIAL'}->{new_rating}"
        )
    normalized_reasons = sorted(
        {
            _text(code, "reason_code")
            for code in reason_codes
        }
    )
    if not normalized_reasons:
        raise RatingEngineError(
            "rating_reason_codes_required"
        )

    snapshot = dict(evidence_snapshot)
    snapshot_hash = _sha256_text(
        snapshot.get("evidence_snapshot_hash"),
        "evidence_snapshot_hash",
    )
    snapshot_body = dict(snapshot)
    snapshot_body.pop("evidence_snapshot_hash", None)
    if _hash(snapshot_body) != snapshot_hash:
        raise RatingEngineError(
            "rating_evidence_snapshot_hash_mismatch"
        )

    core = {
        "schema_version": TRANSITION_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L10",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": identity["pattern_id"],
        "pattern_version": identity["pattern_version"],
        "pattern_spec_hash": identity[
            "pattern_spec_hash"
        ],
        "observed_at": _timestamp(
            observed_at,
            "observed_at",
        ),
        "previous_rating": previous_rating,
        "new_rating": new_rating,
        "reason_codes": normalized_reasons,
        "evidence_snapshot": snapshot,
        "rating_contract_hash": rating_contract_hash(
            contract
        ),
        "boundaries": {
            "confirmation_recomputed": False,
            "discovery_evidence_used_as_confirmation": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
            "portfolio_or_execution_effect_created": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(
        core
    )
    core["transition_id"] = (
        "PRT-" + _hash(core)[:24].upper()
    )
    core["transition_hash"] = _hash(core)
    return core


def verify_rating_transition(
    transition: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    if not isinstance(transition, Mapping):
        raise RatingEngineError(
            "rating_transition_must_be_object"
        )
    if (
        transition.get("schema_version")
        != TRANSITION_SCHEMA_VERSION
    ):
        raise RatingEngineError(
            "rating_transition_schema_invalid"
        )
    if transition.get("research_only") is not True:
        raise RatingEngineError(
            "rating_transition_research_only_guard_missing"
        )
    if (
        transition.get("productive_integration_enabled")
        is not False
    ):
        raise RatingEngineError(
            "rating_transition_productive_integration_forbidden"
        )
    if transition.get("execution_allowed") is not False:
        raise RatingEngineError(
            "rating_transition_execution_forbidden"
        )

    previous = transition.get("previous_rating")
    current = _text(
        transition.get("new_rating"),
        "new_rating",
    )
    if previous is not None:
        previous = _text(
            previous,
            "previous_rating",
        )
    if (
        current not in spec["ratings"]
        or (
            previous is not None
            and previous not in spec["ratings"]
        )
    ):
        raise RatingEngineError(
            "rating_transition_state_invalid"
        )
    if not _transition_allowed(
        previous,
        current,
        spec,
    ):
        raise RatingEngineError(
            "rating_transition_not_allowed_by_contract"
        )
    if not transition.get("reason_codes"):
        raise RatingEngineError(
            "rating_transition_reason_codes_missing"
        )

    snapshot = transition.get("evidence_snapshot")
    if not isinstance(snapshot, Mapping):
        raise RatingEngineError(
            "rating_transition_evidence_snapshot_missing"
        )
    snapshot_hash = _sha256_text(
        snapshot.get("evidence_snapshot_hash"),
        "evidence_snapshot_hash",
    )
    snapshot_body = dict(snapshot)
    snapshot_body.pop("evidence_snapshot_hash", None)
    if _hash(snapshot_body) != snapshot_hash:
        raise RatingEngineError(
            "rating_evidence_snapshot_hash_mismatch"
        )

    if (
        transition.get("rating_contract_hash")
        != rating_contract_hash(spec)
    ):
        raise RatingEngineError(
            "rating_transition_contract_hash_mismatch"
        )

    stored = _sha256_text(
        transition.get("transition_hash"),
        "transition_hash",
    )
    body = dict(transition)
    body.pop("transition_hash", None)
    if _hash(body) != stored:
        raise RatingEngineError(
            "rating_transition_hash_mismatch"
        )

    id_body = dict(body)
    id_body.pop("transition_id", None)
    expected_id = (
        "PRT-" + _hash(id_body)[:24].upper()
    )
    if transition.get("transition_id") != expected_id:
        raise RatingEngineError(
            "rating_transition_id_mismatch"
        )

    boundaries = transition.get("boundaries") or {}
    for field in (
        "confirmation_recomputed",
        "discovery_evidence_used_as_confirmation",
        "promotion_performed",
        "decision_layer_integration_performed",
        "portfolio_or_execution_effect_created",
    ):
        if boundaries.get(field) is not False:
            raise RatingEngineError(
                f"rating_transition_boundary_invalid:{field}"
            )

    PatternDiscoveryBoundary().assert_research_payload(
        transition
    )
    return {
        "valid": True,
        "transition_id": transition[
            "transition_id"
        ],
        "transition_hash": stored,
        "new_rating": current,
    }


def build_rating_history(
    frozen_pattern: Mapping[str, Any],
    confirmation_reports: Sequence[Mapping[str, Any]],
    *,
    retirement: Mapping[str, Any] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build deterministic lifecycle history from unchanged L5/L9 evidence."""
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    identity = _verify_frozen_pattern(
        frozen_pattern
    )
    transitions: list[dict[str, Any]] = []
    current = "D"

    initial = _make_transition(
        identity=identity,
        previous_rating=None,
        new_rating="D",
        observed_at=identity["freeze_timestamp"],
        reason_codes=[
            "PATTERN_FROZEN_DISCOVERY_ONLY"
        ],
        evidence_snapshot=_initial_snapshot(
            frozen_pattern,
            identity,
        ),
        contract=spec,
    )
    transitions.append(initial)

    processed_relevant: list[Mapping[str, Any]] = []
    relevant_look_hashes: list[str] = []
    previous_time = datetime.fromisoformat(
        identity["freeze_timestamp"].replace(
            "Z",
            "+00:00",
        )
    )
    seen_looks: set[str] = set()

    for raw_report in confirmation_reports:
        report = dict(raw_report)
        verify_confirmation_look(report)
        look_hash = _sha256_text(
            report.get("look_hash"),
            "l9.look_hash",
        )
        if look_hash in seen_looks:
            raise RatingEngineError(
                "duplicate_l9_look_hash_in_rating_input"
            )
        seen_looks.add(look_hash)

        applies = False
        result: Mapping[str, Any] | None = None
        if report.get("look_status") == "EVALUATED":
            result = _pattern_result(
                report,
                identity,
            )
            applies = result is not None
        elif (
            report.get("look_status")
            == "UNRESOLVED_NOT_DUE"
        ):
            applies = _unresolved_applies(
                report,
                identity,
            )

        if not applies:
            continue

        evaluated = _timestamp(
            report.get("evaluated_at"),
            "l9.evaluated_at",
        )
        evaluated_dt = datetime.fromisoformat(
            evaluated.replace("Z", "+00:00")
        )
        if evaluated_dt < previous_time:
            raise RatingEngineError(
                "l9_rating_inputs_not_chronological_for_pattern"
            )
        if evaluated_dt < datetime.fromisoformat(
            identity["freeze_timestamp"].replace(
                "Z",
                "+00:00",
            )
        ):
            raise RatingEngineError(
                "pre_freeze_l9_report_forbidden"
            )

        previous_time = evaluated_dt
        processed_relevant.append(report)
        relevant_look_hashes.append(look_hash)

        next_rating, reasons = _derive_next_rating(
            current,
            report,
            result,
            identity,
            processed_relevant,
            spec,
        )
        if next_rating != current:
            transition = _make_transition(
                identity=identity,
                previous_rating=current,
                new_rating=next_rating,
                observed_at=evaluated,
                reason_codes=reasons,
                evidence_snapshot=_l9_snapshot(
                    report,
                    result,
                ),
                contract=spec,
            )
            transitions.append(transition)
            current = next_rating

    if retirement is not None:
        allowed_reasons = set(
            spec["retirement"][
                "allowed_reason_codes"
            ]
        )
        reason = _text(
            retirement.get("reason_code"),
            "retirement.reason_code",
        )
        if reason not in allowed_reasons:
            raise RatingEngineError(
                f"retirement_reason_not_allowed:{reason}"
            )
        retired_at = _timestamp(
            retirement.get("observed_at"),
            "retirement.observed_at",
        )
        retired_dt = datetime.fromisoformat(
            retired_at.replace("Z", "+00:00")
        )
        if retired_dt < previous_time:
            raise RatingEngineError(
                "retirement_precedes_latest_rating_evidence"
            )
        if current != "F":
            transitions.append(
                _make_transition(
                    identity=identity,
                    previous_rating=current,
                    new_rating="F",
                    observed_at=retired_at,
                    reason_codes=[reason],
                    evidence_snapshot=(
                        _retirement_snapshot(
                            identity,
                            retirement,
                        )
                    ),
                    contract=spec,
                )
            )
            current = "F"

    history: dict[str, Any] = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L10",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": identity["pattern_id"],
        "pattern_version": identity[
            "pattern_version"
        ],
        "pattern_spec_hash": identity[
            "pattern_spec_hash"
        ],
        "frozen_record_hash": identity[
            "frozen_record_hash"
        ],
        "rating_contract_hash": rating_contract_hash(
            spec
        ),
        "current_rating": current,
        "transition_count": len(transitions),
        "transitions": transitions,
        "source_l9_look_hashes": relevant_look_hashes,
        "boundaries": {
            "l5_pattern_mutation_performed": False,
            "l9_confirmation_mutation_performed": False,
            "confirmation_recomputed": False,
            "discovery_evidence_used_as_confirmation": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
            "portfolio_or_execution_effect_created": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(
        history
    )
    history["history_hash"] = _hash(history)
    return history


def verify_rating_history(
    history: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    if not isinstance(history, Mapping):
        raise RatingEngineError(
            "rating_history_must_be_object"
        )
    if (
        history.get("schema_version")
        != HISTORY_SCHEMA_VERSION
    ):
        raise RatingEngineError(
            "rating_history_schema_invalid"
        )
    if history.get("research_only") is not True:
        raise RatingEngineError(
            "rating_history_research_only_guard_missing"
        )
    if (
        history.get("productive_integration_enabled")
        is not False
    ):
        raise RatingEngineError(
            "rating_history_productive_integration_forbidden"
        )
    if history.get("execution_allowed") is not False:
        raise RatingEngineError(
            "rating_history_execution_forbidden"
        )
    if (
        history.get("rating_contract_hash")
        != rating_contract_hash(spec)
    ):
        raise RatingEngineError(
            "rating_history_contract_hash_mismatch"
        )

    transitions = history.get("transitions")
    if not isinstance(transitions, list) or not transitions:
        raise RatingEngineError(
            "rating_history_transitions_required"
        )
    if history.get("transition_count") != len(
        transitions
    ):
        raise RatingEngineError(
            "rating_history_transition_count_mismatch"
        )

    previous: str | None = None
    previous_time: datetime | None = None
    for index, transition in enumerate(
        transitions
    ):
        verify_rating_transition(
            transition,
            contract=spec,
        )
        if (
            transition.get("pattern_id")
            != history.get("pattern_id")
            or transition.get("pattern_version")
            != history.get("pattern_version")
        ):
            raise RatingEngineError(
                "rating_history_pattern_identity_mismatch:"
                f"{index}"
            )
        if (
            transition.get("pattern_spec_hash")
            != history.get("pattern_spec_hash")
        ):
            raise RatingEngineError(
                "rating_history_pattern_spec_mismatch:"
                f"{index}"
            )
        if (
            transition.get("previous_rating")
            != previous
        ):
            raise RatingEngineError(
                "rating_history_previous_state_mismatch:"
                f"{index}"
            )
        observed = datetime.fromisoformat(
            str(transition["observed_at"]).replace(
                "Z",
                "+00:00",
            )
        )
        if (
            previous_time is not None
            and observed < previous_time
        ):
            raise RatingEngineError(
                "rating_history_not_chronological"
            )
        previous_time = observed
        previous = str(
            transition["new_rating"]
        )

    if history.get("current_rating") != previous:
        raise RatingEngineError(
            "rating_history_current_rating_mismatch"
        )
    if previous not in spec["ratings"]:
        raise RatingEngineError(
            "rating_history_current_rating_invalid"
        )

    boundaries = history.get("boundaries") or {}
    for field in (
        "l5_pattern_mutation_performed",
        "l9_confirmation_mutation_performed",
        "confirmation_recomputed",
        "discovery_evidence_used_as_confirmation",
        "promotion_performed",
        "decision_layer_integration_performed",
        "portfolio_or_execution_effect_created",
    ):
        if boundaries.get(field) is not False:
            raise RatingEngineError(
                f"rating_history_boundary_invalid:{field}"
            )

    stored = _sha256_text(
        history.get("history_hash"),
        "history_hash",
    )
    body = dict(history)
    body.pop("history_hash", None)
    if _hash(body) != stored:
        raise RatingEngineError(
            "rating_history_hash_mismatch"
        )

    PatternDiscoveryBoundary().assert_research_payload(
        history
    )
    return {
        "valid": True,
        "pattern_id": history["pattern_id"],
        "pattern_version": history[
            "pattern_version"
        ],
        "current_rating": history[
            "current_rating"
        ],
        "transition_count": len(transitions),
        "history_hash": stored,
    }


class RatingHistoryRegistry:
    """Append-only hash-chained registry of auditable L10 status changes."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(
            self.path.suffix + ".lock"
        )
        self.contract = (
            dict(contract)
            if contract is not None
            else load_rating_contract()
        )

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(
                encoding="utf-8"
            ).splitlines(),
            start=1,
        ):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RatingEngineError(
                    "rating_registry_invalid_json:"
                    f"{line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise RatingEngineError(
                    "rating_registry_event_not_object:"
                    f"{line_number}"
                )
            events.append(value)
        return events

    def _verify_chain(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        previous_hash: str | None = None
        previous_rating_by_pattern: dict[
            str,
            str,
        ] = {}
        seen_transitions: set[str] = set()

        for sequence, raw in enumerate(
            events,
            start=1,
        ):
            event = dict(raw)
            if (
                event.get("schema_version")
                != REGISTRY_EVENT_SCHEMA_VERSION
            ):
                raise RatingEngineError(
                    "rating_registry_schema_invalid:"
                    f"{sequence}"
                )
            if event.get("sequence") != sequence:
                raise RatingEngineError(
                    "rating_registry_sequence_invalid:"
                    f"{sequence}"
                )
            if (
                event.get("previous_event_hash")
                != previous_hash
            ):
                raise RatingEngineError(
                    "rating_registry_previous_hash_invalid:"
                    f"{sequence}"
                )

            stored = _sha256_text(
                event.get("entry_hash"),
                f"rating_registry.entry_hash.{sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise RatingEngineError(
                    "rating_registry_entry_hash_invalid:"
                    f"{sequence}"
                )

            transition = event.get("transition")
            if not isinstance(
                transition,
                Mapping,
            ):
                raise RatingEngineError(
                    "rating_registry_transition_missing"
                )
            verify_rating_transition(
                transition,
                contract=self.contract,
            )
            transition_id = _text(
                transition.get("transition_id"),
                "transition_id",
            )
            if transition_id in seen_transitions:
                raise RatingEngineError(
                    "rating_registry_duplicate_transition"
                )
            seen_transitions.add(transition_id)

            key = (
                f"{transition['pattern_id']}::"
                f"{transition['pattern_version']}"
            )
            expected_previous = (
                previous_rating_by_pattern.get(
                    key
                )
            )
            if (
                transition.get("previous_rating")
                != expected_previous
            ):
                raise RatingEngineError(
                    "rating_registry_state_chain_invalid:"
                    f"{key}"
                )
            previous_rating_by_pattern[key] = str(
                transition["new_rating"]
            )
            previous_hash = stored

    def register_history(
        self,
        history: Mapping[str, Any],
        *,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        verify_rating_history(
            history,
            contract=self.contract,
        )
        actor_id = _text(
            actor_id,
            "actor_id",
        )
        actor_role = _text(
            actor_role,
            "actor_role",
        )

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(
                    self.lock_path,
                    os.O_CREAT
                    | os.O_EXCL
                    | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError as exc:
                raise RatingEngineError(
                    "rating_registry_lock_already_held"
                ) from exc

            events = self._read()
            self._verify_chain(events)
            existing_by_transition = {
                str(
                    event["transition"][
                        "transition_id"
                    ]
                ): event
                for event in events
            }

            pattern_key = (
                f"{history['pattern_id']}::"
                f"{history['pattern_version']}"
            )
            existing_pattern_ids = [
                str(
                    event["transition"][
                        "transition_id"
                    ]
                )
                for event in events
                if (
                    f"{event['transition']['pattern_id']}::"
                    f"{event['transition']['pattern_version']}"
                    == pattern_key
                )
            ]
            supplied_ids = [
                str(item["transition_id"])
                for item in history["transitions"]
            ]
            common = min(
                len(existing_pattern_ids),
                len(supplied_ids),
            )
            if (
                existing_pattern_ids[:common]
                != supplied_ids[:common]
            ):
                raise RatingEngineError(
                    "rating_history_diverges_from_registry"
                )
            if (
                len(existing_pattern_ids)
                > len(supplied_ids)
            ):
                raise RatingEngineError(
                    "rating_history_stale_against_registry"
                )

            appended = 0
            for transition in history[
                "transitions"
            ]:
                transition_id = str(
                    transition["transition_id"]
                )
                existing = (
                    existing_by_transition.get(
                        transition_id
                    )
                )
                if existing is not None:
                    if (
                        existing.get("transition")
                        != transition
                    ):
                        raise RatingEngineError(
                            "rating_registry_transition_identity_collision"
                        )
                    continue

                event = {
                    "schema_version": (
                        REGISTRY_EVENT_SCHEMA_VERSION
                    ),
                    "sequence": len(events) + 1,
                    "event_id": (
                        "PRE-"
                        + _hash(
                            {
                                "transition_id": (
                                    transition_id
                                ),
                                "sequence": (
                                    len(events) + 1
                                ),
                            }
                        )[:24].upper()
                    ),
                    "event_type": (
                        "PATTERN_RATING_CHANGED"
                    ),
                    "recorded_at": transition[
                        "observed_at"
                    ],
                    "actor_id": actor_id,
                    "actor_role": actor_role,
                    "transition": dict(
                        transition
                    ),
                    "previous_event_hash": (
                        events[-1]["entry_hash"]
                        if events
                        else None
                    ),
                }
                event["entry_hash"] = _hash(
                    event
                )
                candidate = [
                    *events,
                    event,
                ]
                self._verify_chain(candidate)

                fd = os.open(
                    self.path,
                    os.O_CREAT
                    | os.O_APPEND
                    | os.O_WRONLY,
                    0o644,
                )
                try:
                    os.write(
                        fd,
                        (
                            _canonical_json(event)
                            + "\n"
                        ).encode("utf-8"),
                    )
                    os.fsync(fd)
                finally:
                    os.close(fd)

                events.append(event)
                existing_by_transition[
                    transition_id
                ] = event
                appended += 1

            return {
                "valid": True,
                "idempotent": appended == 0,
                "appended_transition_count": (
                    appended
                ),
                "registry_event_count": len(
                    events
                ),
                "head_hash": (
                    events[-1]["entry_hash"]
                    if events
                    else None
                ),
            }
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def verify_integrity(
        self,
    ) -> dict[str, Any]:
        events = self._read()
        self._verify_chain(events)
        return {
            "valid": True,
            "registry_event_count": len(events),
            "head_hash": (
                events[-1]["entry_hash"]
                if events
                else None
            ),
        }


def rating_registry_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    return str(
        spec["storage"][
            "rating_registry_path"
        ]
    )


def rating_history_repo_path(
    history: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    return str(
        spec["storage"][
            "rating_history_path_template"
        ]
    ).format(
        pattern_id=_text(
            history.get("pattern_id"),
            "pattern_id",
        ),
        pattern_version=_text(
            history.get("pattern_version"),
            "pattern_version",
        ),
        history_hash=_sha256_text(
            history.get("history_hash"),
            "history_hash",
        ),
    )


def persist_rating_history(
    repo_root: str | Path,
    history: Mapping[str, Any],
    *,
    actor_id: str,
    actor_role: str,
    contract: Mapping[str, Any] | None = None,
    boundary: PatternDiscoveryBoundary | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_rating_contract()
    )
    verify_rating_history(
        history,
        contract=spec,
    )

    guard = (
        boundary
        or PatternDiscoveryBoundary()
    )
    root = Path(repo_root).resolve()
    report_repo = rating_history_repo_path(
        history,
        contract=spec,
    )
    registry_repo = rating_registry_repo_path(
        contract=spec,
    )
    guard.assert_write_path_allowed(
        report_repo
    )
    guard.assert_write_path_allowed(
        registry_repo
    )

    report_path = (
        root / report_repo
    ).resolve()
    registry_path = (
        root / registry_repo
    ).resolve()
    for path in (
        report_path,
        registry_path,
    ):
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise RatingEngineError(
                "rating_persistence_path_outside_repo"
            ) from exc

    report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    if report_path.exists():
        try:
            existing = json.loads(
                report_path.read_text(
                    encoding="utf-8"
                )
            )
        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise RatingEngineError(
                "existing_rating_history_unreadable"
            ) from exc
        if existing != dict(history):
            raise RatingEngineError(
                "rating_history_identity_collision"
            )
    else:
        with report_path.open(
            "x",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                dict(history),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")

    registry = RatingHistoryRegistry(
        registry_path,
        contract=spec,
    )
    registry_result = registry.register_history(
        history,
        actor_id=actor_id,
        actor_role=actor_role,
    )
    return {
        "valid": True,
        "history_path": report_repo,
        "registry_path": registry_repo,
        "history_hash": history[
            "history_hash"
        ],
        "current_rating": history[
            "current_rating"
        ],
        "registry": registry_result,
    }
