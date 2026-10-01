"""Typed Phase-5 Confidence shadow/promotion context for the Decision Layer.

W5 exposes the already-existing Phase-5E promotion state without activating the
adaptive shadow policy.  The adapter transports governance/evidence-readiness
only; it never evaluates the policy for the current symbol and never creates a
directional vote, stance or portfolio action.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping


PHASE5_PHASE = "5E_adaptive_shadow_and_promotion"
PHASE5_SCHEMA = "phase5e_adaptive_shadow_v1"
PHASE5_CONTEXT_TYPE = "phase5_confidence_shadow_governance_v1"
PHASE5_HORIZONS = (5, 20, 40, 60)
ELIGIBLE_REVIEW_STATUS = "eligible_for_separate_promotion_review"
INSUFFICIENT_STATUS = "insufficient_evidence"
FORBIDDEN_KEYS = frozenset({"direction", "stance", "vote", "attractiveness"})


class Phase5ShadowAdapterError(ValueError):
    """Raised when Phase-5 shadow state cannot be admitted safely."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise Phase5ShadowAdapterError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Phase5ShadowAdapterError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Phase5ShadowAdapterError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _strip_forbidden(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_forbidden(item)
            for key, item in value.items()
            if str(key) not in FORBIDDEN_KEYS
        }
    if isinstance(value, list):
        return [_strip_forbidden(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_forbidden(item) for item in value]
    return deepcopy(value)


def phase5_shadow_context(
    report: Mapping[str, object],
    *,
    source_commit: str,
    available_from: str,
) -> dict[str, object]:
    """Validate Phase-5E and return a compact non-activating shadow context."""
    if report.get("phase") != PHASE5_PHASE:
        raise Phase5ShadowAdapterError("phase5_report_identity_invalid")
    if report.get("schema_version") != PHASE5_SCHEMA:
        raise Phase5ShadowAdapterError("phase5_report_schema_invalid")
    if report.get("technical_phase5_complete") is not True:
        raise Phase5ShadowAdapterError("phase5_technical_completion_missing")

    source_commit = str(source_commit or "").strip()
    if len(source_commit) < 12:
        raise Phase5ShadowAdapterError("phase5_source_commit_required")
    available = _utc(available_from, "phase5_available_from")

    semantics = report.get("semantics")
    if not isinstance(semantics, Mapping):
        raise Phase5ShadowAdapterError("phase5_semantics_required")
    required_true = (
        "research_only",
        "adaptive_shadow_inference_active",
        "promotion_is_horizon_specific",
        "eligible_status_requires_separate_promotion_review",
    )
    required_false = (
        "production_confidence_changed",
        "adaptive_production_weights_created",
        "portfolio_or_depot_watch_changed",
        "production_change_performed",
    )
    if any(semantics.get(key) is not True for key in required_true):
        raise Phase5ShadowAdapterError("phase5_shadow_guard_missing")
    if any(semantics.get(key) is not False for key in required_false):
        raise Phase5ShadowAdapterError("phase5_unpromoted_guard_invalid")

    raw_horizons = report.get("horizons")
    if not isinstance(raw_horizons, Mapping) or set(map(str, raw_horizons.keys())) != {str(h) for h in PHASE5_HORIZONS}:
        raise Phase5ShadowAdapterError("phase5_horizon_set_invalid")

    horizon_state: dict[str, object] = {}
    eligible_horizons: list[int] = []
    insufficient_horizons: list[int] = []
    for horizon in PHASE5_HORIZONS:
        item = raw_horizons.get(str(horizon))
        if not isinstance(item, Mapping):
            raise Phase5ShadowAdapterError(f"phase5_horizon_missing:{horizon}")
        promotion = item.get("promotion")
        if not isinstance(promotion, Mapping):
            raise Phase5ShadowAdapterError(f"phase5_promotion_missing:{horizon}")
        status = str(promotion.get("status") or "").strip()
        if status not in {INSUFFICIENT_STATUS, ELIGIBLE_REVIEW_STATUS}:
            raise Phase5ShadowAdapterError(f"phase5_unknown_promotion_status:{horizon}:{status}")
        if promotion.get("production_change_performed") is not False:
            raise Phase5ShadowAdapterError(f"phase5_production_change_not_allowed:{horizon}")
        gates = promotion.get("gates")
        if not isinstance(gates, Mapping):
            raise Phase5ShadowAdapterError(f"phase5_promotion_gates_missing:{horizon}")
        if status == ELIGIBLE_REVIEW_STATUS:
            eligible_horizons.append(horizon)
        else:
            insufficient_horizons.append(horizon)
        horizon_state[str(horizon)] = {
            "status": status,
            "gates": deepcopy(dict(gates)),
            "robust_promotion_epochs": int(promotion.get("robust_promotion_epochs") or 0),
            "production_change_performed": False,
        }

    integration_mode = (
        "eligible_after_promotion_review" if eligible_horizons else "shadow_only"
    )
    maturity_state = (
        "not_yet_mature" if eligible_horizons else "insufficient_evidence"
    )
    coverage_state = "limited" if eligible_horizons else "insufficient"
    policy = report.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    context = _strip_forbidden({
        "context_type": PHASE5_CONTEXT_TYPE,
        "phase": PHASE5_PHASE,
        "schema_version": PHASE5_SCHEMA,
        "source_commit": source_commit,
        "source_available_from": available.isoformat(),
        "status": str(report.get("status") or ""),
        "technical_phase5_complete": True,
        "claims": int(report.get("claims") or 0),
        "mature_outcomes": int(report.get("mature_outcomes") or 0),
        "phase5c_versions": int(report.get("phase5c_versions") or 0),
        "phase5d_finalized_evaluations": int(report.get("phase5d_finalized_evaluations") or 0),
        "policy_version": str(policy.get("version") or ""),
        "policy_sha256": str(policy.get("sha256") or ""),
        "horizons": horizon_state,
        "eligible_horizons_for_separate_promotion_review": eligible_horizons,
        "insufficient_evidence_horizons": insufficient_horizons,
        "integration_mode": integration_mode,
        "research_only": True,
        "adaptive_shadow_inference_active": True,
        "production_confidence_changed": False,
        "production_change_performed": False,
        "current_symbol_shadow_policy_evaluated": False,
        "current_symbol_shadow_policy_output": None,
        "changes_universal_stance": False,
        "changes_portfolio_action": False,
        "separate_promotion_review_required": True,
    })
    assert isinstance(context, Mapping)
    return {
        "context": dict(context),
        "integration_mode": integration_mode,
        "maturity_state": maturity_state,
        "coverage_state": coverage_state,
        "available_from": available.isoformat(),
        "source_version": f"{PHASE5_SCHEMA}:{str(policy.get('sha256') or '')[:16]}:{source_commit[:12]}",
    }


def build_phase5_shadow_row(
    *,
    symbol: str,
    selection_claim_id: str,
    report: Mapping[str, object],
    source_commit: str,
    available_from: str,
) -> dict[str, object]:
    """Build one packet-local Phase-5 shadow annotation anchored to Selection."""
    normalized = phase5_shadow_context(
        report,
        source_commit=source_commit,
        available_from=available_from,
    )
    return {
        "family": "confidence",
        "claim_id": f"confidence:{symbol}:phase5-shadow:{source_commit[:12]}",
        "claim_ref": selection_claim_id,
        "as_of": normalized["available_from"],
        "available_from": normalized["available_from"],
        "source_version": normalized["source_version"],
        "coverage_state": normalized["coverage_state"],
        "maturity_state": normalized["maturity_state"],
        "pit_state": "verified",
        "integration_mode": normalized["integration_mode"],
        "payload": normalized["context"],
    }
