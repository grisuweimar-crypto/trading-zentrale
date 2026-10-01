"""Typed adapter from guarded Phase-4 Confidence-vNext into Phase 7A.

W4 transports the existing guarded ordinal reliability context only. It does
not derive Probability, Risk, direction, attractiveness, a scalar Confidence
score, thresholds or portfolio actions.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping


PHASE4_REPORT_ID = "4_confidence_vnext_empirical_research"
PHASE4_DEFAULT_SOURCE_VERSION = "phase4_confidence_research_v1"
FORBIDDEN_CONFIDENCE_KEYS = frozenset({"direction", "stance", "vote", "attractiveness"})


class Phase4ConfidenceAdapterError(ValueError):
    """Raised when guarded Confidence cannot be bound to the exact 7A snapshot."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise Phase4ConfidenceAdapterError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Phase4ConfidenceAdapterError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Phase4ConfidenceAdapterError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _calendar_date(value: object, field: str) -> str:
    """Canonicalize a daily as-of value without weakening day identity.

    Phase-4's legacy PIT compatibility layer normalizes current scanner ``as_of``
    values to timezone-naive midnight datetimes before building the registry.
    The registry may therefore serialize a row as ``YYYY-MM-DD 00:00:00`` while
    its report-level daily identity remains ``YYYY-MM-DD``.  Both encode the same
    daily observation.  A genuinely different calendar day must still fail.
    """
    text = str(value or "").strip()
    if not text:
        raise Phase4ConfidenceAdapterError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Phase4ConfidenceAdapterError(f"invalid_{field}") from exc
    return parsed.date().isoformat()


def _strip_forbidden(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_forbidden(item)
            for key, item in value.items()
            if str(key) not in FORBIDDEN_CONFIDENCE_KEYS
        }
    if isinstance(value, list):
        return [_strip_forbidden(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_forbidden(item) for item in value]
    return deepcopy(value)


def index_guarded_phase4_rows(
    report: Mapping[str, object],
    *,
    expected_snapshot_id: str,
    expected_daily_as_of: str,
    expected_scanner_generated_at: str,
) -> tuple[dict[str, list[dict[str, object]]], str]:
    """Validate exact publication identity and index guarded Phase-4 rows."""
    if report.get("phase") != PHASE4_REPORT_ID:
        raise Phase4ConfidenceAdapterError("phase4_report_identity_invalid")
    semantics = report.get("semantics")
    if not isinstance(semantics, Mapping) or semantics.get("research_only") is not True:
        raise Phase4ConfidenceAdapterError("phase4_research_only_guard_missing")
    if semantics.get("production_confidence_changed") is not False:
        raise Phase4ConfidenceAdapterError("phase4_production_confidence_guard_invalid")
    if semantics.get("scalar_confidence_mapping_created") is not False:
        raise Phase4ConfidenceAdapterError("phase4_scalar_mapping_must_remain_absent")
    if semantics.get("confidence_thresholds_created") is not False:
        raise Phase4ConfidenceAdapterError("phase4_thresholds_must_remain_absent")

    current = report.get("current")
    if not isinstance(current, Mapping):
        raise Phase4ConfidenceAdapterError("phase4_current_required")
    if str(current.get("snapshot_id") or "") != expected_snapshot_id:
        raise Phase4ConfidenceAdapterError("phase4_snapshot_mismatch")
    expected_day = _calendar_date(expected_daily_as_of, "expected_daily_as_of")
    if _calendar_date(current.get("as_of"), "phase4_as_of") != expected_day:
        raise Phase4ConfidenceAdapterError("phase4_as_of_mismatch")
    if _utc(current.get("generated_at"), "phase4_scanner_generated_at") != _utc(
        expected_scanner_generated_at, "expected_scanner_generated_at"
    ):
        raise Phase4ConfidenceAdapterError("phase4_scanner_generation_mismatch")

    rows = current.get("rows")
    if not isinstance(rows, list):
        raise Phase4ConfidenceAdapterError("phase4_current_rows_required")
    index: dict[str, list[dict[str, object]]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise Phase4ConfidenceAdapterError("phase4_current_row_invalid")
        symbol = str(raw.get("symbol") or "").strip()
        if not symbol:
            raise Phase4ConfidenceAdapterError("phase4_current_symbol_required")
        if _calendar_date(raw.get("as_of"), f"phase4_row_as_of:{symbol}") != expected_day:
            raise Phase4ConfidenceAdapterError(f"phase4_row_as_of_mismatch:{symbol}")
        horizon = int(raw.get("horizon_sessions") or 0)
        if horizon <= 0:
            raise Phase4ConfidenceAdapterError(f"phase4_horizon_invalid:{symbol}")
        index.setdefault(symbol, []).append(deepcopy(dict(raw)))
    for symbol in index:
        index[symbol].sort(key=lambda row: int(row["horizon_sessions"]))

    source_version = str(
        ((report.get("config") or {}).get("evidence_version"))
        if isinstance(report.get("config"), Mapping)
        else ""
    ).strip() or PHASE4_DEFAULT_SOURCE_VERSION
    return index, source_version


def build_phase4_confidence_rows(
    *,
    symbol: str,
    selection_claim_id: str,
    phase4_rows: list[dict[str, object]],
    scanner_as_of: str,
    available_from: str,
    source_version: str,
) -> list[dict[str, object]]:
    """Build non-directional Confidence annotations for one current symbol."""
    evidence: list[dict[str, object]] = []
    for row in phase4_rows:
        horizon = int(row.get("horizon_sessions") or 0)
        if horizon <= 0:
            raise Phase4ConfidenceAdapterError(f"phase4_horizon_invalid:{symbol}")

        selection = row.get("selection") if isinstance(row.get("selection"), Mapping) else {}
        timing = row.get("timing") if isinstance(row.get("timing"), Mapping) else {}
        risk = row.get("risk") if isinstance(row.get("risk"), Mapping) else {}
        model_agreement = row.get("model_agreement") if isinstance(row.get("model_agreement"), Mapping) else {}
        data_quality = row.get("data_quality") if isinstance(row.get("data_quality"), Mapping) else {}
        regime = row.get("regime") if isinstance(row.get("regime"), Mapping) else {}

        payload = _strip_forbidden({
            "horizon_sessions": horizon,
            "selection_statistical_state": str(selection.get("state") or "unavailable"),
            "timing_model_state": str(timing.get("state") or "unknown"),
            "risk_model_state": str(risk.get("state") or "unknown"),
            "model_agreement": dict(model_agreement),
            "data_quality": dict(data_quality),
            "regime": dict(regime),
            "phase4_role": "ordinal_reliability_context_not_directional_vote",
        })
        assert isinstance(payload, Mapping)
        evidence.append({
            "family": "confidence",
            "claim_id": f"confidence:{symbol}:phase4:{horizon}T",
            "claim_ref": selection_claim_id,
            "as_of": scanner_as_of,
            "available_from": available_from,
            "source_version": source_version,
            "coverage_state": "available",
            "maturity_state": "not_yet_mature",
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": dict(payload),
        })
    return evidence
