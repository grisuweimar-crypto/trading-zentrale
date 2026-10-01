"""Final same-snapshot evidence bridge for the Decision Layer.

W2 and W3 already place Phase-2 Probability and Phase-3 Risk in the base 7A
packet. W4 therefore consumes guarded Phase-4 output only as typed Confidence
(reliability/evidence context) and keeps the existing non-directional scanner
path-state context. No additional directional vote or portfolio action is
created here.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping

import pandas as pd

from .current_evidence import CURRENT_PACKET_SET_SCHEMA_VERSION
from .input_contract import build_input_packet
from .phase4_confidence import (
    Phase4ConfidenceAdapterError,
    build_phase4_confidence_rows,
    index_guarded_phase4_rows,
)


INTEGRATED_STAGE = "integrated_current_evidence_v1"
PATH_CONTEXT_TYPE = "scanner_path_state_v1"
OVEREXTENSION_RS3M_THRESHOLD = 0.15
OVEREXTENSION_MEMORY_SESSIONS = 20


class IntegratedDecisionEvidenceError(ValueError):
    """Raised when current evidence cannot be integrated without inference."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise IntegratedDecisionEvidenceError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise IntegratedDecisionEvidenceError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntegratedDecisionEvidenceError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if pd.notna(result) else None


def _observed_history(history: pd.DataFrame, symbol: str, current_date: str) -> pd.DataFrame:
    if "symbol" not in history.columns or "date" not in history.columns:
        return pd.DataFrame()
    work = history.loc[history["symbol"].astype(str).eq(symbol)].copy()
    if work.empty:
        return work
    work["_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.loc[work["_date"].notna() & (work["_date"] < pd.Timestamp(current_date))].copy()
    if work.empty:
        return work

    if "observation_type" in work.columns:
        observed = work.loc[work["observation_type"].astype(str).eq("observed_scanner")].copy()
        if not observed.empty:
            work = observed
    sort_columns = ["_date"]
    if "generated_at" in work.columns:
        sort_columns.append("generated_at")
    work = work.sort_values(sort_columns, kind="mergesort")
    work = work.drop_duplicates(subset=["_date"], keep="last")
    return work


def build_scanner_path_state(
    *,
    symbol: str,
    daily_symbol: Mapping[str, object],
    history: pd.DataFrame,
    current_date: str,
) -> dict[str, object]:
    """Build a non-directional path-memory context from PIT scanner history."""
    current = daily_symbol.get("current")
    dynamics = daily_symbol.get("dynamics")
    current = current if isinstance(current, Mapping) else {}
    dynamics = dynamics if isinstance(dynamics, Mapping) else {}

    current_rs3m = _number(current.get("rs3m"))
    active = current_rs3m is not None and current_rs3m >= OVEREXTENSION_RS3M_THRESHOLD

    observed = _observed_history(history, symbol, current_date)
    last_date: str | None = None
    last_rs3m: float | None = None
    sessions_since: int | None = None
    if not observed.empty and "rs3m" in observed.columns:
        observed["_rs3m"] = pd.to_numeric(observed["rs3m"], errors="coerce")
        over = observed.loc[observed["_rs3m"] >= OVEREXTENSION_RS3M_THRESHOLD]
        if not over.empty:
            last = over.iloc[-1]
            last_ts = pd.Timestamp(last["_date"])
            last_date = last_ts.date().isoformat()
            last_rs3m = float(last["_rs3m"])
            sessions_since = int((observed["_date"] > last_ts).sum()) + 1

    if active:
        last_date = current_date
        last_rs3m = current_rs3m
        sessions_since = 0

    recent = sessions_since is not None and sessions_since <= OVEREXTENSION_MEMORY_SESSIONS
    score_falling = (_number(dynamics.get("score_delta_5d")) or 0.0) < 0.0
    rank_delta = _number(dynamics.get("rank_delta_5d"))
    rank_worsening = rank_delta is not None and rank_delta > 0.0
    rs3m_falling = (_number(dynamics.get("rs3m_delta_5d")) or 0.0) < 0.0
    trend_falling = (_number(dynamics.get("trend200_delta_5d")) or 0.0) < 0.0

    current_r = str(current.get("r_code") or "")
    previous_r = str(dynamics.get("r_code_previous") or "")
    r_map = {f"R{i}": i for i in range(1, 6)}
    r_downgrade = current_r in r_map and previous_r in r_map and r_map[current_r] < r_map[previous_r]

    deterioration = {
        "score_falling_5t": score_falling,
        "rank_worsening_5t": rank_worsening,
        "rs3m_falling_5t": rs3m_falling,
        "trend200_falling_5t": trend_falling,
        "r_code_downgrade": r_downgrade,
    }
    secondary = score_falling or rank_worsening or trend_falling or r_downgrade

    if active and rs3m_falling and secondary:
        sequence_state = "overextension_with_fading_dynamics"
        review_state = "profit_protection_review"
    elif recent and not active and rs3m_falling and secondary:
        sequence_state = "post_overextension_decay"
        review_state = "profit_protection_review"
    elif active:
        sequence_state = "active_overextension"
        review_state = "monitor"
    elif recent and not active:
        sequence_state = "post_overextension_memory"
        review_state = "monitor"
    else:
        sequence_state = "no_recent_overextension_context"
        review_state = "none"

    return {
        "context_type": PATH_CONTEXT_TYPE,
        "overextension_threshold_rs3m": OVEREXTENSION_RS3M_THRESHOLD,
        "memory_horizon_sessions": OVEREXTENSION_MEMORY_SESSIONS,
        "overextension_active": active,
        "last_overextension_date": last_date,
        "last_overextension_rs3m": last_rs3m,
        "sessions_since_last_overextension": sessions_since,
        "recent_overextension": recent,
        "sequence_state": sequence_state,
        "review_state": review_state,
        "deterioration": deterioration,
        "review_is_trade_decision": False,
        "execution_allowed": False,
    }


def _path_evidence(
    *,
    symbol: str,
    daily_symbol: Mapping[str, object],
    history: pd.DataFrame,
    daily_as_of: str,
    scanner_available_from: str,
) -> dict[str, object]:
    payload = build_scanner_path_state(
        symbol=symbol,
        daily_symbol=daily_symbol,
        history=history,
        current_date=daily_as_of,
    )
    return {
        "family": "risk",
        "claim_id": f"risk:{symbol}:scanner-path:{daily_as_of}",
        "as_of": scanner_available_from,
        "available_from": scanner_available_from,
        "source_version": PATH_CONTEXT_TYPE,
        "coverage_state": "available",
        "maturity_state": "not_yet_mature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": payload,
    }


def integrate_current_packet_set(
    *,
    packet_set: Mapping[str, object],
    daily: Mapping[str, object],
    history: pd.DataFrame,
    phase4_report: Mapping[str, object],
    finalized_at: str | None = None,
) -> dict[str, object]:
    """Attach guarded Phase-4 Confidence and scanner path memory to current 7A."""
    if packet_set.get("schema_version") != CURRENT_PACKET_SET_SCHEMA_VERSION:
        raise IntegratedDecisionEvidenceError("unsupported_current_packet_set")
    snapshot_id = str(packet_set.get("snapshot_id") or "")
    if snapshot_id != str(daily.get("snapshot_id") or ""):
        raise IntegratedDecisionEvidenceError("daily_packet_snapshot_mismatch")
    daily_as_of = str(daily.get("as_of") or "")
    scanner_available_from = str(daily.get("generated_at") or "")
    scanner_time = _utc(scanner_available_from, "daily_generated_at")
    final_text = finalized_at or datetime.now(timezone.utc).isoformat()
    final_time = _utc(final_text, "finalized_at")
    if final_time < scanner_time:
        raise IntegratedDecisionEvidenceError("integrated_evidence_before_scanner_publication")

    raw_symbols = daily.get("symbols")
    if not isinstance(raw_symbols, Mapping):
        raise IntegratedDecisionEvidenceError("daily_symbols_required")
    try:
        phase4_index, source_version = index_guarded_phase4_rows(
            phase4_report,
            expected_snapshot_id=snapshot_id,
            expected_daily_as_of=daily_as_of,
            expected_scanner_generated_at=scanner_available_from,
        )
    except Phase4ConfidenceAdapterError as exc:
        raise IntegratedDecisionEvidenceError(str(exc)) from exc

    raw_packets = packet_set.get("packets")
    if not isinstance(raw_packets, list):
        raise IntegratedDecisionEvidenceError("packet_set_packets_required")

    integrated: list[dict[str, object]] = []
    phase4_symbols = 0
    phase4_confidence_claims = 0
    path_reviews = 0
    for raw_packet in raw_packets:
        if not isinstance(raw_packet, Mapping):
            raise IntegratedDecisionEvidenceError("packet_must_be_object")
        symbol = str(raw_packet.get("symbol") or "")
        daily_symbol = raw_symbols.get(symbol)
        if not isinstance(daily_symbol, Mapping):
            raise IntegratedDecisionEvidenceError(f"daily_symbol_missing:{symbol}")
        base_evidence = raw_packet.get("evidence")
        if not isinstance(base_evidence, list):
            raise IntegratedDecisionEvidenceError(f"packet_evidence_missing:{symbol}")
        selection_rows = [
            row for row in base_evidence
            if isinstance(row, Mapping) and row.get("family") == "selection"
        ]
        if len(selection_rows) != 1:
            raise IntegratedDecisionEvidenceError(f"selection_claim_count_invalid:{symbol}")
        selection_claim_id = str(selection_rows[0].get("claim_id") or "")

        additions: list[dict[str, object]] = [
            _path_evidence(
                symbol=symbol,
                daily_symbol=daily_symbol,
                history=history,
                daily_as_of=daily_as_of,
                scanner_available_from=scanner_available_from,
            )
        ]
        if additions[0]["payload"]["review_state"] == "profit_protection_review":
            path_reviews += 1

        rows4 = phase4_index.get(symbol, [])
        if rows4:
            phase4_symbols += 1
            confidence = build_phase4_confidence_rows(
                symbol=symbol,
                selection_claim_id=selection_claim_id,
                phase4_rows=rows4,
                scanner_as_of=scanner_available_from,
                available_from=final_text,
                source_version=source_version,
            )
            phase4_confidence_claims += len(confidence)
            additions.extend(confidence)

        packet = build_input_packet(
            symbol=symbol,
            as_of=final_text,
            source_snapshot_id=snapshot_id,
            evidence=[deepcopy(dict(row)) for row in base_evidence] + additions,
        )
        packet["orchestration_stage"] = INTEGRATED_STAGE
        packet["scanner_generated_at"] = scanner_available_from
        integrated.append(packet)

    result = deepcopy(dict(packet_set))
    result["phase"] = "7A-current-integrated-orchestration"
    result["as_of"] = final_text
    result["packet_count"] = len(integrated)
    result["packets"] = integrated
    result["phase4_symbol_count"] = phase4_symbols
    result["phase4_confidence_claim_count"] = phase4_confidence_claims
    result["path_review_count"] = path_reviews
    semantics = deepcopy(dict(result.get("semantics") or {}))
    semantics.update({
        "integrated_evidence_v1": True,
        "probability_reconstructed": False,
        "risk_reconstructed": False,
        "confidence_reconstructed": False,
        "phase2_probability_source_preserved": True,
        "phase3_risk_source_preserved": True,
        "phase4_confidence_annotations_attached": True,
        "confidence_is_directional_vote": False,
        "confidence_encodes_attractiveness": False,
        "phase2_phase3_phase4_outputs_consumed": True,
        "phase5_unpromoted_changes_decision": False,
        "elliott_unpromoted_changes_decision": False,
        "external_evidence_phase8_activated": False,
        "scanner_path_state_is_directional_vote": False,
        "scanner_path_state_is_trade_decision": False,
    })
    result["semantics"] = semantics
    validation = deepcopy(dict(result.get("validation") or {}))
    validation["packet_as_of_utc"] = final_time.isoformat()
    validation["scanner_generated_at_utc"] = scanner_time.isoformat()
    validation["phase4_snapshot_id"] = snapshot_id
    validation["phase4_same_snapshot_verified"] = True
    validation["research_only"] = True
    result["validation"] = validation
    return result
