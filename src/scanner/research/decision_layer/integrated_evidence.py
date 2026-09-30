"""Final same-snapshot evidence bridge for the Decision Layer.

This module does not change any upstream model.  It takes the conservative
current 7A packet set (Selection + frozen Timing), attaches already-computed
Phase-2/3/4 research context, and adds a non-directional scanner path-state
context so transient warnings do not disappear from the Depot Watch merely
because the current scalar crossed back through a threshold.

The bridge is deliberately research-only. Probability and Confidence remain
annotations, Risk/path state remains context, and no additional directional vote
or portfolio action is created here.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping

import pandas as pd

from .current_evidence import CURRENT_PACKET_SET_SCHEMA_VERSION
from .input_contract import build_input_packet


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


def _strip_keys(value: object, forbidden: set[str]) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_keys(item, forbidden)
            for key, item in value.items()
            if str(key) not in forbidden
        }
    if isinstance(value, list):
        return [_strip_keys(item, forbidden) for item in value]
    if isinstance(value, tuple):
        return [_strip_keys(item, forbidden) for item in value]
    return value


def _maturity_from_state(state: object) -> str:
    text = str(state or "").strip()
    if text == "robust":
        return "robust"
    if text in {"directional_only", "robust_claim"}:
        return "directional_but_immature"
    if text in {"immature", "unknown", "middle", "low", "elevated", "mixed"}:
        return "not_yet_mature"
    if text in {"internal_conflict", "conflict", "insufficient_evidence", "proxy_insufficient"}:
        return "insufficient_evidence"
    if text in {"unavailable", "not_applicable", ""}:
        return "unavailable"
    return "not_yet_mature"


def _coverage_from_state(state: object) -> str:
    text = str(state or "").strip()
    if text in {"unavailable", ""}:
        return "unavailable"
    if text in {"immature", "unknown", "proxy_insufficient", "insufficient_evidence"}:
        return "limited"
    return "available"


def _phase4_rows_by_symbol(report: Mapping[str, object]) -> dict[str, list[dict[str, object]]]:
    if report.get("phase") != "4_confidence_vnext_empirical_research":
        raise IntegratedDecisionEvidenceError("phase4_report_identity_invalid")
    current = report.get("current")
    rows = current.get("rows") if isinstance(current, Mapping) else None
    if not isinstance(rows, list):
        raise IntegratedDecisionEvidenceError("phase4_current_rows_required")
    result: dict[str, list[dict[str, object]]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise IntegratedDecisionEvidenceError("phase4_current_row_invalid")
        symbol = str(raw.get("symbol") or "").strip()
        if not symbol:
            raise IntegratedDecisionEvidenceError("phase4_current_symbol_required")
        result.setdefault(symbol, []).append(deepcopy(dict(raw)))
    for symbol in result:
        result[symbol].sort(key=lambda row: int(row.get("horizon_sessions") or 0))
    return result


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
    """Build a non-directional path-memory context from PIT scanner history.

    The only level threshold reused here is the already-existing Daily Research
    overextension definition (RS3M >= 15%).  The review state is deliberately a
    review flag, not a sell signal: it requires recent overextension, falling
    5-session relative strength and at least one additional deterioration in
    Score, ranking, Trend200 or R-code.
    """
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


def _phase4_evidence_rows(
    *,
    symbol: str,
    selection_claim_id: str,
    phase4_rows: list[dict[str, object]],
    scanner_as_of: str,
    available_from: str,
    source_version: str,
) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    for row in phase4_rows:
        horizon = int(row.get("horizon_sessions") or 0)
        if horizon <= 0:
            raise IntegratedDecisionEvidenceError(f"phase4_horizon_invalid:{symbol}")

        selection = row.get("selection") if isinstance(row.get("selection"), Mapping) else {}
        selection_state = str(selection.get("state") or "unavailable")
        probability_payload = _strip_keys(selection, {"direction", "stance", "vote"})
        assert isinstance(probability_payload, Mapping)
        probability_payload = {"horizon_sessions": horizon, **dict(probability_payload)}
        evidence.append({
            "family": "probability",
            "claim_id": f"probability:{symbol}:selection:{horizon}T",
            "claim_ref": selection_claim_id,
            "as_of": scanner_as_of,
            "available_from": available_from,
            "source_version": source_version,
            "coverage_state": _coverage_from_state(selection_state),
            "maturity_state": _maturity_from_state(selection_state),
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": probability_payload,
        })

        risk = row.get("risk") if isinstance(row.get("risk"), Mapping) else {}
        risk_state = str(risk.get("state") or "unavailable")
        risk_payload = _strip_keys(risk, {"direction", "stance", "vote"})
        assert isinstance(risk_payload, Mapping)
        evidence.append({
            "family": "risk",
            "claim_id": f"risk:{symbol}:phase4:{horizon}T",
            "as_of": scanner_as_of,
            "available_from": available_from,
            "source_version": source_version,
            "coverage_state": _coverage_from_state(risk_state),
            "maturity_state": _maturity_from_state(risk_state),
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": {"horizon_sessions": horizon, **dict(risk_payload)},
        })

        model_agreement = row.get("model_agreement") if isinstance(row.get("model_agreement"), Mapping) else {}
        data_quality = row.get("data_quality") if isinstance(row.get("data_quality"), Mapping) else {}
        regime = row.get("regime") if isinstance(row.get("regime"), Mapping) else {}
        confidence_payload = {
            "horizon_sessions": horizon,
            "selection_statistical_state": selection_state,
            "timing_model_state": (row.get("timing") or {}).get("state") if isinstance(row.get("timing"), Mapping) else None,
            "risk_model_state": risk_state,
            "model_agreement": deepcopy(dict(model_agreement)),
            "data_quality": deepcopy(dict(data_quality)),
            "regime": deepcopy(dict(regime)),
            "phase4_role": "ordinal_reliability_context_not_directional_vote",
        }
        confidence_payload = _strip_keys(confidence_payload, {"direction", "stance", "vote", "attractiveness"})
        assert isinstance(confidence_payload, Mapping)
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
            "payload": confidence_payload,
        })
    return evidence


def integrate_current_packet_set(
    *,
    packet_set: Mapping[str, object],
    daily: Mapping[str, object],
    history: pd.DataFrame,
    phase4_report: Mapping[str, object],
    finalized_at: str | None = None,
) -> dict[str, object]:
    """Attach Phase-2/3/4 context and scanner path memory to current 7A packets."""
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
    phase4_index = _phase4_rows_by_symbol(phase4_report)
    source_version = str(((phase4_report.get("config") or {}).get("evidence_version")) or "phase4_confidence_research_v1")

    raw_packets = packet_set.get("packets")
    if not isinstance(raw_packets, list):
        raise IntegratedDecisionEvidenceError("packet_set_packets_required")

    integrated: list[dict[str, object]] = []
    phase4_symbols = 0
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
        selection_rows = [row for row in base_evidence if isinstance(row, Mapping) and row.get("family") == "selection"]
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
            additions.extend(_phase4_evidence_rows(
                symbol=symbol,
                selection_claim_id=selection_claim_id,
                phase4_rows=rows4,
                scanner_as_of=scanner_available_from,
                available_from=final_text,
                source_version=source_version,
            ))

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
    result["path_review_count"] = path_reviews
    semantics = deepcopy(dict(result.get("semantics") or {}))
    semantics.update({
        "integrated_evidence_v1": True,
        "probability_reconstructed": False,
        "risk_reconstructed": False,
        "confidence_reconstructed": False,
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
    validation["research_only"] = True
    result["validation"] = validation
    return result
