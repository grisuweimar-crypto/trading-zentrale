"""Scanner ↔ Elliott cross-system research for Module 6E.

The module joins already-causal Elliott 6D review events to point-in-time scanner
observations and externally supplied frozen model claims.  It deliberately does
not reconstruct historical Elliott counts, discover scanner thresholds, optimise
prior phases, or evaluate forward-return edge.  Those outcome tests belong to
Module 6G.

The one-way contract remains::

    6A pivots -> 6B scenarios -> 6C geometry -> 6D routing -> 6E cross-system research

6E can quantify overlap, coverage, conflicts and lead/lag.  It may label
*incremental-information candidates*, but it cannot claim incremental predictive
value without the later PIT outcome validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import _scanner_rows


SCANNER_NUMERIC_ALIASES: dict[str, tuple[str, ...]] = {
    "score": ("score",),
    "opportunity": ("opportunity", "opportunity_score"),
    "risk": ("risk", "risk_score"),
    "rs3m": ("rs3m", "RS3M"),
    "trend200": ("trend200", "Trend200"),
    "cycle": ("cycle", "cycle_pct", "Zyklus %"),
}

ELLIOTT_REVIEW_ORIENTATION: dict[str, str] = {
    "entry_or_add_review": "supportive_review",
    "reentry_or_add_review": "supportive_review",
    "hold_review": "neutral_review",
    "partial_reduce_review": "defensive_review",
    "profit_protection_review": "defensive_review",
    "larger_reduce_or_exit_review": "defensive_review",
}

MODEL_STANCES = {"supportive", "cautionary", "neutral", "unknown"}
MODEL_MATURITY = {
    "mature",
    "directional_but_immature",
    "insufficient_evidence",
    "unavailable",
}

# These are pre-specified scanner transitions for lead/lag diagnostics.  They are
# descriptive states, not newly discovered trading patterns.
TRANSITION_COLUMNS: tuple[str, ...] = (
    "score_turn_up", "score_turn_down",
    "opportunity_turn_up", "opportunity_turn_down",
    "risk_turn_up", "risk_turn_down",
    "rs3m_turn_up", "rs3m_turn_down",
    "trend200_turn_up", "trend200_turn_down",
    "cycle_turn_up", "cycle_turn_down",
    "trend200_cross_up", "trend200_cross_down",
    "cycle_cross_up_25", "cycle_cross_down_25",
    "cycle_cross_up_50", "cycle_cross_down_50",
    "cycle_cross_up_75", "cycle_cross_down_75",
    "r_upgrade", "r_downgrade",
)


class CrossSystemInputError(ValueError):
    """Raised when PIT/provenance input is malformed rather than merely absent."""


@dataclass(frozen=True)
class CrossSystemConfig:
    stable_start: str = "2026-04-15"
    discovery_reference_end: str = "2026-07-31"
    spent_validation_start: str = "2026-08-01"
    event_window_sessions: int = 20

    def __post_init__(self) -> None:
        for field in ("stable_start", "discovery_reference_end", "spent_validation_start"):
            _iso_date(getattr(self, field), field)
        if self.event_window_sessions < 20:
            raise ValueError("event_window_sessions must be >= 20")


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise CrossSystemInputError(f"invalid_{field}") from exc


def _finite_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _hash_id(payload: Mapping[str, object]) -> str:
    frozen = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(frozen.encode("utf-8")).hexdigest()


def _first_column(frame: pd.DataFrame, names: Sequence[str]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _r_number(value: object) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return np.nan
    text = str(value).strip().upper()
    if text.startswith("R") and text[1:].isdigit():
        number = int(text[1:])
        if 0 <= number <= 5:
            return float(number)
    return np.nan


def _partition(day: str, config: CrossSystemConfig) -> str:
    if day <= config.discovery_reference_end:
        return "legacy_discovery_reference"
    if day >= config.spent_validation_start:
        return "legacy_spent_validation_descriptive_only"
    return "purge_gap_or_unassigned"


def scanner_feature_rows(
    history: pd.DataFrame,
    config: CrossSystemConfig = CrossSystemConfig(),
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Return deterministic PIT scanner features for 6E lead/lag research.

    Same-day reruns use the existing Phase-1A last-published-state rule.  Missing
    fields remain NaN.  No historical value is reconstructed from current data.
    """

    frame = _scanner_rows(history)
    frame = frame.loc[~frame["is_crypto"]].copy()
    frame = frame.loc[frame["date"] >= pd.Timestamp(config.stable_start)].copy()
    frame["symbol"] = frame["symbol"].astype(str)

    sources: dict[str, str | None] = {}
    for canonical, aliases in SCANNER_NUMERIC_ALIASES.items():
        source = _first_column(frame, aliases)
        sources[canonical] = source
        frame[canonical] = pd.to_numeric(frame[source], errors="coerce") if source else np.nan

    frame["r_num"] = frame.get(
        "r_code", pd.Series(index=frame.index, dtype=object)
    ).map(_r_number)
    frame = frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    grouped = frame.groupby("symbol", sort=False)

    for name in SCANNER_NUMERIC_ALIASES:
        for lag in (1, 5, 10):
            frame[f"{name}_d{lag}"] = frame[name] - grouped[name].shift(lag)

    for name in SCANNER_NUMERIC_ALIASES:
        delta = frame[f"{name}_d1"]
        previous_delta = grouped[f"{name}_d1"].shift(1)
        frame[f"{name}_turn_up"] = (
            delta.notna() & previous_delta.notna() & (delta > 0) & (previous_delta <= 0)
        )
        frame[f"{name}_turn_down"] = (
            delta.notna() & previous_delta.notna() & (delta < 0) & (previous_delta >= 0)
        )

    previous_trend = grouped["trend200"].shift(1)
    frame["trend200_cross_up"] = (
        frame["trend200"].notna() & previous_trend.notna()
        & (frame["trend200"] >= 0) & (previous_trend < 0)
    )
    frame["trend200_cross_down"] = (
        frame["trend200"].notna() & previous_trend.notna()
        & (frame["trend200"] < 0) & (previous_trend >= 0)
    )
    previous_cycle = grouped["cycle"].shift(1)
    for level in (25.0, 50.0, 75.0):
        suffix = int(level)
        frame[f"cycle_cross_up_{suffix}"] = (
            frame["cycle"].notna() & previous_cycle.notna()
            & (frame["cycle"] >= level) & (previous_cycle < level)
        )
        frame[f"cycle_cross_down_{suffix}"] = (
            frame["cycle"].notna() & previous_cycle.notna()
            & (frame["cycle"] < level) & (previous_cycle >= level)
        )

    previous_r = grouped["r_num"].shift(1)
    frame["r_upgrade"] = frame["r_num"].notna() & previous_r.notna() & (frame["r_num"] > previous_r)
    frame["r_downgrade"] = frame["r_num"].notna() & previous_r.notna() & (frame["r_num"] < previous_r)

    for column in TRANSITION_COLUMNS:
        if column not in frame.columns:
            frame[column] = False
        frame[column] = frame[column].fillna(False).astype(bool)

    frame["partition"] = frame["date"].dt.date.astype(str).map(lambda value: _partition(value, config))
    coverage = {
        "rows": int(len(frame)),
        "symbols": int(frame["symbol"].nunique()) if len(frame) else 0,
        "date_min": str(frame["date"].min().date()) if len(frame) else None,
        "date_max": str(frame["date"].max().date()) if len(frame) else None,
        "sources": sources,
        "non_null": {
            name: int(frame[name].notna().sum()) for name in (*SCANNER_NUMERIC_ALIASES.keys(), "r_num")
        },
        "historical_missingness_preserved": True,
        "present_day_reconstruction_used": False,
        "same_day_rerun_rule": "last_published_state",
    }
    return frame, coverage


def _scenario_lookup(snapshot: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    scenarios: list[Mapping[str, object]] = []
    primary = snapshot.get("primary_scenario")
    if isinstance(primary, Mapping):
        scenarios.append(primary)
    alternatives = snapshot.get("alternative_scenarios")
    if isinstance(alternatives, list):
        scenarios.extend(item for item in alternatives if isinstance(item, Mapping))
    return {
        str(item["scenario_id"]): item
        for item in scenarios
        if item.get("scenario_id") not in (None, "")
    }


def extract_elliott_events(
    routed_snapshots: Iterable[Mapping[str, object]],
    config: CrossSystemConfig = CrossSystemConfig(),
) -> list[dict[str, object]]:
    """Extract only already-observed 6D routes as causal Elliott events.

    There is intentionally no path from OHLC history to this function.  Historical
    6E analysis therefore requires a PIT 6D event/snapshot stream; absence of one
    remains absence rather than hindsight reconstruction.
    """

    records: list[dict[str, object]] = []
    seen: set[str] = set()
    for snapshot in routed_snapshots:
        if snapshot.get("research_only") is not True:
            raise CrossSystemInputError("6e_requires_research_only_6d_snapshot")
        if snapshot.get("routing_is_trade_decision") is not False:
            raise CrossSystemInputError("6e_requires_6d_review_routing_not_trade_decision")
        symbol = str(snapshot.get("symbol", "")).strip()
        if not symbol:
            raise CrossSystemInputError("snapshot_symbol_required")
        as_of = _iso_date(snapshot.get("as_of"), "snapshot_as_of")
        scenarios = _scenario_lookup(snapshot)
        routes = snapshot.get("swing_routing")
        if routes is None:
            routes = []
        if not isinstance(routes, list):
            raise CrossSystemInputError("swing_routing_must_be_list")

        for route in routes:
            if not isinstance(route, Mapping):
                raise CrossSystemInputError("invalid_swing_route")
            available_from = _iso_date(route.get("available_from"), "route_available_from")
            if available_from > as_of:
                raise CrossSystemInputError("route_available_from_after_snapshot_as_of")
            if route.get("actionability") != "review_only_not_trade_instruction":
                raise CrossSystemInputError("6e_route_actionability_guard_failed")
            context = str(route.get("review_context", ""))
            orientation = ELLIOTT_REVIEW_ORIENTATION.get(context)
            if orientation is None:
                raise CrossSystemInputError(f"unsupported_review_context:{context}")
            scenario_id = str(route.get("scenario_id") or "")
            scenario = scenarios.get(scenario_id, {})
            identity = {
                "symbol": symbol,
                "scenario_id": scenario_id,
                "trigger": str(route.get("trigger", "")),
                "available_from": available_from,
                "source": str(route.get("source", "")),
            }
            event_id = _hash_id(identity)
            if event_id in seen:
                continue
            seen.add(event_id)
            records.append({
                "event_id": event_id,
                "symbol": symbol,
                "snapshot_as_of": as_of,
                "available_from": available_from,
                "partition": _partition(available_from, config),
                "timeframe": str(snapshot.get("timeframe", scenario.get("timeframe", ""))),
                "degree": str(snapshot.get("degree", scenario.get("degree", ""))),
                "scenario_id": scenario_id or None,
                "scenario_role": str(route.get("scenario_role", "")),
                "pattern_class": scenario.get("pattern_class"),
                "wave_stage": scenario.get("stage"),
                "elliott_direction": scenario.get("direction"),
                "trigger": str(route.get("trigger", "")),
                "review_context": context,
                "review_orientation": orientation,
                "source": str(route.get("source", "")),
                "evidence": dict(route.get("evidence", {})) if isinstance(route.get("evidence"), Mapping) else {},
                "requires_external_confirmation": bool(route.get("requires_external_confirmation", True)),
                "historical_outperformance_claimed": False,
                "incremental_value_claimed": False,
                "causal_event_source": "observed_6d_route_only",
                "research_only": True,
            })
    records.sort(key=lambda item: (
        str(item["symbol"]), str(item["available_from"]), str(item["scenario_role"]), str(item["trigger"])
    ))
    return records


def _symbol_sessions(scanner: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        str(symbol): group.sort_values("date", kind="mergesort").reset_index(drop=True)
        for symbol, group in scanner.groupby("symbol", sort=False)
    }


def build_event_windows(
    elliott_events: Sequence[Mapping[str, object]],
    scanner: pd.DataFrame,
    config: CrossSystemConfig = CrossSystemConfig(),
) -> list[dict[str, object]]:
    """Build ±N observed scanner-session windows around causal Elliott events.

    A route becoming available between scanner observations is anchored to the
    first scanner observation on or after ``available_from``.  Positive relative
    offsets are explicitly retrospective post-event observations and must never
    be fed back into the event-time state.
    """

    groups = _symbol_sessions(scanner)
    result: list[dict[str, object]] = []
    value_columns = [
        *SCANNER_NUMERIC_ALIASES.keys(), "r_num",
        *[f"{name}_d{lag}" for name in SCANNER_NUMERIC_ALIASES for lag in (1, 5, 10)],
        *TRANSITION_COLUMNS,
    ]
    for event in elliott_events:
        symbol = str(event.get("symbol", ""))
        series = groups.get(symbol)
        if series is None or series.empty:
            continue
        event_day = pd.Timestamp(_iso_date(event.get("available_from"), "event_available_from"))
        dates = series["date"].values.astype("datetime64[ns]")
        anchor = int(np.searchsorted(dates, np.datetime64(event_day), side="left"))
        if anchor >= len(series):
            continue
        low = max(0, anchor - config.event_window_sessions)
        high = min(len(series) - 1, anchor + config.event_window_sessions)
        anchor_date = pd.Timestamp(series.iloc[anchor]["date"]).date().isoformat()
        for position in range(low, high + 1):
            row = series.iloc[position]
            offset = position - anchor
            payload: dict[str, object] = {
                "event_id": str(event["event_id"]),
                "symbol": symbol,
                "elliott_available_from": str(event["available_from"]),
                "event_anchor_session": anchor_date,
                "scanner_date": pd.Timestamp(row["date"]).date().isoformat(),
                "relative_session": int(offset),
                "scanner_leads_elliott": offset < 0,
                "same_session": offset == 0,
                "elliott_leads_scanner": offset > 0,
                "post_event_analysis_only": offset > 0,
                "wave_stage": event.get("wave_stage"),
                "degree": event.get("degree"),
                "elliott_direction": event.get("elliott_direction"),
                "trigger": event.get("trigger"),
                "review_orientation": event.get("review_orientation"),
                "partition": event.get("partition"),
                "research_only": True,
            }
            for column in value_columns:
                value = row.get(column)
                if isinstance(value, (np.bool_, bool)):
                    payload[column] = bool(value)
                else:
                    numeric = _finite_or_none(value)
                    payload[column] = numeric if numeric is not None else None
            result.append(payload)
    result.sort(key=lambda item: (str(item["event_id"]), int(item["relative_session"])))
    return result


def nearest_scanner_transitions(
    event_windows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return the nearest pre-specified scanner transition for each event/type."""

    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in event_windows:
        grouped.setdefault(str(row["event_id"]), []).append(row)

    records: list[dict[str, object]] = []
    for event_id, rows in grouped.items():
        for transition in TRANSITION_COLUMNS:
            matches = [row for row in rows if row.get(transition) is True]
            if not matches:
                continue
            match = min(
                matches,
                key=lambda row: (abs(int(row["relative_session"])), int(row["relative_session"]) > 0, int(row["relative_session"])),
            )
            offset = int(match["relative_session"])
            records.append({
                "event_id": event_id,
                "symbol": match.get("symbol"),
                "transition": transition,
                "relative_session": offset,
                "scanner_date": match.get("scanner_date"),
                "lead_lag": "scanner_leads" if offset < 0 else "same_session" if offset == 0 else "elliott_leads",
                "post_event_analysis_only": offset > 0,
                "research_only": True,
            })
    records.sort(key=lambda item: (str(item["event_id"]), str(item["transition"])))
    return records


def normalize_model_claims(records: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    """Validate frozen/as-of scanner-model claims supplied by earlier phases.

    6E intentionally does not manufacture these states from today's Phase-2/3/4
    reports.  A historical claim must carry its own causal ``available_from``.
    """

    result: list[dict[str, object]] = []
    for raw in records:
        symbol = str(raw.get("symbol", "")).strip()
        model = str(raw.get("model", "")).strip()
        claim_id = str(raw.get("claim_id", "")).strip()
        version = str(raw.get("model_version", "")).strip()
        if not all((symbol, model, claim_id, version)):
            raise CrossSystemInputError("model_claim_identity_fields_required")
        available_from = _iso_date(raw.get("available_from"), "claim_available_from")
        stance = str(raw.get("stance", "unknown"))
        maturity = str(raw.get("maturity", "unavailable"))
        if stance not in MODEL_STANCES:
            raise CrossSystemInputError(f"unsupported_model_stance:{stance}")
        if maturity not in MODEL_MATURITY:
            raise CrossSystemInputError(f"unsupported_model_maturity:{maturity}")
        result.append({
            "symbol": symbol,
            "model": model,
            "claim_id": claim_id,
            "model_version": version,
            "available_from": available_from,
            "stance": stance,
            "maturity": maturity,
            "horizon_sessions": raw.get("horizon_sessions"),
            "source": raw.get("source"),
            "evidence": dict(raw.get("evidence", {})) if isinstance(raw.get("evidence"), Mapping) else {},
            "historical_state_supplied_not_reconstructed": True,
            "research_only": True,
        })
    result.sort(key=lambda item: (
        str(item["symbol"]), str(item["model"]), str(item["available_from"]), str(item["claim_id"])
    ))
    return result


def _relation(event_orientation: str, claim: Mapping[str, object] | None, claim_preexisting: bool) -> str:
    if claim is None:
        return "elliott_rescue_candidate"
    maturity = str(claim.get("maturity", "unavailable"))
    stance = str(claim.get("stance", "unknown"))
    mature_directional = maturity == "mature" and stance in {"supportive", "cautionary"}
    if event_orientation == "neutral_review":
        return "scanner_rescue_candidate" if mature_directional else "insufficient_or_neutral"
    if not mature_directional:
        return "elliott_rescue_candidate"
    compatible = (
        event_orientation == "supportive_review" and stance == "supportive"
    ) or (
        event_orientation == "defensive_review" and stance == "cautionary"
    )
    if compatible:
        return "redundancy_candidate" if claim_preexisting else "confirmation_candidate"
    return "conflict"


def join_model_claims(
    elliott_events: Sequence[Mapping[str, object]],
    claims: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Join the latest causally available frozen model claim to each Elliott event.

    Claims after the Elliott event are never visible to the event-time relation.
    Models with no as-of claim are retained as missing rather than imputed.
    """

    by_symbol_model: dict[tuple[str, str], list[Mapping[str, object]]] = {}
    models = sorted({str(claim["model"]) for claim in claims})
    for claim in claims:
        by_symbol_model.setdefault((str(claim["symbol"]), str(claim["model"])), []).append(claim)
    for values in by_symbol_model.values():
        values.sort(key=lambda item: (str(item["available_from"]), str(item["claim_id"])))

    records: list[dict[str, object]] = []
    for event in elliott_events:
        event_day = str(event["available_from"])
        symbol = str(event["symbol"])
        for model in models:
            eligible = [
                claim for claim in by_symbol_model.get((symbol, model), [])
                if str(claim["available_from"]) <= event_day
            ]
            claim = eligible[-1] if eligible else None
            preexisting = bool(claim and str(claim["available_from"]) < event_day)
            relation = _relation(str(event["review_orientation"]), claim, preexisting)
            records.append({
                "event_id": event["event_id"],
                "symbol": symbol,
                "elliott_available_from": event_day,
                "wave_stage": event.get("wave_stage"),
                "degree": event.get("degree"),
                "trigger": event.get("trigger"),
                "review_orientation": event.get("review_orientation"),
                "model": model,
                "claim_id": claim.get("claim_id") if claim else None,
                "claim_available_from": claim.get("available_from") if claim else None,
                "claim_stance": claim.get("stance") if claim else "unknown",
                "claim_maturity": claim.get("maturity") if claim else "unavailable",
                "claim_model_version": claim.get("model_version") if claim else None,
                "claim_preexisting": preexisting,
                "relation": relation,
                "relation_is_performance_claim": False,
                "incremental_value_evaluated": False,
                "research_only": True,
            })
    records.sort(key=lambda item: (str(item["event_id"]), str(item["model"])))
    return records


def summarize_cross_system(
    elliott_events: Sequence[Mapping[str, object]],
    event_windows: Sequence[Mapping[str, object]],
    transitions: Sequence[Mapping[str, object]],
    relations: Sequence[Mapping[str, object]] = (),
    config: CrossSystemConfig = CrossSystemConfig(),
) -> dict[str, object]:
    """Produce deterministic coverage/lead-lag summaries without outcome claims."""

    def counts(values: Iterable[object]) -> dict[str, int]:
        result: dict[str, int] = {}
        for value in values:
            key = str(value)
            result[key] = result.get(key, 0) + 1
        return dict(sorted(result.items()))

    offsets = [int(item["relative_session"]) for item in transitions]
    return {
        "schema_version": "elliott_vnext_cross_system_research_v1",
        "module": "6e_scanner_elliott_cross_system",
        "event_window_sessions": config.event_window_sessions,
        "elliott_event_count": len(elliott_events),
        "symbols": len({str(item["symbol"]) for item in elliott_events}),
        "events_by_stage": counts(item.get("wave_stage") for item in elliott_events),
        "events_by_degree": counts(item.get("degree") for item in elliott_events),
        "events_by_trigger": counts(item.get("trigger") for item in elliott_events),
        "events_by_partition": counts(item.get("partition") for item in elliott_events),
        "window_row_count": len(event_windows),
        "transition_count": len(transitions),
        "lead_lag_counts": counts(item.get("lead_lag") for item in transitions),
        "lead_lag_median_sessions": float(np.median(offsets)) if offsets else None,
        "relation_count": len(relations),
        "relation_counts": counts(item.get("relation") for item in relations),
        "historical_pit_elliott_stream_present": len(elliott_events) > 0,
        "cross_system_overlap_testable": bool(elliott_events and event_windows),
        "lead_lag_testable": bool(transitions),
        "stage_specific_incremental_performance_evaluated": False,
        "incremental_predictive_value_claimed": False,
        "forward_return_effects_evaluated": False,
        "uncertainty_intervals_evaluated": False,
        "outcome_validation_deferred_to": "6G",
        "spent_holdout_policy": {
            "legacy_validation_start": config.spent_validation_start,
            "legacy_phase1b_2_validation_is_spent": True,
            "may_be_used_for_6e_descriptive_overlap": True,
            "may_be_used_to_select_or_tune_6e_rules": False,
            "new_unspent_or_prospective_evidence_required_for_incremental_claim": True,
        },
        "present_day_elliott_reconstruction_permitted": False,
        "scanner_rules_reoptimized": False,
        "trade_decision_emitted": False,
        "order_instruction_emitted": False,
        "research_only": True,
    }
