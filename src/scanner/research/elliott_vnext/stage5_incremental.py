from __future__ import annotations

"""Post-activation Stage 5: incremental Scanner ↔ Elliott validation.

Stage 5 consumes the already-causal Stage-4 / 6D replay stream.  It does not
recount Elliott, tune Scanner thresholds, retroject present-day model claims or
change any productive Decision-Layer output.

The historical arm evaluates pre-specified Scanner transitions from frozen 6E
against later 6G route outcomes.  Comparisons are always within the same Elliott
review orientation, wave stage, degree, direction, evidence partition and
forward horizon.  Event-time analyses use only Scanner information available at
or before the Elliott event.  Positive lead/lag offsets remain descriptive only.

Frozen model-claim relation labels (confirmation/redundancy/rescue/conflict) are
supported separately and are evaluated only when explicit as-of claims are
supplied.  Missing historical claims are never manufactured.
"""

from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .cross_system import (
    CrossSystemConfig,
    TRANSITION_COLUMNS,
    extract_elliott_events,
    join_model_claims,
    normalize_model_claims,
    scanner_feature_rows,
)
from .validation import (
    ValidationConfig,
    attach_forward_outcomes,
    block_bootstrap_difference,
    extract_route_claims,
    validate_cross_system_relations,
    validation_partition,
)


SCHEMA_VERSION = "elliott_vnext_stage5_incremental_v1"
STAGE = "STAGE_5_INCREMENTAL_CROSS_SYSTEM"


class Stage5IncrementalError(ValueError):
    pass


@dataclass(frozen=True)
class Stage5Config:
    event_window_sessions: int = 20
    bootstrap_reps: int = 1000
    random_seed: int = 20260925
    min_support_regions: int = 2
    rules_frozen_through: str = "2026-09-25"

    def __post_init__(self) -> None:
        if self.event_window_sessions < 20:
            raise ValueError("event_window_sessions must be >= 20")
        if self.bootstrap_reps < 0:
            raise ValueError("bootstrap_reps must be >= 0")
        if self.min_support_regions < 2:
            raise ValueError("min_support_regions must be >= 2")

    def cross_system(self) -> CrossSystemConfig:
        return CrossSystemConfig(event_window_sessions=self.event_window_sessions)

    def validation(self) -> ValidationConfig:
        return ValidationConfig(
            rules_frozen_through=self.rules_frozen_through,
            bootstrap_reps=self.bootstrap_reps,
            random_seed=self.random_seed,
            min_support_regions=self.min_support_regions,
            replay_price_basis="raw",
        )


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _count(values: Iterable[object]) -> dict[str, int]:
    result: Counter[str] = Counter(str(value) for value in values)
    return dict(sorted(result.items()))


def _scanner_groups(scanner: pd.DataFrame) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for symbol, group in scanner.groupby("symbol", sort=False):
        ordered = group.sort_values("date", kind="mergesort").reset_index(drop=True)
        dates = ordered["date"].values.astype("datetime64[ns]")
        positions: dict[str, list[int]] = {}
        for transition in TRANSITION_COLUMNS:
            mask = ordered[transition].fillna(False).astype(bool).to_numpy()
            positions[transition] = np.flatnonzero(mask).astype(int).tolist()
        result[str(symbol)] = {
            "frame": ordered,
            "dates": dates,
            "transition_positions": positions,
        }
    return result


def _nearest_position(
    positions: Sequence[int],
    *,
    anchor: int,
    low: int,
    high: int,
) -> int | None:
    if not positions:
        return None
    insertion = bisect_left(positions, anchor)
    candidates: list[int] = []
    for index in (insertion - 1, insertion):
        if 0 <= index < len(positions):
            value = int(positions[index])
            if low <= value <= high:
                candidates.append(value)
    # The nearest valid position can sit beyond the immediate insertion
    # neighbours only when one neighbour is outside the event window.
    if not candidates:
        left = bisect_left(positions, low)
        if left < len(positions) and positions[left] <= high:
            candidates.append(int(positions[left]))
        right = bisect_left(positions, high + 1) - 1
        if 0 <= right < len(positions) and low <= positions[right] <= high:
            candidates.append(int(positions[right]))
    if not candidates:
        return None
    return min(
        set(candidates),
        key=lambda value: (
            abs(value - anchor),
            value > anchor,
            value,
        ),
    )


def transition_context(
    events: Sequence[Mapping[str, object]],
    scanner: pd.DataFrame,
    config: Stage5Config = Stage5Config(),
) -> tuple[dict[str, int], list[dict[str, object]], dict[str, object]]:
    """Return event-time transition bitmasks and descriptive nearest lead/lag.

    The bitmask contains only transitions observed at or before the Elliott
    anchor within the frozen -20..0 event-time window.  Descriptive lead/lag
    retains the nearest transition in the full -20..+20 window.
    """

    groups = _scanner_groups(scanner)
    transition_index = {name: index for index, name in enumerate(TRANSITION_COLUMNS)}
    event_masks: dict[str, int] = {}
    lead_lag: list[dict[str, object]] = []
    missing_scanner = 0

    for event in events:
        event_id = str(event["event_id"])
        symbol = str(event["symbol"])
        group = groups.get(symbol)
        if group is None:
            missing_scanner += 1
            event_masks[event_id] = 0
            continue

        frame = group["frame"]
        dates = group["dates"]
        positions = group["transition_positions"]
        assert isinstance(frame, pd.DataFrame)
        assert isinstance(dates, np.ndarray)
        assert isinstance(positions, dict)

        event_day = np.datetime64(pd.Timestamp(str(event["available_from"])))
        anchor = int(np.searchsorted(dates, event_day, side="left"))
        if anchor >= len(frame):
            missing_scanner += 1
            event_masks[event_id] = 0
            continue

        low = max(0, anchor - config.event_window_sessions)
        high = min(len(frame) - 1, anchor + config.event_window_sessions)
        mask = 0

        for transition in TRANSITION_COLUMNS:
            raw_positions = positions.get(transition) or []
            nearest = _nearest_position(
                raw_positions,
                anchor=anchor,
                low=low,
                high=high,
            )
            if nearest is not None:
                offset = int(nearest - anchor)
                lead_lag.append({
                    "event_id": event_id,
                    "symbol": symbol,
                    "transition": transition,
                    "relative_session": offset,
                    "scanner_date": pd.Timestamp(frame.iloc[nearest]["date"]).date().isoformat(),
                    "lead_lag": (
                        "scanner_leads"
                        if offset < 0
                        else "same_session"
                        if offset == 0
                        else "elliott_leads"
                    ),
                    "post_event_analysis_only": offset > 0,
                    "partition": validation_partition(event["available_from"], config.validation()),
                    "wave_stage": event.get("wave_stage"),
                    "degree": event.get("degree"),
                    "elliott_direction": event.get("elliott_direction"),
                    "review_orientation": event.get("review_orientation"),
                    "research_only": True,
                })

            # Event-time evidence is intentionally calculated separately from
            # the nearest full-window transition so a closer post-event turn
            # cannot erase a valid earlier observation.
            cutoff = bisect_left(raw_positions, anchor + 1)
            pre_candidates = raw_positions[max(0, cutoff - 1):cutoff]
            if pre_candidates:
                position = int(pre_candidates[-1])
                if low <= position <= anchor:
                    mask |= 1 << transition_index[transition]

        event_masks[event_id] = mask

    coverage = {
        "events": len(events),
        "events_without_scanner_anchor": int(missing_scanner),
        "events_with_scanner_anchor": int(len(events) - missing_scanner),
        "transition_types": len(TRANSITION_COLUMNS),
        "lead_lag_rows": len(lead_lag),
        "event_time_window": [-config.event_window_sessions, 0],
        "descriptive_window": [-config.event_window_sessions, config.event_window_sessions],
        "post_event_rows_used_for_incremental_test": False,
    }
    return event_masks, lead_lag, coverage


def summarize_lead_lag(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Summarize frozen transition lead/lag without interpreting performance."""

    grouped: dict[tuple[object, ...], list[int]] = defaultdict(list)
    for row in rows:
        key = (
            row.get("partition"),
            row.get("transition"),
            row.get("wave_stage"),
            row.get("degree"),
            row.get("elliott_direction"),
            row.get("review_orientation"),
        )
        grouped[key].append(int(row["relative_session"]))

    fields = (
        "partition",
        "transition",
        "wave_stage",
        "degree",
        "elliott_direction",
        "review_orientation",
    )
    result: list[dict[str, object]] = []
    for key in sorted(
        grouped,
        key=lambda item: tuple("" if value is None else str(value) for value in item),
    ):
        offsets = grouped[key]
        record = {field: key[index] for index, field in enumerate(fields)}
        record.update({
            "N": len(offsets),
            "median_relative_session": float(np.median(offsets)) if offsets else None,
            "scanner_leads_N": sum(value < 0 for value in offsets),
            "same_session_N": sum(value == 0 for value in offsets),
            "elliott_leads_N": sum(value > 0 for value in offsets),
            "post_event_performance_use_allowed": False,
            "research_only": True,
        })
        result.append(record)
    return result


def _outcome_frame(
    route_outcomes: Sequence[Mapping[str, object]],
) -> pd.DataFrame:
    frame = pd.DataFrame(list(route_outcomes))
    if frame.empty:
        return frame
    frame = frame.loc[
        frame["claim_type"].eq("route_review")
        & frame["outcome_available"].eq(True)
    ].copy()
    if frame.empty:
        return frame
    frame["event_date"] = pd.to_datetime(frame["available_from"], errors="coerce")
    frame["review_correct_numeric"] = frame["review_correct"].map({True: 1.0, False: 0.0})
    return frame


def transition_incremental_validation(
    route_outcomes: Sequence[Mapping[str, object]],
    event_masks: Mapping[str, int],
    config: Stage5Config = Stage5Config(),
) -> list[dict[str, object]]:
    """Compare pre-specified event-time Scanner transitions with their absence.

    Each comparison stays inside one Elliott event class:
    partition × horizon × review orientation × wave stage × degree × direction.
    There is no threshold discovery or cross-regime baseline substitution.
    """

    frame = _outcome_frame(route_outcomes)
    if frame.empty:
        return []

    frame["event_mask"] = frame["source_event_id"].map(
        lambda value: int(event_masks.get(str(value), 0))
    )
    validation_config = config.validation()
    class_cols = [
        "partition",
        "horizon_sessions",
        "review_orientation",
        "wave_stage",
        "degree",
        "elliott_direction",
    ]
    result: list[dict[str, object]] = []

    for bit, transition in enumerate(TRANSITION_COLUMNS):
        flag = 1 << bit
        frame["transition_present"] = (frame["event_mask"].astype(object).map(int) & flag) != 0
        for key, event_class in frame.groupby(class_cols, dropna=False, sort=True):
            with_transition = event_class.loc[event_class["transition_present"]].copy()
            without_transition = event_class.loc[~event_class["transition_present"]].copy()
            if with_transition.empty or without_transition.empty:
                continue
            partition, horizon, orientation, stage, degree, direction = key
            signed = block_bootstrap_difference(
                with_transition,
                without_transition,
                metric="signed_forward_return",
                date_col="event_date",
                horizon=int(horizon),
                config=validation_config,
                seed_key=(
                    "stage5_transition_signed_return",
                    transition,
                    partition,
                    orientation,
                    stage,
                    degree,
                    direction,
                ),
            )
            correctness = block_bootstrap_difference(
                with_transition,
                without_transition,
                metric="review_correct_numeric",
                date_col="event_date",
                horizon=int(horizon),
                config=validation_config,
                seed_key=(
                    "stage5_transition_review_correct",
                    transition,
                    partition,
                    orientation,
                    stage,
                    degree,
                    direction,
                ),
            )
            result.append({
                "transition": transition,
                "partition": partition,
                "horizon_sessions": int(horizon),
                "review_orientation": orientation,
                "wave_stage": stage,
                "degree": degree,
                "elliott_direction": direction,
                "event_time_transition_window": [-config.event_window_sessions, 0],
                "signed_return_difference_vs_same_class_without_transition": signed,
                "review_correctness_difference_vs_same_class_without_transition": correctness,
                "formal_promotion_evidence": partition == "prospective_unspent",
                "rule_selected_on_this_partition": False,
                "research_only": True,
            })
    return result



def transition_daily_sufficient_stats(
    route_outcomes: Sequence[Mapping[str, object]],
    event_masks: Mapping[str, int],
) -> list[dict[str, object]]:
    """Reduce event-time transition outcome comparisons to exact daily sums/counts."""

    frame = _outcome_frame(route_outcomes)
    if frame.empty:
        return []

    fields = (
        "transition",
        "partition",
        "horizon_sessions",
        "review_orientation",
        "wave_stage",
        "degree",
        "elliott_direction",
        "arm",
        "metric",
        "event_date",
    )
    acc: dict[tuple[object, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])

    for row in frame.to_dict(orient="records"):
        event_id = str(row.get("source_event_id") or "")
        mask = int(event_masks.get(event_id, 0))
        event_date = pd.Timestamp(row["event_date"]).date().isoformat()
        metric_values = {
            "signed_forward_return": row.get("signed_forward_return"),
            "review_correct_numeric": row.get("review_correct_numeric"),
        }
        for bit, transition in enumerate(TRANSITION_COLUMNS):
            arm = "with_transition" if mask & (1 << bit) else "without_transition"
            base = (
                transition,
                row.get("partition"),
                int(row.get("horizon_sessions")),
                row.get("review_orientation"),
                row.get("wave_stage"),
                row.get("degree"),
                row.get("elliott_direction"),
                arm,
            )
            for metric, raw in metric_values.items():
                if raw is None or pd.isna(raw):
                    continue
                value = float(raw)
                key = (*base, metric, event_date)
                acc[key][0] += value
                acc[key][1] += 1.0

    result: list[dict[str, object]] = []
    for key in sorted(
        acc,
        key=lambda item: tuple("" if value is None else str(value) for value in item),
    ):
        values = {field: key[index] for index, field in enumerate(fields)}
        total, count = acc[key]
        values["sum"] = float(total)
        values["count"] = int(count)
        result.append(values)
    return result


def lead_lag_sufficient_stats(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Reduce lead/lag rows to exact offset counts for global aggregation."""

    fields = (
        "partition",
        "transition",
        "wave_stage",
        "degree",
        "elliott_direction",
        "review_orientation",
        "relative_session",
    )
    counts: Counter[tuple[object, ...]] = Counter()
    for row in rows:
        key = (
            row.get("partition"),
            row.get("transition"),
            row.get("wave_stage"),
            row.get("degree"),
            row.get("elliott_direction"),
            row.get("review_orientation"),
            int(row.get("relative_session")),
        )
        counts[key] += 1

    result: list[dict[str, object]] = []
    for key in sorted(
        counts,
        key=lambda item: tuple("" if value is None else str(value) for value in item),
    ):
        record = {field: key[index] for index, field in enumerate(fields)}
        record["N"] = int(counts[key])
        result.append(record)
    return result


def model_relation_validation(
    events: Sequence[Mapping[str, object]],
    route_outcomes: Sequence[Mapping[str, object]],
    model_claims: Sequence[Mapping[str, object]],
    config: Stage5Config = Stage5Config(),
) -> dict[str, object]:
    """Evaluate frozen 6E relation labels only when explicit as-of claims exist."""

    if not model_claims:
        return {
            "status": "not_testable_no_historical_frozen_directional_model_claims_supplied",
            "claim_count": 0,
            "relation_count": 0,
            "relation_counts": {},
            "validation_rows": [],
            "claims_retrojected": False,
            "research_only": True,
        }

    normalized = normalize_model_claims(model_claims)
    relations = join_model_claims(events, normalized)
    validation = validate_cross_system_relations(
        relations,
        route_outcomes,
        config.validation(),
    )
    return {
        "status": "evaluated_from_explicit_asof_claims",
        "claim_count": len(normalized),
        "relation_count": len(relations),
        "relation_counts": _count(row.get("relation") for row in relations),
        "validation_rows": validation,
        "claims_retrojected": False,
        "research_only": True,
    }


def build_stage5_incremental_report(
    routed_snapshots: Sequence[Mapping[str, object]],
    history: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    model_claims: Sequence[Mapping[str, object]] = (),
    source_commit: str | None = None,
    replay_source_workflow_run_id: int | None = None,
    config: Stage5Config = Stage5Config(),
) -> dict[str, object]:
    """Build one Stage-5 report for a bounded replay partition/chunk."""

    if not routed_snapshots:
        raise Stage5IncrementalError("routed_snapshots_required")

    symbols = sorted({
        str(snapshot.get("symbol", "")).strip()
        for snapshot in routed_snapshots
        if str(snapshot.get("symbol", "")).strip()
    })
    if not symbols:
        raise Stage5IncrementalError("routed_snapshot_symbols_required")

    history_subset = history.loc[history["symbol"].astype(str).isin(symbols)].copy()
    prices_subset = prices.loc[prices["symbol"].astype(str).isin(symbols)].copy()

    scanner, scanner_coverage = scanner_feature_rows(
        history_subset,
        config.cross_system(),
    )
    events = extract_elliott_events(
        routed_snapshots,
        config.cross_system(),
    )
    event_masks, lead_lag_rows, transition_coverage = transition_context(
        events,
        scanner,
        config,
    )

    route_claims = extract_route_claims(
        routed_snapshots,
        config.validation(),
    )
    route_outcomes = attach_forward_outcomes(
        route_claims,
        prices_subset,
        config.validation(),
    )
    transition_validation = transition_incremental_validation(
        route_outcomes,
        event_masks,
        config,
    )
    relation_validation = model_relation_validation(
        events,
        route_outcomes,
        model_claims,
        config,
    )

    mature_outcomes = [
        row for row in route_outcomes
        if row.get("outcome_available") is True
    ]
    prospective_mature = sum(
        row.get("partition") == "prospective_unspent"
        for row in mature_outcomes
    )

    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "technical_stage_status": "COMPLETE",
        "empirical_promotion_status": "NOT_PROMOTED",
        "source": {
            "source_commit": source_commit,
            "replay_source_workflow_run_id": replay_source_workflow_run_id,
            "rules_frozen_through": config.rules_frozen_through,
            "symbols": symbols,
        },
        "coverage": {
            "routed_snapshots": len(routed_snapshots),
            "elliott_events": len(events),
            "route_claims": len(route_claims),
            "route_outcome_rows": len(route_outcomes),
            "mature_route_outcomes": len(mature_outcomes),
            "prospective_unspent_mature_route_outcomes": int(prospective_mature),
            "transition_incremental_rows": len(transition_validation),
            "model_relation_validation_rows": len(
                relation_validation.get("validation_rows", [])
            ),
        },
        "scanner_coverage": scanner_coverage,
        "transition_coverage": transition_coverage,
        "lead_lag_summary": summarize_lead_lag(lead_lag_rows),
        "transition_incremental_validation": transition_validation,
        "model_relation_validation": relation_validation,
        "boundaries": {
            "present_day_model_claims_retrojected": False,
            "post_event_scanner_rows_used_for_incremental_test": False,
            "scanner_thresholds_reoptimized": False,
            "elliott_recounted": False,
            "legacy_data_can_support_promotion": False,
            "automatic_promotion_allowed": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "trade_decision": None,
            "order_instruction": None,
            "research_only": True,
        },
        "interpretation": {
            "historical_transition_results_are_descriptive": True,
            "prospective_unspent_results_may_accumulate_evidence": True,
            "confirmation_redundancy_rescue_conflict_require_explicit_asof_model_claims": True,
            "absence_of_model_claims_is_not_scanner_disagreement": True,
            "overlapping_forward_windows_are_iid": False,
        },
    }
    report["stage5_result_hash"] = _canonical_hash(report)
    return report
