from __future__ import annotations

"""Phase 5B frozen, non-adaptive prospective baseline evaluator.

The evaluator consumes only Phase-5B shadow-v2 claims/outcomes.  State names are
not presumed to be ordinal.  It reports reliability/risk behaviour by the
frozen Phase-4 state labels and uses circular moving observation-date blocks
with effective length 2 x horizon for robust uncertainty when temporal support
is sufficient.
"""

from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    derive_peer_labels_v2,
    validate_v2_archives,
)
from scanner.reports.selection_timing import HORIZONS


GROUP_FIELDS = (
    "selection_statistical_state",
    "agreement_state",
    "timing_model_state",
    "risk_model_state",
    "dq_selection_state",
    "dq_timing_state",
    "dq_risk_state",
)


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected frozen-baseline source schema in {p}")
    return frame


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _forbidden_dates(value: object) -> set[str]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(item) for item in parsed}


def apply_fixed_event_spacing(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply the inherited five-session spacing using claim-time bound context."""

    if frame.empty:
        return frame.copy()
    work = frame.copy()
    work["_start"] = pd.to_datetime(work["start_market_date"], errors="coerce").dt.normalize()
    work = work.loc[
        work["_start"].notna()
        & work["cooldown_context_complete"].map(_parse_bool)
    ].copy()
    keep: list[int] = []
    for _, group in work.groupby("symbol", sort=False):
        last_start: str | None = None
        ordered = group.sort_values(["_start", "snapshot_id", "claim_id"], kind="mergesort")
        for idx, row in ordered.iterrows():
            start_text = pd.Timestamp(row["_start"]).date().isoformat()
            forbidden = _forbidden_dates(row.get("cooldown_forbidden_start_dates"))
            if last_start is None or last_start not in forbidden:
                keep.append(idx)
                last_start = start_text
    if not keep:
        return work.iloc[0:0].drop(columns=["_start"])
    return (
        work.loc[keep]
        .sort_values(["as_of", "symbol", "snapshot_id"], kind="mergesort")
        .drop(columns=["_start"])
        .reset_index(drop=True)
    )


def _baseline_dates(frame: pd.DataFrame) -> list[pd.Timestamp]:
    if frame.empty:
        return []
    return sorted(
        pd.Timestamp(value).normalize()
        for value in pd.to_datetime(frame["as_of"], errors="coerce").dropna().unique()
    )


def _support_regions(frame: pd.DataFrame, dates: list[pd.Timestamp], horizon: int) -> int:
    if frame.empty or not dates:
        return 0
    positions = {day: idx for idx, day in enumerate(dates)}
    occurrence_positions = sorted(
        positions[pd.Timestamp(day).normalize()]
        for day in pd.to_datetime(frame["as_of"], errors="coerce").dropna().unique()
        if pd.Timestamp(day).normalize() in positions
    )
    block_length = max(1, 2 * int(horizon))
    count = 0
    last: int | None = None
    for position in occurrence_positions:
        if last is None or position - last >= block_length:
            count += 1
            last = position
    return count


def _numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    for column in (
        "return", "peer_excess", "signed_peer_excess", "direction_hit",
        "adverse_excursion", "path_max_drawdown",
    ):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    return work


def _summary(frame: pd.DataFrame) -> dict[str, object]:
    work = _numeric_frame(frame)
    directional = work.loc[work["direction_hit"].notna()].copy()
    signed = directional["signed_peer_excess"].dropna()
    return {
        "N": int(len(work)),
        "symbols": int(work["symbol"].astype(str).nunique()) if len(work) else 0,
        "snapshots": int(work["snapshot_id"].astype(str).nunique()) if len(work) else 0,
        "observation_dates": int(pd.to_datetime(work["as_of"], errors="coerce").nunique()) if len(work) else 0,
        "directional_N": int(len(directional)),
        "direction_hit_rate": float(directional["direction_hit"].mean()) if len(directional) else None,
        "direction_error_rate": float(1.0 - directional["direction_hit"].mean()) if len(directional) else None,
        "mean_signed_peer_excess": float(signed.mean()) if len(signed) else None,
        "median_signed_peer_excess": float(signed.median()) if len(signed) else None,
        "signed_peer_excess_std": float(signed.std(ddof=1)) if len(signed) > 1 else None,
        "mean_adverse_excursion": float(work["adverse_excursion"].mean()) if len(work) else None,
        "mean_path_max_drawdown": float(work["path_max_drawdown"].mean()) if len(work) else None,
    }


def _interval(values: list[float]) -> list[float] | None:
    if not values:
        return None
    low, high = np.quantile(np.asarray(values, dtype=float), [0.025, 0.975])
    return [float(low), float(high)]


def _bootstrap_summary(
    frame: pd.DataFrame,
    baseline_dates: list[pd.Timestamp],
    horizon: int,
    *,
    reps: int,
    seed: int,
) -> dict[str, object]:
    block_length = max(1, 2 * int(horizon))
    support_regions = _support_regions(frame, baseline_dates, horizon)
    diagnostics: dict[str, object] = {
        "block_length_sessions": block_length,
        "baseline_date_count": int(len(baseline_dates)),
        "support_regions": int(support_regions),
        "bootstrap_reps": int(reps),
    }
    if frame.empty or len(baseline_dates) < 2 or support_regions < 2 or reps <= 0:
        return {
            **diagnostics,
            "direction_hit_rate_95": None,
            "direction_error_rate_95": None,
            "mean_signed_peer_excess_95": None,
            "mean_adverse_excursion_95": None,
            "mean_path_max_drawdown_95": None,
        }

    work = _numeric_frame(frame)
    work["_obs"] = pd.to_datetime(work["as_of"], errors="coerce").dt.normalize()
    by_day = {day: work.loc[work["_obs"].eq(day)].copy() for day in baseline_dates}
    span = min(block_length, len(baseline_dates))
    blocks = [
        [baseline_dates[(start + offset) % len(baseline_dates)] for offset in range(span)]
        for start in range(len(baseline_dates))
    ]
    draws_per_rep = int(np.ceil(len(baseline_dates) / block_length))
    rng = np.random.default_rng(seed)
    hit_rates: list[float] = []
    error_rates: list[float] = []
    signed_means: list[float] = []
    adverse_means: list[float] = []
    drawdown_means: list[float] = []

    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in blocks[idx]][: len(baseline_dates)]
        parts = [by_day[day] for day in sampled_dates if not by_day[day].empty]
        if not parts:
            continue
        sample = pd.concat(parts, ignore_index=True)
        directional = sample.loc[sample["direction_hit"].notna()]
        if len(directional):
            hit = float(directional["direction_hit"].mean())
            hit_rates.append(hit)
            error_rates.append(1.0 - hit)
            signed = directional["signed_peer_excess"].dropna()
            if len(signed):
                signed_means.append(float(signed.mean()))
        adverse = sample["adverse_excursion"].dropna()
        drawdown = sample["path_max_drawdown"].dropna()
        if len(adverse):
            adverse_means.append(float(adverse.mean()))
        if len(drawdown):
            drawdown_means.append(float(drawdown.mean()))

    return {
        **diagnostics,
        "direction_hit_rate_95": _interval(hit_rates),
        "direction_error_rate_95": _interval(error_rates),
        "mean_signed_peer_excess_95": _interval(signed_means),
        "mean_adverse_excursion_95": _interval(adverse_means),
        "mean_path_max_drawdown_95": _interval(drawdown_means),
    }


def _state_groups(
    frame: pd.DataFrame,
    field: str,
    baseline_dates: list[pd.Timestamp],
    horizon: int,
    reps: int,
    seed: int,
) -> dict[str, object]:
    result: dict[str, object] = {}
    values = frame[field].astype(str).replace("", "missing")
    for state in sorted(values.unique()):
        group = frame.loc[values.eq(state)].copy()
        result[state] = {
            **_summary(group),
            "robust_uncertainty": _bootstrap_summary(
                group,
                baseline_dates,
                horizon,
                reps=reps,
                seed=_stable_seed(seed, horizon, field, state),
            ),
        }
    return result


def _multi_state_groups(
    frame: pd.DataFrame,
    field: str,
    empty_label: str,
    baseline_dates: list[pd.Timestamp],
    horizon: int,
    reps: int,
    seed: int,
) -> dict[str, object]:
    exploded_rows: list[dict[str, object]] = []
    for row in frame.to_dict("records"):
        raw = str(row.get(field) or "")
        states = [value for value in raw.split("|") if value] or [empty_label]
        for state in sorted(set(states)):
            item = dict(row)
            item["_state"] = state
            exploded_rows.append(item)
    if not exploded_rows:
        return {}
    exploded = pd.DataFrame(exploded_rows)
    return _state_groups(
        exploded.rename(columns={"_state": "_group_state"}),
        "_group_state",
        baseline_dates,
        horizon,
        reps,
        seed,
    )


def evaluate_frozen_baseline(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    bootstrap_reps: int = 1000,
    random_seed: int = 20260924,
) -> dict[str, object]:
    validate_v2_archives(claims, outcomes)
    derived = derive_peer_labels_v2(claims, outcomes)
    horizons: dict[str, object] = {}

    for horizon in HORIZONS:
        h = derived.loc[pd.to_numeric(derived["horizon_sessions"], errors="coerce").eq(horizon)].copy() if not derived.empty else derived
        spaced = apply_fixed_event_spacing(h)
        dates = _baseline_dates(spaced)
        overall_support = _support_regions(spaced, dates, horizon)
        groups = {
            field: _state_groups(spaced, field, dates, horizon, bootstrap_reps, random_seed)
            for field in GROUP_FIELDS
        }
        groups["timing_matched_evidence_state"] = _multi_state_groups(
            spaced,
            "timing_matched_states",
            "no_matched_timing_evidence",
            dates,
            horizon,
            bootstrap_reps,
            random_seed,
        )
        groups["risk_feature_evidence_state"] = _multi_state_groups(
            spaced,
            "risk_evidence_states",
            "no_risk_evidence",
            dates,
            horizon,
            bootstrap_reps,
            random_seed,
        )

        temporal: dict[str, object] = {}
        if not spaced.empty:
            months = pd.to_datetime(spaced["as_of"], errors="coerce").dt.to_period("M").astype(str)
            for month in sorted(months.unique()):
                temporal[month] = _summary(spaced.loc[months.eq(month)])

        status = "insufficient_evidence"
        if len(spaced):
            status = "descriptive_only"
        if overall_support >= 2:
            status = "robust_frozen_baseline_available"

        horizons[str(horizon)] = {
            "status": status,
            "raw_mature_outcomes": int(
                pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon).sum()
            ) if not outcomes.empty else 0,
            "spaced_outcomes": int(len(spaced)),
            "support_regions": int(overall_support),
            "overall": {
                **_summary(spaced),
                "robust_uncertainty": _bootstrap_summary(
                    spaced,
                    dates,
                    horizon,
                    reps=bootstrap_reps,
                    seed=_stable_seed(random_seed, horizon, "overall"),
                ),
            },
            "groups": groups,
            "temporal_segments": temporal,
        }

    return {
        "phase": "5B_frozen_baseline",
        "status": "collecting_prospective_evidence",
        "horizons": horizons,
        "semantics": {
            "research_only": True,
            "frozen_phase4_reference": True,
            "state_names_assumed_ordinal": False,
            "probability_counted_as_independent_model": False,
            "low_risk_counted_as_positive_return_support": False,
            "fixed_event_spacing_sessions": 5,
            "robust_uncertainty": "circular moving observation-date bootstrap; full date clusters; effective block length 2 x horizon",
            "minimum_time_separated_support_regions": 2,
            "no_probability_calibration_error_claimed_without_claim_probability": True,
            "direction_error_rate_is_reliability_diagnostic_not_probability_calibration": True,
            "adaptive_weights_created": False,
            "scalar_confidence_mapping_created": False,
            "production_confidence_changed": False,
        },
    }


def run_frozen_baseline(
    claims_path: str | Path,
    outcomes_path: str | Path,
    output_path: str | Path,
    *,
    bootstrap_reps: int = 1000,
    random_seed: int = 20260924,
) -> dict[str, object]:
    claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    result = evaluate_frozen_baseline(
        claims,
        outcomes,
        bootstrap_reps=bootstrap_reps,
        random_seed=random_seed,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result
