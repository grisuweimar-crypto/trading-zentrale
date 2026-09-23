from __future__ import annotations

"""Phase 1B: point-in-time timing-pattern research.

Research-only. Score and R0-R5 remain quality/regime descriptors, not trade
signals. This module studies changes in those descriptors and their underlying
indicators against later relative returns.
"""

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    HORIZONS,
    Phase1AConfig,
    _peer_median,
    _peer_medians,
    _price_rows,
    _scanner_rows,
    build_events,
    cooldown_events,
    _spearman,
)

NUMERIC_ALIASES = {
    "score": ("score",),
    "opportunity": ("opportunity", "opportunity_score"),
    "risk": ("risk", "risk_score"),
    "rs3m": ("rs3m", "RS3M"),
    "trend200": ("trend200", "Trend200"),
    "cycle": ("cycle", "cycle_pct", "Zyklus %"),
}
ELLIOTT_ALIASES = ("elliott_signal", "elliott", "Elliott-Signal", "elliott_signal_raw")


@dataclass(frozen=True)
class Phase1BConfig:
    stable_start: str = "2026-04-15"
    discovery_end: str = "2026-07-31"
    validation_start: str = "2026-08-01"
    cooldown_sessions: int = 5
    min_single_n: int = 30
    min_pattern_discovery_n: int = 30
    min_pattern_validation_n: int = 20
    max_combo_size: int = 3
    max_atoms_for_combos: int = 18
    top_patterns: int = 12


def _first_column(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _normalise_elliott(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return None
    if "BUY" in text or "LONG" in text:
        return "BUY"
    if "SELL" in text or "SHORT" in text:
        return "SELL"
    if "HOLD" in text or "NEUT" in text:
        return "HOLD"
    return text


def _r_number(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return np.nan
    text = str(value).strip().upper()
    if text.startswith("R") and text[1:].isdigit():
        number = int(text[1:])
        if 0 <= number <= 5:
            return float(number)
    return np.nan


def feature_rows(
    history: pd.DataFrame,
    config: Phase1BConfig = Phase1BConfig(),
) -> tuple[pd.DataFrame, dict]:
    frame = _scanner_rows(history)
    frame = frame.loc[
        (~frame["is_crypto"]) & (frame["date"] >= pd.Timestamp(config.stable_start))
    ].copy()

    sources: dict[str, str | None] = {}
    for canonical, aliases in NUMERIC_ALIASES.items():
        source = _first_column(frame, aliases)
        sources[canonical] = source
        frame[canonical] = pd.to_numeric(frame[source], errors="coerce") if source else np.nan

    elliott_source = _first_column(frame, ELLIOTT_ALIASES)
    sources["elliott"] = elliott_source
    frame["elliott_state"] = (
        frame[elliott_source].map(_normalise_elliott) if elliott_source else None
    )
    frame["r_num"] = frame.get(
        "r_code", pd.Series(index=frame.index, dtype=object)
    ).map(_r_number)

    frame = frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    grouped = frame.groupby("symbol", sort=False)
    for name in NUMERIC_ALIASES:
        for lag in (1, 5, 10):
            frame[f"{name}_d{lag}"] = frame[name] - grouped[name].shift(lag)
    frame["r_d1"] = frame["r_num"] - grouped["r_num"].shift(1)
    frame["r_d5"] = frame["r_num"] - grouped["r_num"].shift(5)
    frame["trend_prev1"] = grouped["trend200"].shift(1)
    frame["cycle_prev1"] = grouped["cycle"].shift(1)
    frame["elliott_prev1"] = grouped["elliott_state"].shift(1)

    atoms: dict[str, pd.Series] = {}
    for name in ("score", "opportunity", "risk", "rs3m", "trend200", "cycle"):
        for lag in (1, 5, 10):
            col = f"{name}_d{lag}"
            valid = frame[col].notna()
            atoms[f"{col}_up"] = valid & (frame[col] > 0)
            atoms[f"{col}_down"] = valid & (frame[col] < 0)

    atoms["trend_cross_up"] = (
        frame["trend200"].notna()
        & frame["trend_prev1"].notna()
        & (frame["trend200"] >= 0)
        & (frame["trend_prev1"] < 0)
    )
    atoms["trend_cross_down"] = (
        frame["trend200"].notna()
        & frame["trend_prev1"].notna()
        & (frame["trend200"] < 0)
        & (frame["trend_prev1"] >= 0)
    )
    for level in (25.0, 50.0, 75.0):
        atoms[f"cycle_cross_up_{int(level)}"] = (
            frame["cycle"].notna()
            & frame["cycle_prev1"].notna()
            & (frame["cycle"] >= level)
            & (frame["cycle_prev1"] < level)
        )
        atoms[f"cycle_cross_down_{int(level)}"] = (
            frame["cycle"].notna()
            & frame["cycle_prev1"].notna()
            & (frame["cycle"] < level)
            & (frame["cycle_prev1"] >= level)
        )

    atoms["r_upgrade"] = frame["r_d1"].notna() & (frame["r_d1"] > 0)
    atoms["r_downgrade"] = frame["r_d1"].notna() & (frame["r_d1"] < 0)
    atoms["r_high"] = frame["r_num"].notna() & (frame["r_num"] >= 4)
    atoms["r_low"] = frame["r_num"].notna() & (frame["r_num"] <= 2)

    if elliott_source:
        atoms["elliott_buy"] = frame["elliott_state"].eq("BUY")
        atoms["elliott_sell"] = frame["elliott_state"].eq("SELL")
        has_previous = frame["elliott_prev1"].notna()
        atoms["elliott_to_buy"] = (
            has_previous
            & frame["elliott_state"].eq("BUY")
            & ~frame["elliott_prev1"].eq("BUY")
        )
        atoms["elliott_to_sell"] = (
            has_previous
            & frame["elliott_state"].eq("SELL")
            & ~frame["elliott_prev1"].eq("SELL")
        )

    for name, mask in atoms.items():
        frame[name] = mask.fillna(False).astype(bool)

    coverage = {
        "rows": int(len(frame)),
        "symbols": int(frame["symbol"].nunique()),
        "date_min": str(frame["date"].min().date()) if len(frame) else None,
        "date_max": str(frame["date"].max().date()) if len(frame) else None,
        "sources": sources,
        "non_null": {
            key: int(frame[key].notna().sum())
            for key in (
                "score",
                "opportunity",
                "risk",
                "rs3m",
                "trend200",
                "cycle",
                "r_num",
                "elliott_state",
            )
        },
        "atomic_patterns": list(atoms),
    }
    return frame, coverage


def build_timing_events(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase1BConfig = Phase1BConfig(),
) -> tuple[pd.DataFrame, dict]:
    features, coverage = feature_rows(history, config)
    events = build_events(
        history,
        prices,
        Phase1AConfig(
            stable_start=config.stable_start,
            cooldown_sessions=config.cooldown_sessions,
        ),
    )
    if events.empty:
        return events, coverage

    merge_cols = [
        c for c in features.columns if c not in {"name", "observation_type", "is_crypto"}
    ]
    merged = events.merge(
        features[merge_cols],
        left_on=["obs_date", "symbol"],
        right_on=["date", "symbol"],
        how="left",
        suffixes=("", "_feature"),
    )
    return merged.drop(columns=["date"], errors="ignore"), coverage


def _add_peer_excess(events: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Attach leave-one-symbol-out peer returns before sampling/cooldown."""
    ret = f"return_{horizon}t"
    work = events.copy()
    work[ret] = pd.to_numeric(work[ret], errors="coerce")
    baselines, fallback = _peer_medians(work, ret)
    work[f"peer_excess_{horizon}t"] = work[ret] - work.apply(
        lambda row: _peer_median(row, baselines, fallback), axis=1
    )
    return work


def _strict_windows(
    work: pd.DataFrame,
    horizon: int,
    config: Phase1BConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return purged windows with peer labels rebuilt inside each cohort.

    Discovery peers must be horizon-eligible before they can contribute to a
    same-currency or global daily median. This prevents a row whose target ends
    after the discovery cutoff from leaking its future return into an eligible
    row's discovery peer excess. Peer labels are still built before cooldown.
    """
    discovery_end = pd.Timestamp(config.discovery_end)
    validation_start = pd.Timestamp(config.validation_start)
    end_col = f"end_date_{horizon}t"
    end_date = pd.to_datetime(work[end_col], errors="coerce")
    discovery = work.loc[
        (work["obs_date"] <= discovery_end)
        & end_date.notna()
        & (end_date <= discovery_end)
    ].copy()
    validation = work.loc[work["obs_date"] >= validation_start].copy()

    peer_col = f"peer_excess_{horizon}t"
    discovery = _add_peer_excess(
        discovery.drop(columns=[peer_col], errors="ignore"), horizon
    )
    validation = _add_peer_excess(
        validation.drop(columns=[peer_col], errors="ignore"), horizon
    )
    return discovery, validation


def _continuous_summary_frame(
    work: pd.DataFrame,
    target: str,
    config: Phase1BConfig,
) -> dict:
    columns = [
        f"{name}_d{lag}"
        for name in ("score", "opportunity", "risk", "rs3m", "trend200", "cycle")
        for lag in (1, 5, 10)
    ] + ["r_d1", "r_d5"]
    out: dict[str, dict] = {}
    for column in columns:
        if column not in work.columns:
            continue
        z = work[[column, target]].dropna()
        if len(z) < config.min_single_n or z[column].nunique() < 3:
            continue
        q20, q80 = z[column].quantile([0.2, 0.8])
        low = z.loc[z[column] <= q20, target]
        high = z.loc[z[column] >= q80, target]
        out[column] = {
            "N": int(len(z)),
            "spearman": _spearman(z[column], z[target]),
            "low20_N": int(len(low)),
            "high20_N": int(len(high)),
            "high_minus_low_mean_peer_excess": (
                float(high.mean() - low.mean()) if len(low) and len(high) else None
            ),
            "high_positive_peer_excess_rate": float((high > 0).mean()) if len(high) else None,
            "low_positive_peer_excess_rate": float((low > 0).mean()) if len(low) else None,
        }
    return out


def continuous_feature_summary(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    config: Phase1BConfig,
) -> dict:
    full = _add_peer_excess(events, horizon)
    discovery, validation = _strict_windows(full, horizon, config)
    discovery = cooldown_events(discovery, prices, config.cooldown_sessions)
    validation = cooldown_events(validation, prices, config.cooldown_sessions)
    target = f"peer_excess_{horizon}t"
    return {
        "discovery": _continuous_summary_frame(discovery, target, config),
        "validation": _continuous_summary_frame(validation, target, config),
    }


def _stats_from_occurrences(
    occurrences: pd.DataFrame,
    baseline: pd.DataFrame,
    target: str,
) -> dict | None:
    values = occurrences[target].dropna()
    if values.empty:
        return None
    baseline_values = baseline[target].dropna()
    positive = float((values > 0).mean())
    baseline_positive = (
        float((baseline_values > 0).mean()) if len(baseline_values) else None
    )
    return {
        "N": int(len(values)),
        "mean_peer_excess": float(values.mean()),
        "median_peer_excess": float(values.median()),
        "positive_rate": positive,
        "baseline_positive_rate": baseline_positive,
        "positive_rate_uplift": (
            positive - baseline_positive if baseline_positive is not None else None
        ),
    }


def _session_position_maps(prices: pd.DataFrame) -> dict[str, dict[pd.Timestamp, int]]:
    """Build market-session positions once and reuse them for all candidates."""
    price = _price_rows(prices)
    return {
        str(symbol): {
            pd.Timestamp(day): position
            for position, day in enumerate(group["date"].tolist())
        }
        for symbol, group in price.groupby("symbol", sort=False)
    }


def _cooldown_matching(
    work: pd.DataFrame,
    prices: pd.DataFrame,
    mask: pd.Series,
    sessions: int,
    position_maps: dict[str, dict[pd.Timestamp, int]] | None = None,
) -> pd.DataFrame:
    """Apply cooldown only after a pattern/state occurrence is selected."""
    selected = work.loc[mask].copy()
    if selected.empty:
        return selected
    if sessions <= 0:
        return selected.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)

    maps = position_maps if position_maps is not None else _session_position_maps(prices)
    keep: list[int] = []
    for symbol, group in selected.groupby("symbol", sort=False):
        mapping = maps.get(str(symbol), {})
        last_position: int | None = None
        for idx, row in group.sort_values("start_market_date", kind="mergesort").iterrows():
            position = mapping.get(pd.Timestamp(row["start_market_date"]))
            if position is None:
                continue
            if last_position is None or position - last_position >= sessions:
                keep.append(idx)
                last_position = position
    if not keep:
        return selected.iloc[0:0].copy()
    return (
        selected.loc[keep]
        .sort_values(["obs_date", "symbol"], kind="mergesort")
        .reset_index(drop=True)
    )


def _candidate_discovery_stats(
    discovery: pd.DataFrame,
    prices: pd.DataFrame,
    combo: tuple[str, ...],
    target: str,
    config: Phase1BConfig,
    position_maps: dict[str, dict[pd.Timestamp, int]],
) -> dict | None:
    mask = discovery[list(combo)].all(axis=1)
    if int(mask.sum()) < config.min_pattern_discovery_n:
        return None
    occurrences = _cooldown_matching(
        discovery,
        prices,
        mask,
        config.cooldown_sessions,
        position_maps,
    )
    stats = _stats_from_occurrences(occurrences, discovery, target)
    if not stats or stats["N"] < config.min_pattern_discovery_n:
        return None
    return stats


def discover_patterns(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    coverage: dict,
    config: Phase1BConfig,
) -> dict:
    """Select/freeze candidates on discovery, then evaluate holdout once."""
    full = _add_peer_excess(events, horizon)
    target = f"peer_excess_{horizon}t"
    discovery, validation = _strict_windows(full, horizon, config)
    position_maps = _session_position_maps(prices)

    all_atoms = [
        name
        for name in coverage.get("atomic_patterns", [])
        if name in discovery.columns
    ]

    atom_rows: list[tuple[str, dict]] = []
    for name in all_atoms:
        stats = _candidate_discovery_stats(
            discovery,
            prices,
            (name,),
            target,
            config,
            position_maps,
        )
        if stats:
            atom_rows.append((name, stats))
    atom_rows.sort(
        key=lambda item: abs(item[1]["mean_peer_excess"]), reverse=True
    )
    selected_atoms = [
        name for name, _ in atom_rows[: config.max_atoms_for_combos]
    ]

    discovery_candidates: list[dict] = []
    for size in range(1, config.max_combo_size + 1):
        for combo in combinations(selected_atoms, size):
            stats = _candidate_discovery_stats(
                discovery,
                prices,
                combo,
                target,
                config,
                position_maps,
            )
            if not stats:
                continue
            discovery_candidates.append(
                {
                    "pattern": " & ".join(combo),
                    "conditions": list(combo),
                    "size": size,
                    "discovery": stats,
                    "discovery_abs_effect": abs(stats["mean_peer_excess"]),
                }
            )

    positive = sorted(
        (
            row
            for row in discovery_candidates
            if row["discovery"]["mean_peer_excess"] > 0
        ),
        key=lambda row: row["discovery_abs_effect"],
        reverse=True,
    )[: config.top_patterns]
    negative = sorted(
        (
            row
            for row in discovery_candidates
            if row["discovery"]["mean_peer_excess"] < 0
        ),
        key=lambda row: row["discovery_abs_effect"],
        reverse=True,
    )[: config.top_patterns]
    frozen = positive + negative

    evaluated: list[dict] = []
    for row in frozen:
        conditions = tuple(row["conditions"])
        vmask = validation[list(conditions)].all(axis=1)
        validation_occurrences = _cooldown_matching(
            validation,
            prices,
            vmask,
            config.cooldown_sessions,
            position_maps,
        )
        validation_stats = _stats_from_occurrences(
            validation_occurrences, validation, target
        )
        discovery_sign = np.sign(row["discovery"]["mean_peer_excess"])
        validation_sign = (
            np.sign(validation_stats["mean_peer_excess"])
            if validation_stats and validation_stats["mean_peer_excess"] != 0
            else 0
        )
        evaluated.append(
            {
                **row,
                "validation": validation_stats,
                "validation_sufficient": bool(
                    validation_stats
                    and validation_stats["N"] >= config.min_pattern_validation_n
                ),
                "direction_confirmed": bool(
                    validation_stats
                    and validation_sign != 0
                    and validation_sign == discovery_sign
                ),
            }
        )

    frozen_positive_names = {row["pattern"] for row in positive}
    evaluated_positive = [
        row for row in evaluated if row["pattern"] in frozen_positive_names
    ]
    evaluated_negative = [
        row for row in evaluated if row["pattern"] not in frozen_positive_names
    ]

    return {
        "discovery_window": [config.stable_start, config.discovery_end],
        "discovery_requires_target_end_by_cutoff": True,
        "validation_window": [
            config.validation_start,
            str(full["obs_date"].max().date()) if len(full) else None,
        ],
        "candidate_selection_uses_validation": False,
        "cooldown_applied_after_pattern_match": True,
        "discovery_events": int(len(discovery)),
        "validation_events": int(len(validation)),
        "selected_atoms_discovery_only": selected_atoms,
        "discovery_candidate_count": int(len(discovery_candidates)),
        "frozen_positive": evaluated_positive,
        "frozen_negative": evaluated_negative,
        "validation_confirmed": [
            row
            for row in evaluated
            if row["validation_sufficient"] and row["direction_confirmed"]
        ],
    }


def _categorical_summary_frame(
    work: pd.DataFrame,
    target: str,
) -> dict:
    out: dict[str, dict] = {}
    for column in ("r_code", "elliott_state"):
        if column not in work.columns:
            continue
        states: dict[str, dict] = {}
        for state, group in work.dropna(subset=[column, target]).groupby(column):
            if len(group) < 10:
                continue
            states[str(state)] = {
                "N": int(len(group)),
                "mean_peer_excess": float(group[target].mean()),
                "median_peer_excess": float(group[target].median()),
                "positive_rate": float((group[target] > 0).mean()),
            }
        if states:
            out[column] = states
    return out


def categorical_states(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    config: Phase1BConfig,
) -> dict:
    full = _add_peer_excess(events, horizon)
    discovery, validation = _strict_windows(full, horizon, config)
    discovery = cooldown_events(discovery, prices, config.cooldown_sessions)
    validation = cooldown_events(validation, prices, config.cooldown_sessions)
    target = f"peer_excess_{horizon}t"
    return {
        "discovery": _categorical_summary_frame(discovery, target),
        "validation": _categorical_summary_frame(validation, target),
    }


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase1BConfig = Phase1BConfig(),
) -> dict:
    events, coverage = build_timing_events(history, prices, config)
    result = {
        "phase": "1B_timing_patterns",
        "semantics": {
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "goal": (
                "find point-in-time indicator changes/states associated with "
                "later relative performance"
            ),
        },
        "coverage": coverage,
        "events": int(len(events)),
        "horizons": {},
    }
    for horizon in HORIZONS:
        result["horizons"][str(horizon)] = {
            "continuous_changes": (
                continuous_feature_summary(events, prices, horizon, config)
                if len(events)
                else {"discovery": {}, "validation": {}}
            ),
            "categorical_states": (
                categorical_states(events, prices, horizon, config)
                if len(events)
                else {"discovery": {}, "validation": {}}
            ),
            "patterns": (
                discover_patterns(events, prices, horizon, coverage, config)
                if len(events)
                else {}
            ),
        }
    return result


def run(
    history_path: str | Path,
    price_path: str | Path,
    output_path: str | Path,
    config: Phase1BConfig = Phase1BConfig(),
) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(price_path, low_memory=False)
    result = analyze(history, prices, config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return result
