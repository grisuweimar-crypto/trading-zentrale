from __future__ import annotations

"""Phase 1B: point-in-time timing-pattern research.

Research-only. Score and R0-R5 remain quality/regime descriptors, not trade
signals. This module studies changes in those descriptors and their underlying
indicators against later returns.
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
    _scanner_rows,
    _price_rows,
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
ELLIOTT_ALIASES = (
    "elliott_signal", "elliott", "Elliott-Signal", "elliott_signal_raw"
)


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
    top_patterns: int = 12


def _first_column(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None


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
        n = int(text[1:])
        if 0 <= n <= 5:
            return float(n)
    return np.nan


def feature_rows(history: pd.DataFrame, config: Phase1BConfig = Phase1BConfig()) -> tuple[pd.DataFrame, dict]:
    frame = _scanner_rows(history)
    frame = frame.loc[(~frame["is_crypto"]) & (frame["date"] >= pd.Timestamp(config.stable_start))].copy()

    available: dict[str, str | None] = {}
    for canonical, aliases in NUMERIC_ALIASES.items():
        source = _first_column(frame, aliases)
        available[canonical] = source
        frame[canonical] = pd.to_numeric(frame[source], errors="coerce") if source else np.nan

    elliott_source = _first_column(frame, ELLIOTT_ALIASES)
    available["elliott"] = elliott_source
    frame["elliott_state"] = frame[elliott_source].map(_normalise_elliott) if elliott_source else None
    frame["r_num"] = frame.get("r_code", pd.Series(index=frame.index, dtype=object)).map(_r_number)

    frame = frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    g = frame.groupby("symbol", sort=False)
    for name in NUMERIC_ALIASES:
        for lag in (1, 5, 10):
            frame[f"{name}_d{lag}"] = frame[name] - g[name].shift(lag)
    frame["r_d1"] = frame["r_num"] - g["r_num"].shift(1)
    frame["r_d5"] = frame["r_num"] - g["r_num"].shift(5)
    frame["trend_prev1"] = g["trend200"].shift(1)
    frame["cycle_prev1"] = g["cycle"].shift(1)
    frame["elliott_prev1"] = g["elliott_state"].shift(1)

    # Atomic states are deliberately simple and fixed ex ante. No thresholds are
    # fitted on future returns.
    atoms: dict[str, pd.Series] = {}
    for name in ("score", "opportunity", "risk", "rs3m", "trend200", "cycle"):
        for lag in (1, 5, 10):
            col = f"{name}_d{lag}"
            valid = frame[col].notna()
            atoms[f"{col}_up"] = valid & (frame[col] > 0)
            atoms[f"{col}_down"] = valid & (frame[col] < 0)

    atoms["trend_cross_up"] = frame["trend200"].notna() & frame["trend_prev1"].notna() & (frame["trend200"] >= 0) & (frame["trend_prev1"] < 0)
    atoms["trend_cross_down"] = frame["trend200"].notna() & frame["trend_prev1"].notna() & (frame["trend200"] < 0) & (frame["trend_prev1"] >= 0)
    for level in (25.0, 50.0, 75.0):
        atoms[f"cycle_cross_up_{int(level)}"] = frame["cycle"].notna() & frame["cycle_prev1"].notna() & (frame["cycle"] >= level) & (frame["cycle_prev1"] < level)
        atoms[f"cycle_cross_down_{int(level)}"] = frame["cycle"].notna() & frame["cycle_prev1"].notna() & (frame["cycle"] < level) & (frame["cycle_prev1"] >= level)
    atoms["r_upgrade"] = frame["r_d1"].notna() & (frame["r_d1"] > 0)
    atoms["r_downgrade"] = frame["r_d1"].notna() & (frame["r_d1"] < 0)
    atoms["r_high"] = frame["r_num"].notna() & (frame["r_num"] >= 4)
    atoms["r_low"] = frame["r_num"].notna() & (frame["r_num"] <= 2)
    if elliott_source:
        atoms["elliott_buy"] = frame["elliott_state"].eq("BUY")
        atoms["elliott_sell"] = frame["elliott_state"].eq("SELL")
        atoms["elliott_to_buy"] = frame["elliott_state"].eq("BUY") & ~frame["elliott_prev1"].eq("BUY")
        atoms["elliott_to_sell"] = frame["elliott_state"].eq("SELL") & ~frame["elliott_prev1"].eq("SELL")

    for name, series in atoms.items():
        frame[name] = series.fillna(False).astype(bool)

    coverage = {
        "rows": int(len(frame)),
        "symbols": int(frame["symbol"].nunique()),
        "date_min": str(frame["date"].min().date()) if len(frame) else None,
        "date_max": str(frame["date"].max().date()) if len(frame) else None,
        "sources": available,
        "non_null": {
            key: int(frame[key].notna().sum())
            for key in ["score", "opportunity", "risk", "rs3m", "trend200", "cycle", "r_num", "elliott_state"]
        },
        "atomic_patterns": list(atoms),
    }
    return frame, coverage


def build_timing_events(history: pd.DataFrame, prices: pd.DataFrame, config: Phase1BConfig = Phase1BConfig()) -> tuple[pd.DataFrame, dict]:
    features, coverage = feature_rows(history, config)
    events = build_events(history, prices, Phase1AConfig(stable_start=config.stable_start, cooldown_sessions=config.cooldown_sessions))
    if events.empty:
        return events, coverage

    merge_cols = [c for c in features.columns if c not in {"name", "observation_type", "is_crypto"}]
    merged = events.merge(
        features[merge_cols],
        left_on=["obs_date", "symbol"],
        right_on=["date", "symbol"],
        how="left",
        suffixes=("", "_feature"),
    )
    return merged.drop(columns=["date"], errors="ignore"), coverage


def _add_peer_excess(events: pd.DataFrame, horizon: int) -> pd.DataFrame:
    ret = f"return_{horizon}t"
    work = events.copy()
    currency_median = work.groupby(["obs_date", "currency"])[ret].transform("median")
    global_median = work.groupby("obs_date")[ret].transform("median")
    work[f"peer_excess_{horizon}t"] = work[ret] - currency_median.fillna(global_median)
    return work


def continuous_feature_summary(events: pd.DataFrame, prices: pd.DataFrame, horizon: int, config: Phase1BConfig) -> dict:
    work = _add_peer_excess(cooldown_events(events, prices, config.cooldown_sessions), horizon)
    target = f"peer_excess_{horizon}t"
    cols = [
        f"{name}_d{lag}"
        for name in ("score", "opportunity", "risk", "rs3m", "trend200", "cycle")
        for lag in (1, 5, 10)
    ] + ["r_d1", "r_d5"]
    out = {}
    for col in cols:
        if col not in work.columns:
            continue
        z = work[[col, target]].dropna()
        if len(z) < config.min_single_n or z[col].nunique() < 3:
            continue
        rho = _spearman(z[col], z[target])
        q20, q80 = z[col].quantile([0.2, 0.8])
        low = z.loc[z[col] <= q20, target]
        high = z.loc[z[col] >= q80, target]
        out[col] = {
            "N": int(len(z)),
            "spearman": rho,
            "low20_N": int(len(low)),
            "high20_N": int(len(high)),
            "high_minus_low_mean_peer_excess": float(high.mean() - low.mean()) if len(low) and len(high) else None,
            "high_positive_peer_excess_rate": float((high > 0).mean()) if len(high) else None,
            "low_positive_peer_excess_rate": float((low > 0).mean()) if len(low) else None,
        }
    return out


def _pattern_stats(work: pd.DataFrame, mask: pd.Series, target: str) -> dict | None:
    z = work.loc[mask & work[target].notna(), target]
    if len(z) == 0:
        return None
    baseline = work.loc[work[target].notna(), target]
    return {
        "N": int(len(z)),
        "mean_peer_excess": float(z.mean()),
        "median_peer_excess": float(z.median()),
        "positive_rate": float((z > 0).mean()),
        "baseline_positive_rate": float((baseline > 0).mean()) if len(baseline) else None,
        "positive_rate_uplift": float((z > 0).mean() - (baseline > 0).mean()) if len(baseline) else None,
    }


def discover_patterns(events: pd.DataFrame, prices: pd.DataFrame, horizon: int, coverage: dict, config: Phase1BConfig) -> dict:
    work = _add_peer_excess(cooldown_events(events, prices, config.cooldown_sessions), horizon)
    target = f"peer_excess_{horizon}t"
    discovery = work.loc[work["obs_date"] <= pd.Timestamp(config.discovery_end)].copy()
    validation = work.loc[work["obs_date"] >= pd.Timestamp(config.validation_start)].copy()
    atom_names = [name for name in coverage.get("atomic_patterns", []) if name in work.columns]

    rows = []
    for size in range(1, config.max_combo_size + 1):
        for combo in combinations(atom_names, size):
            dmask = discovery[list(combo)].all(axis=1)
            vmask = validation[list(combo)].all(axis=1)
            ds = _pattern_stats(discovery, dmask, target)
            vs = _pattern_stats(validation, vmask, target)
            if not ds or not vs:
                continue
            if ds["N"] < config.min_pattern_discovery_n or vs["N"] < config.min_pattern_validation_n:
                continue
            dmean = ds["mean_peer_excess"]
            vmean = vs["mean_peer_excess"]
            if dmean == 0 or vmean == 0 or np.sign(dmean) != np.sign(vmean):
                continue
            rows.append({
                "pattern": " & ".join(combo),
                "size": size,
                "discovery": ds,
                "validation": vs,
                "robust_effect": float(np.sign(vmean) * min(abs(dmean), abs(vmean))),
            })

    positive = sorted((r for r in rows if r["validation"]["mean_peer_excess"] > 0), key=lambda r: r["robust_effect"], reverse=True)
    negative = sorted((r for r in rows if r["validation"]["mean_peer_excess"] < 0), key=lambda r: r["robust_effect"])
    return {
        "discovery_window": [config.stable_start, config.discovery_end],
        "validation_window": [config.validation_start, str(work["obs_date"].max().date()) if len(work) else None],
        "eligible_patterns": int(len(rows)),
        "top_positive": positive[: config.top_patterns],
        "top_negative": negative[: config.top_patterns],
    }


def categorical_states(events: pd.DataFrame, prices: pd.DataFrame, horizon: int, config: Phase1BConfig) -> dict:
    work = _add_peer_excess(cooldown_events(events, prices, config.cooldown_sessions), horizon)
    target = f"peer_excess_{horizon}t"
    out = {}
    for column in ("r_code", "elliott_state"):
        if column not in work.columns:
            continue
        states = {}
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


def analyze(history: pd.DataFrame, prices: pd.DataFrame, config: Phase1BConfig = Phase1BConfig()) -> dict:
    events, coverage = build_timing_events(history, prices, config)
    result = {
        "phase": "1B_timing_patterns",
        "semantics": {
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "goal": "find point-in-time indicator changes/states associated with later relative performance",
        },
        "coverage": coverage,
        "events": int(len(events)),
        "horizons": {},
    }
    for horizon in HORIZONS:
        result["horizons"][str(horizon)] = {
            "continuous_changes": continuous_feature_summary(events, prices, horizon, config) if len(events) else {},
            "categorical_states": categorical_states(events, prices, horizon, config) if len(events) else {},
            "validated_patterns": discover_patterns(events, prices, horizon, coverage, config) if len(events) else {},
        }
    return result


def run(history_path: str | Path, price_path: str | Path, output_path: str | Path, config: Phase1BConfig = Phase1BConfig()) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(price_path, low_memory=False)
    result = analyze(history, prices, config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return result
