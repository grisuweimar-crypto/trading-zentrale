from __future__ import annotations

"""Phase 2: calibrated probability and uncertainty research.

Research-only. This module does not modify scanner scoring, R0-R5, watchlists,
or the daily watch. It preserves the Phase 1 split between cross-sectional
selection quality and within-stock timing patterns.
"""

from dataclasses import dataclass
from hashlib import sha256
from math import erfc, sqrt
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    HORIZONS,
    Phase1AConfig,
    _r_score_backbone,
    build_events,
)
from scanner.reports.timing_patterns import (
    Phase1BConfig,
    _add_peer_excess,
    _cooldown_matching,
    _session_position_maps,
    _strict_windows,
    build_timing_events,
    discover_patterns,
)


@dataclass(frozen=True)
class Phase2Config:
    stable_start: str = "2026-04-15"
    discovery_end: str = "2026-07-31"
    validation_start: str = "2026-08-01"
    cooldown_sessions: int = 5
    prior_strength: float = 20.0
    min_pattern_discovery_n: int = 30
    min_pattern_validation_n: int = 20
    cluster_bootstrap_reps: int = 1000
    random_seed: int = 20260923


def _clip01(value: float) -> float:
    return float(min(1.0, max(0.0, value)))


def _wilson_interval(successes: int, n: int, z: float = 1.959963984540054) -> list[float] | None:
    if n <= 0:
        return None
    p = successes / n
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denom
    half = z * sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n)) / denom
    return [_clip01(centre - half), _clip01(centre + half)]


def _beta_shrinkage(successes: int, n: int, baseline_rate: float, prior_strength: float) -> dict:
    baseline = _clip01(float(baseline_rate))
    strength = max(float(prior_strength), 0.0)
    alpha = successes + baseline * strength
    beta = (n - successes) + (1.0 - baseline) * strength
    total = alpha + beta
    if total <= 0:
        return {
            "posterior_mean": baseline,
            "posterior_interval_95": [baseline, baseline],
            "prior_strength": strength,
        }
    mean = alpha / total
    variance = (alpha * beta) / (total * total * (total + 1.0)) if total > 1 else 0.0
    half = 1.959963984540054 * sqrt(max(variance, 0.0))
    return {
        "posterior_mean": float(mean),
        "posterior_interval_95": [_clip01(mean - half), _clip01(mean + half)],
        "prior_strength": strength,
    }


def _normal_binomial_p(successes: int, n: int, baseline_rate: float) -> float | None:
    if n <= 0:
        return None
    p0 = _clip01(float(baseline_rate))
    variance = p0 * (1.0 - p0) / n
    if variance <= 0:
        return 1.0 if successes / n == p0 else 0.0
    z = abs((successes / n - p0) / sqrt(variance))
    return float(erfc(z / sqrt(2.0)))


def _cluster_bootstrap_mean(values: pd.DataFrame, target: str, reps: int, seed: int) -> list[float] | None:
    work = values[["obs_date", target]].dropna().copy()
    if work.empty:
        return None
    groups = [g[target].to_numpy(dtype=float) for _, g in work.groupby("obs_date", sort=False)]
    if len(groups) < 2 or reps <= 0:
        mean = float(work[target].mean())
        return [mean, mean]
    rng = np.random.default_rng(seed)
    means = np.empty(reps, dtype=float)
    count = len(groups)
    for i in range(reps):
        chosen = rng.integers(0, count, size=count)
        sample = np.concatenate([groups[j] for j in chosen])
        means[i] = float(np.mean(sample))
    low, high = np.quantile(means, [0.025, 0.975])
    return [float(low), float(high)]


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    offset = int.from_bytes(digest[:4], "big")
    return int((base + offset) % (2**32 - 1))


def _probability_stats(
    occurrences: pd.DataFrame,
    baseline: pd.DataFrame,
    target: str,
    config: Phase2Config,
    seed_key: tuple[object, ...],
    comparisons: int = 1,
) -> dict | None:
    values = occurrences.dropna(subset=[target]).copy()
    base = baseline.dropna(subset=[target]).copy()
    if values.empty or base.empty:
        return None

    n = int(len(values))
    successes = int((values[target] > 0).sum())
    raw_rate = successes / n
    baseline_rate = float((base[target] > 0).mean())
    shrink = _beta_shrinkage(successes, n, baseline_rate, config.prior_strength)
    p_value = _normal_binomial_p(successes, n, baseline_rate)
    bonferroni = None if p_value is None else min(1.0, p_value * max(int(comparisons), 1))
    symbol_counts = values["symbol"].astype(str).value_counts() if "symbol" in values else pd.Series(dtype=int)

    return {
        "N": n,
        "symbols": int(values["symbol"].nunique()) if "symbol" in values else None,
        "days": int(values["obs_date"].nunique()) if "obs_date" in values else None,
        "top_symbol_share": float(symbol_counts.iloc[0] / n) if len(symbol_counts) else None,
        "mean_peer_excess": float(values[target].mean()),
        "median_peer_excess": float(values[target].median()),
        "cluster_bootstrap_mean_peer_excess_95": _cluster_bootstrap_mean(
            values,
            target,
            config.cluster_bootstrap_reps,
            _stable_seed(config.random_seed, *seed_key),
        ),
        "raw_positive_peer_excess_rate": float(raw_rate),
        "raw_rate_wilson_95": _wilson_interval(successes, n),
        "baseline_positive_peer_excess_rate": baseline_rate,
        "shrunk_positive_peer_excess_probability": shrink["posterior_mean"],
        "shrunk_probability_interval_95": shrink["posterior_interval_95"],
        "probability_advantage_vs_baseline": float(shrink["posterior_mean"] - baseline_rate),
        "prior_strength": shrink["prior_strength"],
        "approx_binomial_p": p_value,
        "bonferroni_adjusted_p": bonferroni,
        "multiple_testing_note": "normal approximation; diagnostic only, not an independence-proof significance test",
    }


def _windowed_peer_events(events: pd.DataFrame, horizon: int, config: Phase2Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    phase1b = Phase1BConfig(
        stable_start=config.stable_start,
        discovery_end=config.discovery_end,
        validation_start=config.validation_start,
        cooldown_sessions=config.cooldown_sessions,
        min_pattern_discovery_n=config.min_pattern_discovery_n,
        min_pattern_validation_n=config.min_pattern_validation_n,
    )
    full = _add_peer_excess(events, horizon)
    return _strict_windows(full, horizon, phase1b)


def _selection_band_rows(frame: pd.DataFrame) -> pd.Series:
    return pd.Series(
        [_r_score_backbone(float(score), float(pct)) for score, pct in zip(frame["score"], frame["score_pct_full"])],
        index=frame.index,
        dtype=object,
    )


def selection_calibration(
    events: pd.DataFrame,
    horizon: int,
    config: Phase2Config,
) -> dict:
    discovery, validation = _windowed_peer_events(events, horizon, config)
    target = f"peer_excess_{horizon}t"
    out: dict[str, dict] = {"discovery": {}, "validation": {}}
    for label, work in (("discovery", discovery), ("validation", validation)):
        if work.empty:
            continue
        work = work.copy()
        work["selection_band"] = _selection_band_rows(work)
        baseline = work
        bands = [b for b in ("B0_score0", "B1", "B2", "B3", "B4", "B5") if (work["selection_band"] == b).any()]
        comparisons = max(len(bands), 1)
        for band in bands:
            group = work.loc[work["selection_band"].eq(band)]
            stats = _probability_stats(
                group,
                baseline,
                target,
                config,
                ("selection", horizon, label, band),
                comparisons,
            )
            if stats:
                out[label][band] = stats
    return out


def _pattern_occurrences(
    work: pd.DataFrame,
    prices: pd.DataFrame,
    conditions: list[str],
    config: Phase2Config,
    position_maps: dict[str, dict[pd.Timestamp, int]],
) -> pd.DataFrame:
    if not conditions or any(c not in work.columns for c in conditions):
        return work.iloc[0:0].copy()
    mask = work[conditions].all(axis=1)
    return _cooldown_matching(
        work,
        prices,
        mask,
        config.cooldown_sessions,
        position_maps,
    )


def timing_pattern_calibration(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    coverage: dict,
    horizon: int,
    config: Phase2Config,
) -> dict:
    phase1b = Phase1BConfig(
        stable_start=config.stable_start,
        discovery_end=config.discovery_end,
        validation_start=config.validation_start,
        cooldown_sessions=config.cooldown_sessions,
        min_pattern_discovery_n=config.min_pattern_discovery_n,
        min_pattern_validation_n=config.min_pattern_validation_n,
    )
    frozen_report = discover_patterns(events, prices, horizon, coverage, phase1b)
    discovery, validation = _windowed_peer_events(events, horizon, config)
    target = f"peer_excess_{horizon}t"
    position_maps = _session_position_maps(prices)
    frozen = frozen_report.get("frozen_positive", []) + frozen_report.get("frozen_negative", [])
    comparisons = max(int(frozen_report.get("discovery_candidate_count", 0)), 1)

    calibrated = []
    for row in frozen:
        conditions = list(row.get("conditions", []))
        d_occ = _pattern_occurrences(discovery, prices, conditions, config, position_maps)
        v_occ = _pattern_occurrences(validation, prices, conditions, config, position_maps)
        d_stats = _probability_stats(
            d_occ,
            discovery,
            target,
            config,
            ("timing", horizon, "discovery", row.get("pattern")),
            comparisons,
        )
        v_stats = _probability_stats(
            v_occ,
            validation,
            target,
            config,
            ("timing", horizon, "validation", row.get("pattern")),
            1,
        )
        discovery_direction = None
        if d_stats and d_stats["mean_peer_excess"] != 0:
            discovery_direction = "positive" if d_stats["mean_peer_excess"] > 0 else "negative"
        validation_direction = None
        if v_stats and v_stats["mean_peer_excess"] != 0:
            validation_direction = "positive" if v_stats["mean_peer_excess"] > 0 else "negative"
        calibrated.append(
            {
                "pattern": row.get("pattern"),
                "conditions": conditions,
                "discovery_direction": discovery_direction,
                "discovery": d_stats,
                "validation": v_stats,
                "validation_sufficient": bool(v_stats and v_stats["N"] >= config.min_pattern_validation_n),
                "direction_confirmed": bool(discovery_direction and validation_direction == discovery_direction),
                "selection_was_discovery_only": True,
            }
        )

    return {
        "discovery_window": [config.stable_start, config.discovery_end],
        "validation_window": [config.validation_start, str(events["obs_date"].max().date()) if len(events) else None],
        "candidate_selection_uses_validation": False,
        "discovery_candidate_count": int(frozen_report.get("discovery_candidate_count", 0)),
        "frozen_pattern_count": len(calibrated),
        "patterns": calibrated,
        "validation_supported": [
            row for row in calibrated if row["validation_sufficient"] and row["direction_confirmed"]
        ],
    }


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase2Config = Phase2Config(),
) -> dict:
    selection_events = build_events(
        history,
        prices,
        Phase1AConfig(stable_start=config.stable_start, cooldown_sessions=config.cooldown_sessions),
    )
    timing_events, coverage = build_timing_events(
        history,
        prices,
        Phase1BConfig(
            stable_start=config.stable_start,
            discovery_end=config.discovery_end,
            validation_start=config.validation_start,
            cooldown_sessions=config.cooldown_sessions,
            min_pattern_discovery_n=config.min_pattern_discovery_n,
            min_pattern_validation_n=config.min_pattern_validation_n,
        ),
    )

    result = {
        "phase": "2_probability_calibration",
        "semantics": {
            "research_only": True,
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "selection_and_timing_kept_separate": True,
            "probability_target": "future peer_excess > 0 versus validated leave-one-symbol-out peer benchmark",
            "note": "These are calibrated research probabilities, not deterministic buy/sell instructions.",
        },
        "config": config.__dict__,
        "coverage": {
            "selection_events": int(len(selection_events)),
            "timing_events": int(len(timing_events)),
            "timing_feature_coverage": coverage,
        },
        "horizons": {},
    }
    for horizon in HORIZONS:
        result["horizons"][str(horizon)] = {
            "selection": selection_calibration(selection_events, horizon, config) if len(selection_events) else {},
            "timing_patterns": (
                timing_pattern_calibration(timing_events, prices, coverage, horizon, config)
                if len(timing_events)
                else {}
            ),
        }
    return result


def run(
    history_path: str | Path,
    price_path: str | Path,
    output_path: str | Path,
    config: Phase2Config = Phase2Config(),
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
