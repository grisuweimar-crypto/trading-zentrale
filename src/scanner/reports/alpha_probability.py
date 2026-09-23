from __future__ import annotations

"""Phase 2: calibrated outperformance probability and alpha research.

Research-only. This module does not change scanner scoring, R0-R5, portfolio
logic, the daily watch, or production recommendations. It calibrates the fixed
selection bands from Phase 1A and the discovery-frozen timing patterns from
Phase 1B against later leave-one-symbol-out peer-relative returns.
"""

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS, cooldown_events
from scanner.reports.timing_patterns import (
    Phase1BConfig,
    _add_peer_excess,
    _cooldown_matching,
    _strict_windows,
    _session_position_maps,
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
    bootstrap_rounds: int = 250
    random_seed: int = 20260923
    min_selection_n: int = 20
    min_pattern_discovery_n: int = 30
    min_pattern_validation_n: int = 20
    max_combo_size: int = 3
    max_atoms_for_combos: int = 18
    top_patterns: int = 12

    def phase1b(self) -> Phase1BConfig:
        return Phase1BConfig(
            stable_start=self.stable_start,
            discovery_end=self.discovery_end,
            validation_start=self.validation_start,
            cooldown_sessions=self.cooldown_sessions,
            min_pattern_discovery_n=self.min_pattern_discovery_n,
            min_pattern_validation_n=self.min_pattern_validation_n,
            max_combo_size=self.max_combo_size,
            max_atoms_for_combos=self.max_atoms_for_combos,
            top_patterns=self.top_patterns,
        )


def _stable_seed(base: int, label: str) -> int:
    digest = sha256(label.encode("utf-8")).digest()
    return (int(base) + int.from_bytes(digest[:8], "big")) % (2**32 - 1)


def _selection_band(score: float, percentile: float) -> str:
    if float(score) == 0.0:
        return "B0_score0"
    if percentile < 0.20:
        return "B1"
    if percentile < 0.45:
        return "B2"
    if percentile < 0.75:
        return "B3"
    if percentile < 0.90:
        return "B4"
    return "B5"


def _baseline(frame: pd.DataFrame, target: str) -> dict:
    z = frame.dropna(subset=[target]).copy()
    if z.empty:
        return {
            "N": 0,
            "symbols": 0,
            "dates": 0,
            "positive_rate": None,
            "mean_alpha": None,
            "median_alpha": None,
        }
    values = pd.to_numeric(z[target], errors="coerce").dropna()
    z = z.loc[values.index]
    return {
        "N": int(len(values)),
        "symbols": int(z["symbol"].nunique()) if "symbol" in z.columns else 0,
        "dates": int(z["obs_date"].nunique()) if "obs_date" in z.columns else 0,
        "positive_rate": float((values > 0).mean()),
        "mean_alpha": float(values.mean()),
        "median_alpha": float(values.median()),
    }


def _cluster_draws(
    frame: pd.DataFrame,
    target: str,
    cluster: str,
    baseline_rate: float,
    prior_strength: float,
    rounds: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    z = frame.dropna(subset=[target, cluster]).copy()
    if z.empty or rounds <= 0:
        return np.asarray([], dtype=float), np.asarray([], dtype=float)
    z[target] = pd.to_numeric(z[target], errors="coerce")
    z = z.dropna(subset=[target])
    grouped = [g[target].to_numpy(dtype=float) for _, g in z.groupby(cluster, sort=False)]
    if not grouped:
        return np.asarray([], dtype=float), np.asarray([], dtype=float)

    rng = np.random.default_rng(seed)
    probs = np.empty(rounds, dtype=float)
    medians = np.empty(rounds, dtype=float)
    count = len(grouped)
    for i in range(rounds):
        chosen = rng.integers(0, count, size=count)
        values = np.concatenate([grouped[j] for j in chosen])
        n = len(values)
        raw = float(np.mean(values > 0))
        probs[i] = (
            raw * n + baseline_rate * prior_strength
        ) / (n + prior_strength)
        medians[i] = float(np.median(values))
    return probs, medians


def _interval(values: np.ndarray) -> list[float] | None:
    if len(values) < 10:
        return None
    lo, hi = np.quantile(values, [0.025, 0.975])
    return [float(lo), float(hi)]


def _conservative_interval(*intervals: list[float] | None) -> list[float] | None:
    valid = [x for x in intervals if x is not None]
    if not valid:
        return None
    return [float(min(x[0] for x in valid)), float(max(x[1] for x in valid))]


def _segment_stats(
    frame: pd.DataFrame,
    target: str,
    baseline_rate: float,
    config: Phase2Config,
    label: str,
) -> dict | None:
    z = frame.dropna(subset=[target]).copy()
    if z.empty:
        return None
    z[target] = pd.to_numeric(z[target], errors="coerce")
    z = z.dropna(subset=[target])
    if z.empty:
        return None

    values = z[target].to_numpy(dtype=float)
    n = len(values)
    successes = int(np.sum(values > 0))
    raw_probability = successes / n
    shrunk_probability = (
        successes + config.prior_strength * baseline_rate
    ) / (n + config.prior_strength)
    advantage = shrunk_probability - baseline_rate

    symbol_prob, symbol_median = _cluster_draws(
        z,
        target,
        "symbol",
        baseline_rate,
        config.prior_strength,
        config.bootstrap_rounds,
        _stable_seed(config.random_seed, f"{label}:symbol"),
    )
    date_prob, date_median = _cluster_draws(
        z,
        target,
        "obs_date",
        baseline_rate,
        config.prior_strength,
        config.bootstrap_rounds,
        _stable_seed(config.random_seed, f"{label}:date"),
    )
    symbol_prob_ci = _interval(symbol_prob)
    date_prob_ci = _interval(date_prob)
    probability_ci = _conservative_interval(symbol_prob_ci, date_prob_ci)
    median_ci = _conservative_interval(_interval(symbol_median), _interval(date_median))

    advantage_draws = []
    if len(symbol_prob):
        advantage_draws.append(symbol_prob - baseline_rate)
    if len(date_prob):
        advantage_draws.append(date_prob - baseline_rate)
    combined_advantage = np.concatenate(advantage_draws) if advantage_draws else np.asarray([])
    sign_p = None
    if len(combined_advantage):
        non_positive = float((np.sum(combined_advantage <= 0) + 1) / (len(combined_advantage) + 1))
        non_negative = float((np.sum(combined_advantage >= 0) + 1) / (len(combined_advantage) + 1))
        # Percentile-bootstrap sign diagnostic; deliberately not presented as an
        # iid p-value because scanner observations are clustered by stock/date.
        sign_p = float(min(1.0, 2.0 * min(non_positive, non_negative)))

    return {
        "N": int(n),
        "symbols": int(z["symbol"].nunique()) if "symbol" in z.columns else 0,
        "dates": int(z["obs_date"].nunique()) if "obs_date" in z.columns else 0,
        "successes": successes,
        "raw_outperformance_probability": float(raw_probability),
        "outperformance_probability": float(shrunk_probability),
        "probability_advantage": float(advantage),
        "mean_alpha": float(np.mean(values)),
        "median_alpha": float(np.median(values)),
        "uncertainty": {
            "probability_ci95": probability_ci,
            "median_alpha_ci95": median_ci,
            "symbol_cluster_probability_ci95": symbol_prob_ci,
            "date_cluster_probability_ci95": date_prob_ci,
            "interval_method": "conservative envelope of symbol/date cluster percentile bootstrap",
            "baseline_rate_treated_as_fixed": True,
            "bootstrap_rounds": int(config.bootstrap_rounds),
        },
        "bootstrap_sign_p_approx": sign_p,
        "shrinkage": {
            "method": "beta-binomial posterior mean with empirical horizon/window baseline prior",
            "prior_strength": float(config.prior_strength),
            "prior_probability": float(baseline_rate),
        },
    }


def _bh_adjust(rows: list[tuple[int, float]]) -> dict[int, float]:
    """Benjamini-Hochberg adjustment for finite validation diagnostics."""
    if not rows:
        return {}
    ordered = sorted(rows, key=lambda item: item[1])
    m = len(ordered)
    adjusted = [0.0] * m
    running = 1.0
    for i in range(m - 1, -1, -1):
        rank = i + 1
        value = min(1.0, ordered[i][1] * m / rank)
        running = min(running, value)
        adjusted[i] = running
    return {ordered[i][0]: float(adjusted[i]) for i in range(m)}


def _selection_window(
    window: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    config: Phase2Config,
    window_name: str,
) -> dict:
    target = f"peer_excess_{horizon}t"
    work = cooldown_events(window, prices, config.cooldown_sessions)
    work = work.dropna(subset=[target, "score", "score_pct_full"]).copy()
    baseline = _baseline(work, target)
    if not baseline["N"] or baseline["positive_rate"] is None:
        return {"baseline": baseline, "bands": {}}

    work["selection_band"] = [
        _selection_band(score, pct)
        for score, pct in zip(work["score"], work["score_pct_full"])
    ]
    bands: dict[str, dict] = {}
    order = ("B0_score0", "B1", "B2", "B3", "B4", "B5")
    for band in order:
        group = work.loc[work["selection_band"].eq(band)]
        if len(group) < config.min_selection_n:
            continue
        stats = _segment_stats(
            group,
            target,
            float(baseline["positive_rate"]),
            config,
            f"selection:{horizon}:{window_name}:{band}",
        )
        if stats:
            bands[band] = stats
    return {"baseline": baseline, "bands": bands}


def _pattern_window_stats(
    window: pd.DataFrame,
    prices: pd.DataFrame,
    conditions: list[str],
    horizon: int,
    config: Phase2Config,
    baseline_rate: float,
    label: str,
    position_maps: dict | None = None,
) -> dict | None:
    if not conditions or any(c not in window.columns for c in conditions):
        return None
    mask = window[conditions].all(axis=1)
    matches = _cooldown_matching(
        window,
        prices,
        mask,
        config.cooldown_sessions,
        position_maps,
    )
    return _segment_stats(
        matches,
        f"peer_excess_{horizon}t",
        baseline_rate,
        config,
        label,
    )


def _timing_calibration(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    coverage: dict,
    config: Phase2Config,
) -> dict:
    p1b = config.phase1b()
    frozen_report = discover_patterns(events, prices, horizon, coverage, p1b)
    full = _add_peer_excess(events, horizon)
    discovery, validation = _strict_windows(full, horizon, p1b)
    discovery_baseline_frame = cooldown_events(discovery, prices, config.cooldown_sessions)
    validation_baseline_frame = cooldown_events(validation, prices, config.cooldown_sessions)
    target = f"peer_excess_{horizon}t"
    discovery_baseline = _baseline(discovery_baseline_frame, target)
    validation_baseline = _baseline(validation_baseline_frame, target)

    position_maps = _session_position_maps(prices)

    frozen: list[dict] = []
    for expected_direction, rows in (
        ("positive", frozen_report.get("frozen_positive", [])),
        ("negative", frozen_report.get("frozen_negative", [])),
    ):
        for source in rows:
            conditions = list(source.get("conditions", []))
            d_stats = None
            v_stats = None
            if discovery_baseline["positive_rate"] is not None:
                d_stats = _pattern_window_stats(
                    discovery,
                    prices,
                    conditions,
                    horizon,
                    config,
                    float(discovery_baseline["positive_rate"]),
                    f"timing:{horizon}:discovery:{source['pattern']}",
                    position_maps,
                )
            if validation_baseline["positive_rate"] is not None:
                v_stats = _pattern_window_stats(
                    validation,
                    prices,
                    conditions,
                    horizon,
                    config,
                    float(validation_baseline["positive_rate"]),
                    f"timing:{horizon}:validation:{source['pattern']}",
                    position_maps,
                )
            frozen.append(
                {
                    "pattern": source["pattern"],
                    "conditions": conditions,
                    "expected_direction_from_discovery_mean_alpha": expected_direction,
                    "phase1b_validation_sufficient": bool(source.get("validation_sufficient", False)),
                    "phase1b_direction_confirmed": bool(source.get("direction_confirmed", False)),
                    "discovery": d_stats,
                    "validation": v_stats,
                }
            )

    q_inputs: list[tuple[int, float]] = []
    for i, row in enumerate(frozen):
        val = row.get("validation")
        if val and val.get("bootstrap_sign_p_approx") is not None:
            q_inputs.append((i, float(val["bootstrap_sign_p_approx"])))
    q_values = _bh_adjust(q_inputs)
    for i, row in enumerate(frozen):
        val = row.get("validation")
        if not val:
            row["validation_evidence"] = "no_completed_validation_outcomes"
            continue
        val["bh_q_approx"] = q_values.get(i)
        expected = 1 if row["expected_direction_from_discovery_mean_alpha"] == "positive" else -1
        advantage = float(val["probability_advantage"])
        consistent = (advantage > 0 and expected > 0) or (advantage < 0 and expected < 0)
        row["validation_probability_direction_consistent"] = bool(consistent)
        if val["N"] < config.min_pattern_validation_n:
            row["validation_evidence"] = "insufficient_N"
        elif not consistent:
            row["validation_evidence"] = "probability_direction_conflict"
        elif val.get("bh_q_approx") is not None and val["bh_q_approx"] <= 0.10:
            row["validation_evidence"] = "direction_consistent_q_le_0.10"
        else:
            row["validation_evidence"] = "direction_consistent_descriptive"

    return {
        "candidate_source": "Phase 1B discovery-frozen patterns only; validation never selects candidates",
        "baseline": {
            "discovery": discovery_baseline,
            "validation": validation_baseline,
        },
        "frozen_patterns": frozen,
        "multiple_testing": {
            "scope": "validation bootstrap sign diagnostics across frozen patterns within this horizon",
            "method": "Benjamini-Hochberg",
            "q_values_are_approximate": True,
            "note": "Cluster bootstrap diagnostics are not iid hypothesis-test p-values and are not trade gates.",
        },
    }


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase2Config = Phase2Config(),
) -> dict:
    events, coverage = build_timing_events(history, prices, config.phase1b())
    result = {
        "phase": "2_alpha_probability",
        "semantics": {
            "research_only": True,
            "changes_production_score": False,
            "outperformance_definition": (
                "peer_excess > 0 versus leave-one-symbol-out same-currency peer median; "
                "global leave-one-out peers are fallback for singleton currencies"
            ),
            "alpha_definition": "peer_excess return, not SPY alpha",
            "selection_and_timing_remain_separate": True,
            "probability_is_calibrated_estimate_not_trade_signal": True,
        },
        "config": config.__dict__,
        "coverage": coverage,
        "events": int(len(events)),
        "horizons": {},
    }
    for horizon in HORIZONS:
        full = _add_peer_excess(events, horizon)
        discovery, validation = _strict_windows(full, horizon, config.phase1b())
        result["horizons"][str(horizon)] = {
            "selection_probability": {
                "band_definition": {
                    "B0_score0": "score == 0",
                    "B1": "score percentile < 20%",
                    "B2": "20% <= percentile < 45%",
                    "B3": "45% <= percentile < 75%",
                    "B4": "75% <= percentile < 90%",
                    "B5": "percentile >= 90%",
                },
                "discovery": _selection_window(discovery, prices, horizon, config, "discovery"),
                "validation": _selection_window(validation, prices, horizon, config, "validation"),
            },
            "timing_probability": _timing_calibration(
                events, prices, horizon, coverage, config
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
