from __future__ import annotations

"""Phase 2: calibrated probability and uncertainty research.

Research-only. Phase 2 consumes the candidates frozen by completed Phase 1B;
it never re-discovers timing patterns from holdout data. Scanner scoring, R0-R5,
watchlists and the daily watch remain unchanged.
"""

from dataclasses import dataclass
from hashlib import sha256
from math import erfc, isfinite, sqrt
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS, Phase1AConfig, _r_score_backbone, build_events
from scanner.reports.timing_patterns import (
    Phase1BConfig,
    _add_peer_excess,
    _cooldown_matching,
    _session_position_maps,
    _strict_windows,
    build_timing_events,
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


def _direction(value: float | None) -> str | None:
    if value is None or value == 0:
        return None
    return "positive" if value > 0 else "negative"


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
    """Shrink a binomial rate toward the baseline using a proper Beta prior."""
    strength = float(prior_strength)
    if not isfinite(strength) or strength < 1.0:
        raise ValueError("prior_strength must be finite and >= 1.0 for a proper Beta prior")
    baseline = _clip01(float(baseline_rate))
    prior_alpha = max(0.5, baseline * strength)
    prior_beta = max(0.5, (1.0 - baseline) * strength)
    alpha = float(successes) + prior_alpha
    beta = float(n - successes) + prior_beta
    total = alpha + beta
    mean = alpha / total
    variance = (alpha * beta) / (total * total * (total + 1.0))
    half = 1.959963984540054 * sqrt(max(variance, 0.0))
    return {
        "posterior_mean": float(mean),
        "posterior_interval_95": [_clip01(mean - half), _clip01(mean + half)],
        "configured_prior_strength": strength,
        "effective_prior_strength": float(prior_alpha + prior_beta),
        "prior_alpha": float(prior_alpha),
        "prior_beta": float(prior_beta),
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


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def _horizon_date_blocks(frame: pd.DataFrame, horizon: int) -> list[list[pd.Timestamp]]:
    if horizon < 1 or frame.empty or "obs_date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["obs_date"], errors="coerce").dropna().drop_duplicates().sort_values().tolist()
    complete = (len(dates) // horizon) * horizon
    if complete == 0:
        return []
    dates = [pd.Timestamp(day) for day in dates[:complete]]
    return [dates[start : start + horizon] for start in range(0, complete, horizon)]


def _block_bootstrap_uncertainty(
    values: pd.DataFrame,
    baseline: pd.DataFrame,
    target: str,
    horizon: int,
    config: Phase2Config,
    seed: int,
) -> dict:
    """Resample complete horizon-length time blocks in sync.

    Occurrence and baseline rows use the same resampled date blocks, preserving
    dependence created by overlapping forward windows. Robust intervals are
    unavailable unless at least two complete baseline blocks and at least two
    occurrence-bearing blocks exist.
    """
    v = values[["obs_date", target]].dropna().copy()
    b = baseline[["obs_date", target]].dropna().copy()
    blocks = _horizon_date_blocks(b, horizon)
    unavailable = {
        "mean_peer_excess_95": None,
        "probability_advantage_95": None,
        "block_count": len(blocks),
        "occurrence_block_count": 0,
    }
    if config.cluster_bootstrap_reps <= 0 or len(blocks) < 2 or v.empty or b.empty:
        return unavailable

    v["obs_date"] = pd.to_datetime(v["obs_date"])
    b["obs_date"] = pd.to_datetime(b["obs_date"])
    v_by_block = [v.loc[v["obs_date"].isin(days), target].to_numpy(dtype=float) for days in blocks]
    b_by_block = [b.loc[b["obs_date"].isin(days), target].to_numpy(dtype=float) for days in blocks]
    occurrence_block_count = sum(1 for part in v_by_block if len(part))
    if occurrence_block_count < 2:
        return {
            **unavailable,
            "occurrence_block_count": occurrence_block_count,
        }

    rng = np.random.default_rng(seed)
    mean_estimates: list[float] = []
    advantage_estimates: list[float] = []
    count = len(blocks)
    for _ in range(config.cluster_bootstrap_reps):
        chosen = rng.integers(0, count, size=count)
        v_parts = [v_by_block[i] for i in chosen if len(v_by_block[i])]
        b_parts = [b_by_block[i] for i in chosen if len(b_by_block[i])]
        if not v_parts or not b_parts:
            continue
        v_sample = np.concatenate(v_parts)
        b_sample = np.concatenate(b_parts)
        if len(v_sample) == 0 or len(b_sample) == 0:
            continue
        mean_estimates.append(float(v_sample.mean()))
        baseline_rate = float((b_sample > 0).mean())
        successes = int((v_sample > 0).sum())
        shrink = _beta_shrinkage(successes, len(v_sample), baseline_rate, config.prior_strength)
        advantage_estimates.append(float(shrink["posterior_mean"] - baseline_rate))

    def interval(items: list[float]) -> list[float] | None:
        if not items:
            return None
        low, high = np.quantile(np.asarray(items, dtype=float), [0.025, 0.975])
        return [float(low), float(high)]

    return {
        "mean_peer_excess_95": interval(mean_estimates),
        "probability_advantage_95": interval(advantage_estimates),
        "block_count": len(blocks),
        "occurrence_block_count": occurrence_block_count,
    }


def _probability_stats(
    occurrences: pd.DataFrame,
    baseline: pd.DataFrame,
    target: str,
    config: Phase2Config,
    seed_key: tuple[object, ...],
    comparisons: int = 1,
    horizon: int = 1,
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
    robust = _block_bootstrap_uncertainty(
        values,
        base,
        target,
        horizon,
        config,
        _stable_seed(config.random_seed, *seed_key),
    )

    return {
        "N": n,
        "symbols": int(values["symbol"].nunique()) if "symbol" in values else None,
        "days": int(values["obs_date"].nunique()) if "obs_date" in values else None,
        "top_symbol_share": float(symbol_counts.iloc[0] / n) if len(symbol_counts) else None,
        "mean_peer_excess": float(values[target].mean()),
        "median_peer_excess": float(values[target].median()),
        "block_bootstrap_mean_peer_excess_95": robust["mean_peer_excess_95"],
        "block_bootstrap_probability_advantage_95": robust["probability_advantage_95"],
        "bootstrap_block_count": int(robust["block_count"]),
        "bootstrap_occurrence_block_count": int(robust["occurrence_block_count"]),
        "bootstrap_block_length_sessions": int(horizon),
        "raw_positive_peer_excess_rate": float(raw_rate),
        "raw_rate_wilson_95_iid_diagnostic": _wilson_interval(successes, n),
        "baseline_positive_peer_excess_rate": baseline_rate,
        "shrunk_positive_peer_excess_probability": shrink["posterior_mean"],
        "shrunk_probability_interval_95_iid_diagnostic": shrink["posterior_interval_95"],
        "probability_advantage_vs_baseline": float(shrink["posterior_mean"] - baseline_rate),
        "configured_prior_strength": shrink["configured_prior_strength"],
        "effective_prior_strength": shrink["effective_prior_strength"],
        "prior_alpha": shrink["prior_alpha"],
        "prior_beta": shrink["prior_beta"],
        "approx_binomial_p_iid_diagnostic": p_value,
        "bonferroni_adjusted_p_iid_diagnostic": bonferroni,
        "uncertainty_note": "strong-validation decisions use horizon-aware block bootstrap with at least two occurrence-bearing blocks; Wilson/Beta intervals and binomial p-values are iid diagnostics only",
    }


def _phase1b_config(config: Phase2Config) -> Phase1BConfig:
    return Phase1BConfig(
        stable_start=config.stable_start,
        discovery_end=config.discovery_end,
        validation_start=config.validation_start,
        cooldown_sessions=config.cooldown_sessions,
        min_pattern_discovery_n=config.min_pattern_discovery_n,
        min_pattern_validation_n=config.min_pattern_validation_n,
    )


def _windowed_peer_events(events: pd.DataFrame, horizon: int, config: Phase2Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    full = _add_peer_excess(events, horizon)
    return _strict_windows(full, horizon, _phase1b_config(config))


def _selection_band_rows(frame: pd.DataFrame) -> pd.Series:
    return pd.Series(
        [_r_score_backbone(float(score), float(pct)) for score, pct in zip(frame["score"], frame["score_pct_full"])],
        index=frame.index,
        dtype=object,
    )


def selection_calibration(
    discovery: pd.DataFrame,
    validation: pd.DataFrame,
    horizon: int,
    config: Phase2Config,
) -> dict:
    target = f"peer_excess_{horizon}t"
    out: dict[str, dict] = {"discovery": {}, "validation": {}}
    for label, source in (("discovery", discovery), ("validation", validation)):
        work = source.dropna(subset=[target]).copy()
        if work.empty:
            continue
        work["selection_band"] = _selection_band_rows(work)
        bands = [b for b in ("B0_score0", "B1", "B2", "B3", "B4", "B5") if work["selection_band"].eq(b).any()]
        for band in bands:
            stats = _probability_stats(
                work.loc[work["selection_band"].eq(band)],
                work,
                target,
                config,
                ("selection", horizon, label, band),
                max(len(bands), 1),
                horizon,
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
    return _cooldown_matching(
        work,
        prices,
        work[conditions].all(axis=1),
        config.cooldown_sessions,
        position_maps,
    )


def _interval_supports_direction(interval: list[float] | None, direction: str | None, reference: float = 0.0) -> bool:
    if not interval or direction not in {"positive", "negative"}:
        return False
    return bool(interval[0] > reference) if direction == "positive" else bool(interval[1] < reference)


def _validation_flags(
    discovery_direction: str | None,
    validation: dict | None,
    min_n: int,
) -> dict:
    sufficient = bool(validation and validation["N"] >= min_n)
    if not validation or discovery_direction not in {"positive", "negative"}:
        return {
            "validation_sufficient": sufficient,
            "alpha_direction_confirmed": False,
            "probability_direction_confirmed": False,
            "joint_direction_confirmed": False,
            "alpha_interval_confirmed": False,
            "probability_interval_confirmed": False,
            "strong_validation": False,
        }
    alpha_direction = _direction(validation["mean_peer_excess"])
    probability_direction = _direction(validation["probability_advantage_vs_baseline"])
    alpha_confirmed = alpha_direction == discovery_direction
    probability_confirmed = probability_direction == discovery_direction
    alpha_interval = validation.get("block_bootstrap_mean_peer_excess_95")
    probability_interval = validation.get("block_bootstrap_probability_advantage_95")
    alpha_interval_confirmed = _interval_supports_direction(alpha_interval, discovery_direction, 0.0)
    probability_interval_confirmed = _interval_supports_direction(probability_interval, discovery_direction, 0.0)
    return {
        "validation_sufficient": sufficient,
        "alpha_direction_confirmed": alpha_confirmed,
        "probability_direction_confirmed": probability_confirmed,
        "joint_direction_confirmed": bool(sufficient and alpha_confirmed and probability_confirmed),
        "alpha_interval_confirmed": alpha_interval_confirmed,
        "probability_interval_confirmed": probability_interval_confirmed,
        "strong_validation": bool(
            sufficient
            and alpha_confirmed
            and probability_confirmed
            and alpha_interval_confirmed
            and probability_interval_confirmed
        ),
    }


def timing_pattern_calibration(
    discovery: pd.DataFrame,
    validation: pd.DataFrame,
    prices: pd.DataFrame,
    frozen_horizon: dict,
    horizon: int,
    config: Phase2Config,
    position_maps: dict[str, dict[pd.Timestamp, int]],
) -> dict:
    target = f"peer_excess_{horizon}t"
    frozen = list(frozen_horizon.get("frozen_patterns", []))
    comparisons = max(int(frozen_horizon.get("discovery_candidate_count", 0)), 1)
    validation_comparisons = max(len(frozen), 1)
    calibrated: list[dict] = []

    for row in frozen:
        conditions = list(row.get("conditions", []))
        discovery_direction = row.get("discovery_direction")
        d_stats = _probability_stats(
            _pattern_occurrences(discovery, prices, conditions, config, position_maps),
            discovery,
            target,
            config,
            ("timing", horizon, "discovery", row.get("pattern")),
            comparisons,
            horizon,
        )
        v_stats = _probability_stats(
            _pattern_occurrences(validation, prices, conditions, config, position_maps),
            validation,
            target,
            config,
            ("timing", horizon, "validation", row.get("pattern")),
            validation_comparisons,
            horizon,
        )
        calibrated.append(
            {
                "pattern": row.get("pattern"),
                "conditions": conditions,
                "discovery_direction": discovery_direction,
                "discovery": d_stats,
                "validation": v_stats,
                "selection_was_discovery_only": True,
                **_validation_flags(discovery_direction, v_stats, config.min_pattern_validation_n),
            }
        )

    return {
        "candidate_source": "frozen_phase1b",
        "candidate_selection_uses_validation": False,
        "discovery_candidate_count": int(frozen_horizon.get("discovery_candidate_count", 0)),
        "frozen_pattern_count": len(calibrated),
        "patterns": calibrated,
        "alpha_supported": [row for row in calibrated if row["validation_sufficient"] and row["alpha_direction_confirmed"]],
        "joint_supported": [row for row in calibrated if row["joint_direction_confirmed"]],
        "strong_supported": [row for row in calibrated if row["strong_validation"]],
    }


def _validation_maturity(validation: pd.DataFrame, horizon: int) -> dict:
    target = f"peer_excess_{horizon}t"
    mature = int(validation[target].notna().sum()) if target in validation else 0
    return {
        "validation_rows": int(len(validation)),
        "mature_target_events": mature,
        "status": "available" if mature > 0 else "not_yet_mature",
    }


def _validate_frozen_catalog(catalog: dict, config: Phase2Config) -> None:
    if catalog.get("schema_version") != "phase1b_frozen_patterns_v1":
        raise ValueError("unsupported frozen Phase 1B pattern schema")
    horizons = catalog.get("horizons", {})
    missing = {str(h) for h in HORIZONS} - set(horizons)
    if missing:
        raise ValueError(f"frozen Phase 1B catalog missing horizons: {sorted(missing)}")
    for horizon in HORIZONS:
        window = horizons[str(horizon)].get("discovery_window")
        if window != [config.stable_start, config.discovery_end]:
            raise ValueError(f"frozen Phase 1B discovery window mismatch for {horizon}T")


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    frozen_patterns: dict,
    config: Phase2Config = Phase2Config(),
) -> dict:
    _validate_frozen_catalog(frozen_patterns, config)
    prior_strength = float(config.prior_strength)
    if not isfinite(prior_strength) or prior_strength < 1.0:
        raise ValueError("prior_strength must be finite and >= 1.0")

    selection_events = build_events(
        history,
        prices,
        Phase1AConfig(stable_start=config.stable_start, cooldown_sessions=config.cooldown_sessions),
    )
    timing_events, coverage = build_timing_events(history, prices, _phase1b_config(config))
    position_maps = _session_position_maps(prices)

    result = {
        "phase": "2_probability_calibration",
        "semantics": {
            "research_only": True,
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "selection_and_timing_kept_separate": True,
            "timing_candidates_are_frozen_from_phase1b": True,
            "probability_target": "future peer_excess > 0 versus validated leave-one-symbol-out peer benchmark",
            "strong_validation_uncertainty": "horizon-aware non-overlapping time-block bootstrap",
            "iid_intervals_and_pvalues_are_diagnostics_only": True,
            "note": "Calibrated research probabilities; not deterministic buy/sell instructions.",
        },
        "config": config.__dict__,
        "frozen_pattern_source": {
            "schema_version": frozen_patterns.get("schema_version"),
            "source_phase": frozen_patterns.get("source_phase"),
            "source_events": frozen_patterns.get("source_events"),
        },
        "coverage": {
            "selection_events": int(len(selection_events)),
            "timing_events": int(len(timing_events)),
            "timing_feature_coverage": coverage,
        },
        "horizons": {},
    }

    for horizon in HORIZONS:
        s_discovery, s_validation = _windowed_peer_events(selection_events, horizon, config)
        t_discovery, t_validation = _windowed_peer_events(timing_events, horizon, config)
        result["horizons"][str(horizon)] = {
            "bootstrap": {"method": "non_overlapping_horizon_time_blocks", "block_length_sessions": int(horizon)},
            "validation_maturity": _validation_maturity(t_validation, horizon),
            "selection": selection_calibration(s_discovery, s_validation, horizon, config),
            "timing_patterns": timing_pattern_calibration(
                t_discovery,
                t_validation,
                prices,
                frozen_patterns["horizons"][str(horizon)],
                horizon,
                config,
                position_maps,
            ),
        }
    return result


def _source_snapshot(metadata_path: str | Path | None) -> dict:
    if not metadata_path:
        return {}
    path = Path(metadata_path)
    if not path.exists():
        return {}
    meta = json.loads(path.read_text(encoding="utf-8"))
    daily = meta.get("daily_run", {})
    return {
        "snapshot_id": meta.get("snapshot_id"),
        "as_of": meta.get("as_of"),
        "generated_at": meta.get("generated_at"),
        "scanner_run_id": daily.get("run_id"),
        "scanner_status": daily.get("scanner_status"),
    }


def run(
    history_path: str | Path,
    price_path: str | Path,
    output_path: str | Path,
    config: Phase2Config = Phase2Config(),
    frozen_patterns_path: str | Path = "artifacts/research/timing_patterns_1b_frozen.json",
    metadata_path: str | Path | None = "artifacts/research/history_metadata.json",
) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(price_path, low_memory=False)
    frozen = json.loads(Path(frozen_patterns_path).read_text(encoding="utf-8"))
    result = analyze(history, prices, frozen, config)
    result["source"] = _source_snapshot(metadata_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result