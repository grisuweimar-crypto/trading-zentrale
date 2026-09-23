from __future__ import annotations

"""Phase 3: empirical risk-factor audit for Scanner-vNext.

Research-only. This module does not alter production scores, risk weights,
R-codes, Confidence, watchlists or portfolio decisions.

The central distinction is deliberate:
- protection effect: does a higher current risk reading predict larger future
  adverse excursion / path drawdown?
- return effect: does a lower current risk reading predict better future
  peer-relative return / outperformance probability?

Those are different questions and are reported on independent available samples.
"""

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    HORIZONS,
    Phase1AConfig,
    _price_rows,
    _scanner_rows,
    _spearman,
    build_events,
    cooldown_events,
)
from scanner.reports.timing_patterns import Phase1BConfig, _strict_windows


RISK_FEATURE_ALIASES: dict[str, tuple[str, ...]] = {
    "aggregate_risk": ("risk", "risk_score"),
    "volatility": ("volatility", "Volatility"),
    "drawdown": ("drawdown", "max_drawdown", "MaxDrawdown"),
    "debt_ratio": ("debt_ratio", "debt_to_equity", "Debt/Equity"),
    "liquidity_risk": ("liquidity_risk",),
    "downside_dev": ("downside_dev", "DownsideDev"),
    "beta": ("beta",),
}


@dataclass(frozen=True)
class Phase3Config:
    stable_start: str = "2026-04-15"
    discovery_end: str = "2026-07-31"
    validation_start: str = "2026-08-01"
    cooldown_sessions: int = 5
    quantile: float = 0.20
    min_feature_n: int = 30
    cluster_bootstrap_reps: int = 1000
    random_seed: int = 20260923
    tail_drawdown_threshold: float = 0.10

    def __post_init__(self) -> None:
        if not 0 < self.quantile < 0.5:
            raise ValueError("quantile must be between 0 and 0.5")
        if self.cooldown_sessions < 1 or self.min_feature_n < 1:
            raise ValueError("cooldown_sessions and min_feature_n must be positive")
        if self.cluster_bootstrap_reps < 0:
            raise ValueError("cluster_bootstrap_reps must be >= 0")
        if not 0 < self.tail_drawdown_threshold < 1:
            raise ValueError("tail_drawdown_threshold must be between 0 and 1")


def _first_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    return next((name for name in aliases if name in frame.columns), None)


def _iso_min(series: pd.Series) -> str | None:
    values = pd.to_datetime(series, errors="coerce").dropna()
    return str(values.min().date()) if len(values) else None


def _iso_max(series: pd.Series) -> str | None:
    values = pd.to_datetime(series, errors="coerce").dropna()
    return str(values.max().date()) if len(values) else None


def risk_feature_rows(
    history: pd.DataFrame,
    config: Phase3Config = Phase3Config(),
) -> tuple[pd.DataFrame, dict]:
    """Return point-in-time scanner rows plus an explicit availability audit.

    Missing factors remain missing. No current fundamentals, liquidity values or
    synthetic factor reconstructions are backfilled into historical rows.
    """
    frame = _scanner_rows(history)
    frame = frame.loc[
        (~frame["is_crypto"]) & (frame["date"] >= pd.Timestamp(config.stable_start))
    ].copy()

    factor_coverage: dict[str, dict] = {}
    total = int(len(frame))
    for canonical, aliases in RISK_FEATURE_ALIASES.items():
        source = _first_column(frame, aliases)
        frame[canonical] = pd.to_numeric(frame[source], errors="coerce") if source else np.nan
        valid = frame[canonical].notna()
        rows = frame.loc[valid]
        factor_coverage[canonical] = {
            "source": source,
            "non_null": int(valid.sum()),
            "coverage_ratio": float(valid.mean()) if total else 0.0,
            "symbols": int(rows["symbol"].nunique()) if len(rows) else 0,
            "date_min": _iso_min(rows["date"]) if len(rows) else None,
            "date_max": _iso_max(rows["date"]) if len(rows) else None,
            "discovery_non_null": int(
                (valid & (frame["date"] <= pd.Timestamp(config.discovery_end))).sum()
            ),
            "validation_non_null": int(
                (valid & (frame["date"] >= pd.Timestamp(config.validation_start))).sum()
            ),
        }

    coverage = {
        "rows": total,
        "symbols": int(frame["symbol"].nunique()) if total else 0,
        "date_min": _iso_min(frame["date"]) if total else None,
        "date_max": _iso_max(frame["date"]) if total else None,
        "features": factor_coverage,
        "missing_values_are_not_backfilled": True,
    }
    return frame, coverage


def _attach_future_path_risk(events: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Add entry-relative adverse excursion and peak-to-trough drawdown.

    Session arithmetic is inherited from the validated adjusted-price pipeline.
    If any adjusted close inside a requested path is missing/non-positive, that
    path outcome is left missing rather than shortening the horizon.
    """
    if events.empty:
        return events.copy()
    price = _price_rows(prices)
    groups: dict[str, tuple[list[pd.Timestamp], np.ndarray]] = {}
    positions: dict[str, dict[pd.Timestamp, int]] = {}
    for symbol, group in price.groupby("symbol", sort=False):
        g = group.sort_values("date", kind="mergesort").reset_index(drop=True)
        days = [pd.Timestamp(x) for x in g["date"].tolist()]
        groups[str(symbol)] = (days, g["adj_close"].to_numpy(dtype=float))
        positions[str(symbol)] = {day: i for i, day in enumerate(days)}

    records: list[dict] = []
    for row in events.to_dict("records"):
        out = dict(row)
        symbol = str(row["symbol"])
        start = pd.Timestamp(row["start_market_date"])
        pos = positions.get(symbol, {}).get(start)
        group = groups.get(symbol)
        for horizon in HORIZONS:
            adverse_key = f"adverse_excursion_{horizon}t"
            drawdown_key = f"path_max_drawdown_{horizon}t"
            if group is None or pos is None or pos + horizon >= len(group[1]):
                out[adverse_key] = np.nan
                out[drawdown_key] = np.nan
                continue
            path = group[1][pos : pos + horizon + 1]
            if len(path) != horizon + 1 or not np.isfinite(path).all() or (path <= 0).any():
                out[adverse_key] = np.nan
                out[drawdown_key] = np.nan
                continue
            start_value = float(path[0])
            entry_returns = path / start_value - 1.0
            out[adverse_key] = max(0.0, -float(np.min(entry_returns)))
            peaks = np.maximum.accumulate(path)
            drawdowns = path / peaks - 1.0
            out[drawdown_key] = max(0.0, -float(np.min(drawdowns)))
        records.append(out)
    return pd.DataFrame(records)


def build_risk_events(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase3Config = Phase3Config(),
) -> tuple[pd.DataFrame, dict]:
    features, coverage = risk_feature_rows(history, config)
    events = build_events(
        history,
        prices,
        Phase1AConfig(stable_start=config.stable_start, cooldown_sessions=config.cooldown_sessions),
    )
    if events.empty:
        return events, coverage

    merge_cols = ["date", "symbol", *RISK_FEATURE_ALIASES.keys()]
    merged = events.merge(
        features[merge_cols],
        left_on=["obs_date", "symbol"],
        right_on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    ).drop(columns=["date"], errors="ignore")
    return _attach_future_path_risk(merged, prices), coverage


def _phase1b_config(config: Phase3Config) -> Phase1BConfig:
    return Phase1BConfig(
        stable_start=config.stable_start,
        discovery_end=config.discovery_end,
        validation_start=config.validation_start,
        cooldown_sessions=config.cooldown_sessions,
        min_single_n=config.min_feature_n,
    )


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def _effective_block_length(horizon: int) -> int:
    return max(1, 2 * int(horizon))


def _circular_moving_time_blocks(
    frame: pd.DataFrame,
    horizon: int,
) -> tuple[pd.DataFrame, list[pd.DataFrame], int]:
    """Return circular moving observation-date blocks of length 2 x horizon.

    All rows sharing an observation date remain together. Every eligible
    date can be a block start, so trailing dates are retained rather than
    truncated. Circular wrapping preserves dependence across former fixed
    block boundaries.
    """
    block_length = _effective_block_length(horizon)
    if horizon < 1 or frame.empty or "obs_date" not in frame.columns:
        return frame.iloc[0:0].copy(), [], block_length
    work = frame.copy()
    work["obs_date"] = pd.to_datetime(work["obs_date"], errors="coerce")
    work = work.dropna(subset=["obs_date"]).copy()
    dates = sorted(pd.Timestamp(day) for day in work["obs_date"].unique())
    if not dates:
        return work, [], block_length
    span = min(block_length, len(dates))
    by_day = {
        day: work.loc[work["obs_date"].eq(day)].copy()
        for day in dates
    }
    blocks: list[pd.DataFrame] = []
    for start in range(len(dates)):
        chosen = [dates[(start + offset) % len(dates)] for offset in range(span)]
        blocks.append(pd.concat([by_day[day] for day in chosen], ignore_index=True))
    return work, blocks, block_length


def _temporal_support_region_count(
    work: pd.DataFrame,
    group_column: str,
    group_name: str,
    block_length: int,
) -> int:
    """Count time-separated support regions for one fixed risk group."""
    dates = sorted(pd.Timestamp(day) for day in work["obs_date"].unique())
    if not dates:
        return 0
    positions = {day: i for i, day in enumerate(dates)}
    group_dates = sorted(
        positions[pd.Timestamp(day)]
        for day in work.loc[work[group_column].eq(group_name), "obs_date"].unique()
    )
    count = 0
    last_start: int | None = None
    for pos in group_dates:
        if last_start is None or pos - last_start >= block_length:
  count += 1
  last_start = pos
    return count


def _cluster_bootstrap_group_difference(
    frame: pd.DataFrame,
    target: str,
    group_column: str,
    positive_group: str,
    negative_group: str,
    reps: int,
    seed: int,
    horizon: int = 1,
) -> list[float] | None:
    """Circular moving-block bootstrap of a fixed-group mean difference.

    Returns mean(positive_group) - mean(negative_group). Quantile
    membership is fixed before resampling. The effective block length is
    twice the evaluated forward horizon. Every eligible observation date
    remains in the bootstrap sampling frame, and uncertainty fails closed
    unless both groups have support in at least two time-separated regions.
    """
    work = frame[["obs_date", target, group_column]].dropna().copy()
    work = work.loc[work[group_column].isin([positive_group, negative_group])]
    if work.empty or reps <= 0:
        return None
    work, blocks, block_length = _circular_moving_time_blocks(work, horizon)
    if len(blocks) < 2:
        return None
    if _temporal_support_region_count(work, group_column, positive_group, block_length) < 2:
        return None
    if _temporal_support_region_count(work, group_column, negative_group, block_length) < 2:
        return None

    def difference(sample: pd.DataFrame) -> float | None:
        pos = sample.loc[sample[group_column].eq(positive_group), target]
        neg = sample.loc[sample[group_column].eq(negative_group), target]
        if pos.empty or neg.empty:
  return None
        return float(pos.mean() - neg.mean())

    if difference(work) is None:
        return None

    dates = sorted(pd.Timestamp(day) for day in work["obs_date"].unique())
    day_groups = {
        day: work.loc[work["obs_date"].eq(day)].copy()
        for day in dates
    }
    block_dates = [
        list(dict.fromkeys(pd.Timestamp(day) for day in block["obs_date"].tolist()))
        for block in blocks
    ]
    draws_per_rep = int(np.ceil(len(dates) / block_length))
    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(block_dates), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in block_dates[idx]][: len(dates)]
        sample = pd.concat([day_groups[day] for day in sampled_dates], ignore_index=True)
        value = difference(sample)
        if value is not None:
  estimates.append(value)
    if not estimates:
        return None
    low, high = np.quantile(np.asarray(estimates, dtype=float), [0.025, 0.975])
    return [float(low), float(high)]

def _validation_maturity(frame: pd.DataFrame, horizon: int) -> dict:
    ret = f"return_{horizon}t"
    adverse = f"adverse_excursion_{horizon}t"
    path_dd = f"path_max_drawdown_{horizon}t"
    return_mature = int(frame[ret].notna().sum()) if ret in frame else 0
    protection_mature = (
        int(frame[[adverse, path_dd]].notna().all(axis=1).sum())
        if adverse in frame and path_dd in frame
        else 0
    )
    return {
        "rows": int(len(frame)),
        "mature_target_events": return_mature,
        "return_mature_events": return_mature,
        "protection_mature_events": protection_mature,
        "status": "available" if (return_mature or protection_mature) else "not_yet_mature",
    }


def _quantile_groups(
    sample: pd.DataFrame,
    feature: str,
    config: Phase3Config,
) -> tuple[str, pd.DataFrame, float | None, float | None]:
    if len(sample) < config.min_feature_n or sample[feature].nunique() < 3:
        return "insufficient", sample, None, None
    low_cut = float(sample[feature].quantile(config.quantile))
    high_cut = float(sample[feature].quantile(1.0 - config.quantile))
    if not np.isfinite(low_cut) or not np.isfinite(high_cut) or low_cut >= high_cut:
        return "insufficient_variation", sample, None, None
    work = sample.copy()
    work["risk_group"] = "middle"
    work.loc[work[feature] <= low_cut, "risk_group"] = "low_risk"
    work.loc[work[feature] >= high_cut, "risk_group"] = "high_risk"
    if not work["risk_group"].eq("low_risk").any() or not work["risk_group"].eq("high_risk").any():
        return "insufficient_groups", work, low_cut, high_cut
    return "available", work, low_cut, high_cut


def _feature_stats(
    cohort: pd.DataFrame,
    feature: str,
    horizon: int,
    config: Phase3Config,
    seed_key: tuple[object, ...],
) -> dict | None:
    """Evaluate return and protection effects on independent available samples."""
    peer = f"peer_excess_{horizon}t"
    ret = f"return_{horizon}t"
    adverse = f"adverse_excursion_{horizon}t"
    path_dd = f"path_max_drawdown_{horizon}t"
    if feature not in cohort.columns:
        return None

    feature_rows = cohort.dropna(subset=[feature]).copy()
    if feature_rows.empty:
        return None

    return_sample = (
        feature_rows.dropna(subset=[peer, ret]).copy()
        if peer in cohort and ret in cohort
        else feature_rows.iloc[0:0].copy()
    )
    protection_sample = (
        feature_rows.dropna(subset=[adverse, path_dd]).copy()
        if adverse in cohort and path_dd in cohort
        else feature_rows.iloc[0:0].copy()
    )

    result: dict = {
        "N": int(len(feature_rows)),
        "symbols": int(feature_rows["symbol"].nunique()),
        "days": int(feature_rows["obs_date"].nunique()),
        "return_N": int(len(return_sample)),
        "protection_N": int(len(protection_sample)),
        "return_symbols": int(return_sample["symbol"].nunique()) if len(return_sample) else 0,
        "protection_symbols": int(protection_sample["symbol"].nunique()) if len(protection_sample) else 0,
        "return_days": int(return_sample["obs_date"].nunique()) if len(return_sample) else 0,
        "protection_days": int(protection_sample["obs_date"].nunique()) if len(protection_sample) else 0,
        "return_coverage_of_feature_rows": float(len(return_sample) / len(feature_rows)),
        "protection_coverage_of_feature_rows": float(len(protection_sample) / len(feature_rows)),
        "spearman_risk_vs_peer_excess": _spearman(return_sample[feature], return_sample[peer]) if len(return_sample) else None,
        "spearman_risk_vs_forward_return": _spearman(return_sample[feature], return_sample[ret]) if len(return_sample) else None,
        "spearman_risk_vs_adverse_excursion": _spearman(protection_sample[feature], protection_sample[adverse]) if len(protection_sample) else None,
        "spearman_risk_vs_path_max_drawdown": _spearman(protection_sample[feature], protection_sample[path_dd]) if len(protection_sample) else None,
    }

    return_status, return_groups, return_low_cut, return_high_cut = _quantile_groups(return_sample, feature, config)
    protection_status, protection_groups, protection_low_cut, protection_high_cut = _quantile_groups(protection_sample, feature, config)
    result["return_quantile_status"] = return_status
    result["protection_quantile_status"] = protection_status
    result["quantile_status"] = (
        "available"
        if return_status == protection_status == "available"
        else "partial"
        if "available" in {return_status, protection_status}
        else "insufficient"
    )

    if return_status == "available":
        low = return_groups.loc[return_groups["risk_group"].eq("low_risk")]
        high = return_groups.loc[return_groups["risk_group"].eq("high_risk")]
        alpha_advantage = float(low[peer].mean() - high[peer].mean())
        result.update(
            {
                "return_low_risk_cutoff": return_low_cut,
                "return_high_risk_cutoff": return_high_cut,
                "return_low_risk_N": int(len(low)),
                "return_high_risk_N": int(len(high)),
                "low_risk_mean_peer_excess": float(low[peer].mean()),
                "high_risk_mean_peer_excess": float(high[peer].mean()),
                "low_risk_median_peer_excess": float(low[peer].median()),
                "high_risk_median_peer_excess": float(high[peer].median()),
                "low_risk_outperformance_rate": float((low[peer] > 0).mean()),
                "high_risk_outperformance_rate": float((high[peer] > 0).mean()),
                "low_risk_alpha_advantage": alpha_advantage,
                "low_risk_alpha_advantage_bootstrap_95": _cluster_bootstrap_group_difference(
                    return_groups,
                    peer,
                    "risk_group",
                    "low_risk",
                    "high_risk",
                    config.cluster_bootstrap_reps,
                    _stable_seed(config.random_seed, *seed_key, "peer"),
                    horizon,
                ),
            }
        )

    if protection_status == "available":
        low = protection_groups.loc[protection_groups["risk_group"].eq("low_risk")]
        high = protection_groups.loc[protection_groups["risk_group"].eq("high_risk")]
        threshold = config.tail_drawdown_threshold
        protection_gap = float(high[adverse].mean() - low[adverse].mean())
        path_gap = float(high[path_dd].mean() - low[path_dd].mean())
        result.update(
            {
                "protection_low_risk_cutoff": protection_low_cut,
                "protection_high_risk_cutoff": protection_high_cut,
                "protection_low_risk_N": int(len(low)),
                "protection_high_risk_N": int(len(high)),
                "low_risk_mean_adverse_excursion": float(low[adverse].mean()),
                "high_risk_mean_adverse_excursion": float(high[adverse].mean()),
                "low_risk_mean_path_max_drawdown": float(low[path_dd].mean()),
                "high_risk_mean_path_max_drawdown": float(high[path_dd].mean()),
                "low_risk_tail_drawdown_rate": float((low[path_dd] >= threshold).mean()),
                "high_risk_tail_drawdown_rate": float((high[path_dd] >= threshold).mean()),
                "protection_gap_high_minus_low_adverse_excursion": protection_gap,
                "path_drawdown_gap_high_minus_low": path_gap,
                "protection_gap_bootstrap_95": _cluster_bootstrap_group_difference(
                    protection_groups,
                    adverse,
                    "risk_group",
                    "high_risk",
                    "low_risk",
                    config.cluster_bootstrap_reps,
                    _stable_seed(config.random_seed, *seed_key, "adverse"),
                    horizon,
                ),
                "path_drawdown_gap_bootstrap_95": _cluster_bootstrap_group_difference(
                    protection_groups,
                    path_dd,
                    "risk_group",
                    "high_risk",
                    "low_risk",
                    config.cluster_bootstrap_reps,
                    _stable_seed(config.random_seed, *seed_key, "path_dd"),
                    horizon,
                ),
            }
        )
    return result


def _cohort_feature_report(
    cohort: pd.DataFrame,
    horizon: int,
    config: Phase3Config,
    label: str,
) -> dict:
    return {
        feature: _feature_stats(cohort, feature, horizon, config, (label, horizon, feature))
        for feature in RISK_FEATURE_ALIASES
    }


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase3Config = Phase3Config(),
) -> dict:
    events, coverage = build_risk_events(history, prices, config)
    result = {
        "phase": "3_risk_vnext_audit",
        "semantics": {
            "research_only": True,
            "production_risk_weights_changed": False,
            "production_score_changed": False,
            "confidence_changed": False,
            "missing_historical_factors_backfilled": False,
            "higher_feature_value_interpreted_as_riskier": True,
            "protection_and_return_effects_kept_separate": True,
            "return_and_protection_samples_decoupled": True,
            "uncertainty_method": "circular moving observation-date block bootstrap",
            "uncertainty_block_length_rule": "effective block length equals 2 x evaluated forward horizon in sessions",
            "bootstrap_uses_all_eligible_dates": True,
            "bootstrap_min_group_temporal_support_regions": 2,
            "bootstrap_quantile_membership_fixed": True,
            "danelfin_low_risk_role": "external research reference only; no Danelfin score is imported",
        },
        "config": config.__dict__,
        "coverage": coverage,
        "events": int(len(events)),
        "horizons": {},
    }

    for horizon in HORIZONS:
        discovery, validation = _strict_windows(events, horizon, _phase1b_config(config))
        discovery = cooldown_events(discovery, prices, config.cooldown_sessions)
        validation = cooldown_events(validation, prices, config.cooldown_sessions)
        result["horizons"][str(horizon)] = {
            "bootstrap": {
                "method": "circular_moving_observation_date_blocks",
                "base_horizon_sessions": int(horizon),
                "block_length_sessions": int(_effective_block_length(horizon)),
                "uses_all_eligible_dates": True,
                "minimum_group_temporal_support_regions": 2,
            },
            "discovery_maturity": _validation_maturity(discovery, horizon),
            "validation_maturity": _validation_maturity(validation, horizon),
            "discovery": _cohort_feature_report(discovery, horizon, config, "discovery"),
            "validation": _cohort_feature_report(validation, horizon, config, "validation"),
        }
    return result


def _source_snapshot(metadata_path: str | Path | None) -> dict:
    if metadata_path is None:
        return {}
    path = Path(metadata_path)
    if not path.exists():
        return {}
    meta = json.loads(path.read_text(encoding="utf-8"))
    return {
        "snapshot_id": meta.get("snapshot_id"),
        "as_of": meta.get("as_of"),
        "generated_at": meta.get("generated_at"),
        "latest_run_complete": meta.get("latest_run_complete"),
    }


def run(
    history_path: str | Path,
    price_path: str | Path,
    output_path: str | Path,
    config: Phase3Config = Phase3Config(),
    metadata_path: str | Path | None = "artifacts/research/history_metadata.json",
) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(price_path, low_memory=False)
    result = analyze(history, prices, config)
    result["source"] = _source_snapshot(metadata_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return result
