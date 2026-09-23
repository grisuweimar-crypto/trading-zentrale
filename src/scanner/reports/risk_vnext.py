from __future__ import annotations

"""Phase 3: empirical risk-factor audit for Scanner-vNext.

Research-only. This module does not alter production scores, risk weights,
R-codes, Confidence, watchlists or portfolio decisions.

The central distinction is deliberate:
- protection effect: does a higher current risk reading predict larger future
  adverse excursion / path drawdown?
- return effect: does a lower current risk reading predict better future
  peer-relative return / outperformance probability?

Those are different questions and are reported separately.
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

    sources: dict[str, str | None] = {}
    factor_coverage: dict[str, dict] = {}
    total = int(len(frame))

    for canonical, aliases in RISK_FEATURE_ALIASES.items():
        source = _first_column(frame, aliases)
        sources[canonical] = source
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
            adverse = max(0.0, -float(np.min(entry_returns)))
            peaks = np.maximum.accumulate(path)
            drawdowns = path / peaks - 1.0
            max_drawdown = max(0.0, -float(np.min(drawdowns)))
            out[adverse_key] = adverse
            out[drawdown_key] = max_drawdown
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
        Phase1AConfig(
            stable_start=config.stable_start,
            cooldown_sessions=config.cooldown_sessions,
        ),
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


def _cluster_bootstrap_group_difference(
    frame: pd.DataFrame,
    target: str,
    group_column: str,
    positive_group: str,
    negative_group: str,
    reps: int,
    seed: int,
) -> list[float] | None:
    """Bootstrap a group mean difference by observation day.

    Returns mean(positive_group) - mean(negative_group). Quantile membership is
    fixed before resampling so the bootstrap measures sampling uncertainty rather
    than repeatedly re-selecting thresholds.
    """
    work = frame[["obs_date", target, group_column]].dropna().copy()
    work = work.loc[work[group_column].isin([positive_group, negative_group])]
    if work.empty:
        return None
    groups = [g for _, g in work.groupby("obs_date", sort=False)]
    if not groups:
        return None

    def difference(sample: pd.DataFrame) -> float | None:
        pos = sample.loc[sample[group_column].eq(positive_group), target]
        neg = sample.loc[sample[group_column].eq(negative_group), target]
        if pos.empty or neg.empty:
            return None
        return float(pos.mean() - neg.mean())

    observed = difference(work)
    if observed is None:
        return None
    if len(groups) < 2 or reps <= 0:
        return [observed, observed]

    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    count = len(groups)
    for _ in range(reps):
        chosen = rng.integers(0, count, size=count)
        sample = pd.concat([groups[i] for i in chosen], ignore_index=True)
        value = difference(sample)
        if value is not None:
            estimates.append(value)
    if not estimates:
        return [observed, observed]
    low, high = np.quantile(np.asarray(estimates, dtype=float), [0.025, 0.975])
    return [float(low), float(high)]


def _validation_maturity(frame: pd.DataFrame, horizon: int) -> dict:
    target = f"return_{horizon}t"
    mature = int(frame[target].notna().sum()) if target in frame else 0
    return {
        "rows": int(len(frame)),
        "mature_target_events": mature,
        "status": "available" if mature > 0 else "not_yet_mature",
    }


def _feature_stats(
    cohort: pd.DataFrame,
    feature: str,
    horizon: int,
    config: Phase3Config,
    seed_key: tuple[object, ...],
) -> dict | None:
    peer = f"peer_excess_{horizon}t"
    ret = f"return_{horizon}t"
    adverse = f"adverse_excursion_{horizon}t"
    path_dd = f"path_max_drawdown_{horizon}t"
    required = [feature, peer, ret, adverse, path_dd]
    if any(column not in cohort.columns for column in required):
        return None

    mature = cohort.dropna(subset=[peer, ret, adverse, path_dd]).copy()
    values = mature.dropna(subset=[feature]).copy()
    if values.empty:
        return None

    result: dict = {
        "N": int(len(values)),
        "symbols": int(values["symbol"].nunique()),
        "days": int(values["obs_date"].nunique()),
        "coverage_of_mature_cohort": float(len(values) / len(mature)) if len(mature) else 0.0,
        "spearman_risk_vs_peer_excess": _spearman(values[feature], values[peer]),
        "spearman_risk_vs_forward_return": _spearman(values[feature], values[ret]),
        "spearman_risk_vs_adverse_excursion": _spearman(values[feature], values[adverse]),
        "spearman_risk_vs_path_max_drawdown": _spearman(values[feature], values[path_dd]),
    }
    if len(values) < config.min_feature_n or values[feature].nunique() < 3:
        result["quantile_status"] = "insufficient"
        return result

    low_cut = float(values[feature].quantile(config.quantile))
    high_cut = float(values[feature].quantile(1.0 - config.quantile))
    if not np.isfinite(low_cut) or not np.isfinite(high_cut) or low_cut >= high_cut:
        result["quantile_status"] = "insufficient_variation"
        return result

    values["risk_group"] = "middle"
    values.loc[values[feature] <= low_cut, "risk_group"] = "low_risk"
    values.loc[values[feature] >= high_cut, "risk_group"] = "high_risk"
    low = values.loc[values["risk_group"].eq("low_risk")]
    high = values.loc[values["risk_group"].eq("high_risk")]
    if low.empty or high.empty:
        result["quantile_status"] = "insufficient_groups"
        return result

    threshold = config.tail_drawdown_threshold
    protection_gap = float(high[adverse].mean() - low[adverse].mean())
    path_gap = float(high[path_dd].mean() - low[path_dd].mean())
    alpha_advantage = float(low[peer].mean() - high[peer].mean())

    result.update(
        {
            "quantile_status": "available",
            "low_risk_cutoff": low_cut,
            "high_risk_cutoff": high_cut,
            "low_risk_N": int(len(low)),
            "high_risk_N": int(len(high)),
            "low_risk_mean_peer_excess": float(low[peer].mean()),
            "high_risk_mean_peer_excess": float(high[peer].mean()),
            "low_risk_median_peer_excess": float(low[peer].median()),
            "high_risk_median_peer_excess": float(high[peer].median()),
            "low_risk_outperformance_rate": float((low[peer] > 0).mean()),
            "high_risk_outperformance_rate": float((high[peer] > 0).mean()),
            "low_risk_mean_adverse_excursion": float(low[adverse].mean()),
            "high_risk_mean_adverse_excursion": float(high[adverse].mean()),
            "low_risk_mean_path_max_drawdown": float(low[path_dd].mean()),
            "high_risk_mean_path_max_drawdown": float(high[path_dd].mean()),
            "low_risk_tail_drawdown_rate": float((low[path_dd] >= threshold).mean()),
            "high_risk_tail_drawdown_rate": float((high[path_dd] >= threshold).mean()),
            "protection_gap_high_minus_low_adverse_excursion": protection_gap,
            "path_drawdown_gap_high_minus_low": path_gap,
            "low_risk_alpha_advantage": alpha_advantage,
            "protection_gap_bootstrap_95": _cluster_bootstrap_group_difference(
                values,
                adverse,
                "risk_group",
                "high_risk",
                "low_risk",
                config.cluster_bootstrap_reps,
                _stable_seed(config.random_seed, *seed_key, "adverse"),
            ),
            "path_drawdown_gap_bootstrap_95": _cluster_bootstrap_group_difference(
                values,
                path_dd,
                "risk_group",
                "high_risk",
                "low_risk",
                config.cluster_bootstrap_reps,
                _stable_seed(config.random_seed, *seed_key, "path_dd"),
            ),
            "low_risk_alpha_advantage_bootstrap_95": _cluster_bootstrap_group_difference(
                values,
                peer,
                "risk_group",
                "low_risk",
                "high_risk",
                config.cluster_bootstrap_reps,
                _stable_seed(config.random_seed, *seed_key, "peer"),
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
    out: dict[str, dict | None] = {}
    for feature in RISK_FEATURE_ALIASES:
        out[feature] = _feature_stats(cohort, feature, horizon, config, (label, horizon, feature))
    return out


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
