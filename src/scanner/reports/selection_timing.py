from __future__ import annotations

"""Phase 1A: separate cross-sectional quality selection from within-stock timing.

Research-only. This module never changes scanner scores, R-codes, watchlists or
production outputs.

Semantics:
- Score is a regime-dependent composite quality score, not a trade signal.
- R0-R5 is a downstream quality/regime class based on score percentile plus
  score status, TrendOK and LiquidityOK; it is not treated as a buy/sell signal.
- Phase 1A tests the score/rank backbone over history. Exact historical R-code
  testing is only possible where r_code was actually stored.
"""

from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

HORIZONS = (5, 20, 40, 60)


@dataclass(frozen=True)
class Phase1AConfig:
    stable_start: str = "2026-04-15"
    cooldown_sessions: int = 5
    min_cross_section: int = 20
    min_within_events: int = 8
    min_prior_scores: int = 20


def _score_percentile(series: pd.Series) -> pd.Series:
    """R-code-compatible score percentile: high score -> high percentile."""
    values = pd.to_numeric(series, errors="coerce")
    n = int(values.notna().sum())
    out = pd.Series(np.nan, index=values.index, dtype=float)
    if n == 0:
        return out
    if n == 1:
        out.loc[values.notna()] = 1.0
        return out
    ranks = values.rank(method="max", ascending=True)
    out.loc[values.notna()] = (ranks.loc[values.notna()] - 1.0) / (n - 1.0)
    return out


def _is_crypto(frame: pd.DataFrame) -> pd.Series:
    symbol = frame["symbol"].fillna("").astype(str).str.upper()
    name = frame.get("name", pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()
    sector = frame.get("sector", pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()
    pair = symbol.str.startswith("CRYPTO:") | symbol.str.endswith(("-USD", "-EUR"))
    named = name.str.contains(
        r"\b(?:bitcoin|ethereum|cardano|solana|dogecoin|ripple)\b",
        regex=True,
        na=False,
    )
    marked = sector.str.contains("krypto|cryptocurrency", regex=True, na=False) & pair
    return pair | named | marked


def _pearson(x: pd.Series, y: pd.Series) -> float | None:
    z = pd.concat([pd.to_numeric(x, errors="coerce"),
                   pd.to_numeric(y, errors="coerce")], axis=1).dropna()
    if len(z) < 3 or z.iloc[:, 0].nunique() < 2 or z.iloc[:, 1].nunique() < 2:
        return None
    value = z.iloc[:, 0].corr(z.iloc[:, 1])
    return None if pd.isna(value) else float(value)


def _spearman(x: pd.Series, y: pd.Series) -> float | None:
    xr = pd.to_numeric(x, errors="coerce").rank(method="average")
    yr = pd.to_numeric(y, errors="coerce").rank(method="average")
    return _pearson(xr, yr)


def _scanner_rows(history: pd.DataFrame) -> pd.DataFrame:
    if "observation_type" in history.columns:
        frame = history.loc[history["observation_type"].eq("observed_scanner")].copy()
    else:
        frame = history.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "score"]).sort_values("date")
    # Same-day reruns are not independent. Last published state wins.
    frame = frame.drop_duplicates(["date", "symbol"], keep="last")
    frame["score_pct_full"] = frame.groupby("date", group_keys=False)["score"].apply(_score_percentile)
    frame["is_crypto"] = _is_crypto(frame)
    return frame


def _price_rows(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["symbol"] = frame["symbol"].astype(str)
    return (frame.dropna(subset=["date", "symbol", "close"])
                 .sort_values(["symbol", "date"])
                 .drop_duplicates(["symbol", "date"], keep="last"))


def build_events(history: pd.DataFrame, prices: pd.DataFrame,
                 config: Phase1AConfig = Phase1AConfig()) -> pd.DataFrame:
    scanner = _scanner_rows(history)
    scanner = scanner.loc[~scanner["is_crypto"]].copy()
    stable_start = pd.Timestamp(config.stable_start)
    scanner = scanner.loc[scanner["date"] >= stable_start].copy()

    price = _price_rows(prices)
    available = set(price["symbol"].unique())
    scanner = scanner.loc[scanner["symbol"].astype(str).isin(available)].copy()
    scanner["price_symbol"] = scanner["symbol"].astype(str)

    price_groups = {
        symbol: group[["date", "close"]].sort_values("date").reset_index(drop=True)
        for symbol, group in price.groupby("symbol")
    }

    records: list[dict] = []
    for row in scanner.itertuples(index=False):
        series = price_groups.get(row.price_symbol)
        if series is None or series.empty:
            continue
        dates = series["date"].values.astype("datetime64[ns]")
        observation = np.datetime64(pd.Timestamp(row.date).to_datetime64())
        position = int(np.searchsorted(dates, observation, side="right") - 1)
        if position < 0:
            continue
        start_date = pd.Timestamp(dates[position])
        if (pd.Timestamp(row.date).normalize() - start_date.normalize()).days > 7:
            continue

        event = {
            "obs_date": pd.Timestamp(row.date).normalize(),
            "symbol": row.price_symbol,
            "score": float(row.score),
            "score_pct_full": float(row.score_pct_full),
            "currency": getattr(row, "currency", None),
            "r_code": getattr(row, "r_code", None),
            "start_market_date": start_date,
            "start_close": float(series.iloc[position]["close"]),
        }
        for horizon in HORIZONS:
            target = position + horizon
            if target < len(series):
                event[f"end_date_{horizon}t"] = series.iloc[target]["date"]
                event[f"return_{horizon}t"] = (
                    float(series.iloc[target]["close"]) / event["start_close"] - 1.0
                )
            else:
                event[f"end_date_{horizon}t"] = pd.NaT
                event[f"return_{horizon}t"] = np.nan
        records.append(event)

    events = pd.DataFrame(records)
    if events.empty:
        return events
    # Weekend/holiday scanner reruns can point to the same market session.
    events = (events.sort_values(["symbol", "start_market_date", "obs_date"])
                   .drop_duplicates(["symbol", "start_market_date"], keep="last"))
    return events.reset_index(drop=True)


def cooldown_events(events: pd.DataFrame, prices: pd.DataFrame,
                    sessions: int = 5) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    price = _price_rows(prices)
    index_maps = {
        symbol: {pd.Timestamp(day): i for i, day in enumerate(group["date"])}
        for symbol, group in price.groupby("symbol")
    }
    keep: list[int] = []
    for symbol, group in events.groupby("symbol", sort=False):
        mapping = index_maps.get(symbol, {})
        last: int | None = None
        for idx, row in group.sort_values("start_market_date").iterrows():
            pos = mapping.get(pd.Timestamp(row["start_market_date"]))
            if pos is None:
                continue
            if last is None or pos - last >= sessions:
                keep.append(idx)
                last = pos
    return events.loc[keep].sort_values(["obs_date", "symbol"]).reset_index(drop=True)


def cross_sectional_summary(events: pd.DataFrame, horizon: int,
                            min_n: int = 20) -> dict:
    ret = f"return_{horizon}t"
    daily = []
    for day, group in events.groupby("obs_date"):
        z = group[["score_pct_full", ret]].dropna()
        if len(z) < min_n:
            continue
        rho = _spearman(z["score_pct_full"], z[ret])
        if rho is None:
            continue
        future_rank = z[ret].rank(method="average", pct=True)
        score = z["score_pct_full"]
        top = future_rank.loc[score >= 0.80]
        low = future_rank.loc[score < 0.20]
        daily.append({
            "date": str(pd.Timestamp(day).date()),
            "N": int(len(z)),
            "spearman": rho,
            "top_quality_future_rank_mean": float(top.mean()) if len(top) else None,
            "low_quality_future_rank_mean": float(low.mean()) if len(low) else None,
        })
    d = pd.DataFrame(daily)
    if d.empty:
        return {"days": 0}
    return {
        "days": int(len(d)),
        "median_daily_N": float(d["N"].median()),
        "mean_daily_spearman": float(d["spearman"].mean()),
        "median_daily_spearman": float(d["spearman"].median()),
        "positive_daily_spearman_rate": float((d["spearman"] > 0).mean()),
        "top_quality_future_rank_mean": float(d["top_quality_future_rank_mean"].mean()),
        "low_quality_future_rank_mean": float(d["low_quality_future_rank_mean"].mean()),
    }


def future_rank_bands(events: pd.DataFrame, horizon: int,
                      min_n: int = 20) -> dict:
    ret = f"return_{horizon}t"
    z = events[["obs_date", "score_pct_full", ret]].dropna().copy()
    sizes = z.groupby("obs_date")[ret].transform("count")
    z = z.loc[sizes >= min_n].copy()
    if z.empty:
        return {}
    z["future_pct"] = z.groupby("obs_date")[ret].rank(method="average", pct=True)
    z["quality_band"] = pd.cut(
        z["score_pct_full"],
        [-0.001, 0.20, 0.40, 0.60, 0.80, 1.001],
        labels=["Q1_low", "Q2", "Q3", "Q4", "Q5_high"],
        include_lowest=True,
    )
    out = {}
    for band, group in z.groupby("quality_band", observed=False):
        out[str(band)] = {
            "N": int(len(group)),
            "mean_future_rank": float(group["future_pct"].mean()),
            "future_top20_rate": float((group["future_pct"] >= 0.80).mean()),
            "future_bottom20_rate": float((group["future_pct"] <= 0.20).mean()),
        }
    return out


def within_stock_summary(events: pd.DataFrame, prices: pd.DataFrame, horizon: int,
                         config: Phase1AConfig = Phase1AConfig()) -> dict:
    ret = f"return_{horizon}t"
    cd = cooldown_events(events, prices, config.cooldown_sessions)
    date_median = events.groupby("obs_date")[ret].median()
    work = cd[["obs_date", "symbol", "score", "score_pct_full", ret]].dropna().copy()
    work["cross_section_excess"] = work[ret] - work["obs_date"].map(date_median)

    correlations = []
    for symbol, group in work.groupby("symbol"):
        if len(group) < config.min_within_events:
            continue
        rho = _spearman(group["score"], group["cross_section_excess"])
        if rho is not None:
            correlations.append(rho)

    if not correlations:
        return {"symbols": 0}
    arr = np.asarray(correlations, dtype=float)
    return {
        "symbols": int(len(arr)),
        "events": int(len(work)),
        "median_symbol_spearman_score_vs_excess": float(np.median(arr)),
        "mean_symbol_spearman_score_vs_excess": float(np.mean(arr)),
        "positive_symbol_rate": float(np.mean(arr > 0)),
    }


def point_in_time_own_score(events: pd.DataFrame, prices: pd.DataFrame, horizon: int,
                            config: Phase1AConfig = Phase1AConfig()) -> dict:
    """Compare unusually high vs low score relative to the stock's own prior history."""
    ret = f"return_{horizon}t"
    daily = events.sort_values(["symbol", "obs_date"]).copy()
    own = {}
    for symbol, group in daily.groupby("symbol"):
        prior: list[float] = []
        for idx, row in group.iterrows():
            if len(prior) >= config.min_prior_scores:
                arr = np.asarray(prior, dtype=float)
                own[idx] = float(np.mean(arr <= float(row["score"])))
            else:
                own[idx] = np.nan
            prior.append(float(row["score"]))
    daily["own_score_pct_prior"] = pd.Series(own)

    cd = cooldown_events(daily, prices, config.cooldown_sessions)
    date_median = events.groupby("obs_date")[ret].median()
    work = cd[["obs_date", "own_score_pct_prior", ret]].dropna().copy()
    work["cross_section_excess"] = work[ret] - work["obs_date"].map(date_median)
    low = work.loc[work["own_score_pct_prior"] <= 0.20]
    high = work.loc[work["own_score_pct_prior"] >= 0.80]
    return {
        "N": int(len(work)),
        "own_low_N": int(len(low)),
        "own_high_N": int(len(high)),
        "own_low_mean_excess": float(low["cross_section_excess"].mean()) if len(low) else None,
        "own_high_mean_excess": float(high["cross_section_excess"].mean()) if len(high) else None,
        "high_minus_low_mean_excess": (
            float(high["cross_section_excess"].mean() - low["cross_section_excess"].mean())
            if len(low) and len(high) else None
        ),
        "own_low_positive_excess_rate": float((low["cross_section_excess"] > 0).mean()) if len(low) else None,
        "own_high_positive_excess_rate": float((high["cross_section_excess"] > 0).mean()) if len(high) else None,
    }


def currency_selection(events: pd.DataFrame, horizon: int,
                       min_n: int = 8) -> dict:
    out = {}
    for currency, group in events.dropna(subset=["currency"]).groupby("currency"):
        if len(group) < 300:
            continue
        summary = cross_sectional_summary(group, horizon, min_n=min_n)
        if summary.get("days", 0) >= 10:
            out[str(currency)] = summary
    return out


def analyze(history: pd.DataFrame, prices: pd.DataFrame,
            config: Phase1AConfig = Phase1AConfig()) -> dict:
    events = build_events(history, prices, config)
    scanner = _scanner_rows(history)
    r_rows = scanner.loc[scanner.get("r_code", pd.Series(index=scanner.index, dtype=object)).notna()]
    result = {
        "phase": "1A_selection_vs_timing",
        "semantics": {
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "r_code_role": "quality/regime input for daily analysis",
            "note": "Exact R0-R5 outcome validation requires stored historical r_code rows; Phase 1A tests the score-percentile backbone separately.",
        },
        "config": config.__dict__,
        "coverage": {
            "events": int(len(events)),
            "symbols": int(events["symbol"].nunique()) if not events.empty else 0,
            "event_date_min": str(events["obs_date"].min().date()) if not events.empty else None,
            "event_date_max": str(events["obs_date"].max().date()) if not events.empty else None,
            "stored_r_code_rows": int(len(r_rows)),
            "stored_r_code_date_min": str(r_rows["date"].min().date()) if len(r_rows) else None,
            "stored_r_code_date_max": str(r_rows["date"].max().date()) if len(r_rows) else None,
        },
        "horizons": {},
    }
    for horizon in HORIZONS:
        result["horizons"][str(horizon)] = {
            "cross_sectional_selection": cross_sectional_summary(
                events, horizon, config.min_cross_section
            ),
            "quality_bands": future_rank_bands(
                events, horizon, config.min_cross_section
            ),
            "within_stock_timing": within_stock_summary(
                events, prices, horizon, config
            ),
            "point_in_time_own_score": point_in_time_own_score(
                events, prices, horizon, config
            ),
            "currency_selection": currency_selection(events, horizon),
        }
    return result


def run(history_path: str | Path, prices_path: str | Path,
        output_path: str | Path | None = None,
        config: Phase1AConfig = Phase1AConfig()) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    result = analyze(history, prices, config)
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return result
