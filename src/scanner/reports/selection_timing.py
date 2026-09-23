from __future__ import annotations

"""Phase 1A: separate cross-sectional quality from within-stock timing.

Research-only. This module never changes scanner scores, R-codes, watchlists or
production outputs.

Semantics:
- Score is a regime-dependent composite quality score, not a trade signal.
- R0-R5 is a downstream quality/regime class based on score percentile plus
  score status, TrendOK and LiquidityOK; it is not treated as a buy/sell signal.
- Historical R4/R5 are not guessed when their gating inputs were not stored.
"""

from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

HORIZONS = (5, 20, 40, 60)
EVENT_BASE_COLUMNS = (
    "obs_date", "symbol", "score", "score_pct_full", "currency", "r_code",
    "start_market_date", "start_close", "start_adjusted_close",
)


@dataclass(frozen=True)
class Phase1AConfig:
    stable_start: str = "2026-04-15"
    cooldown_sessions: int = 5
    min_cross_section: int = 20
    min_within_events: int = 8
    min_prior_scores: int = 20


def _empty_events() -> pd.DataFrame:
    cols = list(EVENT_BASE_COLUMNS)
    for horizon in HORIZONS:
        cols += [f"end_date_{horizon}t", f"return_{horizon}t"]
    return pd.DataFrame(columns=cols)


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


def _norm_currency(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().upper()
    return None if text in {"", "NAN", "NONE", "NULL"} else text


def _is_crypto(frame: pd.DataFrame) -> pd.Series:
    symbol = frame["symbol"].fillna("").astype(str).str.upper()
    name = frame.get("name", pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()
    sector = frame.get("sector", pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()
    pair = symbol.str.startswith("CRYPTO:") | symbol.str.endswith(
        ("-USD", "-EUR", "-USDT", "-USDC", "-BTC", "-ETH")
    )
    named = name.str.contains(
        r"\b(?:bitcoin|ethereum|cardano|solana|dogecoin|ripple|litecoin|polkadot|chainlink)\b",
        regex=True,
        na=False,
    )
    marked = sector.str.contains("krypto|cryptocurrency", regex=True, na=False) & pair
    return pair | named | marked


def _pearson(x: pd.Series, y: pd.Series) -> float | None:
    z = pd.concat(
        [pd.to_numeric(x, errors="coerce"), pd.to_numeric(y, errors="coerce")],
        axis=1,
    ).dropna()
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

    # Preserve append/source order until same-day reruns are deduplicated.
    frame["_source_order"] = np.arange(len(frame), dtype=np.int64)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "score"])
    frame = frame.drop_duplicates(["date", "symbol"], keep="last")
    frame = frame.sort_values(["date", "_source_order"], kind="mergesort")
    frame["score_pct_full"] = (
        frame.groupby("date", group_keys=False)["score"].apply(_score_percentile)
    )
    frame["is_crypto"] = _is_crypto(frame)
    return frame.drop(columns=["_source_order"])


def _price_rows(prices: pd.DataFrame) -> pd.DataFrame:
    """Return observed sessions with raw and split/dividend-adjusted closes.

    Raw close establishes the trading-session calendar.  Research returns use
    adj_close only; absence of the field is a migration error, never a reason to
    silently fall back to raw close.
    """
    if "adj_close" not in prices.columns:
        raise ValueError("adjusted_close_required: refresh price history before research")
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame["symbol"] = frame["symbol"].astype(str)
    return (
        frame.dropna(subset=["date", "symbol", "close"])
        .sort_values(["symbol", "date"], kind="mergesort")
        .drop_duplicates(["symbol", "date"], keep="last")
    )


def build_events(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase1AConfig = Phase1AConfig(),
) -> pd.DataFrame:
    scanner = _scanner_rows(history)
    scanner = scanner.loc[~scanner["is_crypto"]].copy()
    scanner = scanner.loc[scanner["date"] >= pd.Timestamp(config.stable_start)].copy()
    if scanner.empty:
        return _empty_events()

    price = _price_rows(prices)
    if price.empty:
        return _empty_events()

    available = set(price["symbol"].unique())
    scanner = scanner.loc[scanner["symbol"].astype(str).isin(available)].copy()
    if scanner.empty:
        return _empty_events()
    scanner["price_symbol"] = scanner["symbol"].astype(str)

    price_groups = {
        symbol: group[["date", "close", "adj_close"]].sort_values("date", kind="mergesort").reset_index(drop=True)
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
        start_adjusted = series.iloc[position]["adj_close"]
        if pd.isna(start_adjusted) or float(start_adjusted) <= 0:
            # Keep session arithmetic honest: do not collapse missing adjusted
            # sessions and do not substitute the raw close.
            continue

        event = {
            "obs_date": pd.Timestamp(row.date).normalize(),
            "symbol": row.price_symbol,
            "score": float(row.score),
            "score_pct_full": float(row.score_pct_full),
            "currency": _norm_currency(getattr(row, "currency", None)),
            "r_code": getattr(row, "r_code", None),
            "start_market_date": start_date,
            "start_close": float(series.iloc[position]["close"]),
            "start_adjusted_close": float(start_adjusted),
        }
        for horizon in HORIZONS:
            target = position + horizon
            if target < len(series):
                event[f"end_date_{horizon}t"] = series.iloc[target]["date"]
                target_adjusted = series.iloc[target]["adj_close"]
                event[f"return_{horizon}t"] = (
                    float(target_adjusted) / event["start_adjusted_close"] - 1.0
                    if pd.notna(target_adjusted) and float(target_adjusted) > 0
                    else np.nan
                )
            else:
                event[f"end_date_{horizon}t"] = pd.NaT
                event[f"return_{horizon}t"] = np.nan
        records.append(event)

    if not records:
        return _empty_events()

    events = pd.DataFrame(records)
    # Weekend/holiday scanner reruns may point to one market session.
    events = (
        events.sort_values(["symbol", "start_market_date", "obs_date"], kind="mergesort")
        .drop_duplicates(["symbol", "start_market_date"], keep="last")
    )
    return events.reset_index(drop=True)


def cooldown_events(events: pd.DataFrame, prices: pd.DataFrame, sessions: int = 5) -> pd.DataFrame:
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
        for idx, row in group.sort_values("start_market_date", kind="mergesort").iterrows():
            pos = mapping.get(pd.Timestamp(row["start_market_date"]))
            if pos is None:
                continue
            if last is None or pos - last >= sessions:
                keep.append(idx)
                last = pos
    return events.loc[keep].sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)


def cross_sectional_summary(events: pd.DataFrame, horizon: int, min_n: int = 20) -> dict:
    if events.empty:
        return {"days": 0}
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
        daily.append(
            {
                "N": int(len(z)),
                "spearman": rho,
                "top_quality_future_rank_mean": float(top.mean()) if len(top) else None,
                "low_quality_future_rank_mean": float(low.mean()) if len(low) else None,
            }
        )
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


def future_rank_bands(events: pd.DataFrame, horizon: int, min_n: int = 20) -> dict:
    if events.empty:
        return {}
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
        if group.empty:
            continue
        out[str(band)] = {
            "N": int(len(group)),
            "mean_future_rank": float(group["future_pct"].mean()),
            "future_top20_rate": float((group["future_pct"] >= 0.80).mean()),
            "future_bottom20_rate": float((group["future_pct"] <= 0.20).mean()),
        }
    return out


def _r_score_backbone(score: float, percentile: float) -> str:
    """Score-percentile backbone of R0-R5, excluding R4/R5 gates."""
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


def r_score_backbone_summary(events: pd.DataFrame, horizon: int, min_n: int = 20) -> dict:
    if events.empty:
        return {}
    ret = f"return_{horizon}t"
    z = events[["obs_date", "symbol", "score", "score_pct_full", ret]].dropna().copy()
    sizes = z.groupby("obs_date")[ret].transform("count")
    z = z.loc[sizes >= min_n].copy()
    if z.empty:
        return {}
    z["future_pct"] = z.groupby("obs_date")[ret].rank(method="average", pct=True)
    z["band"] = [
        _r_score_backbone(score, pct)
        for score, pct in zip(z["score"], z["score_pct_full"])
    ]
    out = {}
    for band, group in z.groupby("band"):
        out[str(band)] = {
            "N": int(len(group)),
            "symbols": int(group["symbol"].nunique()),
            "mean_future_rank": float(group["future_pct"].mean()),
            "future_top20_rate": float((group["future_pct"] >= 0.80).mean()),
            "future_bottom20_rate": float((group["future_pct"] <= 0.20).mean()),
            "median_return": float(group[ret].median()),
        }

    contrasts = []
    for _, group in z.groupby("obs_date"):
        low = group.loc[group["band"] == "B1"]
        high = group.loc[group["band"] == "B5"]
        if len(low) < 3 or len(high) < 3:
            continue
        contrasts.append(
            {
                "future_rank_diff": high["future_pct"].mean() - low["future_pct"].mean(),
                "bottom20_diff":
                    (high["future_pct"] <= 0.20).mean() - (low["future_pct"] <= 0.20).mean(),
                "top20_diff":
                    (high["future_pct"] >= 0.80).mean() - (low["future_pct"] >= 0.80).mean(),
            }
        )
    if contrasts:
        c = pd.DataFrame(contrasts)
        out["B5_vs_B1_daily"] = {
            "days": int(len(c)),
            "mean_future_rank_diff": float(c["future_rank_diff"].mean()),
            "mean_bottom20_rate_diff": float(c["bottom20_diff"].mean()),
            "mean_top20_rate_diff": float(c["top20_diff"].mean()),
        }
    return out


def _peer_medians(events: pd.DataFrame, ret: str):
    """Leave-one-symbol-out peer baseline keyed by (obs_date, symbol)."""
    baselines: dict[tuple[pd.Timestamp, str], float] = {}
    columns = ["obs_date", "symbol", "currency", ret]
    work = events[columns].copy()
    work[ret] = pd.to_numeric(work[ret], errors="coerce")
    for day, day_group in work.groupby("obs_date", sort=False):
        for row in day_group.itertuples(index=False):
            subject = str(row.symbol)
            peers = day_group.loc[day_group["symbol"].astype(str).ne(subject)].dropna(subset=[ret])
            if peers.empty:
                continue
            current_currency = _norm_currency(row.currency)
            same_currency = (
                peers.loc[peers["currency"].map(_norm_currency).eq(current_currency), ret]
                if current_currency is not None
                else pd.Series(dtype=float)
            )
            values = same_currency if len(same_currency) else peers[ret]
            if len(values):
                baselines[(pd.Timestamp(day), subject)] = float(values.median())
    return baselines, None


def _peer_median(row, by_currency, global_daily=None):
    return by_currency.get((pd.Timestamp(row["obs_date"]), str(row["symbol"])), np.nan)


def within_stock_summary(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    config: Phase1AConfig = Phase1AConfig(),
) -> dict:
    """Test own score level as timing information relative to local peers."""
    if events.empty:
        return {"symbols": 0}
    ret = f"return_{horizon}t"
    cd = cooldown_events(events, prices, config.cooldown_sessions)
    by_currency, global_daily = _peer_medians(events, ret)
    work = cd[["obs_date", "symbol", "score", "currency", ret]].dropna(
        subset=["obs_date", "symbol", "score", ret]
    ).copy()
    work["peer_median_return"] = work.apply(
        lambda row: _peer_median(row, by_currency, global_daily), axis=1
    )
    work["peer_excess"] = work[ret] - work["peer_median_return"]

    correlations = []
    for _, group in work.groupby("symbol"):
        z = group[["score", "peer_excess"]].dropna()
        if len(z) < config.min_within_events:
            continue
        rho = _spearman(z["score"], z["peer_excess"])
        if rho is not None:
            correlations.append(rho)
    if not correlations:
        return {"symbols": 0}
    arr = np.asarray(correlations, dtype=float)
    return {
        "symbols": int(len(arr)),
        "events": int(work["peer_excess"].notna().sum()),
        "median_symbol_spearman_score_vs_peer_excess": float(np.median(arr)),
        "mean_symbol_spearman_score_vs_peer_excess": float(np.mean(arr)),
        "positive_symbol_rate": float(np.mean(arr > 0)),
        "peer_baseline": "leave-one-symbol-out same-currency median; leave-one-out global fallback",
    }


def point_in_time_own_score(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int,
    config: Phase1AConfig = Phase1AConfig(),
) -> dict:
    """Compare unusually high vs low score using only prior own-history."""
    if events.empty:
        return {"N": 0}
    ret = f"return_{horizon}t"
    daily = events.sort_values(["symbol", "obs_date"], kind="mergesort").copy()
    own = {}
    for _, group in daily.groupby("symbol"):
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
    by_currency, global_daily = _peer_medians(events, ret)
    work = cd[["obs_date", "symbol", "currency", "own_score_pct_prior", ret]].dropna(
        subset=["obs_date", "symbol", "own_score_pct_prior", ret]
    ).copy()
    work["peer_excess"] = work[ret] - work.apply(
        lambda row: _peer_median(row, by_currency, global_daily), axis=1
    )
    work = work.dropna(subset=["peer_excess"])
    low = work.loc[work["own_score_pct_prior"] <= 0.20]
    high = work.loc[work["own_score_pct_prior"] >= 0.80]
    return {
        "N": int(len(work)),
        "own_low_N": int(len(low)),
        "own_high_N": int(len(high)),
        "own_low_mean_peer_excess": float(low["peer_excess"].mean()) if len(low) else None,
        "own_high_mean_peer_excess": float(high["peer_excess"].mean()) if len(high) else None,
        "high_minus_low_mean_peer_excess": (
            float(high["peer_excess"].mean() - low["peer_excess"].mean())
            if len(low) and len(high) else None
        ),
        "own_low_positive_peer_excess_rate": float((low["peer_excess"] > 0).mean()) if len(low) else None,
        "own_high_positive_peer_excess_rate": float((high["peer_excess"] > 0).mean()) if len(high) else None,
        "peer_baseline": "leave-one-symbol-out same-currency median; leave-one-out global fallback",
    }


def currency_selection(events: pd.DataFrame, horizon: int, min_n: int = 8) -> dict:
    if events.empty:
        return {}
    out = {}
    for currency, group in events.dropna(subset=["currency"]).groupby("currency"):
        if len(group) < 300:
            continue
        summary = cross_sectional_summary(group, horizon, min_n=min_n)
        if summary.get("days", 0) >= 10:
            out[str(currency)] = summary
    return out


def analyze(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    config: Phase1AConfig = Phase1AConfig(),
) -> dict:
    events = build_events(history, prices, config)
    scanner = _scanner_rows(history)
    r_col = scanner.get("r_code", pd.Series(index=scanner.index, dtype=object))
    r_rows = scanner.loc[r_col.notna()]
    result = {
        "phase": "1A_selection_vs_timing",
        "semantics": {
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "r_code_role": "quality/regime input for daily analysis",
            "market_regime_role": "bull/neutral/bear changes opportunity/risk score weights",
            "note": (
                "Exact historical R0-R5 validation is limited to stored r_code rows; "
                "the R-score backbone is reported separately and does not invent "
                "historical TrendOK/LiquidityOK gates."
            ),
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
            "r_score_backbone": r_score_backbone_summary(
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


def run(
    history_path: str | Path,
    prices_path: str | Path,
    output_path: str | Path | None = None,
    config: Phase1AConfig = Phase1AConfig(),
) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    result = analyze(history, prices, config)
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    return result
