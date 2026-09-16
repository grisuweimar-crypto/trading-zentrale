from __future__ import annotations

import json
import math
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

INPUT = Path("artifacts/snapshots/score_history.csv")
MASTER = Path("data/inputs/universe_master.csv")
OUTDIR = Path("artifacts/reconstruction_temp")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUTPUT = OUTDIR / "score_history_reconstructed.csv"
LOG = OUTDIR / "reconstruction_log.csv"
REPORT = OUTDIR / "reconstruction_report.json"

TARGETS = ["close", "volatility", "drawdown", "rs3m", "trend200"]
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
CRYPTO_BASES = {"BTC", "ETH", "ADA", "SOL", "XRP", "DOGE"}


def norm(s: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(s or "").upper())


def is_crypto_row(row: pd.Series) -> bool:
    sym = str(row.get("symbol", "")).upper()
    name = str(row.get("name", "")).upper()
    sector = str(row.get("sector", "")).upper()
    if "KRYPTO" in name or "CRYPTO" in name or "KRYPTO" in sector or "CRYPTO" in sector:
        return True
    if "-" in sym and sym.split("-", 1)[0] in CRYPTO_BASES:
        return True
    return False


def build_mapping(df: pd.DataFrame, master: pd.DataFrame) -> tuple[dict[int, str], list[dict[str, object]]]:
    master = master.copy()
    for c in ["symbol", "isin", "name", "currency"]:
        if c not in master.columns:
            master[c] = ""
        master[c] = master[c].fillna("").astype(str)
    by_isin: dict[str, list[pd.Series]] = {}
    by_name: dict[str, list[pd.Series]] = {}
    for _, r in master.iterrows():
        if r["isin"]:
            by_isin.setdefault(r["isin"].upper(), []).append(r)
        by_name.setdefault(norm(r["name"]), []).append(r)

    mapping: dict[int, str] = {}
    logs: list[dict[str, object]] = []
    for idx, row in df.iterrows():
        raw = str(row.get("symbol", "")).strip()
        cur = str(row.get("currency", "")).strip().upper()
        chosen = ""
        method = ""

        # Crypto: honor quote currency because some legacy rows reused BTC-USD while storing EUR prices.
        if is_crypto_row(row):
            base = raw.upper().split("-", 1)[0] if "-" in raw else ""
            if base in CRYPTO_BASES and cur in {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"}:
                chosen = f"{base}-{cur}"
                method = "crypto_base_currency"
            elif raw:
                chosen = raw
                method = "crypto_direct"
        elif raw and not ISIN_RE.match(raw.upper()):
            chosen = raw
            method = "direct_symbol"
        else:
            candidates = by_isin.get(raw.upper(), [])
            if candidates:
                same_cur = [r for r in candidates if str(r.get("currency", "")).upper() == cur]
                pool = same_cur or candidates
                name_key = norm(row.get("name", ""))
                same_name = [r for r in pool if norm(r.get("name", "")) == name_key]
                pick = (same_name or pool)[0]
                chosen = str(pick.get("symbol", "")).strip()
                method = "isin_master"
            if not chosen:
                candidates = by_name.get(norm(row.get("name", "")), [])
                if candidates:
                    same_cur = [r for r in candidates if str(r.get("currency", "")).upper() == cur]
                    pick = (same_cur or candidates)[0]
                    chosen = str(pick.get("symbol", "")).strip()
                    method = "name_master"
        if chosen:
            mapping[idx] = chosen
        else:
            logs.append({"row_index": idx, "date": row.get("date"), "symbol": raw, "name": row.get("name"), "status": "unmapped", "detail": method})
    return mapping, logs


def extract_series(dl: pd.DataFrame, ticker: str) -> pd.Series | None:
    if dl is None or dl.empty:
        return None
    s = None
    if isinstance(dl.columns, pd.MultiIndex):
        for key in [(ticker, "Close"), ("Close", ticker)]:
            try:
                x = dl[key]
                if isinstance(x, pd.Series):
                    s = x
                    break
            except Exception:
                pass
    else:
        try:
            x = dl["Close"]
            if isinstance(x, pd.Series):
                s = x
        except Exception:
            pass
    if s is None:
        return None
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    idx = pd.to_datetime(s.index, utc=True, errors="coerce")
    s.index = idx.tz_convert(None).normalize()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def download_prices(tickers: list[str], start: str, end: str) -> tuple[dict[str, pd.Series], list[str]]:
    prices: dict[str, pd.Series] = {}
    failed: list[str] = []
    tickers = sorted(set(tickers))
    chunk_size = 25
    for pos in range(0, len(tickers), chunk_size):
        chunk = tickers[pos:pos + chunk_size]
        for attempt in range(3):
            try:
                dl = yf.download(
                    tickers=chunk,
                    start=start,
                    end=end,
                    interval="1d",
                    auto_adjust=True,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                    timeout=30,
                )
                for t in chunk:
                    s = extract_series(dl, t)
                    if s is not None and not s.empty:
                        prices[t] = s
                break
            except Exception:
                if attempt == 2:
                    pass
                else:
                    time.sleep(2 * (attempt + 1))
        time.sleep(0.5)

    # Retry failures individually: this also handles symbols that upset a batch request.
    for t in tickers:
        if t in prices:
            continue
        ok = False
        for attempt in range(2):
            try:
                dl = yf.download(tickers=t, start=start, end=end, interval="1d", auto_adjust=True,
                                 progress=False, threads=False, timeout=30)
                s = extract_series(dl, t)
                if s is not None and not s.empty:
                    prices[t] = s
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(1.5)
        if not ok:
            failed.append(t)
    return prices, failed


def hist_to(s: pd.Series, d: pd.Timestamp) -> pd.Series:
    return s[s.index <= d]


def last_close(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d)
    return float(h.iloc[-1]) if not h.empty else None


def vol_1y(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d)
    h = h[h.index >= d - pd.Timedelta(days=365)]
    r = h.pct_change().dropna()
    return float(r.std(ddof=1)) if len(r) >= 2 else None


def vol_30(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d).tail(31)
    r = h.pct_change().dropna()
    return float(r.std(ddof=1)) if len(r) >= 2 else None


def vol_63(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d).tail(64)
    r = h.pct_change().dropna()
    return float(r.std(ddof=1)) if len(r) >= 2 else None


def drawdown_calendar_1y(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d)
    h = h[h.index >= d - pd.Timedelta(days=365)]
    if len(h) < 2:
        return None
    dd = h / h.cummax() - 1.0
    return float(abs(dd.min()))


def drawdown_252(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d).tail(252)
    if len(h) < 2:
        return None
    dd = h / h.cummax() - 1.0
    return float(abs(dd.min()))


def trend_200(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d)
    if len(h) < 200:
        return None
    last = float(h.iloc[-1])
    sma = float(h.tail(200).mean())
    return last / sma - 1.0 if sma else None


def ret_lookback_own(s: pd.Series, d: pd.Timestamp, n: int) -> float | None:
    h = hist_to(s, d)
    if len(h) <= n:
        return None
    a0 = float(h.iloc[-(n + 1)])
    a1 = float(h.iloc[-1])
    return a1 / a0 - 1.0 if a0 else None


def ret_lookback_currentstyle(s: pd.Series, d: pd.Timestamp, n: int = 63) -> float | None:
    h = hist_to(s, d)
    if len(h) <= n:
        return None
    a0 = float(h.iloc[-n])
    a1 = float(h.iloc[-1])
    return a1 / a0 - 1.0 if a0 else None


def ret_calendar_3m(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist_to(s, d)
    if h.empty:
        return None
    target = d - pd.DateOffset(months=3)
    before = h[h.index <= target]
    if before.empty:
        return None
    a0 = float(before.iloc[-1])
    a1 = float(h.iloc[-1])
    return a1 / a0 - 1.0 if a0 else None


def relative_return(s: pd.Series, bench: pd.Series, d: pd.Timestamp, mode: str) -> float | None:
    a = hist_to(s, d)
    b = hist_to(bench, d)
    z = pd.concat([a.rename("a"), b.rename("b")], axis=1, join="inner").dropna()
    if z.empty:
        return None
    if mode == "63":
        if len(z) <= 63:
            return None
        a0, a1 = float(z.a.iloc[-64]), float(z.a.iloc[-1])
        b0, b1 = float(z.b.iloc[-64]), float(z.b.iloc[-1])
    elif mode == "current63":
        if len(z) <= 63:
            return None
        a0, a1 = float(z.a.iloc[-63]), float(z.a.iloc[-1])
        b0, b1 = float(z.b.iloc[-63]), float(z.b.iloc[-1])
    else:
        target = d - pd.DateOffset(months=3)
        prev = z[z.index <= target]
        if prev.empty:
            return None
        a0, b0 = float(prev.a.iloc[-1]), float(prev.b.iloc[-1])
        a1, b1 = float(z.a.iloc[-1]), float(z.b.iloc[-1])
    if not a0 or not b0:
        return None
    return (a1 / a0 - 1.0) - (b1 / b0 - 1.0)


def candidate_value(metric: str, candidate: str, s: pd.Series, d: pd.Timestamp, bench: pd.Series | None) -> float | None:
    if metric == "volatility":
        return {"vol_1y": vol_1y, "vol_30": vol_30, "vol_63": vol_63}[candidate](s, d)
    if metric == "drawdown":
        return {"dd_calendar_1y": drawdown_calendar_1y, "dd_252": drawdown_252}[candidate](s, d)
    if metric == "trend200":
        return trend_200(s, d)
    if metric == "rs3m":
        if candidate == "ret_63":
            return ret_lookback_own(s, d, 63)
        if candidate == "ret_current63":
            return ret_lookback_currentstyle(s, d, 63)
        if candidate == "ret_calendar_3m":
            return ret_calendar_3m(s, d)
        if bench is None:
            return None
        if candidate == "rel_63":
            return relative_return(s, bench, d, "63")
        if candidate == "rel_current63":
            return relative_return(s, bench, d, "current63")
        if candidate == "rel_calendar_3m":
            return relative_return(s, bench, d, "calendar")
    return None


def score_candidates(df: pd.DataFrame, mapping: dict[int, str], prices: dict[str, pd.Series], metric: str, candidates: list[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    known = df[df[metric].notna()]
    # Limit a very large validation set deterministically while retaining all dates/symbol types.
    if len(known) > 5000:
        known = known.iloc[np.linspace(0, len(known) - 1, 5000, dtype=int)]
    for cand in candidates:
        diffs = []
        for idx, row in known.iterrows():
            t = mapping.get(idx)
            if not t or t not in prices:
                continue
            d = pd.Timestamp(row["date"]).normalize()
            bench = prices.get("BTC-USD") if is_crypto_row(row) else prices.get("SPY")
            v = candidate_value(metric, cand, prices[t], d, bench)
            if v is None or not math.isfinite(v):
                continue
            diffs.append(abs(float(row[metric]) - v))
        if diffs:
            out[cand] = {"n": float(len(diffs)), "mae": float(np.mean(diffs)), "median_abs_error": float(np.median(diffs)), "p90_abs_error": float(np.quantile(diffs, 0.9))}
    return out


def best_candidate(scores: dict[str, dict[str, float]], fallback: str) -> str:
    if not scores:
        return fallback
    return min(scores, key=lambda k: (scores[k]["median_abs_error"], scores[k]["mae"]))


def main() -> None:
    df = pd.read_csv(INPUT, low_memory=False)
    original = df.copy(deep=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    master = pd.read_csv(MASTER, low_memory=False)
    mapping, logs = build_mapping(df, master)

    min_date = pd.to_datetime(df["date"], errors="coerce").min()
    max_date = pd.to_datetime(df["date"], errors="coerce").max()
    start = (min_date - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    end = (max_date + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    tickers = sorted(set(mapping.values()) | {"SPY", "BTC-USD"})
    prices, failed = download_prices(tickers, start, end)

    vol_scores = score_candidates(df, mapping, prices, "volatility", ["vol_1y", "vol_30", "vol_63"])
    dd_scores = score_candidates(df, mapping, prices, "drawdown", ["dd_calendar_1y", "dd_252"])
    rs_scores = score_candidates(df, mapping, prices, "rs3m", ["ret_63", "ret_current63", "ret_calendar_3m", "rel_63", "rel_current63", "rel_calendar_3m"])
    trend_scores = score_candidates(df, mapping, prices, "trend200", ["trend_200"])

    chosen = {
        "volatility": best_candidate(vol_scores, "vol_1y"),
        "drawdown": best_candidate(dd_scores, "dd_calendar_1y"),
        "rs3m": best_candidate(rs_scores, "ret_63"),
        "trend200": "trend_200",
    }

    fill_counts = {m: 0 for m in TARGETS}
    unresolved_counts = {m: 0 for m in TARGETS}
    changed_rows = []

    for idx, row in df.iterrows():
        t = mapping.get(idx)
        s = prices.get(t) if t else None
        d = pd.Timestamp(row["date"]).normalize() if pd.notna(row["date"]) else None
        for metric in TARGETS:
            if pd.notna(row.get(metric)):
                continue
            if s is None or d is None:
                unresolved_counts[metric] += 1
                continue
            bench = prices.get("BTC-USD") if is_crypto_row(row) else prices.get("SPY")
            if metric == "close":
                v = last_close(s, d)
            else:
                v = candidate_value(metric, chosen[metric], s, d, bench)
            if v is None or not math.isfinite(v):
                unresolved_counts[metric] += 1
                continue
            df.at[idx, metric] = float(v)
            fill_counts[metric] += 1
            changed_rows.append({"row_index": idx, "date": row["date"], "symbol": row["symbol"], "mapped_yahoo": t, "metric": metric, "value": float(v), "status": "filled", "method": chosen.get(metric, "eod_last")})

    # Hard integrity gate: no existing values in target columns may change, and non-target columns must remain identical.
    for c in original.columns:
        if c not in TARGETS:
            a = original[c].fillna("<NA>").astype(str)
            b = df[c].fillna("<NA>").astype(str)
            if not a.equals(b):
                raise RuntimeError(f"Non-target column changed: {c}")
        else:
            mask = original[c].notna()
            a = original.loc[mask, c].fillna("<NA>").astype(str)
            b = df.loc[mask, c].fillna("<NA>").astype(str)
            if not a.equals(b):
                raise RuntimeError(f"Existing values changed in target column: {c}")

    for t in failed:
        logs.append({"row_index": "", "date": "", "symbol": t, "name": "", "status": "yahoo_fetch_failed", "detail": "No usable daily history returned"})
    logs.extend(changed_rows)

    df.to_csv(OUTPUT, index=False)
    pd.DataFrame(logs).to_csv(LOG, index=False)

    report = {
        "input": str(INPUT),
        "rows": int(len(df)),
        "date_min": str(min_date.date()),
        "date_max": str(max_date.date()),
        "mapped_rows": int(len(mapping)),
        "unique_yahoo_tickers_requested": int(len(tickers)),
        "unique_yahoo_tickers_fetched": int(len(prices)),
        "fetch_failed": failed,
        "missing_before": {m: int(original[m].isna().sum()) for m in TARGETS},
        "filled": fill_counts,
        "unresolved_after": {m: int(df[m].isna().sum()) for m in TARGETS},
        "chosen_formulas": chosen,
        "validation": {"volatility": vol_scores, "drawdown": dd_scores, "rs3m": rs_scores, "trend200": trend_scores},
        "notes": [
            "Existing non-null values were never overwritten.",
            "Historical Yahoo daily data were downloaded with auto_adjust=True.",
            "For legacy ISIN rows the current universe master was used to resolve a Yahoo ticker; currency and name were used as tie-breakers.",
            "Formula variants were selected empirically by lowest validation error against preserved historical values already present in score_history.csv.",
            "Rows without a verifiable ticker/history remain blank and are logged."
        ],
    }
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
