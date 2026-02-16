from __future__ import annotations

"""Yahoo Finance enrichment (prices + risk/liq + market regime).

This module is **pipeline support**, not part of the scoring engine.
It only fills *existing* watchlist columns that the scoring engine already
knows how to read:

- Akt. Kurs, Perf %
- SMA200, Trend200, RS3M
- Volatility, DownsideDev, MaxDrawdown
- AvgVolume, DollarVolume
- MarketRegimeStock / MarketTrend200Stock
- MarketRegimeCrypto / MarketTrend200Crypto
- MarketDate

Important constraints
---------------------
- Never calculates or overwrites scores.
- Network access is optional. By default, we only auto-enable on GitHub Actions.
- If a symbol fetch fails, we keep previous values from the input table.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
import re
from typing import Any

import pandas as pd


ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

# Common crypto quote suffixes used on Yahoo (keep small + conservative)
CRYPTO_QUOTES = {"USD", "EUR", "USDT", "USDC", "BTC", "ETH", "GBP", "JPY", "CHF", "AUD", "CAD"}


def should_fetch_yahoo() -> bool:
    """Decide whether the pipeline should hit Yahoo Finance.

    Priority:
      1) Explicit env SCANNER_FETCH_YAHOO
      2) Auto-enable on GitHub Actions (GITHUB_ACTIONS=true)
      3) Otherwise off
    """

    v = os.getenv("SCANNER_FETCH_YAHOO")
    if v is not None:
        return str(v).strip().lower() in {"1", "true", "yes", "on"}

    # Auto-enable on GitHub Actions so scheduled workflows always refresh data
    gh = os.getenv("GITHUB_ACTIONS", "").strip().lower()
    if gh in {"1", "true", "yes", "on"}:
        return True

    return False


@dataclass(frozen=True)
class YahooEnrichReport:
    enabled: bool
    tickers_total: int
    tickers_fetched: int
    tickers_failed: int
    benchmark_stock: str
    benchmark_crypto: str
    market_regime_stock: str | None
    market_trend200_stock: float | None
    market_regime_crypto: str | None
    market_trend200_crypto: float | None
    market_date: str

    def to_text(self) -> str:
        lines = []
        lines.append("Yahoo Finance Enrichment Report")
        lines.append("=" * 32)
        lines.append(f"enabled: {self.enabled}")
        lines.append(f"market_date: {self.market_date}")
        lines.append("")
        lines.append(f"tickers_total:  {self.tickers_total}")
        lines.append(f"tickers_fetched:{self.tickers_fetched}")
        lines.append(f"tickers_failed: {self.tickers_failed}")
        lines.append("")
        lines.append(f"benchmark_stock:  {self.benchmark_stock}")
        lines.append(f"regime_stock:     {self.market_regime_stock} (trend200={_fmt(self.market_trend200_stock)})")
        lines.append(f"benchmark_crypto: {self.benchmark_crypto}")
        lines.append(f"regime_crypto:    {self.market_regime_crypto} (trend200={_fmt(self.market_trend200_crypto)})")
        lines.append("")
        lines.append("Note: Per-symbol failures keep previous values from watchlist.csv.")
        return "\n".join(lines).strip() + "\n"


def _fmt(x: float | None) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):+.4f}"
    except Exception:
        return "—"


def _classify_regime(trend200: float | None) -> str:
    # Must mirror scanner.domain.scoring_engine.config.regime thresholds
    if trend200 is None or not math.isfinite(float(trend200)):
        return "neutral"
    t = float(trend200)
    if t < 0.0:
        return "bear"
    if t < 0.05:
        return "neutral"
    return "bull"


def _pick_symbol(row: pd.Series) -> str:
    # Prefer explicit YahooSymbol; then Yahoo; then Symbol; then Ticker.
    for c in ("YahooSymbol", "Yahoo", "Symbol", "Ticker"):
        if c in row.index:
            v = row.get(c)
            if v is None:
                continue
            s = str(v).strip()
            if not s or s.lower() == "nan":
                continue
            # If it looks like an ISIN, skip (that's not a tradable Yahoo symbol)
            if ISIN_RE.match(s.upper()):
                continue
            return s
    return ""


def _looks_like_crypto_pair(sym: str) -> bool:
    """Best-effort detection for crypto pairs like BTC-USD, ADA-EUR, ..."""
    t = str(sym).strip().upper()
    if "-" not in t:
        return False
    base, quote = t.rsplit("-", 1)
    if not base or not quote:
        return False
    # Reduce false positives: equities like BRK-B have quote 'B' (not in set)
    if quote not in CRYPTO_QUOTES:
        return False
    # Base should be a short token (BTC, ETH, ADA...) not an ISIN
    if ISIN_RE.match(base):
        return False
    return len(base) <= 10


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _series_from_download(dl: pd.DataFrame, ticker: str) -> tuple[pd.Series | None, pd.Series | None]:
    """Return (close, volume) series for a given ticker from yfinance.download output."""
    if dl is None or dl.empty:
        return None, None

    # MultiIndex columns when multiple tickers; single-level when one ticker.
    if isinstance(dl.columns, pd.MultiIndex):
        try:
            close = dl[("Close", ticker)]
        except Exception:
            close = None
        try:
            vol = dl[("Volume", ticker)]
        except Exception:
            vol = None
        if close is None or (isinstance(close, pd.Series) and close.dropna().empty):
            # some yfinance versions invert the levels
            try:
                close = dl[(ticker, "Close")]
            except Exception:
                pass
            try:
                vol = dl[(ticker, "Volume")]
            except Exception:
                pass
    else:
        # Single ticker
        close = dl.get("Close")
        vol = dl.get("Volume")

    if close is None or not isinstance(close, pd.Series):
        return None, None
    if vol is not None and not isinstance(vol, pd.Series):
        vol = None
    return close, vol


def _compute_features(
    close: pd.Series,
    volume: pd.Series | None,
    *,
    benchmark_close: pd.Series | None = None,
    lookback_rs: int = 63,
) -> dict[str, float | None]:
    c = close.dropna()
    if c.empty:
        return {}

    last = float(c.iloc[-1])

    # 1y perf (pct)
    first = float(c.iloc[0])
    perf_pct = None
    if first and math.isfinite(first) and first != 0:
        perf_pct = (last / first - 1.0) * 100.0

    # SMA200 + Trend200 (fraction)
    sma200 = c.rolling(200).mean()
    sma_last = _safe_float(sma200.iloc[-1])
    trend200 = None
    if sma_last and sma_last != 0:
        trend200 = (last / sma_last) - 1.0

    # Returns for risk stats
    r = c.pct_change().dropna()
    if r.empty:
        vol = down = mdd = None
    else:
        ann = math.sqrt(252.0)
        vol = float(r.std()) * ann
        neg = r[r < 0]
        down = float(neg.std()) * ann if not neg.empty else 0.0
        dd = (c / c.cummax()) - 1.0
        mdd = float(abs(dd.min())) if not dd.empty else None

    # Liquidity
    avg_vol = None
    dollar_vol = None
    if volume is not None:
        v = volume.dropna()
        if not v.empty:
            avg_vol = float(v.tail(20).mean())
            dollar_vol = avg_vol * last if avg_vol is not None else None

    # RS3M: 3m outperformance vs benchmark (fraction)
    rs3m = None
    if benchmark_close is not None:
        b = benchmark_close.dropna()
        if not b.empty and len(c) >= 10:
            # align on dates (inner join)
            df = pd.DataFrame({"a": c, "b": b}).dropna()
            if len(df) > lookback_rs:
                a0 = float(df["a"].iloc[-lookback_rs])
                a1 = float(df["a"].iloc[-1])
                b0 = float(df["b"].iloc[-lookback_rs])
                b1 = float(df["b"].iloc[-1])
                if a0 and b0 and a0 != 0 and b0 != 0:
                    ar = (a1 / a0) - 1.0
                    br = (b1 / b0) - 1.0
                    rs3m = ar - br

    return {
        "Akt. Kurs": last,
        "Perf %": perf_pct,
        "SMA200": sma_last,
        "Trend200": trend200,
        "RS3M": rs3m,
        "Volatility": vol,
        "DownsideDev": down,
        "MaxDrawdown": mdd,
        "AvgVolume": avg_vol,
        "DollarVolume": dollar_vol,
    }


def enrich_watchlist_with_yahoo(
    df: pd.DataFrame,
    *,
    benchmark_stock: str = "SPY",
    benchmark_crypto: str = "BTC-USD",
    enabled: bool | None = None,
) -> tuple[pd.DataFrame, YahooEnrichReport]:
    """Enrich a watchlist table in-place (copy) using Yahoo Finance.

    Returns:
      (enriched_df, report)
    """

    if enabled is None:
        enabled = should_fetch_yahoo()

    now = datetime.now(timezone.utc)
    market_date = now.date().isoformat()

    if not enabled:
        rep = YahooEnrichReport(
            enabled=False,
            tickers_total=0,
            tickers_fetched=0,
            tickers_failed=0,
            benchmark_stock=benchmark_stock,
            benchmark_crypto=benchmark_crypto,
            market_regime_stock=None,
            market_trend200_stock=None,
            market_regime_crypto=None,
            market_trend200_crypto=None,
            market_date=market_date,
        )
        out = df.copy()
        if "MarketDate" not in out.columns:
            out["MarketDate"] = market_date
        return out, rep

    # Lazy import (so local offline runs stay fast and do not require yfinance)
    import yfinance as yf  # type: ignore

    out = df.copy()

    symbols: list[str] = []
    row_symbols: list[str] = []
    for _, row in out.iterrows():
        sym = _pick_symbol(row)
        row_symbols.append(sym)
        if sym:
            symbols.append(sym)

    # Deduplicate symbols for download
    symbols_u = sorted({s for s in symbols if s})
    tickers_total = len(symbols_u)

    # Always include benchmarks
    all_dl = sorted({*symbols_u, benchmark_stock, benchmark_crypto})

    # Download 1y daily bars (auto_adjust gives consistent close)
    dl = yf.download(
        tickers=all_dl,
        period="1y",
        interval="1d",
        auto_adjust=True,
        group_by="column",
        threads=True,
        progress=True,  # Enable progress logging
    )

    # Benchmark features
    b_stock_close, _ = _series_from_download(dl, benchmark_stock)
    b_crypto_close, _ = _series_from_download(dl, benchmark_crypto)
    b_stock = _compute_features(b_stock_close, None) if b_stock_close is not None else {}
    b_crypto = _compute_features(b_crypto_close, None) if b_crypto_close is not None else {}

    stock_trend200 = _safe_float(b_stock.get("Trend200"))
    crypto_trend200 = _safe_float(b_crypto.get("Trend200"))
    stock_reg = _classify_regime(stock_trend200)
    crypto_reg = _classify_regime(crypto_trend200)

    # Apply benchmark regime columns globally (same for all rows)
    out["MarketRegimeStock"] = stock_reg
    out["MarketTrend200Stock"] = stock_trend200
    out["MarketRegimeCrypto"] = crypto_reg
    out["MarketTrend200Crypto"] = crypto_trend200
    out["MarketDate"] = market_date

    fetched = 0
    failed = 0

    # Per-row enrichment (keep previous values on failures)
    for idx, sym in zip(out.index, row_symbols):
        if not sym:
            continue
        close, vol = _series_from_download(dl, sym)
        if close is None or close.dropna().empty:
            failed += 1
            continue

        # Choose benchmark for RS: stocks vs crypto pairs
        bench = b_crypto_close if _looks_like_crypto_pair(sym) else b_stock_close
        feats = _compute_features(close, vol, benchmark_close=bench)
        if not feats:
            failed += 1
            continue

        for k, v in feats.items():
            if v is None:
                continue
            out.at[idx, k] = v

        fetched += 1

    rep = YahooEnrichReport(
        enabled=True,
        tickers_total=tickers_total,
        tickers_fetched=fetched,
        tickers_failed=failed,
        benchmark_stock=benchmark_stock,
        benchmark_crypto=benchmark_crypto,
        market_regime_stock=stock_reg,
        market_trend200_stock=stock_trend200,
        market_regime_crypto=crypto_reg,
        market_trend200_crypto=crypto_trend200,
        market_date=market_date,
    )
    return out, rep
