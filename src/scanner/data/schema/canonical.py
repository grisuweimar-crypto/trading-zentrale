from __future__ import annotations

import re

import pandas as pd

# canonical -> mögliche CSV-Spalten
COLUMN_MAP = {
    # identity
    # NOTE: ticker is refined row-wise below because many users (incl. you) have
    # ISIN values in the Ticker column for some rows.
    "ticker": ["Ticker"],
    "name": ["Name"],
    "isin": ["ISIN"],
    "symbol": ["Symbol"],
    "yahoo_symbol": ["YahooSymbol", "Yahoo"],
    # NOTE: In many personal watchlists, 'Sektor' is a manually maintained category
    # (often not matching the official Yahoo/industry taxonomy). We therefore keep
    # it separate from the official 'Sector' field.
    "category": ["Sektor", "Kategorie", "Category"],
    "sector": ["Sector"],
    "industry": ["Industry"],
    "country": ["Country"],
    "currency": ["Currency", "Währung"],

    # price/perf
    "price": ["Akt. Kurs"],
    "price_eur": ["Akt. Kurs [€]"],
    "perf_pct": ["Perf %"],
    "perf_1d_pct": ["Perf 1D %"],
    "perf_1y_pct": ["Perf 1Y %"],

    # scores/signals
    "score": ["Score"],
    "crv": ["CRV"],
    "confidence": ["ConfidenceScore"],
    "diversification_penalty": ["ScoreDiversificationPenalty"],
    "elliott_signal": ["Elliott-Signal"],
    "mc_chance": ["MC-Chance"],

    # cycle/trend/mom
    "cycle": ["Zyklus %"],
    "cycle_status": ["Zyklus-Status"],
    "rs3m": ["RS3M"],
    "trend200": ["Trend200"],
    "sma200": ["SMA200"],

    # risk
    "volatility": ["Volatility"],
    "downside_dev": ["DownsideDev"],
    "max_drawdown": ["MaxDrawdown"],

    # liquidity
    "avg_volume": ["AvgVolume"],
    "dollar_volume": ["DollarVolume"],

    # regime
    "regime_stock": ["MarketRegimeStock"],
    "trend200_stock": ["MarketTrend200Stock"],
    "regime_crypto": ["MarketRegimeCrypto"],
    "trend200_crypto": ["MarketTrend200Crypto"],
    "market_date": ["MarketDate"],
}

NUMERIC_CANONICAL = {
    "price", "price_eur", "perf_pct", "perf_1d_pct", "perf_1y_pct",
    "score", "crv", "confidence", "mc_chance",
    "diversification_penalty",
    "cycle", "rs3m", "trend200", "sma200",
    "volatility", "downside_dev", "max_drawdown",
    "avg_volume", "dollar_volume",
}


ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

# Common crypto quote suffixes used in Yahoo symbols
CRYPTO_QUOTES = {"USD", "EUR", "USDT", "USDC", "BTC", "ETH", "GBP", "JPY", "CHF", "AUD", "CAD"}


def _as_str_series(obj: object, index: pd.Index) -> pd.Series:
    """Return a 1D string Series for a column that may be duplicated (DataFrame) or missing."""
    if obj is None:
        return pd.Series("", index=index, dtype="string")
    if isinstance(obj, pd.DataFrame):
        if obj.shape[1] == 0:
            return pd.Series("", index=index, dtype="string")
        s = obj.iloc[:, 0]
    else:
        s = obj
    try:
        return pd.Series(s, index=index).astype("string").fillna("").str.strip()
    except Exception:
        return pd.Series("", index=index, dtype="string")

def _norm_colname(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip().lower())


def pick_column(df: pd.DataFrame, canonical: str) -> str | None:
    """Pick the best matching source column for a canonical key.

    Supports duplicated headers like 'Sector', 'Sector.1' and case variants.
    We choose the candidate with the highest count of non-empty values.
    """
    wants = COLUMN_MAP.get(canonical, [])
    if not wants:
        return None

    cols = list(df.columns)

    def non_empty_count(col: str) -> int:
        s = df[col]
        # if duplicate headers are truly identical, pandas may return a DataFrame
        if isinstance(s, pd.DataFrame):
            if s.shape[1] == 0:
                return 0
            s = s.iloc[:, 0]
        s = s.astype("string").fillna("").str.strip()
        s = s.mask(s.eq("") | s.str.lower().eq("nan"))
        return int(s.notna().sum())

    # try each base name in priority order; within that, pick the best filled candidate
    for base in wants:
        base_n = _norm_colname(base)
        cand = []
        for c in cols:
            cn = _norm_colname(c)
            if cn == base_n:
                cand.append(c)
                continue
            # pandas duplicates become '.1', '.2' etc
            if cn.startswith(base_n + ".") or cn.startswith(base_n + "_"):
                cand.append(c)
                continue
        if cand:
            return max(cand, key=non_empty_count)

    return None

def canonicalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # add canonical columns (as views/copies)
    for key in COLUMN_MAP.keys():
        if key in out.columns:
            continue
        src = pick_column(out, key)
        if src is not None:
            out[key] = out[src]

    # type casts
    for key in NUMERIC_CANONICAL:
        if key in out.columns:
            out[key] = pd.to_numeric(out[key], errors="coerce")

    # derived flags
    if "trend_ok" not in out.columns and "trend200" in out.columns:
        out["trend_ok"] = out["trend200"] > 0

    if "liquidity_ok" not in out.columns:
        if "dollar_volume" in out.columns:
            out["liquidity_ok"] = out["dollar_volume"] >= 5_000_000
        elif "avg_volume" in out.columns:
            out["liquidity_ok"] = out["avg_volume"] >= 200_000

    # --- asset class + score status (helps presets + UI) ---
    def _is_crypto(frame: pd.DataFrame) -> pd.Series:
        """Best-effort crypto detection.

        We must be robust against:
        - personal watchlists where 'Ticker' contains ISINs,
        - mixed quote pairs (ADA-EUR, BTC-USD, ...),
        - manual categories like 'Krypto 🪙' that should NOT turn crypto-related equities into crypto.

        Signals (OR):
        - explicit class == crypto (only when no ISIN present)
        - symbol/yahoo_symbol/ticker looks like a crypto pair (BTC-USD, ADA-EUR, ...)
        - name/sector/category contains crypto markers (only when no ISIN present)
        """
        idx = frame.index

        def _col(*names: str) -> pd.Series:
            for name in names:
                if name in frame.columns:
                    return _as_str_series(frame[name], idx)
            return pd.Series('', index=idx)


        isin_s = _col('isin', 'ISIN').str.strip()
        # Many personal watchlists store the ISIN in other columns (Ticker/Symbol/YahooSymbol).
        # Treat ISIN-looking values as "has_isin" so we don't misclassify crypto-related equities as crypto.
        has_isin = isin_s.ne('')

        # 1) crypto pair heuristics on multiple symbol fields
        ys_u = _col('yahoo_symbol', 'YahooSymbol', 'Yahoo').str.upper().str.strip()
        sym_u = _col('symbol', 'Symbol').str.upper().str.strip()
        tk_u = _col('ticker', 'Ticker', 'ticker_display').str.upper().str.strip()
        # augment has_isin if any of these fields *look* like an ISIN
        has_isin = has_isin | ys_u.str.match(ISIN_RE.pattern, na=False) | sym_u.str.match(ISIN_RE.pattern, na=False) | tk_u.str.match(ISIN_RE.pattern, na=False)

        # 1a) suffix pairs like BTC-USD / ADA-EUR
        pair = pd.Series(False, index=idx)
        for q in sorted(CRYPTO_QUOTES):
            suf = f'-{q}'
            pair = pair | ys_u.str.endswith(suf) | sym_u.str.endswith(suf) | tk_u.str.endswith(suf)

        # 1b) no-dash pairs like BTCUSD / ETHEUR
        pair_nodash = pd.Series(False, index=idx)
        for q in sorted(CRYPTO_QUOTES):
            pair_nodash = pair_nodash | ys_u.str.endswith(q) | sym_u.str.endswith(q) | tk_u.str.endswith(q)
        # require at least 2 chars before the quote to reduce false positives
        pair_nodash = pair_nodash & (
            ys_u.str.len().ge(5) | sym_u.str.len().ge(5) | tk_u.str.len().ge(5)
        )

        # 2) explicit class (only trustworthy when no ISIN)
        cls = _col('ScoreAssetClass', 'asset_class', 'AssetClass').str.lower()
        class_is_crypto = cls.eq('crypto') & ~has_isin

        # 3) markers in name/sector/category (only when no ISIN)
        nm = _col('name', 'Name').str.lower()
        sec = _col('sector', 'Sector').str.lower()
        cat = _col('category', 'Sektor', 'Kategorie', 'Category').str.lower()

        marker = (
            nm.str.contains(r'crypto|krypto', regex=True, na=False)
            | sec.str.contains(r'crypto|krypto', regex=True, na=False)
            | cat.str.contains(r'crypto|krypto', regex=True, na=False)
        ) & ~has_isin

        # 4) common crypto names (only when no ISIN)
        name_is_crypto = (
            nm.str.contains(
                r'\b(?:bitcoin|ethereum|cardano|solana|dogecoin|ripple|litecoin|polkadot|chainlink|avalanche|polygon|uniswap|cosmos)\b',
                regex=True,
                na=False,
            )
        ) & ~has_isin

        return (pair | pair_nodash | class_is_crypto | marker | name_is_crypto)

    out["is_crypto"] = _is_crypto(out).astype(bool)

    # A score of 0 is allowed and meaningful (typically "avoid").
    # We compute a compact status label for presets and explainability.
    if "score_status" not in out.columns and "score" in out.columns:
        s = pd.to_numeric(out["score"], errors="coerce")
        status = pd.Series("OK", index=out.index)
        status[s.isna()] = "NA"
        if "ScoreError" in out.columns:
            err = out["ScoreError"]
            if isinstance(err, pd.DataFrame):
                err = err.iloc[:, 0] if err.shape[1] else pd.Series("", index=out.index)
            err = err.fillna("").astype(str)
            status[err.str.len().gt(0)] = "ERROR"
        zero = s.fillna(0).eq(0)
        crypto = out["is_crypto"].fillna(False).astype(bool)
        status[zero & crypto] = "AVOID_CRYPTO_BEAR"
        status[zero & ~crypto] = "AVOID"
        out["score_status"] = status

    # --- identity refinement (row-wise) ---
    # We want a usable ticker for UI/sorting, even if the user DB stores ISIN in "Ticker".
    # Prefer: Symbol (if not ISIN-like) -> YahooSymbol (if not ISIN-like) -> Ticker.
    idx = out.index
    ticker_raw = _as_str_series(out.get("Ticker"), idx)
    symbol_raw = _as_str_series(out.get("Symbol"), idx)
    yahoo_raw = _as_str_series(out.get("YahooSymbol", out.get("Yahoo")), idx)

    # Ensure we always have an `isin` column available for UI and dedup logic.
    if "isin" not in out.columns:
        out["isin"] = pd.Series(pd.NA, index=idx, dtype="string")

    def _not_isin(s: pd.Series) -> pd.Series:
        return s.ne("") & ~s.str.upper().str.match(ISIN_RE, na=False)

    t = ticker_raw.copy()
    # if ticker is empty or ISIN-like, try symbol/yahoo
    needs_alt = (t.eq("")) | (t.str.upper().str.match(ISIN_RE, na=False))
    alt1_ok = _not_isin(symbol_raw)
    alt2_ok = _not_isin(yahoo_raw)
    t.loc[needs_alt & alt1_ok] = symbol_raw.loc[needs_alt & alt1_ok]
    t.loc[needs_alt & ~alt1_ok & alt2_ok] = yahoo_raw.loc[needs_alt & ~alt1_ok & alt2_ok]

    out["ticker"] = t
    out["symbol"] = symbol_raw
    out["yahoo_symbol"] = yahoo_raw

    # If ticker still looks like an ISIN, keep it but ensure isin is populated.
    isin_cur = _as_str_series(out.get("isin"), idx)
    m_isin = ticker_raw.str.upper().str.match(ISIN_RE, na=False)
    isin_cur.loc[m_isin & isin_cur.eq("")] = ticker_raw.loc[m_isin]

    # also capture ISIN if user stored it in Symbol/YahooSymbol
    m_isin_sym = symbol_raw.str.upper().str.match(ISIN_RE, na=False)
    isin_cur.loc[m_isin_sym & isin_cur.eq("")] = symbol_raw.loc[m_isin_sym]
    m_isin_yh = yahoo_raw.str.upper().str.match(ISIN_RE, na=False)
    isin_cur.loc[m_isin_yh & isin_cur.eq("")] = yahoo_raw.loc[m_isin_yh]

    # also capture if the refined ticker remains an ISIN
    m_isin2 = t.str.upper().str.match(ISIN_RE, na=False)
    isin_cur.loc[m_isin2 & isin_cur.eq("")] = t.loc[m_isin2]
    out["isin"] = isin_cur.replace({"": pd.NA})

    # Crypto pair parsing (base/quote) for better UI + dedup.
    # We prefer Yahoo symbols for parsing (they usually contain the quote currency).
    ys_parse = yahoo_raw.copy()
    tk_parse = t.copy()
    up = ys_parse.where(ys_parse.ne(""), tk_parse).str.upper()
    base = pd.Series(pd.NA, index=idx, dtype="string")
    quote = pd.Series(pd.NA, index=idx, dtype="string")
    # pattern like ADA-USD
    m = up.str.contains("-", na=False)
    if m.any():
        parts = up[m].str.split("-", n=1, expand=True)
        if parts.shape[1] == 2:
            b = parts.iloc[:, 0].astype("string")
            q = parts.iloc[:, 1].astype("string")
            q_ok = q.isin(list(CRYPTO_QUOTES))
            keep_idx = q.index[q_ok]
            base.loc[keep_idx] = b.loc[keep_idx]
            quote.loc[keep_idx] = q.loc[keep_idx]
    out["crypto_base"] = base
    out["quote_currency"] = quote

    # Stable ID for dedup + UI row keys
    aid = pd.Series("", index=idx, dtype="string")

    # For crypto, dedupe on base-asset (CRYPTO:ADA) instead of quote-pairs (ADA-USD/ADA-EUR)
    if "is_crypto" in out.columns:
        crypto = out["is_crypto"].fillna(False).astype(bool)
        base_ok = out["crypto_base"].fillna("").astype(str).str.len().gt(0)
        aid.loc[crypto & base_ok] = ("CRYPTO:" + out.loc[crypto & base_ok, "crypto_base"].astype("string"))
    aid.loc[aid.eq("") & _not_isin(yahoo_raw)] = yahoo_raw.loc[aid.eq("") & _not_isin(yahoo_raw)]
    aid.loc[aid.eq("") & _not_isin(symbol_raw)] = symbol_raw.loc[aid.eq("") & _not_isin(symbol_raw)]
    isin_s = _as_str_series(out.get("isin"), idx)
    aid.loc[aid.eq("") & isin_s.ne("")] = isin_s.loc[aid.eq("") & isin_s.ne("")]
    aid.loc[aid.eq("") & t.ne("")] = t.loc[aid.eq("") & t.ne("")]
    aid.loc[aid.eq("")] = out.get("name", pd.Series("", index=idx)).astype("string").fillna("")
    out["asset_id"] = aid

    # UI-friendly display ticker
    # - crypto: show BASE (ADA) but keep yahoo_symbol for linking
    # - stocks: show refined ticker
    td = t.copy()
    if "is_crypto" in out.columns:
        crypto = out["is_crypto"].fillna(False).astype(bool)
        base_ok = out["crypto_base"].fillna("").astype(str).str.len().gt(0)
        td.loc[crypto & base_ok] = out.loc[crypto & base_ok, "crypto_base"].astype("string")
    out["ticker_display"] = td
    
    return out
