from __future__ import annotations

"""Watchlist Doctor (B2.7)

Goal: help clean a messy watchlist (mixed IDs, duplicates, missing Yahoo symbols)
WITHOUT silently changing data.

Outputs:
  artifacts/reports/watchlist_doctor/
    - report.md
    - duplicates.csv
    - ticker_looks_like_isin.csv
    - missing_yahoo_symbol.csv
    - crypto_misclassified.csv
    - suggested_watchlist_clean.csv

Run:
  python scripts/watchlist_doctor.py
"""

from pathlib import Path
import argparse
import re

import pandas as pd

from scanner.data.io.paths import artifacts_dir, project_root


ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
CRYPTO_QUOTES = ("USD", "EUR", "USDT", "BTC", "ETH")

# Common crypto names (lowercase) for diagnostics (helps when symbols are stored as plain bases like "BTC")
CRYPTO_NAMES_RE = re.compile(
    r"\b(bitcoin|ethereum|cardano|solana|dogecoin|ripple|xrp|litecoin|polkadot|chainlink|avalanche|polygon|uniswap|cosmos)\b"
)


def _as_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series("", index=df.index)
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        obj = obj.iloc[:, 0] if obj.shape[1] else pd.Series("", index=df.index)
    return obj.fillna("").astype(str).str.strip()


def _coerce_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False).astype(bool)
    st = s.fillna("").astype(str).str.strip().str.lower()
    return st.isin({"1", "true", "t", "yes", "y"})


def _crypto_heuristic(df: pd.DataFrame) -> pd.Series:
    """Robust OR-heuristic used for diagnostics."""
    idx = df.index
    mask = pd.Series(False, index=idx)

    # explicit asset class (OR)
    for col in ("ScoreAssetClass", "asset_class", "AssetClass"):
        if col in df.columns:
            cls = _as_series(df, col).str.lower()
            mask = mask | cls.eq("crypto")
            break

    nm = _as_series(df, "name")
    if nm.eq("").all():
        nm = _as_series(df, "Name")
    nm_l = nm.str.lower()
    mask = mask | nm_l.str.contains(r"\b(?:crypto|krypto|kryptow)\w*\b", regex=True, na=False)

    sec = _as_series(df, "sector")
    if sec.eq("").all():
        sec = _as_series(df, "Sector")
    mask = mask | sec.str.lower().str.contains(r"\b(?:crypto|krypto)\b", regex=True, na=False)

    cat = _as_series(df, "category")
    if cat.eq("").all():
        cat = _as_series(df, "Sektor")
    mask = mask | cat.str.lower().str.contains(r"\b(?:crypto|krypto)\b", regex=True, na=False)

    # pair suffixes
    ys = _as_series(df, "yahoo_symbol")
    if ys.eq("").all():
        ys = _as_series(df, "YahooSymbol")
    tk = _as_series(df, "ticker")
    if tk.eq("").all():
        tk = _as_series(df, "Ticker")
    sym = _as_series(df, "symbol")
    if sym.eq("").all():
        sym = _as_series(df, "Symbol")
    ys_u = ys.str.upper()
    tk_u = tk.str.upper()
    sym_u = sym.str.upper()
    for q in CRYPTO_QUOTES:
        suf = f"-{q}"
        mask = mask | ys_u.str.endswith(suf) | tk_u.str.endswith(suf) | sym_u.str.endswith(suf)
    return mask


def main() -> int:
    ap = argparse.ArgumentParser(description="Watchlist Doctor: duplicates + ID hygiene + crypto classification")
    ap.add_argument(
        "--csv",
        default=str(artifacts_dir() / "watchlist" / "watchlist_full.csv"),
        help="canonical CSV to analyze (default: artifacts/watchlist/watchlist_full.csv)",
    )
    ap.add_argument(
        "--raw",
        default=str(artifacts_dir() / "watchlist" / "watchlist_full_raw.csv"),
        help="raw CSV to optionally use (default: artifacts/watchlist/watchlist_full_raw.csv)",
    )
    ap.add_argument(
        "--outdir",
        default=str(artifacts_dir() / "reports" / "watchlist_doctor"),
        help="output directory under artifacts/",
    )
    ns = ap.parse_args()

    csv_path = Path(ns.csv)
    raw_path = Path(ns.raw)
    outdir = Path(ns.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        print(f"❌ Missing CSV: {csv_path}")
        print("Run: python -m scanner.app.run_daily")
        return 1

    df = pd.read_csv(csv_path)

    df_raw: pd.DataFrame | None = None
    raw_crypto_total: int | None = None
    raw_crypto_bases: int | None = None
    raw_crypto_pairs_df: pd.DataFrame | None = None
    if raw_path.exists():
        try:
            df_raw = pd.read_csv(raw_path)
        except Exception:
            df_raw = None

    def _raw_crypto_pairs(frame: pd.DataFrame) -> tuple[int, int, pd.DataFrame]:
        """Extract crypto pair/base info from the raw DB CSV.

        This is intentionally conservative:
        - It uses explicit pair suffixes (-USD/-EUR/-USDT/...) OR common crypto names.
        - It does NOT use the user's manual category to avoid classifying crypto-related equities (e.g. Coinbase) as crypto.
        """
        idx0 = frame.index
        ys0 = _as_series(frame, "YahooSymbol")
        if ys0.eq("").all():
            ys0 = _as_series(frame, "Yahoo")
        sym0 = _as_series(frame, "Symbol")
        tk0 = _as_series(frame, "Ticker")
        nm0 = _as_series(frame, "Name")

        ys_u0 = ys0.str.upper()
        sym_u0 = sym0.str.upper()
        tk_u0 = tk0.str.upper()
        nm_l0 = nm0.str.lower()

        pair0 = pd.Series(False, index=idx0)
        for q in CRYPTO_QUOTES:
            suf = f"-{q}"
            pair0 = pair0 | ys_u0.str.endswith(suf) | sym_u0.str.endswith(suf) | tk_u0.str.endswith(suf)

        name0 = nm_l0.str.contains(CRYPTO_NAMES_RE, regex=True, na=False)
        crypto0 = pair0 | name0

        # parse base/quote
        best = ys0.where(ys0.ne(""), tk0).where(lambda s: ~s.eq(""), sym0)
        up = best.str.upper()
        base = pd.Series("", index=idx0)
        quote = pd.Series("", index=idx0)
        has_dash = up.str.contains("-", na=False)
        if has_dash.any():
            parts = up[has_dash].str.split("-", n=1, expand=True)
            if parts.shape[1] == 2:
                base.loc[has_dash] = parts.iloc[:, 0].fillna("").astype(str)
                quote.loc[has_dash] = parts.iloc[:, 1].fillna("").astype(str)
        # no-dash: use token itself as base when name marker triggers
        base.loc[~has_dash & name0] = up.loc[~has_dash & name0]

        out0 = pd.DataFrame(
            {
                "base": base,
                "quote": quote,
                "Ticker": tk0,
                "Symbol": sym0,
                "YahooSymbol": ys0,
                "Name": nm0,
            }
        )
        out0 = out0.loc[crypto0].copy()
        # clean
        out0["base"] = out0["base"].replace({"": pd.NA})
        out0["quote"] = out0["quote"].replace({"": pd.NA})
        bases = int(out0["base"].fillna("").nunique()) if not out0.empty else 0
        total = int(len(out0))
        return total, bases, out0

    if df_raw is not None and not df_raw.empty:
        raw_crypto_total, raw_crypto_bases, raw_crypto_pairs_df = _raw_crypto_pairs(df_raw)

    # --- normalize key columns (best-effort) ---
    ticker = _as_series(df, "ticker")
    if ticker.eq("").all():
        ticker = _as_series(df, "Ticker")
    isin = _as_series(df, "isin")
    if isin.eq("").all():
        isin = _as_series(df, "ISIN")
    symbol = _as_series(df, "symbol")
    if symbol.eq("").all():
        symbol = _as_series(df, "Symbol")
    yahoo = _as_series(df, "yahoo_symbol")
    if yahoo.eq("").all():
        yahoo = _as_series(df, "YahooSymbol")
    name = _as_series(df, "name")
    if name.eq("").all():
        name = _as_series(df, "Name")

    if "is_crypto" in df.columns:
        is_crypto = _coerce_bool(df["is_crypto"] if not isinstance(df["is_crypto"], pd.DataFrame) else df["is_crypto"].iloc[:, 0])
    else:
        is_crypto = pd.Series(False, index=df.index)

    asset_id = _as_series(df, "asset_id")

    # --- diagnostics ---
    diag_crypto = _crypto_heuristic(df)
    crypto_mis = df.loc[diag_crypto != is_crypto].copy()

    # duplicates
    dup = pd.DataFrame()
    if asset_id.ne("").any():
        dup_mask = asset_id.duplicated(keep=False)
        dup = df.loc[dup_mask].copy()
        dup.insert(0, "dup_key", asset_id.loc[dup_mask].values)
        dup = dup.sort_values(by=["dup_key"])
    else:
        # fallback: ISIN duplicates, else YahooSymbol
        key = None
        if isin.ne("").any():
            key = isin
        elif yahoo.ne("").any():
            key = yahoo
        if key is not None:
            dup_mask = key.duplicated(keep=False)
            dup = df.loc[dup_mask].copy()
            dup.insert(0, "dup_key", key.loc[dup_mask].values)
            dup = dup.sort_values(by=["dup_key"])

    # ticker looks like ISIN
    t_isin = ticker.str.upper().str.match(ISIN_RE, na=False)
    ticker_isin_rows = df.loc[t_isin].copy()

    # missing yahoo symbol (mostly hurts fetching)
    miss_yahoo = (~is_crypto) & yahoo.eq("") & (symbol.ne("") | isin.ne("") | ticker.ne(""))
    missing_yahoo_rows = df.loc[miss_yahoo].copy()

    # suggested cleaned watchlist (non-destructive)
    suggested = pd.DataFrame(
        {
            "Ticker": ticker.where(~ticker.eq(""), symbol).where(lambda s: ~s.eq(""), yahoo),
            "Name": name,
            "YahooSymbol": yahoo,
            "ISIN": isin.where(isin.ne(""), ticker.where(ticker.str.upper().str.match(ISIN_RE, na=False), "")),
        }
    )
    # If ticker is an ISIN and we have a non-ISIN symbol, use that as Ticker
    t_isin2 = suggested["Ticker"].fillna("").astype(str).str.upper().str.match(ISIN_RE, na=False)
    sym_non_isin = symbol.ne("") & ~symbol.str.upper().str.match(ISIN_RE, na=False)
    suggested.loc[t_isin2 & sym_non_isin, "Ticker"] = symbol.loc[t_isin2 & sym_non_isin]
    # Drop exact duplicates (this is only a suggestion file)
    suggested = suggested.drop_duplicates()

    # --- write outputs ---
    if not dup.empty:
        dup.to_csv(outdir / "duplicates.csv", index=False)
    ticker_isin_rows.to_csv(outdir / "ticker_looks_like_isin.csv", index=False)
    if not missing_yahoo_rows.empty:
        missing_yahoo_rows.to_csv(outdir / "missing_yahoo_symbol.csv", index=False)
    if not crypto_mis.empty:
        crypto_mis.to_csv(outdir / "crypto_misclassified.csv", index=False)
    suggested.to_csv(outdir / "suggested_watchlist_clean.csv", index=False)

    # markdown report
    lines: list[str] = []
    lines.append("# Watchlist Doctor Report")
    lines.append("")
    lines.append(f"Source: `{csv_path}`")
    if raw_path.exists():
        lines.append(f"Raw: `{raw_path}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Rows: **{len(df)}**")
    if not dup.empty:
        lines.append(f"- Duplicates: **{len(dup)}** rows (see `duplicates.csv`)")
    else:
        lines.append("- Duplicates: **0**")
    lines.append(f"- Ticker looks like ISIN: **{int(t_isin.sum())}** rows (see `ticker_looks_like_isin.csv`)")
    lines.append(f"- Missing YahooSymbol (non-crypto): **{int(miss_yahoo.sum())}** rows (see `missing_yahoo_symbol.csv`)")
    lines.append(f"- Crypto misclassified: **{len(crypto_mis)}** rows (see `crypto_misclassified.csv`)")

    # --- crypto counts in RAW DB (helps explain base-dedup vs pair rows) ---
    if df_raw is not None and not df_raw.empty:
        raw_name = _as_series(df_raw, "Name").str.lower()
        raw_tk = _as_series(df_raw, "Ticker").str.upper()
        raw_sym = _as_series(df_raw, "Symbol").str.upper()
        raw_ys = _as_series(df_raw, "YahooSymbol").str.upper()

        # detect crypto rows without relying on manual categories
        raw_pair = pd.Series(False, index=df_raw.index)
        for q in CRYPTO_QUOTES:
            suf = f"-{q}"
            raw_pair = raw_pair | raw_tk.str.endswith(suf) | raw_sym.str.endswith(suf) | raw_ys.str.endswith(suf)
        raw_name_hit = raw_name.str.contains(CRYPTO_NAMES_RE, na=False)

        raw_crypto = raw_pair | raw_name_hit

        # parse base/quote for a quick explanation
        bases = pd.Series(pd.NA, index=df_raw.index, dtype="string")
        quotes = pd.Series(pd.NA, index=df_raw.index, dtype="string")
        up = raw_ys.where(raw_ys.ne(""), raw_tk)
        m = up.str.contains("-", na=False)
        if m.any():
            parts = up[m].str.split("-", n=1, expand=True)
            if parts.shape[1] == 2:
                bases.loc[m] = parts.iloc[:, 0].astype("string")
                quotes.loc[m] = parts.iloc[:, 1].astype("string")
        # fallback: plain base ticker (BTC/ETH/ADA/...) if name says it's crypto
        bases.loc[bases.isna() & raw_name_hit] = raw_tk.loc[bases.isna() & raw_name_hit].astype("string")

        crypto_pairs = int(raw_crypto.sum())
        crypto_bases = int(bases.loc[raw_crypto].fillna("").astype(str).replace({"": pd.NA}).nunique(dropna=True))

        lines.append("")
        lines.append("## Crypto in DB (raw)")
        lines.append("")
        lines.append(f"- Crypto rows detected in RAW: **{crypto_pairs}**")
        lines.append(f"- Unique crypto bases (RAW): **{crypto_bases}**")
        if crypto_pairs and crypto_bases and crypto_pairs != crypto_bases:
            lines.append(
                "- Hinweis: Wenn du pro Coin mehrere Quote-Paare pflegst (z.B. ADA-EUR und ADA-USD), "
                "kann die UI je nach Dedup-Strategie nur 1 Zeile pro Base zeigen."
            )
    lines.append("")
    lines.append("## Suggested next action")
    lines.append("")
    lines.append("1) Fix obvious ID issues (Ticker=ISIN but symbol exists) using `ticker_looks_like_isin.csv`.")
    lines.append("2) Fill `YahooSymbol` for missing rows (see `missing_yahoo_symbol.csv`).")
    lines.append("3) If you want to clean your DB file, start from `suggested_watchlist_clean.csv` (manual review!).")
    lines.append("")

    (outdir / "report.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"✅ Watchlist Doctor wrote: {outdir}")
    print(f"   - report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
