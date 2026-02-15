#!/usr/bin/env python
from __future__ import annotations

"""Watchlist Migration v2 (non-destructive).

Reads your watchlist.csv "DB" and writes a consolidated v2 preview.

Target columns (v2)
- Ticker       : backward-compatible display key (we set it to Symbol when reasonable)
- Name         : display name
- ISIN         : identity/reference (optional)
- Symbol       : display key (human-friendly, e.g. NVDA, ASML, BTC)
- YahooSymbol  : fetch/link key (e.g. NVDA, ASML, BTC-USD)
- Category     : user-maintained manual bucket (optional)
- Country      : optional

Nothing is modified in-place.
Outputs:
- artifacts/reports/watchlist_migration/mapping_report.md
- artifacts/reports/watchlist_migration/watchlist_v2_preview.csv
- artifacts/watchlist/watchlist.v2.csv (only with --write)
"""

import argparse
import re
from pathlib import Path

import pandas as pd


ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def project_root() -> Path:
    # scripts/ is located at <root>/scripts
    return Path(__file__).resolve().parents[1]


def find_watchlist_csv(root: Path) -> Path:
    p1 = root / "artifacts" / "watchlist" / "watchlist.csv"
    if p1.exists():
        return p1
    p2 = root / "data" / "inputs" / "watchlist.csv"
    if p2.exists():
        return p2
    raise FileNotFoundError(f"No watchlist.csv found at: {p1} or {p2}")


def non_empty_count(s: pd.Series) -> int:
    return int((s.fillna("").astype(str).str.strip() != "").sum())


def best_duplicate_col(df: pd.DataFrame, base: str) -> str | None:
    """Pick the best column among base, base.1, base.2 ... by non-empty count."""
    esc = re.escape(base)
    cands = [c for c in df.columns if c == base or re.match(rf"^{esc}\.\d+$", c)]
    if not cands:
        return None
    return max(cands, key=lambda c: non_empty_count(df[c]))


def get_best_series(df: pd.DataFrame, base: str) -> pd.Series:
    col = best_duplicate_col(df, base)
    if col is None:
        return pd.Series([""] * len(df), index=df.index)
    return df[col]


def looks_like_isin(v: str) -> bool:
    v = (v or "").strip().upper()
    return bool(ISIN_RE.match(v))


def looks_like_yahoo_symbol(v: str) -> bool:
    v = (v or "").strip()
    if not v:
        return False
    if looks_like_isin(v):
        return False
    if " " in v:
        return False
    # allow A-Z, 0-9 and common yahoo punctuation
    return bool(re.match(r"^[A-Za-z0-9\-\.\=\^]+$", v))


def first_non_empty(*vals: str) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def derive_symbol_from_yahoo(yh: str) -> str:
    yh = (yh or "").strip()
    if not yh:
        return ""
    if looks_like_isin(yh):
        return ""
    # Crypto pairs like BTC-USD, ETH-EUR: display as base
    if "-" in yh:
        base = yh.split("-", 1)[0].strip()
        if base and looks_like_yahoo_symbol(base):
            return base
    return yh


def main() -> int:
    ap = argparse.ArgumentParser(description="Create a clean watchlist v2 preview (non-destructive).")
    ap.add_argument("--write", action="store_true", help="Also write artifacts/watchlist/watchlist.v2.csv")
    args = ap.parse_args()

    root = project_root()
    src = find_watchlist_csv(root)

    df = pd.read_csv(src, dtype=str, keep_default_na=False)

    # Pull best-of duplicate columns (legacy files often have ...,.1,.2)
    s_ticker = get_best_series(df, "Ticker")
    s_name = get_best_series(df, "Name")

    s_isin = get_best_series(df, "ISIN")
    s_symbol = get_best_series(df, "Symbol")
    s_yh = get_best_series(df, "YahooSymbol")
    s_yh_legacy = get_best_series(df, "Yahoo")

    s_cat = get_best_series(df, "Category")
    s_cat2 = get_best_series(df, "Sektor")
    s_cat3 = get_best_series(df, "Kategorie")

    s_country = get_best_series(df, "Country")

    out = pd.DataFrame(index=df.index)

    # Normalize strings
    def norm_series(s: pd.Series) -> pd.Series:
        return s.fillna("").astype(str).str.strip()

    t = norm_series(s_ticker)
    nm = norm_series(s_name)
    isin = norm_series(s_isin)
    sym = norm_series(s_symbol)
    y1 = norm_series(s_yh)
    y2 = norm_series(s_yh_legacy)
    cat = norm_series(s_cat)
    cat2 = norm_series(s_cat2)
    cat3 = norm_series(s_cat3)
    ctry = norm_series(s_country)

    # Step counters for report
    c_isin_from_other = 0
    c_yahoo_from_other = 0
    c_symbol_from_yahoo = 0
    c_ticker_replaced = 0

    # ISIN: prefer explicit, otherwise extract from any id-like field
    isin_out = isin.copy()
    for i in out.index:
        if isin_out.at[i]:
            continue
        cand = first_non_empty(
            t.at[i],
            sym.at[i],
            y1.at[i],
            y2.at[i],
        ).upper()
        if looks_like_isin(cand):
            isin_out.at[i] = cand
            c_isin_from_other += 1

    # YahooSymbol: prefer YahooSymbol, then Yahoo legacy, then Symbol/Ticker if yahoo-like
    yahoo_out = y1.copy()
    for i in out.index:
        if yahoo_out.at[i]:
            continue
        cand = first_non_empty(y2.at[i], sym.at[i], t.at[i])
        if looks_like_yahoo_symbol(cand):
            yahoo_out.at[i] = cand
            c_yahoo_from_other += 1

    # Symbol (display): prefer Symbol when it is not ISIN; otherwise derive from YahooSymbol
    symbol_out = sym.copy()
    for i in out.index:
        if symbol_out.at[i] and not looks_like_isin(symbol_out.at[i]):
            continue
        derived = derive_symbol_from_yahoo(yahoo_out.at[i])
        if derived:
            symbol_out.at[i] = derived
            c_symbol_from_yahoo += 1

    # Category: explicit Category first, then legacy German columns
    category_out = cat.copy()
    for i in out.index:
        if category_out.at[i]:
            continue
        category_out.at[i] = first_non_empty(cat2.at[i], cat3.at[i])

    # Ticker: keep backward-compat, but replace ISIN-like ticker with Symbol
    ticker_out = t.copy()
    for i in out.index:
        tv = ticker_out.at[i]
        sv = symbol_out.at[i]
        if (not tv) and sv:
            ticker_out.at[i] = sv
            c_ticker_replaced += 1
        elif looks_like_isin(tv) and sv and not looks_like_isin(sv):
            ticker_out.at[i] = sv
            c_ticker_replaced += 1

    out["Ticker"] = ticker_out
    out["Name"] = nm
    out["ISIN"] = isin_out
    out["Symbol"] = symbol_out
    out["YahooSymbol"] = yahoo_out
    out["Category"] = category_out
    if non_empty_count(ctry) > 0:
        out["Country"] = ctry

    # Write outputs
    rep_dir = root / "artifacts" / "reports" / "watchlist_migration"
    rep_dir.mkdir(parents=True, exist_ok=True)

    preview_path = rep_dir / "watchlist_v2_preview.csv"
    out.to_csv(preview_path, index=False)

    report_path = rep_dir / "mapping_report.md"
    report = []
    report.append("# Watchlist Migration v2\n")
    report.append(f"Source: `{src}`\n")
    report.append("\n## Summary\n")
    report.append(f"- Rows: **{len(out)}**\n")
    report.append(f"- ISIN derived from other columns: **{c_isin_from_other}**\n")
    report.append(f"- YahooSymbol filled from other columns: **{c_yahoo_from_other}**\n")
    report.append(f"- Symbol derived from YahooSymbol: **{c_symbol_from_yahoo}**\n")
    report.append(f"- Ticker replaced/filled from Symbol: **{c_ticker_replaced}**\n")

    # quick diagnostics
    n_isin_like_ticker = int(out["Ticker"].fillna("").astype(str).apply(looks_like_isin).sum())
    n_missing_yahoo = int((out["YahooSymbol"].fillna("").astype(str).str.strip() == "").sum())
    report.append("\n## Diagnostics\n")
    report.append(f"- Ticker still looks like ISIN: **{n_isin_like_ticker}**\n")
    report.append(f"- Missing YahooSymbol: **{n_missing_yahoo}**\n")

    report.append("\n## Files\n")
    report.append(f"- Preview CSV: `{preview_path}`\n")
    if args.write:
        report.append(f"- Written v2 DB: `artifacts/watchlist/watchlist.v2.csv`\n")
    report_path.write_text("".join(report), encoding="utf-8")

    if args.write:
        out_dir = root / "artifacts" / "watchlist"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "watchlist.v2.csv"
        out.to_csv(out_path, index=False)
        print(f"✅ wrote: {out_path}")

    print(f"✅ wrote: {preview_path}")
    print(f"✅ wrote: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
