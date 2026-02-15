#!/usr/bin/env python
from __future__ import annotations

"""Fetch and cache official sector/industry taxonomy from Yahoo Finance.

Writes a mapping file under:
  artifacts/mapping/yahoo_taxonomy.csv

The main pipeline *never* calls Yahoo. It only merges this cache if present.

Usage:
  python scripts/enrich_official_taxonomy.py --input artifacts/watchlist/watchlist.csv

Notes:
- Requires the `yfinance` package.
- Yahoo may throttle; use --sleep to be nice.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import time

import pandas as pd

from scanner.data.io.paths import artifacts_dir


def _norm(s: object) -> str:
    if s is None:
        return ""
    try:
        x = str(s).strip()
    except Exception:
        return ""
    if x.lower() == "nan":
        return ""
    return x


def _pick_symbol_cols(df: pd.DataFrame) -> list[str]:
    candidates = ["yahoo_symbol", "YahooSymbol", "Yahoo", "ticker", "Ticker", "symbol", "Symbol"]
    return [c for c in candidates if c in df.columns]


def _fetch_one(sym: str) -> dict[str, str]:
    try:
        import yfinance as yf
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "yfinance ist nicht installiert. Installiere es z.B. mit: pip install yfinance"
        ) from e

    t = yf.Ticker(sym)

    # yfinance evolved over time; support both get_info() and .info
    info = {}
    try:
        if hasattr(t, "get_info"):
            info = t.get_info() or {}
        else:
            info = t.info or {}
    except Exception:
        info = {}

    def g(*keys: str) -> str:
        for k in keys:
            if k in info and info[k] not in (None, ""):
                return _norm(info[k])
        return ""

    sector = g("sector")
    industry = g("industry")
    country = g("country")
    currency = g("currency")
    long_name = g("longName", "shortName")

    return {
        "yahoo_symbol": sym,
        "sector": sector,
        "industry": industry,
        "country": country,
        "currency": currency,
        "long_name": long_name,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default=str(artifacts_dir() / "watchlist" / "watchlist.csv"),
        help="Input watchlist CSV (DB)",
    )
    ap.add_argument(
        "--out",
        default=str(artifacts_dir() / "mapping" / "yahoo_taxonomy.csv"),
        help="Output mapping CSV",
    )
    ap.add_argument("--limit", type=int, default=0, help="Max. neue Symbole (0 = alle)")
    ap.add_argument("--sleep", type=float, default=0.6, help="Pause zwischen Requests (Sekunden)")
    ap.add_argument("--force", action="store_true", help="Bereits vorhandene Symbole neu fetchen")

    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        raise FileNotFoundError(f"Input nicht gefunden: {inp}")

    df = pd.read_csv(inp)
    cols = _pick_symbol_cols(df)
    if not cols:
        raise RuntimeError("Keine Symbolspalten gefunden (erwartet: YahooSymbol/ticker/Symbol …)")

    syms = []
    for c in cols:
        syms += [_norm(x) for x in df[c].tolist()]
    syms = [s for s in syms if s]

    # dedupe while preserving order
    seen = set()
    uniq = []
    for s in syms:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame()
    if out.exists():
        try:
            existing = pd.read_csv(out)
        except Exception:
            existing = pd.DataFrame()

    existing_syms = set()
    if not existing.empty and "yahoo_symbol" in existing.columns:
        existing_syms = set(existing["yahoo_symbol"].fillna("").astype(str).str.strip().tolist())

    to_fetch = uniq if args.force else [s for s in uniq if s not in existing_syms]
    if args.limit and args.limit > 0:
        to_fetch = to_fetch[: args.limit]

    print(f"Symbols total: {len(uniq)} | already cached: {len(existing_syms)} | fetching: {len(to_fetch)}")

    rows = []
    for i, sym in enumerate(to_fetch, start=1):
        try:
            row = _fetch_one(sym)
            rows.append(row)
            print(f"[{i}/{len(to_fetch)}] OK: {sym} | {row.get('sector','')} / {row.get('industry','')}")
        except Exception as e:
            print(f"[{i}/{len(to_fetch)}] FAIL: {sym} | {e}")
        time.sleep(max(0.0, args.sleep))

    new_df = pd.DataFrame(rows)
    merged = existing.copy() if not existing.empty else pd.DataFrame(columns=new_df.columns)
    if not new_df.empty:
        merged = pd.concat([merged, new_df], ignore_index=True)
        # keep last
        merged["yahoo_symbol"] = merged["yahoo_symbol"].fillna("").astype(str).str.strip()
        merged = merged.drop_duplicates(subset=["yahoo_symbol"], keep="last")

    merged.to_csv(out, index=False)
    print(f"Wrote mapping: {out} (rows={len(merged)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
