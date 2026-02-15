#!/usr/bin/env python
from __future__ import annotations

"""Import pillar metadata from an Excel watchlist into artifacts/mapping/pillars.csv.

This script is meant to migrate *legacy / manual* categories (incl. fantasy sectors)
into a clean pillar metadata layer.

It does NOT touch scoring.

Typical usage:
  python scripts/import_pillars_from_xlsx.py --xlsx UrsprungsWatchlist.xlsx

By default it writes:
  artifacts/mapping/pillars.csv
"""

import argparse
from pathlib import Path
import re

import pandas as pd

from scanner.data.io.paths import artifacts_dir

ALLOWED_PILLARS = {"Gehirn", "Hardware", "Energie", "Fundament", "Recycling", "Playground"}

# normalized token -> pillar
PILLAR_SYNONYMS = {
    "gehirn": "Gehirn",
    "brain": "Gehirn",
    "ki": "Gehirn",
    "ai": "Gehirn",
    "software": "Gehirn",
    "hardware": "Hardware",
    "robotik": "Hardware",
    "automation": "Hardware",
    "energie": "Energie",
    "power": "Energie",
    "strom": "Energie",
    "netz": "Energie",
    "fundament": "Fundament",
    "rohstoffe": "Fundament",
    "mining": "Fundament",
    "minen": "Fundament",
    "metalle": "Fundament",
    "recycling": "Recycling",
    "urbanmining": "Recycling",
    "playground": "Playground",
    "spielplatz": "Playground",
}


def _norm_token(s: object) -> str:
    if s is None:
        return ""
    x = str(s).strip().lower()
    x = re.sub(r"[\W_]+", "", x, flags=re.UNICODE)
    return x


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    # try case-insensitive
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        c = lower.get(n.lower())
        if c is not None:
            return c
    return None


def infer_pillar_from_legacy(value: str) -> tuple[str, str, int] | None:
    """Return (pillar_primary, bucket_type, confidence) or None."""
    if not value:
        return None
    tok = _norm_token(value)
    if not tok:
        return None

    # direct match
    for p in ALLOWED_PILLARS:
        if tok == _norm_token(p):
            bt = "playground" if p == "Playground" else "pillar"
            return p, bt, 95

    # synonym scan (contains)
    for k, p in PILLAR_SYNONYMS.items():
        if k and k in tok:
            bt = "playground" if p == "Playground" else "pillar"
            return p, bt, 85

    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=str, default="UrsprungsWatchlist.xlsx", help="Excel input file")
    ap.add_argument("--sheet", type=str, default=None, help="Optional sheet name")
    ap.add_argument("--out", type=str, default=None, help="Output CSV (default: artifacts/mapping/pillars.csv)")
    args = ap.parse_args()

    xlsx = Path(args.xlsx)
    if not xlsx.exists():
        raise SystemExit(f"Excel file not found: {xlsx}")

    df = pd.read_excel(xlsx, sheet_name=args.sheet)

    col_isin = _pick_col(df, ["ISIN", "isin"])
    col_yh = _pick_col(df, ["YahooSymbol", "Yahoo", "yahoo_symbol"])
    col_tk = _pick_col(df, ["Ticker", "ticker", "Symbol"])  # fallback
    # legacy category / manual sector column
    col_legacy = _pick_col(df, ["Sektor", "Kategorie", "Category", "Cluster", "Klassifikation"])  # best-effort

    if col_legacy is None:
        raise SystemExit("No legacy category column found (expected: Sektor/Kategorie/Category/Cluster)")

    out_rows = []
    for _, r in df.iterrows():
        legacy = str(r.get(col_legacy, "") or "").strip()
        inferred = infer_pillar_from_legacy(legacy)
        if inferred is None:
            continue
        pillar_primary, bucket_type, conf = inferred
        out_rows.append(
            {
                "isin": str(r.get(col_isin, "") or "").strip() if col_isin else "",
                "yahoo_symbol": str(r.get(col_yh, "") or "").strip() if col_yh else "",
                "ticker": str(r.get(col_tk, "") or "").strip() if col_tk else "",
                "pillar_primary": pillar_primary,
                "bucket_type": bucket_type,
                "pillar_confidence": conf,
                "pillar_reason": f"Migrated from legacy category: {legacy}",
                "pillar_tags": "",
                "source": "xlsx_migration",
            }
        )

    out_df = pd.DataFrame(out_rows)
    if out_df.empty:
        print("No rows inferred. Nothing written.")
        return 0

    # prefer ISIN uniqueness, otherwise yahoo_symbol/ticker
    # keep last occurrence
    for key in ["isin", "yahoo_symbol", "ticker"]:
        if key in out_df.columns:
            mask = out_df[key].fillna("").astype(str).str.strip().ne("")
            if mask.any():
                out_df = out_df[mask].drop_duplicates(subset=[key], keep="last")
                break

    out_path = Path(args.out) if args.out else (artifacts_dir() / "mapping" / "pillars.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote: {out_path} ({len(out_df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
