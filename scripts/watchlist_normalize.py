#!/usr/bin/env python
"""Normalize the input watchlist DB file (watchlist.csv).

Goal: help you clean legacy/duplicated columns without breaking your workflow.
This script DOES NOT overwrite your DB by default.

Outputs:
- artifacts/reports/watchlist_normalize/columns_audit.md
- artifacts/reports/watchlist_normalize/watchlist_normalized.csv

Optional:
- --write will write artifacts/watchlist/watchlist.normalized.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re

import pandas as pd

from scanner.data.schema.canonical import canonicalize_df


ROOT = Path(__file__).resolve().parents[1]


def _find_input_watchlist() -> Path | None:
    a = ROOT / "artifacts" / "watchlist" / "watchlist.csv"
    b = ROOT / "data" / "inputs" / "watchlist.csv"
    if a.exists():
        return a
    if b.exists():
        return b
    return None


def _norm_name(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


def audit_columns(df: pd.DataFrame) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Return (duplicate_groups, empty_columns)."""
    cols = list(df.columns)
    groups: dict[str, list[str]] = {}
    for c in cols:
        base = _norm_name(c)
        # treat pandas duplicate suffixes '.1' '.2' as same base
        base = re.sub(r"\.[0-9]+$", "", base)
        groups.setdefault(base, []).append(c)

    dups = [(k, v) for k, v in groups.items() if len(v) > 1]
    dups.sort(key=lambda x: (-len(x[1]), x[0]))

    empty = []
    for c in cols:
        s = df[c]
        if isinstance(s, pd.DataFrame):
            if s.shape[1] == 0:
                empty.append(c)
                continue
            s = s.iloc[:, 0]
        s = s.astype("string").fillna("").str.strip()
        if (s == "").all():
            empty.append(c)

    return dups, empty


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="Write a normalized CSV next to artifacts/watchlist (no overwrite).")

    args = ap.parse_args()

    src = _find_input_watchlist()
    if not src:
        print("❌ Keine watchlist.csv gefunden. Lege eine ab unter artifacts/watchlist/watchlist.csv oder data/inputs/watchlist.csv")
        return 2

    df = pd.read_csv(src, dtype=str)

    out_dir = ROOT / "artifacts" / "reports" / "watchlist_normalize"
    out_dir.mkdir(parents=True, exist_ok=True)

    dups, empty = audit_columns(df)

    # Canonicalize first (this resolves best columns even with duplicates like Sector.1)
    can = canonicalize_df(df)

    keep = [
        "ticker", "name", "isin", "symbol", "yahoo_symbol",
        "category", "sector", "industry", "country", "currency",
    ]
    cols = [c for c in keep if c in can.columns]
    norm = can.loc[:, cols].copy()

    # strip whitespace in key string columns
    for c in cols:
        if norm[c].dtype == object:
            norm[c] = norm[c].astype("string").fillna("").str.strip()

    # write outputs
    norm_path = out_dir / "watchlist_normalized.csv"
    norm.to_csv(norm_path, index=False)

    # markdown audit
    md = []
    md.append(f"# Watchlist Normalize – Columns Audit\n\nSource: `{src}`\n")
    md.append(f"\n## Summary\n\n- Columns: **{len(df.columns)}**\n- Duplicate groups: **{len(dups)}**\n- Empty columns: **{len(empty)}**\n")
    if dups:
        md.append("\n## Duplicate column groups\n\n(These often come from legacy columns or multiple project phases.)\n")
        for k, v in dups[:40]:
            md.append(f"- `{k}` → {', '.join(f'`{x}`' for x in v)}")
        if len(dups) > 40:
            md.append(f"\n… and {len(dups)-40} more groups.")
    if empty:
        md.append("\n## Empty columns\n\n(These columns are completely empty and can usually be removed.)\n")
        for c in empty[:80]:
            md.append(f"- `{c}`")
        if len(empty) > 80:
            md.append(f"\n… and {len(empty)-80} more empty columns.")
    md.append("\n## Normalized output\n")
    md.append(f"`{norm_path}` (manual review).\n")
    md_path = out_dir / "columns_audit.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")

    if args.write:
        dst = ROOT / "artifacts" / "watchlist" / "watchlist.normalized.csv"
        dst.parent.mkdir(parents=True, exist_ok=True)
        norm.to_csv(dst, index=False)
        print(f"✅ Wrote: {dst}")
    print(f"✅ Wrote: {norm_path}")
    print(f"✅ Wrote: {md_path}")
    print("Hinweis: Dieses Script überschreibt NICHT automatisch deine DB. Wenn du willst, übernimm die normalized CSV manuell.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
