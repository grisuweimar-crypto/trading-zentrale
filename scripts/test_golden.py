"""Regression test using frozen "golden" rows.

This is stricter than test_pipeline.py:
- Ensures a handful of specific assets still exist in the output
- Ensures those assets still have sane scores (not NA; not accidentally all zeros)

Workflow
--------
1) Run the pipeline:
     python -m scanner.app.run_daily

2) Freeze golden rows once (or whenever you change your universe):
     python scripts/freeze_golden.py

3) Then, on every refactor, run:
     python scripts/test_golden.py

Exit codes
----------
0 = OK
1 = FAIL (golden rows missing or assertions failed)
2 = Not configured (golden file missing/empty)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_row(df: pd.DataFrame, ident: str, id_col: str | None) -> pd.Series | None:
    candidates = []
    if id_col and id_col in df.columns:
        candidates.append(id_col)
    for c in ("YahooSymbol", "Ticker", "ticker", "Symbol", "symbol"):
        if c in df.columns and c not in candidates:
            candidates.append(c)

    for col in candidates:
        s = df[col].fillna("").astype(str)
        m = s.str.strip().eq(ident)
        if m.any():
            return df.loc[m].iloc[0]

    # last resort: case-insensitive contains
    for col in candidates:
        s = df[col].fillna("").astype(str)
        m = s.str.upper().str.strip().eq(str(ident).upper().strip())
        if m.any():
            return df.loc[m].iloc[0]

    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_full.csv")
    ap.add_argument("--golden", default=r"tests/golden_rows.json")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    golden_path = Path(args.golden)

    if not golden_path.exists():
        print(f"❌ Missing golden file: {golden_path}")
        print("Run: python scripts/freeze_golden.py")
        return 2

    data = _load_json(golden_path)
    rows = data.get("rows") or []
    if not rows:
        print(f"❌ Golden file is empty: {golden_path}")
        print("Run: python scripts/freeze_golden.py")
        return 2

    if not csv_path.exists():
        print(f"❌ Missing CSV: {csv_path}")
        return 2

    df = pd.read_csv(csv_path)
    if "score" not in df.columns:
        print("❌ CSV has no 'score' column")
        return 1

    problems: list[str] = []

    for g in rows:
        ident = str(g.get("id", "")).strip()
        id_col = g.get("id_col")
        allow_zero = bool(g.get("allow_zero", False))
        expect = g.get("expect") or {}
        score_min = float(expect.get("score_min", 0.0))

        if not ident:
            continue

        r = _find_row(df, ident, id_col)
        if r is None:
            problems.append(f"missing row: {ident}")
            continue

        score = pd.to_numeric(r.get("score"), errors="coerce")
        if pd.isna(score):
            problems.append(f"{ident}: score is NA")
            continue

        score_f = float(score)
        if (not allow_zero) and score_f <= 0:
            problems.append(f"{ident}: expected score>0, got {score_f}")
        if score_f < score_min:
            problems.append(f"{ident}: score {score_f} < min {score_min}")

    if problems:
        print("❌ FAIL")
        for p in problems:
            print(" -", p)
        return 1

    print(f"✅ OK ({len(rows)} golden rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
