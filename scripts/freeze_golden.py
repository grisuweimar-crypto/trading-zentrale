"""Freeze a small set of "golden" rows for regression testing.

Why this exists
---------------
Your pipeline can be technically correct but still silently regress (e.g. score column
missing, all zeros, bad joins, broken canonical mapping). A small, stable set of
representative assets gives you a cheap early-warning test.

This script *creates* tests/golden_rows.json from your latest run output.

Usage (PowerShell)
------------------
  python -m scanner.app.run_daily
  python scripts/freeze_golden.py

Optional:
  python scripts/freeze_golden.py --top-stocks 5 --crypto 2

Notes
-----
- Reads:  artifacts/watchlist/watchlist_full.csv (default)
- Writes: tests/golden_rows.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _best_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _is_crypto(df: pd.DataFrame) -> pd.Series:
    if "is_crypto" in df.columns:
        return df["is_crypto"].fillna(False).astype(bool)
    if "IsCrypto" in df.columns:
        return df["IsCrypto"].fillna(False).astype(bool)
    for col in ("ScoreAssetClass", "asset_class"):
        if col in df.columns:
            s = df[col]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0] if s.shape[1] else pd.Series("", index=df.index)
            return s.fillna("").astype(str).str.lower().eq("crypto")
    ys = df[_best_col(df, ["YahooSymbol", "Yahoo", "yahoo_symbol"]) ] if _best_col(df, ["YahooSymbol", "Yahoo", "yahoo_symbol"]) else pd.Series("", index=df.index)
    tk = df[_best_col(df, ["Ticker", "ticker", "Symbol", "symbol"]) ] if _best_col(df, ["Ticker", "ticker", "Symbol", "symbol"]) else pd.Series("", index=df.index)
    ys = ys.fillna("").astype(str)
    tk = tk.fillna("").astype(str)
    return ys.str.upper().str.endswith("-USD") | tk.str.upper().str.endswith("-USD")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_full.csv")
    ap.add_argument("--out", default=r"tests/golden_rows.json")
    ap.add_argument("--top-stocks", type=int, default=5, help="How many top scored non-crypto rows to freeze")
    ap.add_argument("--crypto", type=int, default=2, help="How many crypto rows to include (if present)")
    ap.add_argument("--min-score", type=float, default=0.0001, help="Minimum score for a row to be considered 'scored'")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ Missing CSV: {csv_path}")
        return 2

    df = pd.read_csv(csv_path)
    if "score" not in df.columns:
        print("❌ CSV has no 'score' column")
        return 2

    score = pd.to_numeric(df["score"], errors="coerce")
    crypto = _is_crypto(df)

    # Prefer YahooSymbol, fallback to Ticker
    id_col = _best_col(df, ["YahooSymbol", "Yahoo", "yahoo_symbol", "Ticker", "ticker", "Symbol", "symbol"])
    if not id_col:
        print("❌ Could not find an identifier column (YahooSymbol/Ticker/Symbol)")
        return 2

    # Top non-crypto scored
    stocks = df.loc[(score >= args.min_score) & (~crypto)].copy()
    stocks["_score"] = score
    stocks = stocks.sort_values("_score", ascending=False).head(max(0, args.top_stocks))

    # Crypto sample (may include zeros; that's a good sentinel)
    cryptos = df.loc[crypto].copy()
    cryptos["_score"] = score
    if not cryptos.empty:
        # take lowest scores first to keep the "bear clamp" sentinel, then fill with highs
        cryptos = pd.concat([
            cryptos.sort_values("_score", ascending=True).head(max(0, args.crypto)),
            cryptos.sort_values("_score", ascending=False).head(max(0, args.crypto)),
        ]).drop_duplicates(subset=[id_col]).head(max(0, args.crypto))

    rows: list[dict] = []

    def add_rows(frame: pd.DataFrame, allow_zero_default: bool):
        nonlocal rows
        for _, r in frame.iterrows():
            ident = str(r.get(id_col, "") or "").strip()
            if not ident:
                continue
            sc = float(pd.to_numeric(r.get("score", 0), errors="coerce") or 0)
            allow_zero = allow_zero_default
            score_min = 0.0 if allow_zero else args.min_score
            rows.append(
                {
                    "id": ident,
                    "id_col": id_col,
                    "allow_zero": bool(allow_zero),
                    "expect": {"score_min": float(score_min)},
                    "snapshot": {
                        "score": sc,
                        "trend_ok": bool(r.get("trend_ok")) if "trend_ok" in df.columns else None,
                        "liquidity_ok": bool(r.get("liquidity_ok")) if "liquidity_ok" in df.columns else None,
                        "cycle": float(r.get("cycle")) if "cycle" in df.columns and pd.notna(r.get("cycle")) else None,
                    },
                }
            )

    add_rows(stocks, allow_zero_default=False)
    add_rows(cryptos, allow_zero_default=True)

    # Python 3.14 deprecates utcnow(); use timezone-aware UTC timestamps.
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    out = {
        "generated_at": ts,
        "source_csv": str(csv_path).replace("\\", "/"),
        "rows": rows,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"✅ Wrote {len(rows)} golden rows -> {out_path}")
    if rows:
        print("IDs:")
        for r in rows:
            print(" -", r["id"])
    else:
        print("⚠️ No rows were written (do you have any scored rows?)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
