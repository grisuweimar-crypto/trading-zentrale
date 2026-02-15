"""Calibrate TOP thresholds from a real scored watchlist.

Why this exists
---------------
If your score scale changes (e.g. max score ~45), a hardcoded TOP min like 70 yields
an empty shortlist. This script suggests a TOP score threshold based on quantiles
(top X% of scored assets) and can optionally write it into presets.json.

Usage (PowerShell)
------------------
  python scripts/calibrate_top.py
  python scripts/calibrate_top.py --top-frac 0.10
  python scripts/calibrate_top.py --top-frac 0.10 --write

Notes
-----
- Reads:  artifacts/watchlist/watchlist_full.csv
- Writes (optional): src/scanner/presets/presets.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_full.csv")
    # NOTE: argparse uses %-formatting for help strings; a single '%' breaks it.
    ap.add_argument(
        "--top-frac",
        type=float,
        default=0.10,
        help="Top fraction, e.g. 0.10 for top 10%%",
    )
    ap.add_argument("--write", action="store_true", help="Write threshold into TOP preset")
    ap.add_argument("--preset", default=r"src/scanner/presets/presets.json")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ Missing CSV: {csv_path}")
        return 2

    df = pd.read_csv(csv_path)
    if "score" not in df.columns:
        print("❌ CSV has no 'score' column")
        return 2

    s = pd.to_numeric(df["score"], errors="coerce")
    scored = s[s > 0].dropna()
    if scored.empty:
        print("❌ No scored assets (score>0) found.")
        return 1

    top_frac = args.top_frac
    if not (0.01 <= top_frac <= 0.50):
        print("❌ --top-frac must be within [0.01, 0.50]")
        return 2

    q = 1.0 - top_frac
    thr = float(scored.quantile(q))

    print("--- Score distribution (scored only) ---")
    print(f"count: {len(scored)}")
    print(f"min/max: {float(scored.min()):.4f} / {float(scored.max()):.4f}")
    print("quantiles:", {k: float(scored.quantile(k)) for k in [0.5, 0.75, 0.9, 0.95, 0.99]})
    print()
    print(f"Suggested TOP threshold for top {top_frac:.0%}: score >= {thr:.4f}")

    # show expected row counts with common guardrails if present
    tmp = df.copy()
    tmp["score"] = s
    mask = tmp["score"] >= thr
    for flag in ("trend_ok", "liquidity_ok"):
        if flag in tmp.columns:
            mask &= tmp[flag] == True
    expected = int(mask.sum())
    print(f"Expected TOP rows with trend_ok & liquidity_ok (if present): {expected}")

    if args.write:
        preset_path = Path(args.preset)
        if not preset_path.exists():
            print(f"❌ Missing presets file: {preset_path}")
            return 2

        data = json.loads(preset_path.read_text(encoding="utf-8"))
        if "TOP" not in data:
            print("❌ presets.json has no TOP preset")
            return 2

        # ensure TOP has a score filter, otherwise add it
        filters = data["TOP"].get("filters", [])
        if not isinstance(filters, list):
            print("❌ TOP.filters is not a list")
            return 2

        found = False
        for f in filters:
            if isinstance(f, dict) and (f.get("field") == "score"):
                f["min"] = round(thr, 4)
                found = True
                break
        if not found:
            filters.insert(0, {"field": "score", "min": round(thr, 4), "on_missing": "skip"})
            data["TOP"]["filters"] = filters

        preset_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"✅ Wrote updated TOP.min into: {preset_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
