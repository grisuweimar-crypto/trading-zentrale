"""Generate Reality Check report (data integrity / mapping sanity).

Outputs:
  - artifacts/reports/reality_check.json
  - artifacts/reports/reality_check.csv
"""

from __future__ import annotations

import argparse
import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.reports.reality_check import build_reality_check, write_reality_check_outputs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="artifacts/watchlist/watchlist_full.csv")
    args = ap.parse_args()

    root = artifacts_dir().parent
    wl_path = root / args.watchlist
    if not wl_path.exists():
        print(f"❌ Missing watchlist CSV: {wl_path}")
        print("Run: python -m scanner.app.run_daily")
        return 2

    df_full = pd.read_csv(wl_path)
    df_out, payload = build_reality_check(df_full)
    out = write_reality_check_outputs(df_out, payload)

    print("✅ Reality Check outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
