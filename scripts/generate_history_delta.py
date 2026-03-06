"""Generate History Delta report (score/rank changes).

History Delta shows the internal progression of the scanner based on local daily snapshots.
It is NOT market performance or price performance - it's scanner-internal ranking changes.

This script maintains the local snapshot store and computes deltas between the latest
two snapshots. The snapshot store is the canonical source for all History Delta data.

Outputs:
  - artifacts/reports/history_delta.json
  - artifacts/reports/history_delta.csv

Also maintains snapshot store:
  - artifacts/snapshots/score_history.csv (upsert by date+symbol)

This script is explainability-only (reads existing CSV outputs).
"""

from __future__ import annotations

import argparse

import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.reports.history_delta import (
    resolve_score_history_path,
    build_snapshot_from_watchlist,
    upsert_daily_snapshot,
    compute_history_delta,
    write_history_delta_outputs,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="artifacts/watchlist/watchlist_full.csv")
    ap.add_argument("--date", default=None, help="Override snapshot date (YYYY-MM-DD).")
    args = ap.parse_args()

    root = artifacts_dir().parent
    wl_path = root / args.watchlist
    if not wl_path.exists():
        print(f"❌ Missing watchlist CSV: {wl_path}")
        print("Run: python -m scanner.app.run_daily")
        return 2

    df_full = pd.read_csv(wl_path)
    snap = build_snapshot_from_watchlist(df_full, date=args.date)

    hist_path = resolve_score_history_path()
    hist = upsert_daily_snapshot(hist_path, snap)

    delta_df, payload = compute_history_delta(hist)
    out = write_history_delta_outputs(delta_df, payload)

    print("✅ History Delta outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
