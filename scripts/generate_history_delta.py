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
  - artifacts/snapshots/score_history_recent.csv (last 12 weeks of the archive)

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
    write_recent_score_history,
    compute_history_delta,
    write_history_delta_outputs,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="artifacts/watchlist/watchlist_full.csv")
    ap.add_argument("--date", default=None, help="Override snapshot date (YYYY-MM-DD).")
    ap.add_argument("--report-only", action="store_true", help="Read validated history; do not upsert a watchlist.")
    args = ap.parse_args()

    if args.report_only:
        hist_path = resolve_score_history_path()
        hist = pd.read_csv(hist_path, dtype=str, keep_default_na=False)
        # Preserve the archive. For each date, report only its most recently
        # appended run, so same-day retries cannot mix different universes.
        if "run_id" in hist:
            selected = hist.groupby("date", sort=False)["run_id"].last()
            hist = hist[hist["run_id"].eq(hist["date"].map(selected))].copy()
        write_recent_score_history(hist_path, artifacts_dir() / "snapshots" / "score_history_recent.csv")
        delta_df, payload = compute_history_delta(hist)
        write_history_delta_outputs(delta_df, payload)
        return 0

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
    recent_path = artifacts_dir() / "snapshots" / "score_history_recent.csv"
    recent = write_recent_score_history(hist_path, recent_path)
    print(f"Recent history: {recent_path.as_posix()} ({len(recent)} rows)")

    delta_df, payload = compute_history_delta(hist)
    out = write_history_delta_outputs(delta_df, payload)

    print("✅ History Delta outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
