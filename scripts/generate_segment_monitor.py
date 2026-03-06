"""Generate Segment Monitor report (pillar/cluster/bucket distributions + changes).

Outputs:
  - artifacts/reports/segment_monitor.json
  - artifacts/reports/segment_monitor.csv

Snapshot store:
  - artifacts/snapshots/segment_history.csv (upsert by date+symbol)
"""

from __future__ import annotations

import argparse
import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.reports.segment_monitor import (
    resolve_segment_history_path,
    build_segment_snapshot,
    upsert_segment_snapshot,
    compute_segment_monitor,
    write_segment_monitor_outputs,
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
    snap = build_segment_snapshot(df_full, date=args.date)

    hist_path = resolve_segment_history_path()
    hist = upsert_segment_snapshot(hist_path, snap)

    df_out, payload = compute_segment_monitor(hist, snap)
    out = write_segment_monitor_outputs(df_out, payload)

    print("✅ Segment Monitor outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
