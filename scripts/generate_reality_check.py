"""Generate Reality Check report (data integrity / mapping sanity).

Outputs:
  - artifacts/reports/reality_check.json
  - artifacts/reports/reality_check.csv
"""

from __future__ import annotations

import argparse
import json
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
    reports_dir = artifacts_dir() / "reports"
    history_delta_path = reports_dir / "history_delta.json"
    segment_monitor_path = reports_dir / "segment_monitor.json"
    history_delta = (
        json.loads(history_delta_path.read_text(encoding="utf-8"))
        if history_delta_path.exists()
        else {}
    )
    segment_monitor = (
        json.loads(segment_monitor_path.read_text(encoding="utf-8"))
        if segment_monitor_path.exists()
        else {}
    )
    df_out, payload = build_reality_check(
        df_full,
        history_delta=history_delta,
        segment_monitor=segment_monitor,
    )
    out = write_reality_check_outputs(df_out, payload)

    print("✅ Reality Check outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
