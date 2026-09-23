from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scanner.reports.selection_timing import _scanner_rows


def _summary(series: pd.Series) -> dict[str, float | int | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"N": 0, "p10": None, "p20": None, "median": None, "p80": None, "p90": None}
    q = values.quantile([0.10, 0.20, 0.50, 0.80, 0.90])
    return {
        "N": int(len(values)),
        "p10": float(q.loc[0.10]),
        "p20": float(q.loc[0.20]),
        "median": float(q.loc[0.50]),
        "p80": float(q.loc[0.80]),
        "p90": float(q.loc[0.90]),
    }


def _safe_ratio(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return float(a / b)


def analyze(history: pd.DataFrame, latest: pd.DataFrame, recent_start: str = "2026-09-17") -> dict:
    hist = _scanner_rows(history)
    hist = hist.loc[~hist["is_crypto"]].copy()
    current = _scanner_rows(latest)
    current = current.loc[~current["is_crypto"]].copy()

    latest_date = pd.to_datetime(current["date"], errors="coerce").max()
    current = current.loc[pd.to_datetime(current["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())].copy()
    split = pd.Timestamp(recent_start)

    result = {
        "phase": "4_risk_scale_diagnostic",
        "research_only": True,
        "latest_date": None if pd.isna(latest_date) else latest_date.date().isoformat(),
        "recent_start": recent_start,
        "stocks_only": True,
        "features": {},
    }

    for feature in ("volatility", "drawdown"):
        if feature not in hist.columns or feature not in current.columns:
            result["features"][feature] = {"status": "missing"}
            continue
        hist_feature = hist[["date", "symbol", feature]].copy()
        hist_feature[feature] = pd.to_numeric(hist_feature[feature], errors="coerce")
        pre = hist_feature.loc[hist_feature["date"] < split]
        recent = hist_feature.loc[hist_feature["date"] >= split]
        cur = current[["symbol", feature]].copy()
        cur[feature] = pd.to_numeric(cur[feature], errors="coerce")

        pre_summary = _summary(pre[feature])
        recent_summary = _summary(recent[feature])
        current_summary = _summary(cur[feature])
        daily = (
            hist_feature.dropna(subset=[feature])
            .groupby("date")[feature]
            .agg(["count", "median"])
            .tail(20)
            .reset_index()
        )
        result["features"][feature] = {
            "status": "available",
            "pre_recent": pre_summary,
            "recent": recent_summary,
            "current": current_summary,
            "recent_to_pre_median_ratio": _safe_ratio(recent_summary["median"], pre_summary["median"]),
            "current_to_pre_median_ratio": _safe_ratio(current_summary["median"], pre_summary["median"]),
            "last_20_daily_medians": [
                {
                    "date": pd.Timestamp(row.date).date().isoformat(),
                    "N": int(row.count),
                    "median": float(row.median),
                }
                for row in daily.itertuples(index=False)
            ],
        }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Phase 4 current-vs-historical risk metric scales")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--latest", default="artifacts/research/latest_scanner.csv")
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_risk_scale_4.json")
    parser.add_argument("--recent-start", default="2026-09-17")
    args = parser.parse_args()

    report = analyze(
        pd.read_csv(args.history, low_memory=False),
        pd.read_csv(args.latest, low_memory=False),
        args.recent_start,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
