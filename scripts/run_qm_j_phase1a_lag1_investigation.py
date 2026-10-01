from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_j_phase1a_lag1_investigation import run_files


def main() -> int:
    parser = argparse.ArgumentParser(description="QM-J Lag-1 post-trigger investigation")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--output", default="artifacts/research/qm/qm_j_phase1a_lag1_investigation.json")
    args = parser.parse_args()
    result = run_files(args.history, args.prices)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "finding_id": result["finding_id"],
        "matched_observations": result["coverage"]["matched_observations"],
        "global_current_lag_spearman": result["score_persistence"]["global_current_lag_spearman"],
        "exact_same_score_rate": result["score_persistence"]["exact_same_score_rate"],
        "current_mean_daily_spearman": result["predictive_comparison"]["current_mean_daily_spearman"],
        "lag_mean_daily_spearman": result["predictive_comparison"]["lag_mean_daily_spearman"],
        "score_change_mean_daily_spearman": result["predictive_comparison"]["score_change_mean_daily_spearman"],
        "rank_change_mean_daily_spearman": result["predictive_comparison"]["rank_change_mean_daily_spearman"],
        "root_cause_assigned_by_code": result["root_cause_assigned_by_code"],
        "output": str(output),
    }, indent=2, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
