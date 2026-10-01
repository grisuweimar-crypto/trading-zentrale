from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_j_phase1a_selection import run_files


def main() -> int:
    parser = argparse.ArgumentParser(description="QM-J Phase-1A Selection permutation falsification")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--config", default="configs/qm_j_phase1a_selection_placebo_v1.json")
    parser.add_argument("--output", default="artifacts/research/qm/qm_j_phase1a_selection_falsification.json")
    args = parser.parse_args()

    result = run_files(args.history, args.prices, config_path=args.config)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    evaluation = result["falsification_evaluation"]
    print(json.dumps({
        "application": result["application"],
        "coverage": result["coverage"],
        "real_metric": result["real_metric"],
        "placebo_distribution": result["placebo_distribution"],
        "status": evaluation["status"],
        "promotion_blocked_by_qm_j": evaluation["promotion_blocked_by_qm_j"],
        "capa_required": evaluation["capa_required"],
        "output": str(output),
    }, indent=2, ensure_ascii=False, allow_nan=False))
    # A scientific falsification trigger is a valid test result, not CI failure.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
