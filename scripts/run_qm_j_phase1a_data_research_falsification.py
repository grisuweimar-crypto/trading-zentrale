from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_j_phase1a_data_research import run_files


def main() -> int:
    parser = argparse.ArgumentParser(
        description="QM-J Phase-1A data and research follow-on falsification"
    )
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument(
        "--config", default="configs/qm_j_phase1a_data_research_controls_v1.json"
    )
    parser.add_argument(
        "--output",
        default="artifacts/research/qm/qm_j_phase1a_data_research_falsification.json",
    )
    args = parser.parse_args()

    result = run_files(args.history, args.prices, config_path=args.config)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "application": result["application"],
                "status": result["status"],
                "data_control": {
                    "real_metric": result["data_control"]["real_metric"],
                    "control_metric": result["data_control"]["control_metric"],
                    "status": result["data_control"]["evaluation"]["status"],
                },
                "research_control": {
                    "real_metric": result["research_control"]["real_metric"],
                    "p95": result["research_control"]["control_distribution"]["p95_higher"],
                    "status": result["research_control"]["evaluation"]["status"],
                },
                "promotion_blocked_by_qm_j": result["promotion_blocked_by_qm_j"],
                "capa_required": result["capa_required"],
                "output": str(output),
            },
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
    )
    # A scientific falsification trigger is a valid result, not an execution failure.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
