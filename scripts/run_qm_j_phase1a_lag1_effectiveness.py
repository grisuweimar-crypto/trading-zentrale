from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_j_phase1a_lag1_effectiveness import run_files


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate prospective-unspent Phase-1A Lag-1 CAPA effectiveness."
    )
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--config", default="configs/qm_j_phase1a_lag1_effectiveness_v1.json")
    parser.add_argument("--freeze", default="configs/qm_j_phase1a_lag1_effectiveness_freeze_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()

    result = run_files(
        args.history,
        args.prices,
        config_path=args.config,
        freeze_path=args.freeze,
    )
    text = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
