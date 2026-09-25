from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scanner.research.elliott_vnext.market_context import (
    HISTORY_COLUMNS,
    build_market_context_history,
    normalize_context_assignments,
    normalize_context_registry,
    summarize_market_context,
)

ROOT = Path(__file__).resolve().parents[1]


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build/validate Module 6F market-context research artifacts")
    parser.add_argument(
        "--registry",
        type=Path,
        default=ROOT / "data" / "inputs" / "market_context_registry.csv",
    )
    parser.add_argument(
        "--assignments",
        type=Path,
        default=ROOT / "data" / "inputs" / "market_context_assignments.csv",
    )
    parser.add_argument(
        "--raw-prices",
        type=Path,
        default=ROOT / "artifacts" / "research" / "market_context_history.csv",
        help="Observed context OHLC(V) rows. Existing validated artifact is the no-op default.",
    )
    parser.add_argument(
        "--history-output",
        type=Path,
        default=ROOT / "artifacts" / "research" / "market_context_history.csv",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=ROOT / "artifacts" / "research" / "elliott_market_context_6f.json",
    )
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args()

    registry = normalize_context_registry(_read_csv(args.registry))
    assignments = normalize_context_assignments(_read_csv(args.assignments), registry)
    raw = _read_csv(args.raw_prices)
    history = build_market_context_history(raw, registry, as_of=args.as_of)

    args.history_output.parent.mkdir(parents=True, exist_ok=True)
    if history.empty:
        pd.DataFrame(columns=HISTORY_COLUMNS).to_csv(args.history_output, index=False)
    else:
        serial = history.copy()
        for col in ("date", "valid_from", "valid_to"):
            serial[col] = pd.to_datetime(serial[col], errors="coerce").dt.strftime("%Y-%m-%d")
        serial.to_csv(args.history_output, index=False)

    summary = summarize_market_context(history, registry, assignments)
    summary.update(
        {
            "module": "6F_external_market_sector_context",
            "history_artifact": str(args.history_output.relative_to(ROOT)),
            "registry": str(args.registry.relative_to(ROOT)),
            "assignment_registry": str(args.assignments.relative_to(ROOT)),
            "as_of": args.as_of,
            "status": "coverage_only_no_performance_validation",
        }
    )
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
