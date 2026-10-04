#!/usr/bin/env python3
from __future__ import annotations

"""Run Elliott-vNext post-activation Stage 4 historical validation."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import subprocess

import pandas as pd

from scanner.research.elliott_vnext.stage4_historical import (
    build_stage4_historical_validation,
)
from scanner.research.elliott_vnext.validation import ValidationConfig


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage4_historical_validation.json"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _git_head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
    ).strip()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional explicit symbol subset. Default: all symbols in the OHLCV source.",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    output_path = _resolve(root, args.output)
    if not prices_path.exists():
        raise FileNotFoundError(f"stage4_price_source_missing:{prices_path}")

    prices = pd.read_csv(prices_path, low_memory=False)
    config = ValidationConfig(bootstrap_reps=args.bootstrap_reps)
    result = build_stage4_historical_validation(
        prices,
        symbols=args.symbols,
        price_source_sha256=_sha256(prices_path),
        source_commit=_git_head(root),
        config=config,
    )
    _atomic_json(output_path, result)

    print(
        json.dumps(
            {
                "schema_version": result["schema_version"],
                "stage": result["stage"],
                "technical_stage_status": result["technical_stage_status"],
                "empirical_promotion_status": result["empirical_promotion_status"],
                "source": result["source"],
                "replay": result["replay"],
                "coverage": result["coverage"],
                "structure_resolution_counts": result["structure_summary"]["resolution_counts"],
                "projection_summary_rows": len(result["projection_summary"]),
                "route_summary_rows": len(result["route_summary"]),
                "promotion_status_from_6g": result["promotion_status_from_6g"],
                "output": str(output_path),
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
