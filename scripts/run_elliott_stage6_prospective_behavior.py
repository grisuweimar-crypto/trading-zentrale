#!/usr/bin/env python3
from __future__ import annotations

"""Build the compact Elliott-vNext Stage-6 prospective behavior audit."""

import argparse
import json
from pathlib import Path
from typing import Iterator

from scanner.research.elliott_vnext.stage6_prospective_behavior import (
    build_stage6_prospective_behavior,
)


DEFAULT_HISTORY = "artifacts/research/elliott_vnext_prospective_history_6h.jsonl"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage6_prospective_behavior.json"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _captures(path: Path) -> Iterator[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid_capture_json_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"capture_line_must_be_object:{line_number}")
            yield value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    root = args.root.resolve()
    history_path = _resolve(root, args.history)
    output_path = _resolve(root, args.output)
    if not history_path.exists():
        raise FileNotFoundError(f"prospective_capture_history_missing:{history_path}")

    report = build_stage6_prospective_behavior(_captures(history_path))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            report,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )

    print(json.dumps({
        "technical_stage_status": report["technical_stage_status"],
        "prospective_observation_status": report["prospective_observation_status"],
        "empirical_promotion_status": report["empirical_promotion_status"],
        "source": report["source"],
        "coverage": report["coverage"],
        "scenario_stability": report["scenario_stability"],
        "wave_stage_stability": report["wave_stage_stability"],
        "review_contexts": report["review_contexts"],
        "output": str(output_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
