#!/usr/bin/env python3
from __future__ import annotations

"""Build the compact Stage-4 Elliott prospective observation report."""

import argparse
import json
from pathlib import Path

from scanner.research.elliott_vnext.prospective_observation import (
    build_prospective_observation,
    load_capture_archive,
)


DEFAULT_ARCHIVE = "artifacts/research/elliott_vnext_prospective_history_6h.jsonl"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_prospective_observation.json"


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    root = args.root.resolve()
    archive_path = _resolve(root, args.archive)
    output_path = _resolve(root, args.output)

    captures = load_capture_archive(archive_path)
    report = build_prospective_observation(captures)
    _atomic_json(output_path, report)

    print(
        json.dumps(
            {
                "schema_version": report["schema_version"],
                "phase": report["phase"],
                "status": report["status"],
                "effective_capture_count": report["effective_capture_count"],
                "current_snapshot_id": report["current_snapshot_id"],
                "comparable_dimension_count": report["comparable_dimension_count"],
                "latest_capture_comparison_count": report["latest_capture_comparison_count"],
                "output": str(output_path),
                "empirical_conclusion_allowed": report["guards"]["empirical_conclusion_allowed"],
                "productive_promotion_performed": report["guards"]["productive_promotion_performed"],
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
