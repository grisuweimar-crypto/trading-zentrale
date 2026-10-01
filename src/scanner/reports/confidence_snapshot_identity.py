"""Exact scanner-publication identity for guarded Phase-4 Confidence output.

This module adds provenance only. It does not alter Confidence evidence,
thresholds, model-agreement rules or applicability guards.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def stamp_current_snapshot_identity(
    result: dict,
    latest_path: str | Path,
    output_path: str | Path,
) -> None:
    """Bind a guarded Phase-4 registry to one exact scanner publication."""
    latest = pd.read_csv(Path(latest_path), dtype=str, keep_default_na=False)
    required = {"snapshot_id", "generated_at", "as_of"}
    missing = sorted(required.difference(latest.columns))
    if missing:
        raise ValueError("phase4_snapshot_identity_columns_missing:" + ",".join(missing))
    if latest.empty:
        raise ValueError("phase4_latest_scanner_empty")

    def unique(field: str) -> str:
        values = {str(value).strip() for value in latest[field] if str(value).strip()}
        if len(values) != 1:
            raise ValueError(f"phase4_{field}_not_unique")
        return next(iter(values))

    current = result.get("current")
    if not isinstance(current, dict):
        raise ValueError("phase4_current_registry_missing")
    current["snapshot_id"] = unique("snapshot_id")
    current["generated_at"] = unique("generated_at")
    scanner_as_of = unique("as_of")
    if str(current.get("as_of") or "") != scanner_as_of:
        raise ValueError("phase4_current_as_of_snapshot_mismatch")

    Path(output_path).write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
