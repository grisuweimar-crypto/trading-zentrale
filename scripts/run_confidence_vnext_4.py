from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile

import pandas as pd

from scanner.reports.confidence_vnext import Phase4Config, run


def _apply_stable_start(history_path: Path, stable_start: str | None) -> tuple[Path, Path | None]:
    """Optionally materialize an inclusive lower-bounded audit input.

    The Phase-4 audit itself defaults to the complete archive. When a researcher
    explicitly supplies --stable-start, filtering happens before the audit so the
    reported row counts, formula epochs and diagnostics genuinely use that window.
    """
    if stable_start is None:
        return history_path, None

    cutoff = pd.to_datetime(stable_start, errors="coerce")
    if pd.isna(cutoff):
        raise ValueError(f"invalid --stable-start date: {stable_start!r}")
    cutoff = pd.Timestamp(cutoff).normalize()

    history = pd.read_csv(history_path, low_memory=False)
    if "date" not in history.columns:
        raise ValueError("history input has no 'date' column")
    dates = pd.to_datetime(history["date"], errors="coerce")
    filtered = history.loc[dates >= cutoff].copy()

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        prefix="confidence-vnext-stable-",
        delete=False,
        encoding="utf-8",
        newline="",
    )
    temp_path = Path(handle.name)
    try:
        filtered.to_csv(handle, index=False)
    finally:
        handle.close()
    return temp_path, temp_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 4 Confidence-vNext research audit")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_4.json")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument(
        "--stable-start",
        default=None,
        help="Optional inclusive lower bound (YYYY-MM-DD). Default: audit the complete archive.",
    )
    args = parser.parse_args()

    history_path = Path(args.history)
    audit_input, cleanup_path = _apply_stable_start(history_path, args.stable_start)
    # The core audit contract historically carried a stable_start field. Keep it
    # synchronized with the actually applied runner filter; None means full archive.
    config = Phase4Config(stable_start=args.stable_start)
    try:
        result = run(
            audit_input,
            Path(args.output),
            config,
            metadata_path=Path(args.metadata) if args.metadata else None,
        )
    finally:
        if cleanup_path is not None:
            cleanup_path.unlink(missing_ok=True)

    audit = result["history_audit"]
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "history_source": str(history_path),
                "stable_start": args.stable_start,
                "scanner_rows": audit["scanner_rows_after_same_day_symbol_dedup"],
                "scanner_dates": audit["scanner_dates"],
                "scanner_symbols": audit["scanner_symbols"],
                "first_confidence": audit["aggregate_confidence"]["first_observed_non_null_date"],
                "formula_epochs": audit["aggregate_confidence"]["formula_epochs"],
                "output": args.output,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
