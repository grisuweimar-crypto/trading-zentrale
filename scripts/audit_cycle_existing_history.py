"""Read-only source/PIT eligibility inventory of *actual* scanner history files.

Crucial: numerical historic Cycle values, price_backfill closes and current
Yahoo adjusted bars are never automatically original observation-time evidence.
The separate CY-03 ledger+mask is the ONLY admissible Cycle source, and its
current externally-unverified v1 remains research-blocked by contract.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import re


LEGACY_FILES = (
    "artifacts/research/history_analysis.csv",
    "artifacts/research/history_recent.csv",
    "artifacts/snapshots/score_history.csv",
    "artifacts/research/price_backfill.csv",
)
CYCLE_ROOT = "artifacts/cycle_history"
SHA = re.compile(r"^[a-f0-9]{64}$")


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def _first(row: dict[str, str], names: tuple[str, ...]) -> str:
    for key in names:
        value = str(row.get(key) or "").strip()
        if value and value.lower() not in {"nan", "null", "none", "na", "n/a"}:
            return value
    return ""


def _number(value: str) -> float | None:
    if not value:
        return None
    try:
        x = float(value)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def audit_file(root: Path, relative: str) -> dict:
    path = root / relative
    if not path.is_file() or path.is_symlink():
        raise ValueError("history_audit:missing_or_symlink:" + relative)
    digest = _sha_file(path)
    qualities: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    rows = 0
    value_count = zeros = fifties = unverified_numbers = 0
    candidate_provenance = 0
    timestamp_candidates = 0
    candidate_by_date: Counter[str] = Counter()
    candidate_snapshots: set[str] = set()
    dates: set[str] = set()
    assets: set[str] = set()
    snapshot_ids: set[str] = set()
    invalid_cycle_range = 0
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = list(reader.fieldnames or [])
        if not columns:
            raise ValueError("history_audit:missing_csv_header:" + relative)
        cycle_column = "cycle" if "cycle" in columns else ("Zyklus %" if "Zyklus %" in columns else "")
        for row in reader:
            rows += 1
            raw_cycle = str(row.get(cycle_column) or "").strip() if cycle_column else ""
            value = _number(raw_cycle)
            if value is not None:
                value_count += 1
                zeros += value == 0
                fifties += value == 50
                invalid_cycle_range += not 0 <= value <= 100
            quality = _first(row, ("cycle_quality",))
            source = _first(row, ("cycle_source",))
            qualities[quality or "UNRECORDED"] += 1
            sources[source or "UNRECORDED"] += 1
            if value is not None and (quality != "VALID" or source != "YAHOO_PIT_CYCLE_V1"):
                unverified_numbers += 1
            when = _first(row, ("as_of", "date", "Date"))
            if len(when) >= 10:
                dates.add(when[:10])
            asset = _first(row, ("asset_id", "symbol", "YahooSymbol", "ticker"))
            if asset:
                assets.add(asset)
            sid = _first(row, ("snapshot_id",))
            if sid:
                snapshot_ids.add(sid)
            stamp = _first(row, ("price_retrieved_at", "retrieved_at", "cycle_as_of"))
            if stamp:
                timestamp_candidates += 1
            if (value is not None and 0 <= value <= 100
                and quality == "VALID" and source == "YAHOO_PIT_CYCLE_V1"
                and _first(row, ("cycle_formula_version",)) == "cycle_detrended_sma20_range40_v1"
                and SHA.fullmatch(_first(row, ("cycle_price_sha256",)))
                and sid and when and asset):
                candidate_provenance += 1
                candidate_by_date[when[:10]] += 1
                candidate_snapshots.add(sid)
    return {
        "path": relative, "sha256": digest, "bytes": path.stat().st_size,
        "rows": rows, "columns": columns,
        "unique_dates": len(dates), "first_date": min(dates) if dates else None,
        "last_date": max(dates) if dates else None,
        "assets_with_identity": len(assets), "distinct_snapshot_ids": len(snapshot_ids),
        "cycle_numeric": value_count, "cycle_zero": zeros, "cycle_fifty": fifties,
        "cycle_nonfinite_or_out_of_range": invalid_cycle_range,
        "numeric_without_complete_verified_claim": unverified_numbers,
        "formula_and_row_sha_candidates_NOT_RELEASED": candidate_provenance,
        "candidate_dates_NOT_RELEASED": dict(sorted(candidate_by_date.items())),
        "candidate_snapshot_ids_NOT_RELEASED": len(candidate_snapshots),
        "time_fields_present_NOT_CERTIFIED": timestamp_candidates,
        "cycle_qualities": dict(sorted(qualities.items())),
        "cycle_sources": dict(sorted(sources.items())),
    }


def audit_cy03(root: Path) -> dict:
    folder = root / CYCLE_ROOT
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    required = {"observations.csv", "eligibility.csv", "coverage.csv"}
    if set(manifest.get("file_sha256", {})) != required:
        raise ValueError("history_audit:unexpected_cycle_manifest_files")
    for name in required:
        if _sha_file(folder / name) != manifest["file_sha256"][name]:
            raise ValueError("history_audit:cycle_manifest_sha_mismatch:" + name)
    mask = Counter()
    statuses = Counter()
    assets = set()
    rows = 0
    with (folder / "eligibility.csv").open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            rows += 1
            assets.add(row["asset_id"])
            statuses[row["research_status"]] += 1
            for lag in (1, 5, 10):
                mask[f"lag_{lag}obs:{row[f'lag_{lag}obs']}"] += 1
    if rows != manifest["observations"]:
        raise ValueError("history_audit:eligibility_row_count_mismatch")
    # This v1 adapter has no authorized external source release. Never
    # reinterpret a manually modified manifest or a syntactically-valid row.
    if (manifest["schema_version"] != "cycle_observations_cy03_v1"
        or manifest["research_gate"] != "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269"
        or manifest["research_eligible"] != 0
        or statuses != Counter({"BLOCKED_EXTERNAL_VERIFICATION_269": rows})):
        raise ValueError("history_audit:unrecognized_cy03_release_authority")
    return {
        "schema": manifest["schema_version"],
        "snapshot_count": manifest["snapshots"],
        "rows": rows, "assets": len(assets),
        "latest_snapshot_id": manifest["latest_snapshot_id"],
        "latest_as_of": manifest["as_of"],
        "quality_valid_current": manifest["new_current_valid"],
        "quality_excluded_current": manifest["new_current_excluded"],
        "lags": dict(sorted(mask.items())),
        "research_status": dict(statuses),
        "research_eligible": 0, "release": "BLOCKED_EXTERNAL_VERIFICATION_269",
        "manifest_verified_sha256": True,
    }


def audit(root: Path) -> dict:
    reports = [audit_file(root, name) for name in LEGACY_FILES]
    cy03 = audit_cy03(root)
    return {
        "schema_version": "cycle_dir_pit_history_eligibility_audit_v1",
        "files": reports,
        "cy03": cy03,
        "global_research_cycle_eligible": 0,
        "positive_alpha_or_trading_validated": False,
        "findings": [
            "Historical numeric Cycle or price rows are not independent bar-publisher PIT certification.",
            "Legacy numeric 0 and 50 may be real or imputed; without matching original-source proof they remain UNVERIFIED.",
            "price_backfill contains historical prices but does not certify Yahoo historical first-publication time or original asset-listing line.",
            "Only a separately externally released CY03 schema plus chain-quality and real forward outcomes can permit CY06 discovery.",
        ],
        "gate_status": "BLOCKED_NO_EXTERNALLY_RELEASED_CYCLE_HISTORY",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.root)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
