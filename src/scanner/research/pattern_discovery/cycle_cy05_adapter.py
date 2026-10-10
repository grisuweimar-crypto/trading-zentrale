"""Read-only CY-03 -> CY-05 Cycle research input adapter.

This adapter is intentionally fail-closed: CY-03 v1 provides internally
replayable but externally unverified provisional observations. Hash-verified
bytes are *not* independent proof of source PIT/currency/venue. A future,
separately reviewed schema/release contract must authorize research use.

No trading signals, forward outcomes, backfill, or archive rewrites.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
import json
import math
from pathlib import Path
from typing import Any, Mapping

from scanner.reports.cycle_history import (
    COVERAGE, MASK, ROOT, VERSION, coverage_rows, csv_bytes,
    lag_mask, read_ledger, verify_existing_history,
)


class CycleCY05ArchiveError(ValueError):
    """Immutable archive, source-identity or research-release gate failed."""


def _manifest_read(path: Path) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise CycleCY05ArchiveError("cy05:cycle_manifest_missing_or_symlink")
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CycleCY05ArchiveError("cy05:cycle_manifest_unreadable") from exc
    if not isinstance(result, dict) or result.get("schema_version") != VERSION:
        raise CycleCY05ArchiveError("cy05:cycle_archive_schema_not_registered")
    return result


def inspect_cycle_archive(repo_root: str | Path) -> dict[str, Any]:
    """Validate and summarize CY-03 v1 *without* making its data researchable.

    The verification matches recorded immutable file bytes to the manifest
    and independently recomputes all existing v1 masks/coverage; it cannot
    independently certify the upstream market-data provider.
    """
    folder = Path(repo_root) / ROOT
    ledger_path = folder / "observations.csv"
    if ledger_path.is_symlink() or not ledger_path.is_file():
        raise CycleCY05ArchiveError("cy05:observations_missing_or_symlink")
    manifest = _manifest_read(folder / "manifest.json")
    try:
        ledger = read_ledger(ledger_path)
        verify_existing_history(folder, ledger)
        computed_mask = lag_mask(ledger)
        if (folder / "eligibility.csv").read_bytes() != csv_bytes(MASK, computed_mask):
            raise CycleCY05ArchiveError("cy05:eligibility_not_reproducible")
        if (folder / "coverage.csv").read_bytes() != csv_bytes(
            COVERAGE, coverage_rows(computed_mask)
        ):
            raise CycleCY05ArchiveError("cy05:coverage_not_reproducible")
    except (ValueError, KeyError, TypeError, OSError) as exc:
        if isinstance(exc, CycleCY05ArchiveError):
            raise
        raise CycleCY05ArchiveError("cy05:cycle_archive_integrity_failed") from exc

    if not ledger:
        raise CycleCY05ArchiveError("cy05:empty_cycle_archive")

    # The CY-03 v1 status contract has *no* externally verified release mode.
    # Never accept a hand-edited manifest flag as an actual release authority.
    if (manifest.get("research_gate") != "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269"
        or manifest.get("research_eligible") != 0):
        raise CycleCY05ArchiveError("cy05:unrecognized_research_release_contract")
    if any(row["research_status"] != "BLOCKED_EXTERNAL_VERIFICATION_269"
           for row in computed_mask):
        raise CycleCY05ArchiveError("cy05:unrecognized_eligibility_status")

    newest = max(
        ledger,
        key=lambda row: (
            date.fromisoformat(row["as_of"]),
            row["generated_at"],
            row["snapshot_id"],
        ),
    )
    if (manifest.get("latest_snapshot_id") != newest["snapshot_id"]
        or manifest.get("run_id") != newest["run_id"]
        or manifest.get("as_of") != newest["as_of"]
        or manifest.get("snapshots") != len({row["snapshot_id"] for row in ledger})):
        raise CycleCY05ArchiveError("cy05:cycle_manifest_latest_identity_mismatch")

    keys = {(row["snapshot_id"], row["asset_id"]) for row in ledger}
    mask_keys = {(row["snapshot_id"], row["asset_id"]) for row in computed_mask}
    if len(keys) != len(ledger) or mask_keys != keys:
        raise CycleCY05ArchiveError("cy05:duplicate_or_unmatched_archive_asset")

    counts = {
        str(lag): dict(sorted(Counter(row[f"lag_{lag}obs"] for row in computed_mask).items()))
        for lag in (1, 5, 10)
    }
    valid = sum(row["availability"] == "PROVISIONAL_REPLAYED" for row in ledger)
    return {
        "schema_version": "cycle_cy05_archive_inspection_v1",
        "archive_version": VERSION,
        "verified_internal_archive_integrity": True,
        "independent_external_pit_verified": False,
        "research_released": False,
        "research_gate": manifest["research_gate"],
        "latest_snapshot_id": manifest["latest_snapshot_id"],
        "as_of": manifest["as_of"],
        "snapshots": manifest["snapshots"],
        "observations": len(ledger),
        "internally_replayable": valid,
        "excluded": len(ledger) - valid,
        "lag_status_counts": counts,
        "research_eligible": 0,
        "manifest_file_hashes": dict(manifest["file_sha256"]),
    }


def load_cycle_cy05_rows(
    repo_root: str | Path,
    *,
    diagnostic_only: bool = False,
) -> list[dict[str, Any]]:
    """Produce a *quarantined* diagnostic projection, never a released dataset.

    Production/research callers must leave diagnostic_only=False and will
    fail until a future independently audited CY-03 release/adapter version
    exists. Diagnostic rows retain actual blocked mask statuses and cannot
    satisfy the CY-05 v2 L2 gate.
    """
    inspection = inspect_cycle_archive(repo_root)
    if not diagnostic_only:
        raise CycleCY05ArchiveError("cy05:research_gate_not_released")
    folder = Path(repo_root) / ROOT
    ledger = read_ledger(folder / "observations.csv")
    mask = lag_mask(ledger)
    lookup = {(r["snapshot_id"], r["asset_id"]): r for r in mask}
    rows = []
    for r in ledger:
        status = lookup[r["snapshot_id"], r["asset_id"]]
        raw = r["cycle"]
        try:
            cycle = float(raw) if raw else None
        except (ValueError, TypeError) as exc:
            raise CycleCY05ArchiveError("cy05:cycle_non_numeric") from exc
        if cycle is not None and (not math.isfinite(cycle) or not 0 <= cycle <= 100):
            raise CycleCY05ArchiveError("cy05:cycle_out_of_range")
        if cycle is None and r["availability"] == "PROVISIONAL_REPLAYED":
            raise CycleCY05ArchiveError("cy05:missing_valid_cycle")
        rows.append({
            "symbol": r["asset_id"],
            "as_of": r["as_of"],
            "cycle": cycle,
            "cycle_quality": r["quality"],
            "cycle_research_status": status["research_status"],
            "cycle_history_source": "CY03_VERIFIED_LEDGER",
            "cycle_snapshot_id": r["snapshot_id"],
            "cycle_asset_id": r["asset_id"],
            "cycle_formula": r["formula"],
            "cycle_currency": r["currency"],
            "cycle_listing_symbol": r["listing_symbol"],
            "cycle_price_symbol": r["price_symbol"],
            "cycle_lag_1obs": status["lag_1obs"],
            "cycle_lag_5obs": status["lag_5obs"],
            "cycle_lag_10obs": status["lag_10obs"],
        })
    if len(rows) != inspection["observations"]:
        raise CycleCY05ArchiveError("cy05:observation_count_changed")
    return rows
