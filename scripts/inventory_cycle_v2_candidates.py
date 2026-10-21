"""CY-03-v2 proof queue: provisional 10obs rows, never a released cohort.

Read-only with respect to scanner/CY-03 archives. Produces no CY-05 freeze,
outcomes, discovery, or research_eligible flags. Reviewer proof fields default
to UNVERIFIED and cannot be inferred from client-side provider receipts.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive
from scanner.reports.cycle_history import ROOT, lag_mask, read_ledger
from scanner.reports.cycle_prefetch_evidence import (
    ARCHIVE_DIR, inspect_archived_witnesses,
)


def build_inventory(repo_root: Path) -> dict:
    root = Path(repo_root)
    audited = inspect_cycle_archive(root)  # immutable SHA/bar/mask replay gate
    observations = read_ledger(root / ROOT / "observations.csv")
    witness_audit = inspect_archived_witnesses(root, observations)
    witness_ids = {
        p.stem for p in (root / ARCHIVE_DIR).glob("*.json")
    } if (root / ARCHIVE_DIR).is_dir() else set()
    mask = lag_mask(observations)
    eligible = {
        (r["snapshot_id"], r["asset_id"]): r
        for r in mask if r["lag_10obs"] == "PROVISIONAL_CHAIN"
    }
    queue = []
    for obs in observations:
        key = (obs["snapshot_id"], obs["asset_id"])
        if key not in eligible:
            continue
        queue.append({
            "snapshot_id": obs["snapshot_id"],
            "asset_id": obs["asset_id"],
            "as_of": obs["as_of"],
            "listing_symbol": obs["listing_symbol"],
            "price_symbol": obs["price_symbol"],
            "currency_declared": obs["currency"],
            "cycle_formula": obs["formula"],
            "price_basis": obs["price_basis"],
            "price_sha256": obs["price_sha256"],
            "archived_bars_sha256": obs["bars_sha256"],
            "current_and_10_prior_observations_provisional": True,
            "client_prefetch_witness_present": obs["snapshot_id"] in witness_ids,
            "original_quote_unit_independently_verified": False,
            "publisher_bar_pit_independently_verified": False,
            "relevant_lag_chain_proof_verified": False,
            "research_eligible": False,
            "review_status": "PENDING_INDEPENDENT_ASSET_SNAPSHOT_LAG_EVIDENCE",
        })
    queue.sort(key=lambda r: (r["as_of"], r["asset_id"], r["snapshot_id"]))
    if len(queue) != audited["lag_status_counts"]["10"].get("PROVISIONAL_CHAIN", 0):
        raise ValueError("cy03_v2:provisional_inventory_count_mismatch")
    return {
        "schema_version": "cycle_cy03_v2_pre_release_proof_queue_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_archive_version": audited["archive_version"],
        "archive_latest_snapshot_id": audited["latest_snapshot_id"],
        "archive_internal_integrity": audited["verified_internal_archive_integrity"],
        "provisional_10obs_records": len(queue),
        "snapshots_with_prefetch_client_witness": witness_audit["witness_snapshots"],
        "external_pit_certified_count": 0,
        "released_research_rows": 0,
        "cy05_frozen_pre_run_created": False,
        "research_release_performed": False,
        "issue_269": "REQUIRES_INDEPENDENT_PUBLISHER_QUOTE_SESSION_AND_PIT_PROOF",
        "issue_290": "REQUIRES_VERSIONED_EXTERNAL_REVIEWED_CY03_V2_RELEASE",
        "proof_requirements": [
            "independent_listing_and_original_quote_unit_per_asset",
            "exchange_session_calendar_and_close_timezone",
            "publisher_bar_availability_before_each_snapshot_and_relevant_lag",
            "proof_hashes_and_reviewer_UTC_record",
            "negative_missingness_and_identity_masks",
            "separate_independently_audited_v2_release_contract",
        ],
        "rows": queue,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    doc = build_inventory(args.repo_root)
    rendered = json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
