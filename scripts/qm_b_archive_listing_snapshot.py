#!/usr/bin/env python3
"""Archive one already-acquired prospective listing snapshot.

Network acquisition is deliberately outside this CLI. The caller must provide the
raw file plus its actual retrieval timestamp; this prevents hidden fetch timing from
being guessed by the parser.

Durable public-repository persistence is separately gated. Public availability of a
source never implies permission to commit or redistribute its data.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_listing_persistence_gate import (  # noqa: E402
    ListingPersistenceGateError,
    require_persistence_allowed,
)
from scanner.research.governance.qm_b_prospective_listing_snapshots import (  # noqa: E402
    ProspectiveListingSnapshotError,
    archive_snapshot,
    build_snapshot_envelope,
    load_snapshot_contract,
    parse_nasdaq_symbol_directory,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B archive one prospective listing source snapshot")
    p.add_argument("--source-id", choices=["nasdaq_symbol_directory"], required=True)
    p.add_argument("--source-url", required=True)
    p.add_argument("--raw-file", required=True)
    p.add_argument("--retrieved-at", required=True, help="Timezone-aware ISO-8601 actual retrieval timestamp")
    p.add_argument("--repo-root", default=str(ROOT))
    p.add_argument("--ledger")
    p.add_argument("--http-etag")
    p.add_argument("--http-last-modified")
    p.add_argument(
        "--persistence-scope",
        choices=["local_ephemeral", "public_repository"],
        default="local_ephemeral",
        help="Public repository persistence is fail-closed unless source redistribution rights are explicitly cleared.",
    )
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        gate = require_persistence_allowed(
            args.source_id,
            persistence_scope=args.persistence_scope,
        )
        repo_root = Path(args.repo_root)
        raw_path = Path(args.raw_file)
        raw = raw_path.read_bytes()
        contract = load_snapshot_contract()
        parsed = parse_nasdaq_symbol_directory(raw, filename=raw_path.name)
        raw_root = contract["archive_layout"]["raw_root"]
        normalized_root = contract["archive_layout"]["normalized_root"]
        envelope = build_snapshot_envelope(
            source_id=args.source_id,
            source_url=args.source_url,
            retrieved_at=args.retrieved_at,
            raw_payload=raw,
            parser_result=parsed,
            raw_archive_path=f"{raw_root}/pending.bin",
            normalized_archive_path=f"{normalized_root}/pending.json",
            http_etag=args.http_etag,
            http_last_modified=args.http_last_modified,
        )
        envelope["raw_archive_path"] = f"{raw_root}/{envelope['snapshot_id']}.bin"
        envelope["normalized_archive_path"] = f"{normalized_root}/{envelope['snapshot_id']}.json"
        ledger = Path(args.ledger) if args.ledger else repo_root / contract["ledger"]["default_path"]
        result = archive_snapshot(
            ledger_path=ledger,
            raw_payload=raw,
            envelope=envelope,
            repository_root=repo_root,
        )
        print(json.dumps({
            "persistence_gate": gate,
            "envelope": {k: v for k, v in envelope.items() if k != "records"},
            "archive": result,
        }, indent=2, sort_keys=True))
        return 0
    except (OSError, KeyError, ListingPersistenceGateError, ProspectiveListingSnapshotError) as exc:
        print(f"QM-B PROSPECTIVE SNAPSHOT ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
