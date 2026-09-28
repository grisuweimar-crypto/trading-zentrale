#!/usr/bin/env python3
"""Bind one outcome-blind Scanner_vNext snapshot into the frozen Phase-8G split manifest."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.research_8g import (
    bind_snapshot_to_manifest,
    validate_challenger_specs,
    validate_split_manifest,
    validate_split_plan,
)


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bind a feature-side Scanner snapshot to frozen 8G factor/horizon slots without reading outcomes"
    )
    parser.add_argument("--specs", default="configs/external_evidence_8g_challenger_specs_v1.json")
    parser.add_argument("--plan", default="configs/external_evidence_8g_split_plan_v1.json")
    parser.add_argument("--manifest", default="artifacts/research/external_evidence_8g_split_manifest_v1.json")
    parser.add_argument("--snapshot-metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--macro-ledger", required=True)
    parser.add_argument("--exposure-map", default="configs/external_evidence_8f_exposure_map_v1.json")
    parser.add_argument("--output", default="artifacts/research/external_evidence_8g_split_manifest_v1.json")
    parser.add_argument("--audit-output", default="artifacts/research/external_evidence_8g_split_binding_latest.json")
    args = parser.parse_args()

    specs = _load(args.specs)
    plan = _load(args.plan)
    manifest = _load(args.manifest)
    metadata = _load(args.snapshot_metadata)
    ledger = _load(args.macro_ledger)
    exposure_map = _load(args.exposure_map)

    validate_challenger_specs(specs)
    validate_split_plan(plan, specs)
    validate_split_manifest(manifest, specs, plan)
    updated, audit = bind_snapshot_to_manifest(
        manifest=manifest,
        snapshot_metadata=metadata,
        macro_ledger=ledger,
        exposure_map=exposure_map,
        specs=specs,
        plan=plan,
    )

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(updated, indent=2, sort_keys=True), encoding="utf-8")
    Path(args.audit_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.audit_output).write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "manifest_id": updated["manifest_id"],
        "snapshot_id": audit["snapshot_id"],
        "bound_count": len(audit["bound"]),
        "skipped_count": len(audit["skipped"]),
        "outcomes_read": False,
        "output": args.output,
        "audit_output": args.audit_output,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
