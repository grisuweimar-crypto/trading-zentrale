#!/usr/bin/env python3
"""Bind one outcome-blind Scanner_vNext sampling day into the frozen Phase-8G manifest."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.research_8g import (
    validate_challenger_specs,
    validate_split_manifest,
    validate_split_plan,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    bind_snapshot_to_manifest_guarded,
)


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Bind a feature-side Scanner sampling day to frozen 8G factor/horizon slots "
            "without reading outcomes; repeated reruns with the same as_of date do not "
            "consume additional slots"
        )
    )
    parser.add_argument("--specs", default="configs/external_evidence_8g_challenger_specs_v1.json")
    parser.add_argument("--plan", default="configs/external_evidence_8g_split_plan_v1.json")
    parser.add_argument("--manifest", default="artifacts/research/external_evidence_8g_split_manifest_v1.json")
    parser.add_argument("--snapshot-metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--macro-ledger", required=True)
    parser.add_argument("--exposure-map", default="configs/external_evidence_8f_exposure_map_v1.json")
    parser.add_argument(
        "--source-identity-correction",
        default="configs/external_evidence_source_identity_correction_v1.json",
        help=(
            "Versioned outcome-blind alias-to-canonical correction receipt. "
            "Use an empty string only for legacy/synthetic inputs already carrying frozen 8G source aliases."
        ),
    )
    parser.add_argument("--output", default="artifacts/research/external_evidence_8g_split_manifest_v1.json")
    parser.add_argument("--audit-output", default="artifacts/research/external_evidence_8g_split_binding_latest.json")
    args = parser.parse_args()

    specs = _load(args.specs)
    plan = _load(args.plan)
    manifest = _load(args.manifest)
    metadata = _load(args.snapshot_metadata)
    ledger = _load(args.macro_ledger)
    exposure_map = _load(args.exposure_map)
    correction = _load(args.source_identity_correction) if args.source_identity_correction else None

    validate_challenger_specs(specs)
    validate_split_plan(plan, specs)
    validate_split_manifest(manifest, specs, plan)
    updated, audit = bind_snapshot_to_manifest_guarded(
        manifest=manifest,
        snapshot_metadata=metadata,
        macro_ledger=ledger,
        exposure_map=exposure_map,
        specs=specs,
        plan=plan,
        source_identity_correction=correction,
    )

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(updated, indent=2, sort_keys=True), encoding="utf-8")
    Path(args.audit_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.audit_output).write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "manifest_id": updated["manifest_id"],
        "snapshot_id": audit["snapshot_id"],
        "as_of": audit["as_of"],
        "bound_count": len(audit["bound"]),
        "skipped_count": len(audit["skipped"]),
        "outcomes_read": False,
        "source_identity_correction_sha256": audit.get("source_identity_correction_sha256"),
        "output": args.output,
        "audit_output": args.audit_output,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
