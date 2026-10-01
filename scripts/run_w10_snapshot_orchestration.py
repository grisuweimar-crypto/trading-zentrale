#!/usr/bin/env python3
"""Record and validate the W10 same-snapshot Decision endworkflow contract."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.w10_orchestration import (
    W10OrchestrationError,
    begin_manifest,
    freeze_pre_7a,
    record_preexisting_artifact,
    record_runtime_artifact,
    resolve_phase6,
    seal_final_7a,
    validate_sealed_manifest,
)


DEFAULT_MANIFEST = "artifacts/research/decision_snapshot_w10.json"


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise W10OrchestrationError(f"manifest_must_be_object:{path}")
    return value


def _write(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _git_provenance(root: Path, artifact: str, commit: str | None) -> tuple[str, str]:
    if commit:
        source_commit = commit.strip()
    else:
        source_commit = subprocess.check_output(
            ["git", "log", "-1", "--format=%H", "--", artifact],
            cwd=root,
            text=True,
        ).strip()
    if not source_commit:
        raise W10OrchestrationError(f"git_source_commit_missing:{artifact}")
    available = subprocess.check_output(
        ["git", "show", "-s", "--format=%cI", source_commit],
        cwd=root,
        text=True,
    ).strip()
    if not available:
        raise W10OrchestrationError(f"git_source_time_missing:{artifact}")
    return source_commit, available


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init")

    runtime = sub.add_parser("record-runtime")
    runtime.add_argument("--stage", required=True)
    runtime.add_argument("--artifact", required=True)

    existing = sub.add_parser("record-git")
    existing.add_argument("--stage", required=True)
    existing.add_argument("--artifact", required=True)
    existing.add_argument("--source-commit")
    existing.add_argument("--require-snapshot-match", action="store_true")

    phase6 = sub.add_parser("resolve-phase6")
    phase6.add_argument("--artifact")

    sub.add_parser("freeze")

    seal = sub.add_parser("seal")
    seal.add_argument(
        "--packet-set",
        default="artifacts/research/current_decision_packets_7a.json",
    )
    seal.add_argument(
        "--archive",
        default="artifacts/research/decision_evidence_7a.jsonl",
    )

    sub.add_parser("validate")

    args = parser.parse_args()
    root = args.root.resolve()
    manifest_path = Path(args.manifest)
    if not manifest_path.is_absolute():
        manifest_path = root / manifest_path

    if args.command == "init":
        manifest = begin_manifest(validate_daily_research(root))
        _write(manifest_path, manifest)
    else:
        manifest = _load(manifest_path)

        if args.command == "record-runtime":
            artifact = Path(args.artifact)
            if not artifact.is_absolute():
                artifact = root / artifact
            manifest = record_runtime_artifact(
                manifest,
                stage=args.stage,
                artifact_path=artifact,
            )
            _write(manifest_path, manifest)

        elif args.command == "record-git":
            artifact_arg = args.artifact
            artifact = Path(artifact_arg)
            if not artifact.is_absolute():
                artifact = root / artifact
            source_commit, available = _git_provenance(
                root, artifact_arg, args.source_commit
            )
            manifest = record_preexisting_artifact(
                manifest,
                stage=args.stage,
                artifact_path=artifact,
                source_commit=source_commit,
                source_available_from=available,
                require_snapshot_match=args.require_snapshot_match,
            )
            _write(manifest_path, manifest)

        elif args.command == "resolve-phase6":
            artifact = Path(args.artifact) if args.artifact else None
            if artifact is not None and not artifact.is_absolute():
                artifact = root / artifact
            manifest = resolve_phase6(manifest, artifact_path=artifact)
            _write(manifest_path, manifest)

        elif args.command == "freeze":
            manifest = freeze_pre_7a(manifest)
            _write(manifest_path, manifest)

        elif args.command == "seal":
            packet_set = Path(args.packet_set)
            archive = Path(args.archive)
            if not packet_set.is_absolute():
                packet_set = root / packet_set
            if not archive.is_absolute():
                archive = root / archive
            manifest = seal_final_7a(
                manifest,
                packet_set_path=packet_set,
                archive_path=archive,
            )
            _write(manifest_path, manifest)

        elif args.command == "validate":
            daily = validate_daily_research(root)
            manifest = validate_sealed_manifest(
                manifest,
                expected_snapshot_id=str(daily["snapshot_id"]),
            )

    print(
        json.dumps(
            {
                "schema_version": manifest["schema_version"],
                "snapshot_id": manifest["snapshot_id"],
                "status": manifest["status"],
                "pre_7a_frozen_at": manifest.get("pre_7a_frozen_at"),
                "sealed_at": manifest.get("sealed_at"),
                "stage_status": {
                    key: value.get("status")
                    for key, value in manifest["stages"].items()
                    if isinstance(value, dict)
                },
                "manifest": str(manifest_path),
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
