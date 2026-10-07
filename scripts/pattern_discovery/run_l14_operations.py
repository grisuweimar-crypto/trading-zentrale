from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    OperationsRegistry,
    build_operations_cycle,
    load_operations_contract,
    persist_operations_cycle,
)
from scanner.research.pattern_discovery.run_contract import verify_run_manifest


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def _registry(root: Path):
    contract = load_operations_contract()
    return OperationsRegistry(root / contract["storage"]["registry_path"])


def cycle(args) -> int:
    root = Path(args.repo_root).resolve()
    extraordinary = (
        _json(_path(root, args.extraordinary_request))
        if args.extraordinary_request
        else None
    )
    result = build_operations_cycle(
        root,
        evaluated_at=args.evaluated_at,
        extraordinary_request=extraordinary,
    )
    persisted = None
    if args.persist:
        persisted = persist_operations_cycle(
            root,
            result,
            actor_id=args.actor_id,
            actor_role=args.actor_role,
        )
    print(
        json.dumps(
            {
                "phase": "L14",
                "cycle_hash": result["cycle_hash"],
                "cycle_status": result["cycle_status"],
                "blocking_reasons": result["blocking_reasons"],
                "discovery_trigger": result["discovery"]["trigger"],
                "work_items": [
                    {
                        "stage": row["stage"],
                        "status": row["status"],
                    }
                    for row in result["work_items"]
                ],
                "decay_alert_count": len(result["decay_alerts"]),
                "persisted": persisted,
            },
            ensure_ascii=False,
        )
    )
    return 0


def classify_run(args) -> int:
    root = Path(args.repo_root).resolve()
    manifest = _json(_path(root, args.manifest))
    verified = verify_run_manifest(manifest)
    result = _registry(root).classify_discovery_run(
        run_id=str(verified["run_id"]),
        manifest_hash=str(verified["manifest_hash"]),
        mode=args.mode,
        trigger_cycle_hash=args.trigger_cycle_hash,
        recorded_at=args.recorded_at,
        actor_id=args.actor_id,
        actor_role=args.actor_role,
    )
    print(json.dumps({"phase": "L14", "classification": result}))
    return 0


def verify_registry(args) -> int:
    root = Path(args.repo_root).resolve()
    result = _registry(root).verify_integrity()
    print(json.dumps({"phase": "L14", "registry": result}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Operate Pattern Discovery Lab v2 Phase L14."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    p_cycle = sub.add_parser("cycle")
    p_cycle.add_argument("--evaluated-at", required=True)
    p_cycle.add_argument("--extraordinary-request")
    p_cycle.add_argument("--persist", action="store_true")
    p_cycle.add_argument("--actor-id", default="l14-operator")
    p_cycle.add_argument("--actor-role", default="research_automation")
    p_cycle.add_argument("--repo-root", default=".")
    p_cycle.set_defaults(handler=cycle)

    p_classify = sub.add_parser("classify-run")
    p_classify.add_argument("--manifest", required=True)
    p_classify.add_argument(
        "--mode",
        choices=["INITIAL", "REGULAR", "EXTRAORDINARY"],
        required=True,
    )
    p_classify.add_argument("--trigger-cycle-hash")
    p_classify.add_argument("--recorded-at", required=True)
    p_classify.add_argument("--actor-id", required=True)
    p_classify.add_argument("--actor-role", default="research_automation")
    p_classify.add_argument("--repo-root", default=".")
    p_classify.set_defaults(handler=classify_run)

    p_verify = sub.add_parser("verify-registry")
    p_verify.add_argument("--repo-root", default=".")
    p_verify.set_defaults(handler=verify_registry)

    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
