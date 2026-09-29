from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from scanner.research.governance.qm_a import GovernanceLedger, GovernanceLedgerError


DEFAULT_LEDGER = "artifacts/research/qm/qm_a_governance_events.jsonl"


def _load_json(path: str | None) -> dict:
    if not path:
        return {}
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise GovernanceLedgerError("json_input_must_be_object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QM-A research-governance ledger")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    sub = parser.add_subparsers(dest="command", required=True)

    verify = sub.add_parser("verify", help="verify hash chain and governance invariants")
    verify.set_defaults(action="verify")

    register = sub.add_parser("register", help="register a new immutable analysis version")
    register.add_argument("--analysis-id", required=True)
    register.add_argument("--version-id", required=True)
    register.add_argument("--actor-id", required=True)
    register.add_argument("--actor-role", required=True)
    register.add_argument("--supersedes-version-id")
    register.add_argument("--description", default="")
    register.set_defaults(action="register")

    transition = sub.add_parser("transition", help="append an evidence-state transition")
    transition.add_argument("--analysis-id", required=True)
    transition.add_argument("--version-id", required=True)
    transition.add_argument("--to-state", required=True)
    transition.add_argument("--actor-id", required=True)
    transition.add_argument("--actor-role", required=True)
    transition.add_argument("--reason", required=True)
    transition.add_argument("--identity-json")
    transition.add_argument("--review-reference")
    transition.set_defaults(action="transition")

    inspect = sub.add_parser("inspect", help="append an evidence inspection/decision record")
    inspect.add_argument("--analysis-id", required=True)
    inspect.add_argument("--version-id", required=True)
    inspect.add_argument("--actor-id", required=True)
    inspect.add_argument("--actor-role", required=True)
    inspect.add_argument("--record-json", required=True)
    inspect.set_defaults(action="inspect")

    show = sub.add_parser("show", help="show derived state for one analysis version")
    show.add_argument("--analysis-id", required=True)
    show.add_argument("--version-id", required=True)
    show.set_defaults(action="show")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ledger = GovernanceLedger(args.ledger)
    try:
        if args.action == "verify":
            result = ledger.verify_integrity()
        elif args.action == "register":
            result = ledger.register_analysis(
                analysis_id=args.analysis_id,
                version_id=args.version_id,
                actor_id=args.actor_id,
                actor_role=args.actor_role,
                supersedes_version_id=args.supersedes_version_id,
                description=args.description,
            )
        elif args.action == "transition":
            result = ledger.transition(
                analysis_id=args.analysis_id,
                version_id=args.version_id,
                to_state=args.to_state,
                actor_id=args.actor_id,
                actor_role=args.actor_role,
                reason=args.reason,
                analysis_identity=_load_json(args.identity_json) if args.identity_json else None,
                review_or_approval_reference=args.review_reference,
            )
        elif args.action == "inspect":
            result = ledger.log_inspection(
                analysis_id=args.analysis_id,
                version_id=args.version_id,
                actor_id=args.actor_id,
                actor_role=args.actor_role,
                record=_load_json(args.record_json),
            )
        elif args.action == "show":
            result = ledger.get_analysis(args.analysis_id, args.version_id)
        else:
            raise GovernanceLedgerError(f"unknown_action:{args.action}")
    except (GovernanceLedgerError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2

    print(json.dumps({"ok": True, "result": result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
