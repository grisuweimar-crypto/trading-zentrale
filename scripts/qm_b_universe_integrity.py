#!/usr/bin/env python3
"""CLI for QM-B point-in-time universe integrity controls."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b import (  # noqa: E402
    QMBIntegrityError,
    UniverseIntegrityBundle,
    inspect_current_universe_master,
)


def _write_or_print(payload: dict, output: str | None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


def _load_sample(path: Path) -> list[dict[str, str]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise QMBIntegrityError("sample_json_must_be_array_of_objects")
        return [dict(row) for row in payload]
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="QM-B: as-of universe, identity, coverage, survivorship and investability validation"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    verify = sub.add_parser("verify", help="validate a QM-B bundle and print immutable versions")
    verify.add_argument("--bundle", required=True)

    normalize = sub.add_parser("normalize", help="validate and write bundle with deterministic hashes")
    normalize.add_argument("--bundle", required=True)
    normalize.add_argument("--output", required=True)

    resolve = sub.add_parser("resolve", help="resolve one historical identifier")
    resolve.add_argument("--bundle", required=True)
    resolve.add_argument("--type", required=True, dest="identifier_type")
    resolve.add_argument("--value", required=True)
    resolve.add_argument("--as-of", required=True, dest="as_of")
    resolve.add_argument("--venue")

    membership = sub.add_parser("membership", help="show explicit membership state for one date")
    membership.add_argument("--bundle", required=True)
    membership.add_argument("--instrument-id", required=True)
    membership.add_argument("--as-of", required=True, dest="as_of")
    membership.add_argument("--venue")

    coverage = sub.add_parser("coverage", help="show provider/family coverage for one date")
    coverage.add_argument("--bundle", required=True)
    coverage.add_argument("--instrument-id", required=True)
    coverage.add_argument("--as-of", required=True, dest="as_of")
    coverage.add_argument("--source-id", required=True)
    coverage.add_argument("--family", required=True)

    outcome = sub.add_parser("outcome", help="show explicit outcome availability for one date")
    outcome.add_argument("--bundle", required=True)
    outcome.add_argument("--instrument-id", required=True)
    outcome.add_argument("--as-of", required=True, dest="as_of")
    outcome.add_argument("--outcome-id", required=True)

    audit = sub.add_parser("audit-sample", help="audit a JSON/CSV sample against the as-of universe ledger")
    audit.add_argument("--bundle", required=True)
    audit.add_argument("--sample", required=True)
    audit.add_argument("--output")

    inventory = sub.add_parser(
        "inventory-current",
        help="inspect current universe master without inferring any historical membership",
    )
    inventory.add_argument("--universe", default=str(ROOT / "data" / "inputs" / "universe_master.csv"))
    inventory.add_argument("--output")

    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "inventory-current":
            result = inspect_current_universe_master(args.universe)
            _write_or_print(result, args.output)
            return 0

        bundle = UniverseIntegrityBundle.from_path(args.bundle)

        if args.command == "verify":
            _write_or_print(
                {
                    "schema_version": "qm_b_verification_v1",
                    "valid": True,
                    "bundle_as_of": bundle.payload["bundle_as_of"],
                    "counts": {
                        "instruments": len(bundle.instruments),
                        "identifier_aliases": len(bundle.aliases),
                        "universe_membership": len(bundle.memberships),
                        "provider_coverage": len(bundle.provider_rows),
                        "outcome_availability": len(bundle.outcome_rows),
                        "taxonomy_assignments": len(bundle.taxonomy_rows),
                    },
                    "versions": bundle.versions(),
                },
                None,
            )
            return 0

        if args.command == "normalize":
            _write_or_print(bundle.normalized_payload(), args.output)
            return 0

        if args.command == "resolve":
            result = bundle.resolve_identifier(
                identifier_type=args.identifier_type,
                identifier_value=args.value,
                as_of_date=args.as_of,
                listing_venue=args.venue,
            )
            _write_or_print(result, None)
            return 0

        if args.command == "membership":
            result = bundle.membership_as_of(
                instrument_id=args.instrument_id,
                as_of_date=args.as_of,
                listing_venue=args.venue,
            )
            _write_or_print(result, None)
            return 0

        if args.command == "coverage":
            result = bundle.provider_coverage_as_of(
                instrument_id=args.instrument_id,
                as_of_date=args.as_of,
                source_id=args.source_id,
                external_family=args.family,
            )
            _write_or_print(result, None)
            return 0

        if args.command == "outcome":
            result = bundle.outcome_as_of(
                instrument_id=args.instrument_id,
                as_of_date=args.as_of,
                outcome_id=args.outcome_id,
            )
            _write_or_print(result, None)
            return 0

        if args.command == "audit-sample":
            result = bundle.audit_sample(_load_sample(Path(args.sample)))
            _write_or_print(result, args.output)
            return 2 if result["fail_closed"] else 0

        raise QMBIntegrityError(f"unsupported_command:{args.command}")
    except (QMBIntegrityError, OSError, json.JSONDecodeError) as exc:
        print(f"QM-B ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
