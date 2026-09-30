#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))

from scanner.research.governance.qm_b_investability_gap_integration import audit_investability_as_of
from scanner.research.governance.qm_b_project_restrictions import build_project_restriction_evidence


def main() -> int:
    p = argparse.ArgumentParser(description='QM-B current investability gap audit')
    p.add_argument('--membership', required=True)
    p.add_argument('--policy-commit-sha', required=True)
    p.add_argument('--policy-observed-at', required=True)
    p.add_argument('--as-of', required=True)
    p.add_argument('--output')
    args = p.parse_args()
    membership = json.loads(Path(args.membership).read_text(encoding='utf-8'))
    restrictions = build_project_restriction_evidence(
        membership,
        policy_commit_sha=args.policy_commit_sha,
        policy_observed_at=args.policy_observed_at,
    )
    result = audit_investability_as_of(
        membership,
        as_of=args.as_of,
        additional_evidence=restrictions['evidence'],
    )
    result['project_restriction_status_counts'] = restrictions['status_counts']
    result['project_restriction_evidence_valid_from'] = restrictions['evidence_valid_from']
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + '\n', encoding='utf-8')
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
