#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))

from scanner.research.governance.qm_b_project_restrictions import build_project_restriction_evidence


def main() -> int:
    p = argparse.ArgumentParser(description='QM-B project restriction evidence')
    p.add_argument('--membership', required=True)
    p.add_argument('--policy-commit-sha', required=True)
    p.add_argument('--policy-observed-at', required=True)
    p.add_argument('--output')
    args = p.parse_args()
    membership = json.loads(Path(args.membership).read_text(encoding='utf-8'))
    result = build_project_restriction_evidence(
        membership,
        policy_commit_sha=args.policy_commit_sha,
        policy_observed_at=args.policy_observed_at,
    )
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + '\n', encoding='utf-8')
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
