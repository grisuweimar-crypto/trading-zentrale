#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT/'src') not in sys.path:
    sys.path.insert(0,str(ROOT/'src'))

from scanner.research.governance.qm_b_execution_channel import audit_membership_execution_gap


def main()->int:
    p=argparse.ArgumentParser(description='QM-B execution channel audit')
    p.add_argument('--membership',required=True)
    p.add_argument('--as-of',required=True)
    p.add_argument('--evidence')
    p.add_argument('--output')
    args=p.parse_args()
    membership=json.loads(Path(args.membership).read_text(encoding='utf-8'))
    evidence=[]
    if args.evidence:
        evidence=json.loads(Path(args.evidence).read_text(encoding='utf-8'))
        if not isinstance(evidence,list):
            raise SystemExit('--evidence must be a JSON array')
    result=audit_membership_execution_gap(membership,as_of=args.as_of,evidence_records=evidence)
    text=json.dumps(result,indent=2,sort_keys=True)
    if args.output:
        Path(args.output).write_text(text+'\n',encoding='utf-8')
    print(text)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
