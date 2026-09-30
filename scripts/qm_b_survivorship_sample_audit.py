#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))
from scanner.research.governance.qm_b_survivorship_sample_audit import scan_repository

p=argparse.ArgumentParser()
p.add_argument('--root', default=str(ROOT))
p.add_argument('--output', required=True)
args=p.parse_args()
result=scan_repository(args.root)
Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True)+'\n', encoding='utf-8')
print(json.dumps({k:result[k] for k in ['files_scanned','candidate_count','classification_counts']}, indent=2, sort_keys=True))
