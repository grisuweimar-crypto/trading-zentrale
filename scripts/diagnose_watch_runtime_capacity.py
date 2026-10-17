#!/usr/bin/env python3
"""Read-only capacity diagnostics, retaining the existing BA-QM10 safety thresholds."""
from __future__ import annotations
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from scanner.research.governance.ba_qm10_production_operations import audit_current_runtime_capacity

def main():
    report = audit_current_runtime_capacity(ROOT)
    fields = ("status", "health_status", "snapshot_id", "partition_strategy", "max_shard_id", "max_shard_bytes", "headroom_ratio", "largest_symbol", "largest_symbol_bytes", "largest_symbol_packet_count", "packet_count", "symbol_count", "largest_symbols_by_bytes")
    result = {key: report.get(key) for key in fields}
    dest = Path("runtime_capacity_diagnostic.json")
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if report["health_status"] != "PASS":
        print("Capacity guard remains active; do not promote Watch.", file=sys.stderr)
        raise SystemExit(2)

if __name__ == "__main__":
    main()
