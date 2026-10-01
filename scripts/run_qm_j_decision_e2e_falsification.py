#!/usr/bin/env python3
"""Run frozen BA-QM7 Decision/E2E negative controls on repository artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.governance.qm_j_decision_e2e import load_plan, run_falsification

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", default=str(ROOT / "artifacts/research/daily_research.json"))
    parser.add_argument("--archive", default=str(ROOT / "artifacts/research/decision_evidence_7a.jsonl"))
    parser.add_argument("--manifest", default=str(ROOT / "artifacts/research/decision_snapshot_w10.json"))
    parser.add_argument("--config", default=str(ROOT / "configs/qm_j_decision_e2e_falsification_v1.json"))
    parser.add_argument("--output", default=str(ROOT / "artifacts/research/qm/qm_j_decision_e2e_falsification.json"))
    args = parser.parse_args()

    daily = json.loads(Path(args.daily).read_text(encoding="utf-8"))
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    packets, archive_meta = load_evidence_archive(args.archive, missing_ok=False)
    result = run_falsification(
        daily=daily,
        archive_packets=packets,
        manifest=manifest,
        plan=load_plan(args.config),
    )
    result["archive_metadata"] = {
        "packet_count": archive_meta.get("packet_count"),
        "symbol_count": archive_meta.get("symbol_count"),
        "snapshot_count": archive_meta.get("snapshot_count"),
        "as_of_min": archive_meta.get("as_of_min"),
        "as_of_max": archive_meta.get("as_of_max"),
    }
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "application": result["application"],
        "snapshot_id": result["snapshot_id"],
        "baseline": result["baseline"],
        "placebo_sidecar": result["placebo_sidecar"],
        "destroyed_information": result["destroyed_information"],
        "status": result["status"],
        "promotion_blocked_by_qm_j": result["promotion_blocked_by_qm_j"],
        "capa_required": result["capa_required"],
        "output": str(target),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
