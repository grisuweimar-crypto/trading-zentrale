#!/usr/bin/env python3
"""Run frozen BA-QM7 Decision/E2E negative controls on repository artifacts."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.governance.qm_j_decision_e2e import load_plan, run_falsification

ROOT = Path(__file__).resolve().parents[1]


def _utc(value: object) -> datetime:
    text = str(value or "").strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sealed_final_7a_view(packets, daily, manifest):
    """Keep only the sealed final-7A revision for the current scanner snapshot.

    The append-only archive may contain earlier revisions under the same scanner
    snapshot identity. Existing Decision orchestration resolves those revisions
    by decision time. W10 supplies the authoritative final-7A available_from;
    QM-J mirrors that existing rule rather than redefining currentness.
    """
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    stages = manifest.get("stages") or {}
    final_7a = stages.get("final_7a") or {}
    decision_time = _utc(final_7a.get("available_from"))
    retained = []
    dropped_current_revisions = 0
    current_symbols = set()
    for packet in packets:
        if str(packet.get("source_snapshot_id") or "") != snapshot_id:
            retained.append(packet)
            continue
        if _utc(packet.get("as_of")) != decision_time:
            dropped_current_revisions += 1
            continue
        symbol = str(packet.get("symbol") or "")
        if symbol in current_symbols:
            raise ValueError(f"duplicate_sealed_final_7a_symbol:{symbol}")
        current_symbols.add(symbol)
        retained.append(packet)
    if not current_symbols:
        raise ValueError("sealed_final_7a_packets_missing")
    return retained, {
        "sealed_final_7a_available_from": decision_time.isoformat(),
        "current_symbol_count": len(current_symbols),
        "dropped_earlier_current_snapshot_revisions": dropped_current_revisions,
        "archive_revision_resolution_redefined": False,
    }


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
    packets, revision_meta = _sealed_final_7a_view(packets, daily, manifest)
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
        "effective_packet_count_after_sealed_revision_selection": len(packets),
        **revision_meta,
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
        "archive_revision_resolution": revision_meta,
        "status": result["status"],
        "promotion_blocked_by_qm_j": result["promotion_blocked_by_qm_j"],
        "capa_required": result["capa_required"],
        "output": str(target),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
