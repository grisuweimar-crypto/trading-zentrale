#!/usr/bin/env python3
"""Run frozen BA-QM7 Decision/E2E negative controls on repository artifacts."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
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


def _canonical_revision_view(packets, daily, manifest):
    """Canonicalize append-only 7A revisions using existing Decision semantics.

    The production orchestrator's `_stance_history` keeps the latest evidence
    revision of each (symbol, scanner-snapshot) at or before the current
    Decision time, while `_current_packet_for_symbol` requires the current
    snapshot revision to match that Decision time exactly. QM-J mirrors those
    rules before running the isolated controls. This changes no claim content
    and deliberately fails closed on equal-time duplicate revisions.
    """
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    stages = manifest.get("stages") or {}
    final_7a = stages.get("final_7a") or {}
    decision_time = _utc(final_7a.get("available_from"))

    by_symbol_snapshot = {}
    dropped_future_revisions = 0
    dropped_earlier_current_revisions = 0
    superseded_revisions = 0

    for packet in packets:
        packet_time = _utc(packet.get("as_of"))
        packet_snapshot = str(packet.get("source_snapshot_id") or "")
        symbol = str(packet.get("symbol") or "")
        if packet_time > decision_time:
            dropped_future_revisions += 1
            continue
        if packet_snapshot == snapshot_id and packet_time != decision_time:
            dropped_earlier_current_revisions += 1
            continue

        key = (symbol, packet_snapshot)
        previous = by_symbol_snapshot.get(key)
        if previous is None:
            by_symbol_snapshot[key] = packet
            continue
        previous_time = _utc(previous.get("as_of"))
        if packet_time == previous_time:
            raise ValueError(
                f"duplicate_symbol_snapshot_revision:{symbol}:{packet_snapshot}"
            )
        if packet_time > previous_time:
            by_symbol_snapshot[key] = packet
            superseded_revisions += 1
        else:
            superseded_revisions += 1

    retained = sorted(
        by_symbol_snapshot.values(),
        key=lambda packet: (
            _utc(packet.get("as_of")),
            str(packet.get("source_snapshot_id") or ""),
            str(packet.get("symbol") or ""),
        ),
    )
    current_symbols = {
        str(packet.get("symbol") or "")
        for packet in retained
        if str(packet.get("source_snapshot_id") or "") == snapshot_id
        and _utc(packet.get("as_of")) == decision_time
    }
    if not current_symbols:
        raise ValueError("sealed_final_7a_packets_missing")

    return retained, {
        "sealed_final_7a_available_from": decision_time.isoformat(),
        "current_symbol_count": len(current_symbols),
        "dropped_earlier_current_snapshot_revisions": dropped_earlier_current_revisions,
        "dropped_future_revisions": dropped_future_revisions,
        "superseded_historical_revisions": superseded_revisions,
        "canonical_packet_count": len(retained),
        "archive_revision_resolution_redefined": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", default=str(ROOT / "artifacts/research/daily_research.json"))
    parser.add_argument("--archive", default=str(ROOT / DEFAULT_ARCHIVE))
    parser.add_argument("--manifest", default=str(ROOT / "artifacts/research/decision_snapshot_w10.json"))
    parser.add_argument("--config", default=str(ROOT / "configs/qm_j_decision_e2e_falsification_v1.json"))
    parser.add_argument("--output", default=str(ROOT / "artifacts/research/qm/qm_j_decision_e2e_falsification.json"))
    args = parser.parse_args()

    daily = json.loads(Path(args.daily).read_text(encoding="utf-8"))
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    packets, archive_meta = load_evidence_archive(args.archive, missing_ok=False)
    packets, revision_meta = _canonical_revision_view(packets, daily, manifest)
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
        "effective_packet_count_after_canonical_revision_selection": len(packets),
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
