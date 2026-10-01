#!/usr/bin/env python3
"""One-off privacy-safe W11 probe: run every daily symbol as hypothetical long.

This does NOT encode or reveal a real portfolio. It exercises the exact private
7D->7H path for the full authoritative scanner universe, then emits a compact
per-symbol summary plus the W11 acceptance receipt.
"""
from __future__ import annotations

import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest
from scanner.research.decision_layer.w11_end_to_end import validate_w11_end_to_end

ROOT = Path(__file__).resolve().parents[1]


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def main() -> int:
    daily = validate_daily_research(ROOT)
    manifest = validate_sealed_manifest(
        _load(ROOT / "artifacts/research/decision_snapshot_w10.json"),
        expected_snapshot_id=str(daily["snapshot_id"]),
    )
    packets, archive_meta = load_evidence_archive(ROOT / DEFAULT_ARCHIVE, missing_ok=False)

    position_as_of = str(daily.get("generated_at") or daily["as_of"])
    symbols = sorted(str(symbol) for symbol in daily["symbols"].keys())
    position_book = {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": str(daily["snapshot_id"]),
        "as_of": position_as_of,
        "positions": [
            {
                "schema_version": "decision_position_snapshot_v1",
                "symbol": symbol,
                "source_snapshot_id": str(daily["snapshot_id"]),
                "as_of": position_as_of,
                "position_state": "long",
            }
            for symbol in symbols
        ],
    }

    watch, diagnostics = build_orchestrated_depot_watch(
        daily,
        position_book,
        packets,
        elliott_6h_source=None,
    )
    receipt = validate_w11_end_to_end(
        manifest=manifest,
        archive_packets=packets,
        watch=watch,
        diagnostics=diagnostics,
    )

    rows = []
    for row in watch["rows"]:
        decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
        policy = row.get("depot_action_policy") if isinstance(row.get("depot_action_policy"), dict) else {}
        state_history = row.get("state_history_context") if isinstance(row.get("state_history_context"), dict) else {}
        path_review = row.get("path_review") if isinstance(row.get("path_review"), dict) else {}
        rows.append({
            "symbol": row.get("symbol"),
            "availability": row.get("availability"),
            "presentation_group": row.get("presentation_group"),
            "attention_required": row.get("attention_required"),
            "universal_stance_state": decision.get("universal_stance_state"),
            "universal_stance_direction": decision.get("universal_stance_direction"),
            "transition_status": decision.get("transition_status"),
            "stable_directional_anchor": decision.get("stable_directional_anchor"),
            "pending_direction": decision.get("pending_direction"),
            "portfolio_action_state": decision.get("portfolio_action_state"),
            "portfolio_action_reason_code": decision.get("portfolio_action_reason_code"),
            "reliability_assessment": decision.get("reliability_assessment"),
            "coverage_admission_state": decision.get("coverage_admission_state"),
            "evidence_gap_count": decision.get("evidence_gap_count"),
            "missing_or_limited_evidence": decision.get("missing_or_limited_evidence"),
            "path_review_state": path_review.get("review_state"),
            "path_sequence_state": path_review.get("sequence_state"),
            "state_history_state": state_history.get("state"),
            "w8_scenario": policy.get("scenario"),
            "w8_action_changed": policy.get("action_changed"),
            "w8_conflict": policy.get("conflict"),
            "w8_reassessment_required": policy.get("reassessment_required"),
            "execution_allowed": row.get("execution_allowed"),
        })

    out = {
        "probe_schema": "w11_all_long_privacy_probe_v1",
        "privacy_note": "All authoritative daily symbols were hypothetically marked long; this is not a real portfolio.",
        "snapshot_id": daily["snapshot_id"],
        "snapshot_as_of": daily["as_of"],
        "scanner_generated_at": daily.get("generated_at"),
        "universe_size": len(symbols),
        "archive_status": archive_meta.get("status"),
        "watch_status": watch.get("watch_status"),
        "watch_row_count": len(watch["rows"]),
        "diagnostics": diagnostics,
        "w11_receipt": receipt,
        "rows": rows,
    }
    output = ROOT / "w11_all_long_probe_result.json"
    output.write_text(json.dumps(out, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "snapshot_id": out["snapshot_id"],
        "watch_status": out["watch_status"],
        "watch_row_count": out["watch_row_count"],
        "w11_status": receipt["status"],
        "review_now": sum(1 for row in rows if row["presentation_group"] == "review_now"),
        "waiting_confirmation": sum(1 for row in rows if row["presentation_group"] == "waiting_confirmation"),
        "hold_or_no_action": sum(1 for row in rows if row["presentation_group"] == "hold_or_no_action"),
        "unavailable": sum(1 for row in rows if row["presentation_group"] == "unavailable"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
