"""Read-only, snapshot-bound human-readable Watch evidence overview.

This module does not change any 7D/7E/7F/7G decision fields.
"""
from __future__ import annotations
from typing import Mapping


def explain_watch(watch: Mapping, w10_manifest: Mapping) -> dict:
    snapshot_id = str(watch.get("source_snapshot_id") or "")
    if not snapshot_id or snapshot_id != str(w10_manifest.get("snapshot_id") or ""):
        raise ValueError("watch_manifest_snapshot_mismatch")
    phase6 = (w10_manifest.get("stages") or {}).get("phase6_elliott") or {}
    stage_status = str(phase6.get("status") or "unknown")
    if stage_status not in ("available", "not_supplied"):
        stage_status = "unknown"
    result = []
    for row in watch.get("rows") or []:
        if not isinstance(row, Mapping):
            raise ValueError("watch_row_invalid")
        decision = row.get("decision")
        decision = decision if isinstance(decision, Mapping) else {}
        elliott_present = isinstance(row.get("elliott_swing_context"), Mapping)
        if elliott_present and stage_status == "not_supplied":
            raise ValueError("late_elliott_injection_not_allowed")
        result.append({
            "symbol": row.get("symbol"),
            "availability": row.get("availability"),
            "universal_stance": decision.get("universal_stance_state"),
            "portfolio_action": decision.get("portfolio_action_state"),
            "reliability": decision.get("reliability_assessment"),
            "reliability_is_return_probability": False,
            "elliott_source": (
                "present" if elliott_present
                else "not_supplied_at_w10" if stage_status == "not_supplied"
                else "no_symbol_context"
            ),
            "missing_evidence": decision.get("missing_or_limited_evidence") or [],
            "decision_triggers": decision.get("decision_change_triggers") or [],
            "information_triggers": decision.get("information_completion_triggers") or [],
            "attention_required": row.get("attention_required") is True,
        })
    return {
        "snapshot_id": snapshot_id,
        "as_of": watch.get("as_of"),
        "watch_status": watch.get("watch_status"),
        "elliott_stage_status": stage_status,
        "read_only": True,
        "changes_portfolio_action": False,
        "rows": result,
    }


def render_markdown(report: Mapping) -> str:
    lines = [
        "# A-Watch: Entscheidungs- und Evidenzübersicht",
        "",
        f"Snapshot: `{report['snapshot_id']}` | Stand: {report.get('as_of') or 'unbekannt'}",
        f"Watch: {report.get('watch_status') or 'unbekannt'} | Elliott W10: {report['elliott_stage_status']}",
        "",
        "Nur Interpretation; keine Neuberechnung von Score, Stance oder Portfolio Action.",
        "Confidence und Zuverlässigkeit sind keine Wahrscheinlichkeit positiver Rendite.",
        "",
    ]
    for row in report["rows"]:
        lines.extend([
            f"## {row.get('symbol') or 'Unbekannt'}",
            f"- Entscheidungszustand: {row.get('universal_stance') or 'nicht verfügbar'}",
            f"- Portfolio-Review: {row.get('portfolio_action') or 'nicht verfügbar'}",
            f"- Zuverlässigkeit: {row.get('reliability') or 'nicht verfügbar'}",
            f"- Elliott: {row['elliott_source']}",
            f"- Aufmerksamkeit: {'ja' if row['attention_required'] else 'nein'}",
            f"- Evidenzlücken: {row['missing_evidence'] or 'keine explizit dokumentiert'}",
            f"- Entscheidungs-Trigger: {row['decision_triggers'] or 'keine dokumentiert'}",
            f"- Ergänzungs-Trigger: {row['information_triggers'] or 'keine dokumentiert'}",
            "",
        ])
    return "\n".join(lines)
