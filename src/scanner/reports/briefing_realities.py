from __future__ import annotations

"""Build Briefing & Realities text (explainability-only).

Explainability-only. It merges:
- briefing.json (top picks + reasons)
- history_delta.json (scanner-internal rank/score changes)
- reality_check.json (top issues)
- segment_monitor.json (segment changes)

History Delta shows the internal progression of the scanner based on local daily snapshots.
It is NOT market performance or price performance - it's scanner-internal ranking changes.

Outputs
-------
- artifacts/reports/briefing_realities.txt
- artifacts/reports/briefing_realities.json (optional, lightweight)
"""

import json
from pathlib import Path
from typing import Any

from scanner.data.io.paths import artifacts_dir


SCHEMA_VERSION = 1


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_briefing_realities_text() -> tuple[str, dict[str, Any]]:
    rep = artifacts_dir() / "reports"
    briefing = _read_json(rep / "briefing.json") or {}
    delta = _read_json(rep / "history_delta.json") or {}
    reality = _read_json(rep / "reality_check.json") or {}
    segment = _read_json(rep / "segment_monitor.json") or {}

    top = briefing.get("top") if isinstance(briefing, dict) else None
    if not isinstance(top, list):
        top = []

    # quick lookup helpers
    # delta movers: symbol -> dict
    d_by_sym: dict[str, dict[str, Any]] = {}
    for k in ("movers_up", "movers_down"):
        arr = delta.get(k)
        if isinstance(arr, list):
            for it in arr:
                if isinstance(it, dict) and isinstance(it.get("symbol"), str):
                    d_by_sym[it["symbol"]] = it

    # reality issues: symbol -> problems
    r_by_sym: dict[str, dict[str, Any]] = {}
    arr = reality.get("top_issues")
    if isinstance(arr, list):
        for it in arr:
            if isinstance(it, dict) and isinstance(it.get("symbol"), str):
                r_by_sym[it["symbol"]] = it

    # segment changes: symbol -> list
    s_by_sym: dict[str, list[dict[str, Any]]] = {}
    arr = segment.get("changes")
    if isinstance(arr, list):
        for it in arr:
            if isinstance(it, dict) and isinstance(it.get("symbol"), str):
                ch = it.get("changes")
                if isinstance(ch, list):
                    s_by_sym[it["symbol"]] = [c for c in ch if isinstance(c, dict)]

    lines: list[str] = []
    lines.append("Briefing & Realities")
    lines.append("—")
    if delta.get("latest_date"):
        lines.append(f"History Delta: {delta.get('prev_date')} → {delta.get('latest_date')}")
    if reality.get("date"):
        stats = reality.get("stats") or {}
        if isinstance(stats, dict):
            lines.append(f"Reality Check: ok={stats.get('ok')} warn={stats.get('warn')} error={stats.get('error')}")
    lines.append("")

    if not top:
        lines.append("Noch kein Briefing verfügbar. (scripts/generate_briefing.py ausführen)")
    else:
        lines.append("Top-Picks (aus Briefing) mit Reality/Delta:")
        for it in top[:6]:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("symbol", "") or "")
            name = str(it.get("name", "") or "")
            score = it.get("score")
            score_s = f"{score}" if score is not None else "?"
            lines.append(f"- {sym} · {name} · Score {score_s}")

            d = d_by_sym.get(sym)
            if d:
                rd = d.get("rank_delta")
                sd = d.get("score_delta")
                extra = []
                if rd is not None:
                    extra.append(f"ΔRank {rd:+d}")
                if sd is not None:
                    try:
                        extra.append(f"ΔScore {float(sd):+.2f}")
                    except Exception:
                        extra.append(f"ΔScore {sd}")
                if extra:
                    lines.append(f"  • History: " + " / ".join(extra))

            r = r_by_sym.get(sym)
            if r:
                problems = str(r.get("problems", "") or "")
                if problems:
                    lines.append(f"  • Reality: {problems}")

            sc = s_by_sym.get(sym)
            if sc:
                parts = []
                for c in sc[:3]:
                    parts.append(f"{c.get('field')}: {c.get('from')} → {c.get('to')}")
                if parts:
                    lines.append("  • Segment: " + " / ".join(parts))

            # add one reason line from briefing if present
            reasons = it.get("reasons")
            if isinstance(reasons, list) and reasons:
                lines.append("  • Briefing: " + str(reasons[0]))

    text = "\n".join(lines).strip() + "\n"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "latest_date": delta.get("latest_date"),
        "prev_date": delta.get("prev_date"),
        "briefing_top_n": len(top),
        "reality_stats": reality.get("stats"),
        "segment_changed": (segment.get("stats") or {}).get("changed") if isinstance(segment, dict) else None,
    }
    return text, payload


def write_briefing_realities_outputs(text: str, payload: dict[str, Any]) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_txt = out_dir / "briefing_realities.txt"
    p_json = out_dir / "briefing_realities.json"
    p_txt.write_text(text, encoding="utf-8")
    p_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"txt": p_txt, "json": p_json}
