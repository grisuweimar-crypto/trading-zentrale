from __future__ import annotations

"""Static UI generator (Phase B2 MVP).

Creates a single self-contained HTML file with:
- Preset switcher (CORE/SCORED/TOP/...)
- Search filter
- Sortable table
- Badges for trend_ok / liquidity_ok / score_status / is_crypto

Run
---
  python -m scanner.app.run_daily
  python -m scanner.ui.generator

Output
------
  artifacts/ui/index.html
"""

import argparse
import html
import json
from datetime import datetime, timezone
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Fix for running directly from ui directory
if Path(__file__).parent.name == "ui":
    # Add src to path so scanner modules can be found
    src_path = Path(__file__).parent.parent.parent
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

from scanner._version import __version__, __build__
from scanner.data.io.paths import project_root
from scanner.data.schema.contract import validate_csv
from scanner.presets.load import load_presets
from scanner.data.io.paths import artifacts_dir


DEFAULT_COLUMNS = [
    # identity
    "ticker",
    "ticker_display",
    "yahoo_symbol",
    "YahooSymbol",
    "symbol",
    "name",
    "isin",
    "sector",
    "Sector",
    "Sektor",
    "industry",
    "Industry",
    "country",
    "currency",
    "Währung",
    # pricing / momentum
    "price",
    "Akt. Kurs",
    "price_eur",
    "perf_pct",
    "Perf %",
    "rs3m",
    "trend200",
    "sma200",
    # scores / signals
    "score",
    "confidence",
    "crv",
    "mc_chance",
    "elliott_signal",
    "cycle",
    "cycle_status",
    # liquidity / risk
    "dollar_volume",
    "avg_volume",
    "volatility",
    "max_drawdown",
    # status flags
    "trend_ok",
    "liquidity_ok",
    "is_crypto",
    "score_status",
    "market_date",
    # derived clusters / private metadata
    "cluster_official",
    "pillar_primary",
    "bucket_type",
    "pillar_confidence",
    "pillar_reason",
    "pillar_tags",
    # keep legacy category around for migration / derivation
    "category",
]


def _to_json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    # Ensure JSON-safe primitives (no numpy types)
    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        row: dict[str, Any] = {}
        for k, v in r.items():
            if pd.isna(v):
                row[k] = None
            elif isinstance(v, (bool, int, float, str)):
                row[k] = v
            else:
                # pandas / numpy scalars
                try:
                    row[k] = v.item()  # type: ignore[attr-defined]
                except Exception:
                    row[k] = str(v)
        out.append(row)
    return out


def _render_fallback_tbody(df: pd.DataFrame, limit: int = 250) -> str:
    """Pre-render a simple tbody so the page isn't blank if JS fails.

    JS will replace this immediately on successful load.
    """
    if df.empty:
        return '<tr><td colspan="10" class="muted">Keine Daten.</td></tr>'

    work = df.copy()
    if "score" in work.columns:
        work = work.sort_values(by="score", ascending=False, na_position="last")
    work = work.head(limit)

    def esc(v: Any) -> str:
        s = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        return html.escape(s)

    rows: list[str] = []
    for _, r in work.iterrows():
        price = r.get("price")
        if price is None or (isinstance(price, float) and pd.isna(price)):
            price = r.get("Akt. Kurs")

        rows.append(
            "<tr>"
            f'<td class="mono">{esc(r.get("ticker"))}</td>'
            f'<td>{esc(r.get("name"))}</td>'
            f'<td class="mono right">{esc(price)}</td>'
            f'<td class="mono right">{esc(r.get("score"))}</td>'
            f'<td class="mono right hide-sm">{esc(r.get("confidence"))}</td>'
            f'<td class="mono right hide-sm">{esc(r.get("cycle"))}</td>'
            f'<td class="mono">{esc(r.get("trend_ok"))}</td>'
            f'<td class="mono">{esc(r.get("liquidity_ok"))}</td>'
            f'<td class="mono">{esc(r.get("score_status"))}</td>'
            f'<td class="mono hide-sm">{"CRYPTO" if bool(r.get("is_crypto")) else "STOCK"}</td>'
            "</tr>"
        )
    return "".join(rows)


def build_ui(
    *,
    csv_path: str | Path,
    out_html: str | Path,
    contract_path: str | Path,
    columns: list[str] | None = None,
) -> Path:
    root = project_root()
    csv_path = (root / csv_path) if not Path(csv_path).is_absolute() else Path(csv_path)
    out_html = (root / out_html) if not Path(out_html).is_absolute() else Path(out_html)
    contract_path = (root / contract_path) if not Path(contract_path).is_absolute() else Path(contract_path)

    # If the default ALL view is requested but not yet generated, fall back gracefully.
    if not csv_path.exists():
        candidates = []
        # Prefer ALL -> CORE -> full
        try:
            rel = str(csv_path).replace(str(root) + '/', '')
        except Exception:
            rel = csv_path.as_posix()
        if rel.endswith('artifacts/watchlist/watchlist_ALL.csv'):
            candidates = [root / 'artifacts/watchlist/watchlist_CORE.csv', root / 'artifacts/watchlist/watchlist_full.csv']
        else:
            candidates = [root / 'artifacts/watchlist/watchlist_ALL.csv', root / 'artifacts/watchlist/watchlist_CORE.csv', root / 'artifacts/watchlist/watchlist_full.csv']
        for c in candidates:
            if c.exists():
                csv_path = c
                break
    # Validate contract (fail fast)
    res = validate_csv(csv_path, contract_path)
    if not res.ok:
        msg = "\n".join(["Contract validation failed:"] + [" - " + e for e in res.errors])
        raise RuntimeError(msg)

    df = pd.read_csv(csv_path)

    # Ticker Normalizer: Ensure ticker_display is always a real ticker (not ISIN)
    import re
    isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
    
    def normalize_ticker_fields(row):
        # Priority order for real ticker candidates
        candidates = []
        for col in ['yahoo_symbol', 'YahooSymbol', 'symbol', 'ticker']:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                val = str(row[col]).strip()
                if not isin_pattern.match(val):
                    candidates.append(val)
        
        # Get current ticker_display
        current_td = str(row.get('ticker_display', '')).strip()
        
        # If ticker_display is empty or ISIN-like, replace with first valid candidate
        if not current_td or isin_pattern.match(current_td):
            if candidates:
                row['ticker_display'] = candidates[0]
                # Also fix yahoo_symbol/YahooSymbol if they are empty or ISIN-like
                for col in ['yahoo_symbol', 'YahooSymbol']:
                    if col in row:
                        current_val = str(row[col]).strip()
                        if not current_val or isin_pattern.match(current_val):
                            row[col] = candidates[0]
        
        return row
    
    df = df.apply(normalize_ticker_fields, axis=1)

    cols = columns or DEFAULT_COLUMNS
    # Keep only columns that exist (UI should not crash if optional fields are missing)
    keep = [c for c in cols if c in df.columns]
    df = df[keep].copy()

    # Mild normalization for display
    for c in ("ticker", "ticker_display", "yahoo_symbol", "YahooSymbol", "symbol", "name", "sector", "country", "currency", "score_status"):
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("").str.strip()

    data_records = _to_json_records(df)
    fallback_tbody_html = _render_fallback_tbody(df)
    presets = load_presets()

    # Optional daily briefing text (generated by scripts/generate_briefing.py)
    # Prefer AI-enhanced version if present.
    briefing_text = ""
    briefing_source = ""
    try:
        reports_dir = artifacts_dir() / "reports"
        bp_ai = reports_dir / "briefing_ai.txt"
        bp = reports_dir / "briefing.txt"
        if bp_ai.exists():
            briefing_text = bp_ai.read_text(encoding="utf-8", errors="replace")
            briefing_source = "artifacts/reports/briefing_ai.txt"
        elif bp.exists():
            briefing_text = bp.read_text(encoding="utf-8", errors="replace")
            briefing_source = "artifacts/reports/briefing.txt"
    except Exception:
        briefing_text = ""
        briefing_source = ""

    # Load passive reports (precomputed; never influence scoring)
    history_delta: dict[str, Any] = {}
    segment_monitor: dict[str, Any] = {}
    reality_check: dict[str, Any] = {}
    brief_realities_text = ""
    brief_realities_source = ""
    try:
        reports_dir = artifacts_dir() / "reports"
        hp = reports_dir / "history_delta.json"
        sp = reports_dir / "segment_monitor.json"
        rp = reports_dir / "reality_check.json"
        brp = reports_dir / "briefing_realities.txt"
        if hp.exists():
            history_delta = json.loads(hp.read_text(encoding="utf-8", errors="replace") or "{}")
        if sp.exists():
            segment_monitor = json.loads(sp.read_text(encoding="utf-8", errors="replace") or "{}")
        if rp.exists():
            reality_check = json.loads(rp.read_text(encoding="utf-8", errors="replace") or "{}")
        if brp.exists():
            brief_realities_text = brp.read_text(encoding="utf-8", errors="replace")
            brief_realities_source = "artifacts/reports/briefing_realities.txt"
    except Exception:
        history_delta = {}
        segment_monitor = {}
        reality_check = {}
        brief_realities_text = ""
        brief_realities_source = ""

    # Run meta (UI header): prefer artifacts/reports/briefing.json -> meta.generated_at (fallback: mtime)
    run_at = ''
    run_src = ''
    run_universe = ''
    try:
        reports_dir = artifacts_dir() / 'reports'
        bj = reports_dir / 'briefing.json'
        meta = {}
        if bj.exists():
            obj = json.loads(bj.read_text(encoding='utf-8', errors='replace') or '{}')
            meta = (obj.get('meta') or {}) if isinstance(obj, dict) else {}
        ga = str((meta.get('generated_at') or '')).strip()
        if ga:
            try:
                dt = datetime.fromisoformat(ga.replace('Z', '+00:00')).astimezone(timezone.utc)
                run_at = dt.strftime('%Y-%m-%d %H:%MZ')
            except Exception:
                run_at = ga
        sc = str((meta.get('source_csv') or '')).strip()
        if sc:
            run_src = sc
        uc = meta.get('universe_count', None)
        if uc is not None:
            run_universe = str(uc)
        if not run_at:
            fp = bj if bj.exists() else (reports_dir / 'briefing.txt')
            if fp.exists():
                dt = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)
                run_at = dt.strftime('%Y-%m-%d %H:%MZ')
    except Exception:
        run_at = run_at or ''
        run_src = run_src or ''
        run_universe = run_universe or ''

    html = _render_html(
        data_records=data_records,
        presets=presets,
        source_csv=str(csv_path),
        version=__version__,
        build=__build__,
        briefing_text=briefing_text,
        briefing_source=briefing_source,
        history_delta=history_delta,
        segment_monitor=segment_monitor,
        reality_check=reality_check,
        briefing_realities_text=brief_realities_text,
        briefing_realities_source=brief_realities_source,
        run_at=run_at,
        run_src=run_src,
        run_universe=run_universe,
        fallback_tbody_html=fallback_tbody_html,
    )

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8-sig")

    # Help / project description page (static)
    help_path = out_html.parent / "help.html"
    help_html = _render_help_html(version=__version__, build=__build__)
    help_path.write_text(help_html, encoding="utf-8-sig")

    return out_html


def _render_html(*, data_records: list[dict[str, Any]], presets: dict[str, Any], source_csv: str, version: str, build: str, briefing_text: str, briefing_source: str, history_delta: dict[str, Any], segment_monitor: dict[str, Any], reality_check: dict[str, Any], briefing_realities_text: str, briefing_realities_source: str, run_at: str, run_src: str, run_universe: str, fallback_tbody_html: str) -> str:
    data_json = json.dumps(data_records, ensure_ascii=False)
    presets_json = json.dumps(presets, ensure_ascii=False)
    briefing_json = json.dumps({"text": briefing_text, "source": briefing_source}, ensure_ascii=False)
    history_delta_json = json.dumps(history_delta or {}, ensure_ascii=False)
    segment_monitor_json = json.dumps(segment_monitor or {}, ensure_ascii=False)
    reality_check_json = json.dumps(reality_check or {}, ensure_ascii=False)
    briefing_realities_json = json.dumps({"text": briefing_realities_text, "source": briefing_realities_source}, ensure_ascii=False)

    # Server-side preset <option> fallback (so UI isn't empty if JS fails)
    preset_labels = {
        "ALL": "Alle Werte",
        "CORE": "bersicht",
        "SCORED": "Bewertet",
        "TOP": "Top",
        "TOP_RELAXED": "Top (entspannt)",
        "AVOID": "Vermeiden",
        "BROKEN": "Fehler/NA",
    }
    names = list((presets or {}).keys())
    names.sort(key=lambda k: (0 if k == "ALL" else (1 if k == "CORE" else 2), k))
    opts = []
    for n in names:
        desc = str(((presets.get(n, {}) or {}).get("description", ""))).strip()
        label = preset_labels.get(n, n)
        txt = f"{label} ({n})" + (f"  {desc}" if desc else "")
        opts.append(f'<option value="{html.escape(n)}">{html.escape(txt)}</option>')
    preset_options_html = "\n".join(opts)

    # NOTE: We intentionally avoid Python f-strings for the HTML template because the
    # embedded CSS/JS contains many curly braces. We inject values via simple tokens.
    template = """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Scanner_vNext  Research Dashboard</title>
  <style>
    :root {
      --bg: #0b0f14;
      --card: rgba(17,24,39,.72);
      --muted: #94a3b8;
      --text: #e5e7eb;
      --accent: #60a5fa;
      --good: #34d399;
      --warn: #fbbf24;
      --bad: #fb7185;
      --chip: rgba(31,41,55,.85);
      --border: #243244;
      --shadow: 0 10px 30px rgba(0,0,0,.35);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      --w-ticker: 110px;
      --w-name: 220px;
      --w-price: 130px;
      --w-score: 170px;
      --w-dscore: 110px;
      --w-conf: 70px;
      --w-cycle: 70px;
      --w-trend: 70px;
      --w-liq: 70px;
      --w-status: 120px;
      --w-class: 80px;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: var(--sans); background: radial-gradient(1000px 600px at 10% 0%, rgba(96,165,250,.12), transparent 60%), radial-gradient(800px 500px at 90% 10%, rgba(52,211,153,.10), transparent 55%), var(--bg); color: var(--text); }
    header { padding: 16px 18px; border-bottom: 1px solid var(--border); background: rgba(17,24,39,.72); backdrop-filter: blur(10px); position: sticky; top: 0; z-index: 50; }
    .title { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
    .title h1 { margin: 0; font-size: 18px; font-weight: 700; }
    .meta { color: var(--muted); font-family: var(--mono); font-size: 12px; }
    .helpLink { color: var(--accent); text-decoration: none; }
    .helpLink:hover { text-decoration: underline; }

    .wrap { max-width: 1400px; margin: 0 auto; padding: 18px; }
    .panel { background: var(--card); border: 1px solid var(--border); border-radius: 14px; box-shadow: var(--shadow); }

    /* Briefing box (passive text; must not influence scoring) */
    .briefingBox { border: 1px solid rgba(148,163,184,.15); background: rgba(15,23,42,.35); border-radius: 12px; padding: 10px; min-width: 0; }
    .cardHeader { display:flex; align-items:center; justify-content:space-between; gap:10px; }
    .cardTitle { font-weight: 700; }
    .cardActions { display:flex; align-items:center; justify-content:flex-end; gap:8px; flex-wrap:nowrap; position:relative; }
    .briefHead { display:flex; align-items:center; justify-content: space-between; gap: 10px; margin-bottom: 6px; }
    .briefingBox .muted { margin-bottom: 8px; }
    .briefingText { margin: 0; padding: 10px; border-radius: 10px; border: 1px solid rgba(148,163,184,.12); background: rgba(15,23,42,.55); white-space: pre-wrap; max-height: 300px; overflow: auto; font-family: inherit; line-height: 1.50; font-size: 12px; max-width: 100%; overflow-x: hidden; overflow-wrap: anywhere; word-break: break-word; }
    @media (min-width: 980px) { .briefingText { max-height: 520px; } }

    /* Passive report boxes (History Delta / Segment / Reality) */
    .reportText { margin: 0; padding: 10px; border-radius: 10px; border: 1px solid rgba(148,163,184,.12); background: rgba(15,23,42,.55); white-space: pre-wrap; max-height: 200px; overflow: auto; font-family: inherit; line-height: 1.50; font-size: 12px; max-width: 100%; overflow-x: hidden; overflow-wrap: anywhere; word-break: break-word; }
    @media (min-width: 980px) { .reportText { max-height: 180px; } }


    .controls { display: grid; grid-template-columns: 220px 1fr 220px 220px auto; gap: 12px; padding: 14px; align-items: center; }
    .controls label { font-size: 12px; color: var(--muted); }
    select, input { width: 100%; background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 10px; outline: none; }
    input::placeholder { color: #64748b; }
    .count { justify-self: end; color: var(--muted); font-size: 12px; font-family: var(--mono); }

    .kpis { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .kpis .label { color: var(--muted); font-size: 12px; font-family: var(--mono); flex: 0 0 60px; margin-right: 0; }

    .clusters { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .clusters .label { color: var(--muted); font-size: 12px; font-family: var(--mono); flex: 0 0 60px; margin-right: 0; }
    .clusters .chip { padding: 4px 8px; font-size: 11px; line-height: 1.4; }

    .pillars { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .pillars .label { color: var(--muted); font-size: 12px; font-family: var(--mono); flex: 0 0 60px; margin-right: 0; }
    .pillars .chip { padding: 4px 8px; font-size: 11px; line-height: 1.4; }

    .disclaimer {
      margin: 0 0 12px 0;
      padding: 10px 14px;
      border: 1px solid rgba(251,191,36,.35);
      background: rgba(251,191,36,.08);
      border-radius: 14px;
      display:flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
    }
    .disclaimer b { font-weight: 800; }
    .disclaimer .txt { color: rgba(226,232,240,.95); line-height: 1.35; font-size: 13px; }
    .disclaimer .btn { white-space: nowrap; }
    @media (max-width: 860px) {
      .disclaimer { flex-direction: column; align-items: flex-start; }
    }
  .filters { display:flex; gap: 8px; flex-wrap: wrap; align-items: center; padding: 0 14px 14px 14px; }
    .fbtn { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 6px 10px; border-radius: 999px; cursor: pointer; font-size: 12px; }
    .fbtn:hover { border-color: rgba(96,165,250,.45); }
    .fbtn.active { border-color: rgba(96,165,250,.60); box-shadow: 0 0 0 2px rgba(96,165,250,.14) inset; }
    .fsep { flex: 1; }
    .hintbtn { margin-left: 10px; color: var(--muted); font-size: 12px; display:inline-flex; align-items:center; gap:6px; cursor: pointer; user-select:none; border: 1px solid rgba(148,163,184,.20); background: rgba(148,163,184,.06); padding: 6px 10px; border-radius: 999px; }
    .hintbtn:hover { border-color: rgba(148,163,184,.35); }
    .hintbtn .i { width: 18px; height: 18px; display:inline-flex; align-items:center; justify-content:center; border-radius: 999px; border: 1px solid rgba(148,163,184,.20); background: rgba(148,163,184,.08); font-weight: 700; font-size: 12px; color: #cbd5e1; }

    .popover { position: absolute; z-index: 60; min-width: 280px; max-width: 420px; padding: 12px 12px; border-radius: 14px; border: 1px solid rgba(255,255,255,.12); background: rgba(15,23,42,.98); box-shadow: 0 20px 60px rgba(0,0,0,.55); color: var(--text); display:none; }
    .popover.show { display:block; }
    .popover .title { font-weight: 700; margin-bottom: 6px; }
    .popover .close { float:right; }
    .popover ul { margin: 8px 0 0 16px; padding: 0; }
    .popover li { margin: 6px 0; color: var(--muted); }

    a.yf { color: var(--text); text-decoration: none; border-bottom: 1px solid rgba(96,165,250,.22); }
    a.yf:hover { color: #bfdbfe; border-bottom-color: rgba(96,165,250,.60); }

    table { border-collapse: collapse; }
    .table-wrap { overflow: auto; max-height: 72vh; }
    #tbl { table-layout: fixed; width: max-content; min-width: 100%; }
    #tbl col.col-ticker { width: var(--w-ticker); }
    #tbl col.col-name   { width: var(--w-name); }
    #tbl col.col-price  { width: var(--w-price); }
    #tbl col.col-score  { width: var(--w-score); }
    #tbl col.col-dscore { width: var(--w-dscore); }
    #tbl col.col-conf   { width: var(--w-conf); }
    #tbl col.col-cycle  { width: var(--w-cycle); }
    #tbl col.col-trend  { width: var(--w-trend); }
    #tbl col.col-liq    { width: var(--w-liq); }
    #tbl col.col-status { width: var(--w-status); }
    #tbl col.col-class  { width: var(--w-class); }

    /* Sticky first columns (Ticker + Name) inside the scroll container */
    #tbl thead th:nth-child(1),
    #tbl tbody td:nth-child(1) { position: sticky; left: 0; z-index: 6; }
    #tbl thead th:nth-child(2),
    #tbl tbody td:nth-child(2) { position: sticky; left: var(--w-ticker); z-index: 5; }

    /* Ensure sticky cells are opaque */
    #tbl thead th:nth-child(1),
    #tbl thead th:nth-child(2) { z-index: 12; }
    #tbl tbody td:nth-child(1),
    #tbl tbody td:nth-child(2) { background: rgba(11,15,20,.96); }

    /* Sticky text columns: allow 2 lines, but ellipsis on the main line */
    #tbl th:nth-child(1), #tbl th:nth-child(2) { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    #tbl td:nth-child(1), #tbl td:nth-child(2) { overflow: hidden; }
    .tickerMain, .row-title .name { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    /* Table header is sticky *inside the table scroll container* (not the whole page).
       Therefore top should be 0, otherwise it will "float" too low and hide the first row. */
    thead th { position: sticky; top: 0; background: #0f172a; border-bottom: 1px solid var(--border); border-right: 1px solid rgba(36,50,68,.45); padding: 10px 10px; text-align: left; font-size: 12px; color: #cbd5e1; cursor: pointer; user-select: none; }
    tbody td { border-bottom: 1px solid rgba(36,50,68,.55); border-right: 1px solid rgba(36,50,68,.35); padding: 10px 10px; font-size: 13px; vertical-align: middle; }
    tbody tr:hover td { background: rgba(96,165,250,.07); }

    thead th:last-child, tbody td:last-child { border-right: none; }


    .mono { font-family: var(--mono); }
    .muted { color: var(--muted); }
    .deltaUp { color: var(--good); }
    .deltaDown { color: var(--bad); }
    .deltaFlat { color: var(--muted); }
    .row-title { display:flex; flex-direction:column; gap:2px; }
    .name { font-size: 13px; }
    .sub { font-size: 11px; color: var(--muted); }

    .priceCell { display:flex; flex-direction:column; gap:2px; }
    .priceMain { font-family: var(--mono); }
    .chg { font-size: 11px; }
    .chg.pos { color: var(--good); }
    .chg.neg { color: var(--bad); }
    .chg.flat { color: var(--muted); }

    .tickerCell { display:flex; flex-direction:column; gap:2px; }
    .tickerMain { display:flex; align-items:center; gap:8px; }
    .tinychip { display:inline-flex; align-items:center; padding: 2px 6px; border-radius: 999px; font-size: 10px; border: 1px solid rgba(148,163,184,.18); background: rgba(148,163,184,.08); color: #cbd5e1; }

    .chip { display:inline-flex; align-items:center; gap:6px; padding: 4px 8px; border-radius: 999px; background: var(--chip); border: 1px solid rgba(148,163,184,.15); font-size: 11px; }
    .chip.good { border-color: rgba(52,211,153,.25); color: #a7f3d0; }
    .chip.warn { border-color: rgba(251,191,36,.25); color: #fde68a; }
    .chip.bad  { border-color: rgba(251,113,133,.25); color: #fecdd3; }
    .chip.blue { border-color: rgba(96,165,250,.25); color: #bfdbfe; }


    /* Encoded recommendation signal pill (private code) */
    .sig { display:inline-flex; align-items:center; justify-content:center; min-width: 28px; padding: 2px 8px; border-radius: 999px; font-size: 10px; border: 1px solid rgba(148,163,184,.15); background: rgba(148,163,184,.10); color: #e2e8f0; }
    .sig.good { border-color: rgba(52,211,153,.25); color: #a7f3d0; background: rgba(52,211,153,.08); }
    .sig.warn { border-color: rgba(251,191,36,.25); color: #fde68a; background: rgba(251,191,36,.08); }
    .sig.bad  { border-color: rgba(251,113,133,.25); color: #fecdd3; background: rgba(251,113,133,.08); }
    .sig.blue { border-color: rgba(96,165,250,.25); color: #bfdbfe; background: rgba(96,165,250,.08); }

    /* Bucket matrix (Score  Risk) */
    .matrixPanel { padding: 12px 14px 14px; border-top: 1px solid var(--border); }
    .matrixHead { display:flex; justify-content: space-between; align-items:flex-end; gap: 12px; margin-bottom: 10px; }
    .matrixTitle { font-weight: 700; }
    .matrixLayout { display: grid; grid-template-columns: 1fr; gap: 12px; grid-auto-rows: minmax(0, 1fr); }
    @media (min-width: 980px) { .matrixLayout { grid-template-columns: minmax(0, .70fr) minmax(0, 1.30fr); align-items: stretch; grid-auto-rows: minmax(0, 1fr); } }
    .leftStack { display:flex; flex-direction:column; gap: 24px; min-width: 0; height: 100%; }
    .rightStack { display:flex; flex-direction:column; gap: 12px; min-width: 0; height: 100%; min-height: 0; }
    .rightTopGrid { display:grid; grid-template-columns: 1fr; gap: 12px; align-items: stretch; min-width: 0; height: 100%; min-height: 0; }
    .briefingRealityContent { max-height: 320px; overflow: auto; min-height: 0; }
    .briefingRealitySplit { display: grid; grid-template-columns: minmax(0, 0.85fr) minmax(0, 1.15fr); gap: 12px; }
    .briefingRealitySection { display: flex; flex-direction: column; }
    .sectionTitle { font-weight: 600; font-size: 12px; color: var(--text); margin-bottom: 6px; padding-bottom: 4px; border-bottom: 1px solid rgba(148,163,184,.15); }
    .briefingRealitySection .briefingText,
    .briefingRealitySection .reportText { max-height: none; margin: 0; }
    @media (min-width: 980px) { .briefingRealityContent { max-height: 280px; } }
        @media (max-width: 1200px) { 
      .briefingRealitySplit { grid-template-columns: 1fr; }
    }
    #segmentBox { flex: 1 1 auto; min-height: 0; }
    #segmentText { height: 100%; overflow-y: auto; overflow-x: hidden; }
    
    /* Segment Monitor layout: fill remaining height under Briefing, align with Heatmap */
    #segmentBox .cardBody { display:flex; flex-direction:column; min-height:0; }
    .card.is-collapsed #segmentBox .cardBody { display:none; }
    #segmentText { flex: 1 1 auto; min-height:0; }
    .segmentTables { height: 100%; }

    /* Segment Monitor table: keep headers readable */
    .segmentTable { table-layout: fixed; width: 100%; }
    .segmentTable th, .segmentTable td { padding: 8px 8px; }
    .segmentTable th { white-space: nowrap; }
    .segmentTable th:nth-child(1), .segmentTable td:nth-child(1) { width: 38%; }
    .segmentTable th:nth-child(2), .segmentTable td:nth-child(2) { width: 16%; }
    .segmentTable th:nth-child(3), .segmentTable td:nth-child(3) { width: 14%; }
    .segmentTable th:nth-child(4), .segmentTable td:nth-child(4) { width: 14%; }
    .segmentTable th:nth-child(5), .segmentTable td:nth-child(5) { width: 14%; }
    .segmentTable th:nth-child(6), .segmentTable td:nth-child(6) { width: 4.5em; }

    /* Movers (Market Context)  1D + 1Y lines like old screenshot */
    .moversList { font-family: var(--mono); font-size: 11px; display:flex; flex-direction: column; gap: 8px; }
    .moversItem { display:grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; align-items: start; }
    .moversItem .sym { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 220px; }
    .moversItem .mvVals { display:flex; flex-direction:column; gap: 2px; align-items: flex-end; }
    .moversItem .mvLine { white-space: nowrap; }
    .moversItem .mvLine.pos { color: var(--good); }
    .moversItem .mvLine.neg { color: var(--bad); }
    .moversItem .mvLine.flat { color: var(--muted); }

.segmentTables { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; height: 100%; }
    @media (max-width: 1200px) { .segmentTables { grid-template-columns: 1fr; } }

.segmentTable { width: 100%; border-collapse: collapse; font-size: 11px; }
    .segmentTable th,
    .segmentTable td { padding: 6px 8px; border-bottom: 1px solid rgba(36,50,68,.55); text-align: left; }
    .segmentTable th { background: rgba(15,23,42,.55); color: #cbd5e1; font-weight: 600; position: sticky; top: 0; }
    .segmentTable tr:hover td { background: rgba(96,165,250,.07); }
    .segmentDelta { font-family: var(--mono); font-weight: 600; }
    .segmentDelta.pos { color: var(--good); }
    .segmentDelta.neg { color: var(--bad); }
    .segmentDelta.zero { color: var(--muted); }

    /* Segment Monitor (v4.1): compact table + ellipsis */
    .segmentTable { table-layout: fixed; font-family: var(--mono); font-size: 11px; line-height: 1.25; }
    .segmentTable th { white-space: nowrap; }
    .segmentTable td { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .segmentTable th:nth-child(1), .segmentTable td:nth-child(1) { width: 46%; }
    .segmentTable th:nth-child(2), .segmentTable td:nth-child(2) { width: 14%; }
    .segmentTable th:nth-child(3), .segmentTable td:nth-child(3) { width: 12%; }
    .segmentTable th:nth-child(4), .segmentTable td:nth-child(4) { width: 12%; }
    .segmentTable th:nth-child(5), .segmentTable td:nth-child(5) { width: 12%; }
    .segmentTable th:nth-child(6), .segmentTable td:nth-child(6) { width: 4.5em; }

    .heatControls { display:flex; gap: 8px; align-items:center; }
    .heatControls select { width: auto; min-width: 160px; padding: 8px 10px; border-radius: 10px; }
    /* Insights (collapsed by default) */
    details.insightsDetails { margin-top: 12px; }
    details.insightsDetails > summary {
      cursor: pointer;
      list-style: none;
      user-select: none;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid rgba(148,163,184,.15);
      background: rgba(15,23,42,.30);
      color: var(--text);
      font-weight: 600;
    }
    details.insightsDetails > summary::-webkit-details-marker { display: none; }
    details.insightsDetails[open] > summary { background: rgba(15,23,42,.45); }
    .insightsGrid { display: grid; grid-template-columns: 1fr; gap: 12px; margin-top: 12px; }
    @media (min-width: 980px) { .insightsGrid { grid-template-columns: 1fr 1fr; align-items: start; } }


    .matrixGrid { display: grid; grid-template-columns: 84px repeat(5, 1fr); gap: 4px; }
    .matrixLabel { font-size: 10px; color: var(--muted); display:flex; flex-direction:column; align-items:center; justify-content:center; padding: 4px 0; line-height: 1.05; text-align: center; }
    .matrixLabel .lbl { font-family: var(--mono); color: #cbd5e1; }
    .matrixLabel .hint { font-size: 9px; color: var(--muted); }
    .matrixAxis { display:flex; flex-direction:column; align-items:center; justify-content:center; gap:2px; padding: 6px 4px; border-radius: 10px; border: 1px dashed rgba(148,163,184,.18); background: rgba(148,163,184,.04); }
    .matrixAxis .lbl { font-weight: 700; color: #e2e8f0; }
    .matrixAxis .hint { font-size: 9px; color: var(--muted); font-family: var(--mono); }
    .cell { background: rgba(148,163,184,.06); border: 1px solid rgba(148,163,184,.15); border-radius: 9px; min-height: 28px; display:flex; align-items:center; justify-content:center; cursor: pointer; user-select:none; transition: border-color .12s ease, transform .06s ease; }
    .cell:hover { border-color: rgba(96,165,250,.45); }
    .cell.active { box-shadow: 0 0 0 2px rgba(96,165,250,.25) inset; }
    .cell.zero { opacity: .45; cursor: default; }
    .cell .cnt { font-family: var(--mono); font-size: 11px; }
    .matrixNote { margin-top: 8px; color: var(--muted); font-size: 11px; }

    /* Heatmap styling like Bucket matrix */
    .heatWrap .matrixGrid .cell { background: hsla(205, 70%, 50%, 0.06); }
    .heatWrap .matrixGrid .cell:hover { border-color: rgba(96,165,250,.45); }
    .heatWrap .matrixGrid .cell.active { box-shadow: 0 0 0 2px rgba(96,165,250,.25) inset; }

    /* Info button and popover */
    .iBtn { display:inline-flex; align-items:center; justify-content:center; width: 18px; height: 18px; border-radius: 999px; border: 1px solid rgba(148,163,184,.20); background: rgba(148,163,184,.08); color: #cbd5e1; font-size: 12px; font-weight: 700; cursor: pointer; user-select: none; transition: all .12s ease; }
    .iBtn:hover { border-color: rgba(96,165,250,.45); background: rgba(96,165,250,.12); }
    .iPop { position: absolute; top: 100%; right: 0; margin-top: 4px; min-width: 200px; max-width: 280px; padding: 8px 10px; border-radius: 8px; border: 1px solid rgba(148,163,184,.20); background: rgba(15,23,42,.95); color: var(--text); font-size: 11px; line-height: 1.4; z-index: 100; display: none; box-shadow: 0 4px 12px rgba(0,0,0,.3); }
    .card.is-collapsed .cardBody { display:none; }
    .debugInfo, .renderProof { display: none; }



/* Market Context (Finviz-inspired patterns, scanner-owned data & logic) */
.marketPanel { padding: 12px 14px 14px; border-top: 1px solid var(--border); }
.marketHead { display:flex; justify-content: space-between; align-items:flex-end; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
.marketHead select { width: auto; min-width: 170px; padding: 8px 10px; border-radius: 10px; }
.marketGrid { display:grid; grid-template-columns: 1fr; gap: 12px; }
@media (min-width: 980px) { .marketGrid { grid-template-columns: 1fr 1fr 1.2fr; align-items: stretch; } }

.marketCard { border: 1px solid rgba(148,163,184,.15); background: rgba(15,23,42,.35); border-radius: 12px; padding: 10px; display:flex; flex-direction:column; min-height:0; }
.marketCardTitle { font-weight: 700; margin-bottom: 8px; }
.breadthRow { display:flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 6px; }
.breadthLvl1 { display:flex; flex-direction:column; gap: 6px; margin-bottom: 8px; }
.breadthHeadline { display:flex; align-items:center; gap: 8px; font-weight: 700; }
.breadthHeadline .ampel { width: 10px; height: 10px; border-radius: 999px; display:inline-block; }
.breadthHeadline .ampel.riskOn { background: var(--good); box-shadow: 0 0 0 2px rgba(52,211,153,.18); }
.breadthHeadline .ampel.mixed { background: var(--warn); box-shadow: 0 0 0 2px rgba(251,191,36,.18); }
.breadthHeadline .ampel.riskOff { background: var(--bad); box-shadow: 0 0 0 2px rgba(251,113,133,.18); }
.breadthPct { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }
.breadthDetails { margin-top: 6px; }
.breadthSummary { cursor: pointer; list-style: none; display:inline-flex; align-items:center; border: 1px solid rgba(148,163,184,.20); border-radius: 999px; padding: 4px 10px; font-size: 12px; color: var(--text); background: rgba(148,163,184,.06); user-select: none; }
.breadthSummary::-webkit-details-marker { display: none; }
.breadthDetailsBody { margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(148,163,184,.15); display:flex; flex-direction:column; gap: 8px; }

.moversGrid { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }

/* History Delta: fill half Market-Card height with scrollbar */
#historyCard { min-height: 0; }
#historyCard #historyText { flex: 1 1 auto; min-height: 0; max-height: 280px; overflow: auto; }

/* Divider zwischen Top und Weak (Grid-Spalten) */
.moversGrid > div + div {
  border-left: 1px solid rgba(148,163,184,.12);
  padding-left: 10px;
}
.moversList { font-family: var(--mono); font-size: 11px; display:flex; flex-direction: column; gap: 6px; }
.moversItem { display:flex; justify-content: space-between; gap: 10px; }
.moversItem .sym { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 160px; }
.moversItem .val { white-space: nowrap; }
.moversItem .val.pos { color: var(--good); }
.moversItem .val.neg { color: var(--bad); }
.moversItem .val.flat { color: var(--muted); }

.heatWrap { overflow:auto; }
.heatTbl { width: 100%; border-collapse: collapse; font-size: 11px; }
.heatTbl th, .heatTbl td { border: 1px solid rgba(36,50,68,.40); padding: 6px 6px; }
.heatTbl th { background: rgba(15,23,42,.55); color: #cbd5e1; position: sticky; top: 0; z-index: 2; }
.heatTbl th:first-child { left: 0; z-index: 3; }
.heatTbl td:first-child { position: sticky; left: 0; background: rgba(11,15,20,.96); z-index: 1; white-space: nowrap; }
.heatTbl td { cursor: pointer; }
.heatTbl td:hover { outline: 1px solid rgba(96,165,250,.45); }
.heatTbl td.heatCell.active { outline: 2px solid rgba(251,191,36,.75); box-shadow: 0 0 0 2px rgba(0,0,0,.25) inset; }
.heatTbl td.heatRowLabel.active { font-weight: 700; color: var(--text); }

.heatCell { text-align: center; font-family: var(--mono); }
.heatCell.zero { color: rgba(148,163,184,.55); }

    /* KPI chips are clickable quick-filters */
    button.chip { appearance: none; -webkit-appearance: none; display:inline-flex; align-items:center; gap:6px; padding: 4px 8px; border-radius: 999px; background: var(--chip); border: 1px solid rgba(148,163,184,.15); color: var(--text); font-size: 11px; cursor: pointer; white-space: nowrap; }
    button.chip.kpi { cursor: pointer; }
    button.chip.kpi:hover { border-color: rgba(96,165,250,.45); }
    button.chip.kpi.active { box-shadow: 0 0 0 2px rgba(96,165,250,.25) inset; }

    /* KPI chips same size as normal chips */
    button.chip.kpi { padding: 4px 8px; font-size: 11px; line-height: 1.4; }

    .jsError { display:none; margin: 10px 14px 0 14px; padding: 10px 12px; border-radius: 14px; background: rgba(251,113,133,.08); border: 1px solid rgba(251,113,133,.25); color: #fecdd3; font-family: var(--mono); font-size: 12px; }
    .jsError.show { display:block; }

    .scorebar { width: 120px; height: 10px; border-radius: 999px; background: rgba(148,163,184,.15); overflow: hidden; border: 1px solid rgba(148,163,184,.10); }
    .scorebar > div { height: 100%; border-radius: 999px; background: linear-gradient(90deg, rgba(96,165,250,.9), rgba(52,211,153,.9)); }
    .scorecell { display:flex; align-items:center; gap:10px; }

    .right { text-align: right; }
    .small { font-size: 11px; }

    .footer { padding: 10px 14px; color: var(--muted); font-size: 12px; display:flex; justify-content: space-between; border-top: 1px solid var(--border); }
    .kbd { font-family: var(--mono); background: rgba(148,163,184,.12); border: 1px solid rgba(148,163,184,.18); padding: 2px 6px; border-radius: 6px; }

    .overlay { position: fixed; inset: 0; background: rgba(0,0,0,.6); display: none; align-items: flex-end; justify-content: center; padding: 18px; z-index: 90; }
    .drawer { width: min(720px, 96vw); max-height: 88vh; overflow: auto; }
    .drawer-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; padding: 14px; border-bottom: 1px solid var(--border); }
    .drawer-actions { display: flex; gap: 8px; align-items: center; }
    .drawer-title { font-weight: 700; }
    .drawer-body { padding: 14px; }
    .btn { display:inline-flex; align-items:center; gap:6px; padding: 4px 8px; border-radius: 999px; background: var(--chip); border: 1px solid rgba(148,163,184,.15); color: var(--text); font-size: 11px; cursor: pointer; white-space: nowrap; }
    .btn:hover { border-color: rgba(96,165,250,.45); }
    .kv { display: grid; grid-template-columns: 160px 1fr; gap: 6px 12px; }
    .kv div { padding: 4px 0; border-bottom: 1px dashed rgba(148,163,184,.15); }
    .kv .k { color: var(--muted); font-size: 12px; }
    .kv .v { font-family: var(--mono); }
    .why { margin-top: 12px; }
    .why ul { margin: 6px 0 0 18px; padding: 0; }

    @media (max-width: 860px) {
      .controls { grid-template-columns: 1fr; }
      #tbl thead th:nth-child(2),
      #tbl tbody td:nth-child(2) { position: static; }
      thead th { top: 0; }
      .count { justify-self: start; }
      .hide-sm { display:none; }
    }

    /* Briefing & Reality Check Panel Styles */
    .briefingPick {
      border: 1px solid rgba(148,163,184,.15);
      border-radius: 14px;
      padding: 10px 12px;
      margin-bottom: 10px;
      background: rgba(15,23,42,.35);
      box-shadow: 0 2px 8px rgba(0,0,0,.15);
    }
    .briefingPickHeader {
      display: flex;
      gap: 8px;
      align-items: baseline;
      padding: 6px 8px;
      border: 1px solid rgba(251,191,36,.28);
      background: rgba(251,191,36,.06);
      border-radius: 8px;
      margin-bottom: 8px;
    }
    .briefingPickRank {
      font-weight: 700;
      color: var(--warn);
      font-family: var(--mono);
    }
    .briefingPickSymbol {
      font-weight: 600;
      color: var(--text);
      font-family: var(--mono);
    }
    .briefingPickName {
      color: var(--text);
    }
    .briefingPickBadges {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 8px;
      align-items: center;
    }
    .briefingBadge {
      display: inline-flex;
      align-items: center;
      padding: 3px 6px;
      border-radius: 6px;
      font-size: 10px;
      font-weight: 600;
      background: rgba(15,23,42,.55);
      color: var(--muted);
      border: 1px solid rgba(148,163,184,.15);
      max-width: 100%;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .briefingPickReasons {
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .briefingPickReasons li {
      position: relative;
      padding-left: 12px;
      margin-bottom: 2px;
      font-size: 11px;
      color: var(--muted);
      line-height: 1.3;
    }
    .briefingPickReasons li:before {
      content: "";
      position: absolute;
      left: 0;
      color: var(--accent);
    }
    .briefingFallback {
      border: 1px solid rgba(148,163,184,.15);
      border-radius: 12px;
      padding: 10px;
      background: rgba(15,23,42,.35);
    }
    .briefingLine {
      margin-bottom: 6px;
      line-height: 1.4;
    }
    .briefingBullet {
      margin-bottom: 4px;
      padding-left: 12px;
      position: relative;
      font-size: 11px;
      color: var(--muted);
    }
    .briefingBullet:before {
      content: "";
      position: absolute;
      left: 0;
      color: var(--accent);
    }
    .realityTable {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      margin-top: 8px;
    }
    .realityTable th,
    .realityTable td {
      padding: 8px;
      border-bottom: 1px solid rgba(36,50,68,.55);
      text-align: left;
      font-size: 11px;
    }
    .realityTable th {
      background: rgba(15,23,42,.55);
      color: #cbd5e1;
      font-weight: 600;
      position: sticky;
      top: 0;
    }
    .realityTable tr:hover td {
      background: rgba(96,165,250,.07);
    }
    /* Responsive improvements for small screens */
    @media (max-width: 1200px) {
      .realityTable th,
      .realityTable td {
        line-height: 1.4;
      }
      .realityTable th {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
    }
    @media (max-width: 900px) {
      .realityTable th,
      .realityTable td {
        padding: 6px;
        font-size: 10px;
      }
    }
    .signalBadge {
      display: inline-flex;
      align-items: center;
      padding: 2px 6px;
      border-radius: 999px;
      font-size: 10px;
      font-weight: 600;
      white-space: nowrap;
    }
    .signalBadge.positive {
      background: rgba(52,211,153,.08);
      border: 1px solid rgba(52,211,153,.25);
      color: #a7f3d0;
    }
    .signalBadge.neutral {
      background: rgba(251,191,36,.08);
      border: 1px solid rgba(251,191,36,.25);
      color: #fde68a;
    }
    .signalBadge.contra {
      background: rgba(251,113,133,.08);
      border: 1px solid rgba(251,113,133,.25);
      color: #fecdd3;
    }
    .realitySummary {
      display: flex;
      gap: 6px;
      margin-bottom: 8px;
    }
    .summaryChip {
      display: inline-flex;
      align-items: center;
      padding: 2px 6px;
      border-radius: 999px;
      font-size: 10px;
      font-weight: 600;
    }
    .summaryChip.ok {
      background: rgba(52,211,153,.08);
      border: 1px solid rgba(52,211,153,.25);
      color: #a7f3d0;
    }
    .summaryChip.warn {
      background: rgba(251,191,36,.08);
      border: 1px solid rgba(251,191,36,.25);
      color: #fde68a;
    }
    .summaryChip.error {
      background: rgba(251,113,133,.08);
      border: 1px solid rgba(251,113,133,.25);
      color: #fecdd3;
    }

    /* Unified Help Popover System */
    .helpPop {
      position: fixed;
      z-index: 9999;
      max-width: 360px;
      background: rgba(15,23,42,.95);
      border: 1px solid rgba(148,163,184,.20);
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(0,0,0,.4);
      padding: 0;
    }
    .helpPopInner {
      padding: 12px 14px;
    }
    .helpPopTitle {
      font-weight: 700;
      color: var(--text);
      margin-bottom: 8px;
      font-size: 13px;
    }
    .helpPopBody {
      color: var(--text);
      font-size: 12px;
      line-height: 1.5;
    }
    .helpPopBody ul {
      margin: 8px 0 0 16px;
      padding: 0;
    }
    .helpPopBody li {
      margin-bottom: 4px;
    }
    .hidden {
      display: none;
    }
    .helpPop.show {
      display: block;
    }

    /* Dark scrollbars */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: rgba(15,23,42,.3); border-radius: 4px; }
    ::-webkit-scrollbar-thumb { background: rgba(148,163,184,.3); border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: rgba(148,163,184,.5); }
    
    /* Let the briefing breathe a bit more on larger screens */
    @media (min-width: 980px) {
      .briefingText { max-height: 520px; }
    }

    /* History Delta (Market Context): strukturiert + scanbar */
#historyCard #historyText { white-space: normal; }

.hdWrap { display:flex; flex-direction:column; gap: 10px; }
.hdMeta { color: var(--muted); font-family: var(--mono); font-size: 11px; }

.hdGrid { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
.hdColTitle { color: var(--muted); font-size: 11px; margin-bottom: 6px; }
.hdList { display:flex; flex-direction:column; gap: 6px; }

.hdItem { display:grid; grid-template-columns: 72px 1fr 100px; gap: 8px; align-items:center; }
.hdSym { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

.hdVals { display:flex; flex-direction:column; gap:2px; align-items:flex-end; font-family: var(--mono); font-size: 11px; line-height: 1.15; }
.hdLine { white-space:nowrap; }
.hdLine.pos { color: var(--good); }
.hdLine.neg { color: var(--bad); }
.hdLine.flat { color: var(--muted); }

.hdSeg {
  justify-self:end;
  width: 100px; max-width: 100px;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
  padding: 2px 6px;
  border-radius: 999px;
  border: 1px solid rgba(148,163,184,.18);
  background: rgba(15,23,42,.55);
  color: rgba(226,232,240,.85);
  font-size: 10px;
  font-family: var(--mono);
}

@media (max-width: 980px) { .hdGrid { grid-template-columns: 1fr; } }

    /* MOVERS_LAYOUT_v4_2: Symbol | 1D/1Y | Segment pill (right) */
    .moversList { font-family: var(--mono); font-size: 11px; display:flex; flex-direction:column; gap:8px; }

    .moversItem {
      display: grid;
      grid-template-columns: 72px 78px 40px;
      gap: 4px;
      align-items: center;
    }

    .moversItem .mvSym { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .moversItem .mvVals { display:flex; flex-direction:column; gap:2px; align-items:flex-end; line-height:1.15; }
    .moversItem .mvLine { white-space: nowrap; }
    .moversItem .mvLine.pos { color: var(--good); }
    .moversItem .mvLine.neg { color: var(--bad); }
    .moversItem .mvLine.flat { color: var(--muted); }

    .moversItem .mvSeg {
      justify-self: end;
      width: 40px; max-width: 40px;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      padding: 2px 6px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,.18);
      background: rgba(15,23,42,.55);
      color: rgba(226,232,240,.85);
      font-size: 10px;
    }

    /* Dezente Trennlinie zwischen Top und Weak */
    .moversSection {
      position: relative;
    }
    .moversSection:not(:last-child)::after {
      content: '';
      position: absolute;
      bottom: -4px;
      left: 0;
      right: 0;
      height: 1px;
      background: rgba(148,163,184,.15);
    }

    /* Mobile: Segment unter die Zeile */
    @media (max-width: 900px) {
      .moversItem { grid-template-columns: 1fr 82px; grid-template-rows: auto auto; }
      .moversItem .mvSeg { grid-column: 1 / -1; justify-self: start; width:auto; max-width:100%; }
    }
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <div class="title">
        <h1>Scanner_vNext  Research Dashboard</h1>
        <div class="meta">version __VERSION__ · build __BUILD__ · <span title="Quelle: __RUN_SRC__">run __RUN_AT__</span> · universe __RUN_UNIVERSE__ · <a class=\"helpLink\" href=\"help.html\" target=\"_blank\" rel=\"noopener\">Hilfe / Projektbeschreibung</a></div>
      </div>
    </div>
  </header>

  <div class="wrap">
    <div class="disclaimer" id="disclaimer">
      <div class="txt"><b>Privates, experimentelles Projekt.</b> Keine Anlageberatung, keine Gewähr. Nutzung auf eigene Verantwortung.</div>
      <button type="button" class="btn" id="discOk" title="Hinweis ausblenden">Verstanden</button>
    </div>
    <div id="jsErrorBanner" class="jsError"></div>

    <div class="panel">
      <div class="controls">
        <div>
          <label for="preset">Preset</label>
          <select id="preset">__PRESET_OPTIONS__</select>
        </div>
        <div>
          <label for="search">Suche (Symbol/Name/Kategorie/Land)</label>
          <input id="search" placeholder="z.B. NVDA, Gold, Deutschland, Krypto"/>
        </div>
        <div>
          <label for="clusterSel">Cluster/Sektor</label>
          <select id="clusterSel"><option value="">Alle</option></select>
        </div>
        <div>
          <label for="pillarSel">Säule (5Säulen/Playground)</label>
          <select id="pillarSel"><option value="">Alle</option></select>
        </div>
        <div class="count" id="count"></div>
      </div>

      <div class="kpis" id="kpis"></div>
      <div class="pillars" id="pillars"></div>
      <div class="clusters" id="clusters"></div>

      <div class="filters" id="filters">
        <button type="button" class="fbtn active" data-action="toggle" data-key="hideAvoid" title="AVOID-Zeilen ausblenden (score_status beginnt mit AVOID_)">AVOID ausblenden</button>
        <button type="button" class="fbtn" data-action="toggle" data-key="onlyOK" title="Nur score_status = OK anzeigen">Nur OK</button>
        <button type="button" class="fbtn" data-action="toggle" data-key="trendOK" title="Nur trend_ok = true anzeigen">Trend OK</button>
        <button type="button" class="fbtn" data-action="toggle" data-key="liqOK" title="Nur liquidity_ok = true anzeigen">Liq OK</button>
        <span class="fsep"></span>
        <button type="button" class="iBtn"
          data-action="help"
          data-help-title="Quick-Filter"
          data-help-html="<ul>
            <li><strong>AVOID ausblenden:</strong> Versteckt Zeilen, deren <span class='mono'>score_status</span> mit <span class='mono'>AVOID_</span> beginnt.</li>
            <li><strong>Nur OK:</strong> Zeigt nur Zeilen mit <span class='mono'>score_status = OK</span>.</li>
            <li><strong>Trend OK:</strong> Zeigt nur Zeilen mit <span class='mono'>trend_ok = true</span>.</li>
            <li><strong>Liq OK:</strong> Zeigt nur Zeilen mit <span class='mono'>liquidity_ok = true</span>.</li>
            <li><em>Hinweis:</em> Diese Filter beeinflussen nur die angezeigte Liste (Universe), nicht das Scoring.</li>
          </ul>"
          aria-haspopup="dialog" aria-expanded="false">i</button>
        <button type="button" class="fbtn" data-action="toggle" data-key="onlyStock" title="Nur Aktien (is_crypto = false)">Aktien</button>
        <button type="button" class="fbtn" data-action="toggle" data-key="onlyCrypto" title="Nur Krypto (is_crypto = true)">Krypto</button>
        <button type="button" class="fbtn" data-action="resetSort" title="Nur Sort-Override löschen (Preset-Sort bleibt)">Sortierung zurück</button>
        <button type="button" class="fbtn" data-action="resetAll" title="Alles zurücksetzen (Preset, Suche, Filter, Sort & Persistenz)">Reset</button>
      </div>
      </div>

      <div class="matrixPanel" id="matrixPanel">
        <div class="matrixLayout">
  <div class="leftStack">
    <section class="card" data-panel="bucket">
      <div class="briefingBox">
        <div class="cardHeader">
          <div class="cardTitle">BucketMatrix (Score  Risk)</div>
          <div class="cardActions">
            <button type="button" class="btn btnToggle" data-toggle="bucket">Ausblenden</button>
            <button type="button" class="btn" id="matrixClear" title="MatrixFilter zurücksetzen">Reset</button>
            <button type="button" class="iBtn" data-help-title="Bucket-Matrix" data-help-html="<ul><li><strong>Achsen:</strong> Score () vs Risk () Verteilung</li><li><strong>Klick:</strong> Setzt Matrix-Filter zusätzlich zu Preset/Suche</li><li><strong>Reset:</strong> Matrix-Filter löschen oder Preset zurücksetzen</li></ul>" aria-haspopup="dialog" aria-expanded="false">i</button>
          </div>
        </div>
        <div class="cardBody" data-body="bucket">
          <div class="muted small">Klick auf ein Feld = MatrixFilter (zusätzlich zu Preset/Suche/QuickFilter).</div>
          <div class="matrixGrid" id="matrix"></div>
          <div class="matrixNote" id="matrixNote"></div>
        </div>
      </div>
    </section>

    <section class="card" data-panel="heatmap">
      <div class="briefingBox" id="heatCardLeft">
        <div class="cardHeader">
          <div class="cardTitle">Heatmap</div>
          <div class="cardActions">
            <div class="heatControls">
              <select id="heatMode" title="Heatmap-Modus">
                <option value="pillar">Heatmap: Säulen</option>
                <option value="cluster">Heatmap: Cluster</option>
              </select>
            </div>
            <button type="button" class="btn btnToggle" data-toggle="heatmap">Ausblenden</button>
            <button type="button" class="btn" id="heatmapClear" title="HeatmapFilter zurücksetzen">Reset</button>
            <button type="button" class="iBtn" data-help-title="Heatmap" data-help-html="<ul><li><strong>Was:</strong> Verteilung nach Score-Buckets</li><li><strong>Klick Zeile:</strong> Filtert Säule/Cluster</li><li><strong>Klick Zelle:</strong> Filtert Zeile + Score-Bucket</li><li><strong>Modus:</strong> Säule/Cluster umschalten</li><li><strong>Reset:</strong> Filter löschen</li></ul>" aria-haspopup="dialog" aria-expanded="false">i</button>
          </div>
        </div>
        <div class="cardBody" data-body="heatmap">
          <div class="muted small">Klick auf ein Feld = HeatmapFilter (zusätzlich zu Preset/Suche/QuickFilter).</div>
          <div id="heatmap" class="heatWrap"></div>
        </div>
      </div>
    </section>
  </div>

  <div class="rightStack">
    <div class="rightTopGrid">
      <section class="card" data-panel="briefingReality">
        <div class="briefingBox" id="briefingRealityBox">
          <div class="cardHeader">
            <div class="cardTitle">Briefing & Reality Check</div>
            <div class="cardActions">
              <button type="button" class="btn btnToggle" data-toggle="briefingReality">Ausblenden</button>
              <button type="button" class="iBtn" data-help-title="Briefing & Reality Check" data-help-html="<ul><li><strong>Briefing:</strong> Top-3 Picks aus aktuellem Scanner-Run (passiver Report)</li><li><strong>Badges:</strong> Score, Percentil, Bucket, Confidence, Trend, Liquidität</li><li><strong>Reality:</strong> Daten-Mapping Check (intern vs. Yahoo/Markt)</li><li><strong>Warn/Error:</strong> Zeigen Datenqualitätsprobleme</li></ul>" aria-haspopup="dialog" aria-expanded="false">i</button>
            </div>
          </div>
          <div class="cardBody" data-body="briefingReality">
            <div class="muted small">Briefing: Privat/experimentell · Reality Check: Daten-/Mapping-Qualität</div>
            <div class="briefingRealityContent">
              <div class="briefingRealitySplit">
                <div class="briefingRealitySection">
                  <div class="sectionTitle">Briefing</div>
                  <div id="briefingText" class="briefingText"></div>
                </div>
                <div class="briefingRealitySection">
                  <div class="sectionTitle">Reality Check</div>
                  <div id="realityText" class="reportText"></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>

    <section class="card" data-panel="segment">
      <div class="briefingBox segmentBox" id="segmentBox">
        <div class="cardHeader">
          <div class="cardTitle">Segment Monitor</div>
          <div class="cardActions">
            <button type="button" class="btn btnToggle" data-toggle="segment">Ausblenden</button>
            <button type="button" class="iBtn" data-help-title="Segment Monitor" data-help-html="<ul><li><strong>Segment:</strong> Säule/Cluster/Bucket Kombination</li><li><strong>Changed:</strong> Anzahl veränderter Segmente vs. letzter Snapshot</li><li><strong>Snapshot:</strong> Vergleichszeitpunkt</li><li><strong>Shifts:</strong> Zeigen wo sich Cluster neu bilden oder auflösen</li></ul>" aria-haspopup="dialog" aria-expanded="false">i</button>
          </div>
        </div>
        <div class="cardBody" data-body="segment">
          <div class="muted small">Säulen/Cluster/Bucket · inkl. nderungen vs. letzter Snapshot.</div>
          <div id="segmentText" class="reportText"></div>
        </div>
      </div>
    </section>

  </div>
</div>


<div class="marketPanel" id="marketPanel">
  <div class="marketHead">
    <div>
      <div class="matrixTitle">Market Context</div>
      <div class="muted small">Passiv aus deiner Watchlist (kein Einfluss auf Scoring) · Basis: gefiltertes Universe (Preset/Suche/Quick/Cluster/Säule)</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap: wrap;">
      <button type="button" class="btn" id="marketToggle" data-action="togglePanel" data-target="market" title="Market Context ein-/ausblenden">Ausblenden</button>
    </div>
  </div>

  <div id="marketBody" class="marketGrid">
    <div class="marketCard" id="breadthCard">
      <div class="marketCardTitle" style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <span>Breadth</span>
        <button type="button" class="iBtn"
          data-action="help"
          data-help-title="Breadth"
          data-help-html="<ul>
            <li><strong>Median:</strong> typischer Wert.</li>
            <li><strong>IQR:</strong> Streuung (wie gemischt es ist).</li>
            <li><strong>Perzentil:</strong> wie weit oben im Feld.</li>
            <li><strong>Trend OK:</strong> Anteil mit positivem Trend-Flag.</li>
            <li><strong>Liq OK:</strong> Anteil mit ausreichender Handelbarkeit.</li>
          </ul>"
          aria-haspopup="dialog" aria-expanded="false">i</button>
      </div>
      <div id="breadthBox"></div>
    </div>

    <div class="marketCard" id="moversCard">
      <div class="marketCardTitle" style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <span>Movers</span>
        <button type="button" class="iBtn"
          data-action="help"
          data-help-title="Movers"
          data-help-html="<ul>
            <li><strong>Was:</strong> Top/Weak Tages-Performer im aktuellen Universe.</li>
            <li><strong>1D:</strong> <span class='mono'>Perf %</span> (nur Anzeige/Context).</li>
            <li><strong>1Y:</strong> falls vorhanden (<span class='mono'>Perf 1Y %</span>), sonst n/a.</li>
            <li><strong>Segment:</strong> offizielles Cluster/Industry/Sector (gekürzt; Hover zeigt voll).</li>
          </ul>"
          aria-haspopup="dialog" aria-expanded="false">i</button>
      </div>
      <div class="moversGrid">
        <div>
          <div class="muted small">Top </div>
          <div id="moversUp" class="moversList"></div>
        </div>
        <div>
          <div class="muted small">Weak </div>
          <div id="moversDown" class="moversList"></div>
        </div>
      </div>
    </div>

    <div class="marketCard" id="historyCard">
      <div class="marketCardTitle" style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <span>History Delta</span>
        <button type="button" class="iBtn"
          data-action="help"
          data-help-title="History Delta"
          data-help-html="<ul>
            <li><strong>Was:</strong> nderungen vs. letzter Snapshot (passiv, kein Einfluss auf Scoring).</li>
            <li><strong>S:</strong> Score-Delta aus <span class='mono'>history_delta.json</span>.</li>
            <li><strong>R:</strong> Rank-Delta (falls vorhanden).</li>
            <li><strong>Filter:</strong> Respektiert das aktuelle Universe (Preset/Suche/Quick/Cluster/Säule).</li>
          </ul>"
          aria-haspopup="dialog" aria-expanded="false">i</button>
      </div>
      <div class="muted small">Score/Rank-nderungen vs. letzter Snapshot.</div>
      <div id="historyText" class="reportText"></div>
    </div>
  </div>
</div>

      <div class="table-wrap">
        <table id="tbl">
          <colgroup>
            <col class="col-ticker"/>
            <col class="col-name"/>
            <col class="col-price"/>
            <col class="col-score"/>
            <col class="col-dscore"/>
            <col class="col-conf"/>
            <col class="col-cycle"/>
            <col class="col-trend"/>
            <col class="col-liq"/>
            <col class="col-status"/>
            <col class="col-class"/>
          </colgroup>
          <thead>
            <tr>
              <th data-k="ticker" title="Anzeige-Symbol (oben) + ISIN (unten) und ggf. Quote-Währung">Symbol/ISIN</th>
              <th data-k="name" title="Name + Kategorie/Land/Währung">Name</th>
              <th data-k="price" class="right" title="Aktueller Kurs (Originalwährung) + Tagesänderung (Perf %)">Kurs</th>
              <th data-k="score" class="right" title="Gesamtscore (höher = besser)">Score</th>
              <th data-k="dscore_1d" class="hide-sm right" title="ScoreVeränderung vs. letzter Snapshot (History Delta)">dScore 1D</th>
              <th data-k="confidence" class="hide-sm right" title="Confidence/Vertrauen in das Scoring">Konf</th>
              <th data-k="cycle" class="hide-sm right" title="Zyklus in % (ca. 50 = neutral)">Zyklus</th>
              <th data-k="trend_ok" title="Trend-Filter (z.B. Trend200 > 0)">Trend</th>
              <th data-k="liquidity_ok" title="Liquiditäts-Filter (z.B. DollarVolume/AvgVolume)">Liq</th>
              <th data-k="score_status" title="OK / AVOID / AVOID_CRYPTO_BEAR / NA / ERROR">Status</th>
              <th data-k="is_crypto" class="hide-sm" title="Assetklasse">Art</th>
            </tr>
          </thead>
          <tbody>__FALLBACK_TBODY__</tbody>
        </table>
      </div>

      <div class="footer">
        <div>Tipp: <span class="kbd">Klick</span> Header = Sortierung · <span class="kbd">Esc</span> = Suche leeren</div>
        <div class="mono" id="sortHint"></div>
      </div>
    </div>
  </div>

  <div id="drawerOverlay" class="overlay" aria-hidden="true">
    <div class="drawer panel" role="dialog" aria-modal="true" aria-label="Why Score">
      <div class="drawer-head">
        <div>
          <div id="drawerTitle" style="font-weight:700;"></div>
          <div id="drawerSub" class="muted small"></div>
        </div>
        <div class="drawer-actions">
          <div id="drawerActions"></div>
          <button class="btn" id="drawerClose">Schlieen</button>
        </div>
      </div>
      <div class="drawer-body" id="drawerBody"></div>
    </div>
  </div>

  <script id="DATA" type="application/json">__DATA_JSON__</script>
  <script id="PRESETS" type="application/json">__PRESETS_JSON__</script>
  <script id="BRIEFING" type="application/json">__BRIEFING_JSON__</script>
  <script id="HISTORY_DELTA" type="application/json">__HISTORY_DELTA_JSON__</script>
  <script id="SEGMENT_MONITOR" type="application/json">__SEGMENT_MONITOR_JSON__</script>
  <script id="REALITY_CHECK" type="application/json">__REALITY_CHECK_JSON__</script>
  <script id="BRIEFING_REALITIES" type="application/json">__BRIEFING_REALITIES_JSON__</script>

  <script>
  (function() {
    const banner = document.getElementById('jsErrorBanner');
    function show(msg) {
      try {
        if (!banner) return;
        banner.textContent = msg;
        banner.classList.add('show');
      } catch (e) {}
    }
    window.__showJsError = show;
    window.addEventListener('error', (ev) => {
      const m = (ev && (ev.message || (ev.error && ev.error.message))) ? (ev.message || ev.error.message) : 'Unbekannter Fehler';
      show('UIFehler (JS): ' + m);
    });
    window.addEventListener('unhandledrejection', (ev) => {
      const r = ev && ev.reason;
      const m = (r && r.message) ? r.message : String(r);
      show('UIFehler (Promise): ' + m);
    });
    // If the main UI never sets jsok, show a helpful message (covers parse errors)
    setTimeout(() => {
      const ok = document.documentElement && document.documentElement.dataset && document.documentElement.dataset.jsok;
      if (!ok) show('UI konnte nicht initialisiert werden (JS lädt nicht). ffne die Konsole (F12) für Details.');
    }, 700);
  })();
  </script>

  <script>
  (function() {
    try {
    const DATA = JSON.parse((document.getElementById('DATA')?.textContent) || '[]');
    const PRESETS = JSON.parse((document.getElementById('PRESETS')?.textContent) || '{}');
    const BRIEFING = JSON.parse((document.getElementById('BRIEFING')?.textContent) || '{"text":""}');
    const briefing = BRIEFING;
    const HISTORY_DELTA = JSON.parse((document.getElementById('HISTORY_DELTA')?.textContent) || '{}');
    const SEGMENT_MONITOR = JSON.parse((document.getElementById('SEGMENT_MONITOR')?.textContent) || '{}');
    const REALITY_CHECK = JSON.parse((document.getElementById('REALITY_CHECK')?.textContent) || '{}');
    const BRIEFING_REALITIES = JSON.parse((document.getElementById('BRIEFING_REALITIES')?.textContent) || '{"text":""}');
    const HD_BY = (HISTORY_DELTA && HISTORY_DELTA.by_symbol) ? HISTORY_DELTA.by_symbol : {};

    // UI State for Multi-Select Filters
    const uiState = {
      selPillars: new Set(),
      selClusters: new Set(),
      quick: {
        hideAvoid: true,
        onlyOK: false,
        onlyAvoid: false,
        onlyNA: false,
        onlyERR: false,
        trendOK: false,
        onlyTrendFail: false,
        liqOK: false,
        onlyLiqFail: false,
        onlyStock: false,
        onlyCrypto: false,
      },
    };

    function historyKey(r) {
      return normStr(r.asset_id) || normStr(r.symbol) || normStr(r.ticker_display) || normStr(r.ticker);
    }

    function attachDScore(rows) {
      if (!rows) return rows;
      for (const r of rows) {
        const k = historyKey(r);
        const rec = k ? HD_BY[k] : null;
        const d = rec ? (rec.score_delta ?? rec.scoreDelta ?? rec.delta ?? null) : null;
        r.dscore_1d = (d === null || d === undefined) ? null : Number(d);
      }
      return rows;
    }


    const elPreset = document.getElementById('preset');
    const elSearch = document.getElementById('search');
    const elCount = document.getElementById('count');
    const elSortHint = document.getElementById('sortHint');
    const elKpis = document.getElementById('kpis');
    const elPillars = document.getElementById('pillars');
    const elClusters = document.getElementById('clusters');
    const elClusterSel = document.getElementById('clusterSel');
    const elPillarSel = document.getElementById('pillarSel');
    const elDisclaimer = document.getElementById('disclaimer');
    const btnDiscOk = document.getElementById('discOk');
    const tbody = document.querySelector('#tbl tbody');
    const elMatrix = document.getElementById('matrix');
    const elMatrixNote = document.getElementById('matrixNote');
    const btnMatrixClear = document.getElementById('matrixClear');

// Market Context (passive; watchlist-derived; no scoring influence)
const elMarketPanel = document.getElementById('marketPanel');
const elMarketBody = document.getElementById('marketBody');
const btnMarketToggle = document.getElementById('marketToggle');
const elBreadthBox = document.getElementById('breadthBox');
const elMoversUp = document.getElementById('moversUp');
const elMoversDown = document.getElementById('moversDown');
const elHeatmap = document.getElementById('heatmap');
const elHeatMode = document.getElementById('heatMode');


    const elBriefing = document.getElementById('briefingText');
    const btnBriefingToggle = document.getElementById('briefingToggle');
    const elBriefReal = document.getElementById('briefRealText');
    const btnBriefRealToggle = document.getElementById('briefRealToggle');
    const elReality = document.getElementById('realityText');
    const btnRealityToggle = document.getElementById('realityToggle');
    const elSegment = document.getElementById('segmentText');
    const btnSegmentToggle = document.getElementById('segmentToggle');
    const elHistory = document.getElementById('historyText');
    const btnHistoryToggle = document.getElementById('historyToggle');
    const drawerOverlay = document.getElementById('drawerOverlay');
    const drawerClose = document.getElementById('drawerClose');
    const drawerTitle = document.getElementById('drawerTitle');
    const drawerSub = document.getElementById('drawerSub');
    const drawerBody = document.getElementById('drawerBody');
    const drawerActions = document.getElementById('drawerActions');

    // Flow info popover (Preset  Quick-Filter) - REMOVED: Now using unified help system

    // ---- briefing toggle (UI-only; must not affect scoring) ----
    let briefingVisible = true;
    function setBriefingVisible(on) {
      try {
        if (!elBriefing) return;
        elBriefing.style.display = on ? 'block' : 'none';
        if (btnBriefingToggle) btnBriefingToggle.textContent = on ? 'Ausblenden' : 'Einblenden';
      } catch (e) {}
    }
    


    // ---- passive panels toggles ----
    function _mkToggle(btn, el, label) {
      let on = true;
      function set(on_) {
        try {
          on = !!on_;
          if (el) el.style.display = on ? 'block' : 'none';
          if (btn) btn.textContent = on ? 'Ausblenden' : 'Einblenden';
        } catch (e) {}
      }
      if (btn) {
        btn.addEventListener('click', () => set(!on));
      }
      return { set };
    }

    const tBriefReal = _mkToggle(btnBriefRealToggle, elBriefReal, 'Briefing & Realities');
    const tReality = _mkToggle(btnRealityToggle, elReality, 'Reality Check');
    const tSegment = _mkToggle(btnSegmentToggle, elSegment, 'Segment Monitor');
    const tHistory = _mkToggle(btnHistoryToggle, elHistory, 'History Delta');



    // ---- state ----
    let activePreset = 'ALL';
    let userSort = null; // {k, dir} dir: 'asc'|'desc'
    // Multi-select: use arrays of strings. Ctrl/-Click toggles, normal click selects single.
    let clusterPick = []; // Cluster/Sektor filter (array)
        let pillarPick = [];  // Säulen-Filter (array; private metadata)

// Market Context UI state (passive)
let marketVisible = true;
let heatMode = 'pillar'; // 'pillar' | 'cluster'
let heatFilter = { cat: null, sb: null, mode: null };


    const DEFAULT_SORT = [{k:'score', dir:'desc'},{k:'confidence', dir:'desc'},{k:'name', dir:'asc'}];

    let quick = uiState.quick;

    // Bucket-matrix filter (Score  Risk)
    let matrix = { sb: null, rb: null };
    const DEFAULT_MATRIX = { sb: null, rb: null };
    const DEFAULT_HEAT_FILTER = { cat: null, sb: null, mode: null };

    const DEFAULT_QUICK = {
      hideAvoid: true,
      onlyOK: false,
      onlyAvoid: false,
      onlyNA: false,
      onlyERR: false,
      trendOK: false,
      onlyTrendFail: false,
      liqOK: false,
      onlyLiqFail: false,
      onlyStock: false,
      onlyCrypto: false,
    };

    const STORAGE_KEY = 'scanner_vnext.ui_state.v1';

    function syncSelectionArrays() {
      clusterPick = Array.from(uiState.selClusters);
      pillarPick = Array.from(uiState.selPillars);
    }

    function loadState() {
      try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return null;
        const st = JSON.parse(raw);
        return (st && typeof st === 'object') ? st : null;
      } catch (e) {
        return null;
      }
    }

    function saveState() {
      try {
        const st = {
          preset: activePreset,
          search: elSearch ? (elSearch.value || '') : '',
          quick: uiState.quick,
          matrix: matrix,
          sort: userSort,
          cluster: Array.from(uiState.selClusters),
          pillar: Array.from(uiState.selPillars),
          marketVisible: marketVisible,
          heatMode: heatMode,
          heatFilter: heatFilter,
        };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(st));
      } catch (e) {}
    }

    function clearState() {
      try { localStorage.removeItem(STORAGE_KEY); } catch (e) {}
    }

    function syncFilterButtons() {
      const fb = document.getElementById('filters');
      if (!fb) return;
      fb.querySelectorAll('button.fbtn').forEach(b => {
        const kk = b.getAttribute('data-key');
        if (!kk) return;
        const on = !!uiState.quick[kk];
        if (on) b.classList.add('active'); else b.classList.remove('active');
      });
    }

    

      function resetAll() {
      activePreset = (PRESETS && PRESETS.ALL) ? 'ALL' : ((PRESETS && PRESETS.CORE) ? 'CORE' : (Object.keys(PRESETS || {})[0] || 'CORE'));
      userSort = null;
      uiState.quick = Object.assign({}, DEFAULT_QUICK);
      quick = uiState.quick;
      uiState.selPillars.clear();
      uiState.selClusters.clear();
      matrix = Object.assign({}, DEFAULT_MATRIX);
      heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER);
      if (elSearch) elSearch.value = '';
      syncSelectionArrays();
      if (elClusterSel) elClusterSel.value = '';
      if (elPillarSel) elPillarSel.value = '';
      if (elPreset) elPreset.value = activePreset;
      syncFilterButtons();
      clearState();
      refresh();
    }

    // ---- info popover (Preset  Quick-Filter) - REMOVED: Now using unified help system

    // Try restoring last UI state
    (function restoreState() {
      const st = loadState();
      if (!st) return;

      const p = (st.preset || '').toString();
      if (p && PRESETS && PRESETS[p]) activePreset = p;

      if (elSearch && st.search !== undefined) {
        elSearch.value = (st.search || '').toString();
      }

      // cluster/pillar can be stored as string (legacy) or array (multi-select)
      if (st.cluster !== undefined && st.cluster !== null) {
        if (Array.isArray(st.cluster)) {
          clusterPick = st.cluster.map(x => (x ?? '').toString().trim()).filter(Boolean);
        } else {
          const s = (st.cluster || '').toString().trim();
          clusterPick = s ? [s] : [];
        }
        uiState.selClusters = new Set(clusterPick);
      }

      if (st.pillar !== undefined && st.pillar !== null) {
        if (Array.isArray(st.pillar)) {
          pillarPick = st.pillar.map(x => (x ?? '').toString().trim()).filter(Boolean);
        } else {
          const s = (st.pillar || '').toString().trim();
          pillarPick = s ? [s] : [];
        }
        uiState.selPillars = new Set(pillarPick);
      }

if (st.marketVisible !== undefined && st.marketVisible !== null) {
  marketVisible = !!st.marketVisible;
}
if (st.heatMode !== undefined && st.heatMode !== null) {
  const hm = (st.heatMode || '').toString();
  heatMode = (hm === 'cluster' || hm === 'pillar') ? hm : heatMode;
}


      if (st.quick && typeof st.quick === 'object') {
        quick = Object.assign({}, DEFAULT_QUICK, st.quick);
        uiState.quick = quick;
      }

      if (st.matrix && typeof st.matrix === 'object') {
        const sb = (st.matrix.sb === null || st.matrix.sb === undefined) ? null : Number(st.matrix.sb);
        const rb = (st.matrix.rb === null || st.matrix.rb === undefined) ? null : Number(st.matrix.rb);
        matrix = Object.assign({}, DEFAULT_MATRIX, {sb: Number.isFinite(sb) ? sb : null, rb: Number.isFinite(rb) ? rb : null});
      }

      if (st.heatFilter && typeof st.heatFilter === 'object') {
        const cat = (st.heatFilter.cat ?? '').toString().trim() || null;
        const sb = (st.heatFilter.sb === null || st.heatFilter.sb === undefined) ? null : Number(st.heatFilter.sb);
        const mode = (st.heatFilter.mode ?? '').toString();
        heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER, {
          cat,
          sb: Number.isFinite(sb) ? sb : null,
          mode: (mode === 'cluster' || mode === 'pillar') ? mode : null,
        });
      }

      if (st.sort && typeof st.sort === 'object' && st.sort.k) {
        const k = (st.sort.k || '').toString();
        const dir = ((st.sort.dir || 'desc').toString().toLowerCase() === 'asc') ? 'asc' : 'desc';
        if (k) userSort = {k, dir};
      }
    })();


// ---- market context toggle (UI-only; must not affect scoring) ----
function setMarketVisible(on) {
  try {
    if (elMarketBody) elMarketBody.style.display = on ? 'grid' : 'none';
    if (btnMarketToggle) btnMarketToggle.textContent = on ? 'Ausblenden' : 'Einblenden';
  } catch (e) {}
}

// apply restored state
if (elHeatMode) elHeatMode.value = heatMode;
setMarketVisible(marketVisible);


if (elHeatMode) {
  elHeatMode.addEventListener('change', () => {
    const v = (elHeatMode.value || '').toString();
    heatMode = (v === 'cluster' || v === 'pillar') ? v : heatMode;
    heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER);
    saveState();
    refresh();
  });
}

    // ---- helpers ----
    function asNum(v) {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }
    function asBool(v) {
      if (v === true || v === false) return v;
      if (v === 1 || v === 0) return !!v;
      const s = (v ?? '').toString().trim().toLowerCase();
      if (['true','t','yes','y','1'].includes(s)) return true;
      if (['false','f','no','n','0'].includes(s)) return false;
      return null;
    }
    function normStr(v) {
      return (v ?? '').toString().trim();
    }

// ---- disclaimer (UI-only) ----
(function initDisclaimer() {
  try {
    if (!elDisclaimer || !btnDiscOk) return;
    const DK = 'scanner_vnext.disclaimer_ok.v1';
    const ok = (sessionStorage.getItem(DK) || '') === '1';
    if (ok) { elDisclaimer.style.display = 'none'; return; }
    btnDiscOk.addEventListener('click', () => {
      try { sessionStorage.setItem(DK, '1'); } catch(e) {}
      elDisclaimer.style.display = 'none';
    });
  } catch (e) {}
})();

// ---- cluster / sektor helpers (UI-only; never affects scoring) ----
function clusterLabel(r) {
  const isC = asBool(r.is_crypto) === true;
  if (isC) return 'Krypto';
  const derived = normStr(r.cluster_official);
  const industry = normStr(r.industry) || normStr(r.Industry);
  const sector = normStr(r.sector) || normStr(r.Sector);
  return derived || industry || sector || '';
}

function computeClusterCounts(rows) {
  const m = new Map();
  for (const r of rows || []) {
    const c = clusterLabel(r);
    if (!c) continue;
    m.set(c, (m.get(c) || 0) + 1);
  }
  const arr = Array.from(m.entries()).map(([k,v]) => ({k, v}));
  arr.sort((a,b) => b.v - a.v || a.k.localeCompare(b.k));
  return arr;
}

function renderClusterOptions(counts) {
  if (!elClusterSel) return;
  const cur = (Array.isArray(clusterPick) ? (clusterPick.length===1 ? clusterPick[0] : '') : (clusterPick || '') ) || '';
  elClusterSel.innerHTML = '';
  const opt0 = document.createElement('option');
  opt0.value = '';
  opt0.textContent = 'Alle';
  elClusterSel.appendChild(opt0);
  for (const x of counts) {
    const opt = document.createElement('option');
    opt.value = x.k;
    opt.textContent = `${x.k} (${x.v})`;
    elClusterSel.appendChild(opt);
  }
  elClusterSel.value = cur;
}

function renderClusterChips(counts) {
  if (!elClusters) return;
  const top = (counts || []).slice(0, 12);
  const activeSet = uiState.selClusters;
  let html = '<span class="label">Cluster:</span>';
  
  // "Alle" chip
  const allActive = activeSet.size === 0;
  html += `<button type="button" class="chip kpi ${allActive ? 'blue active' : 'blue'}" data-chip="cluster" data-val="__ALL__" title="Alle Cluster anzeigen">Alle</button>`;
  
  if (activeSet.size) {
    const label = activeSet.size + ' selected';
    const tip = Array.from(activeSet).join(', ');
    html += `<button type="button" class="chip kpi warn active" data-chip="cluster" data-val="__CLEAR__" title="Cluster-Filter löschen: ${esc(tip)}"> ${esc(label)}</button>`;
  }
  for (const x of top) {
    const isOn = activeSet.has(x.k);
    const kind = isOn ? 'warn active' : 'blue';
    html += `<button type="button" class="chip kpi ${kind}" data-chip="cluster" data-val="${esc(x.k)}" title="Filter: nur Cluster ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
  }
  if (!top.length) html += '<span class="muted"></span>';
  elClusters.innerHTML = html;
}

function applyClusterFilter(rows) {
  if (uiState.selClusters.size === 0) return rows;
  return (rows || []).filter(r => uiState.selClusters.has(clusterLabel(r)));
}

// ---- 5Säulen / Playground helpers (UI-only; private metadata; never affects scoring) ----
const PILLAR_ORDER = ['Gehirn','Hardware','Energie','Fundament','Recycling','Playground'];

function _derivePillarFromLegacy(catRaw) {
  const cat = (catRaw || '').toString().trim();
  if (!cat) return '';
  const s = cat.toLowerCase();
  // Playground / experiments
  if (s.includes('experiment') || s.includes('playground') || s.includes('spielplatz') || s.includes('play')) return 'Playground';
  // explicit pillar names
  if (s.includes('gehirn')) return 'Gehirn';
  if (s.includes('hardware')) return 'Hardware';
  if (s.includes('energie') || s.includes('uran')) return 'Energie';
  if (s.includes('fundament')) return 'Fundament';
  if (s.includes('recycling') || s.includes('urban mining') || s.includes('urban-mining')) return 'Recycling';
  // legacy mining buckets  Fundament
  if (s.includes('mining') || s.includes('mine') || s.includes('edelmetall') || s.includes('metall') || s.includes('rohstoff')) return 'Fundament';
  // ambiguous tech buckets  Gehirn (default), hardware-specific keywords  Hardware
  if (s.includes('robot') || s.includes('automation') || s.includes('sensor') || s.includes('vision') || s.includes('machine')) return 'Hardware';
  if (s.includes('software') || s.includes('internet') || s.includes('ki') || s.includes('ai') || s.includes('data') || s.includes('cloud') || s.includes('chip') || s.includes('semiconductor')) return 'Gehirn';
  return '';
}

function pillarLabel(r) {
  const p = normStr(r.pillar_primary);
  if (p) return p;
  // fallback for older universes: derive from legacy categories so the concept is still visible in UI
  const legacy = normStr(r.Sektor) || normStr(r.category);
  return _derivePillarFromLegacy(legacy);
}

function computePillarCounts(rows) {
  const m = new Map();
  for (const k of PILLAR_ORDER) m.set(k, 0);
  for (const r of rows || []) {
    const p = pillarLabel(r);
    if (!p) continue;
    m.set(p, (m.get(p) || 0) + 1);
  }
  const arr = Array.from(m.entries()).map(([k,v]) => ({k, v}));
  // stable order
  arr.sort((a,b) => PILLAR_ORDER.indexOf(a.k) - PILLAR_ORDER.indexOf(b.k));
  return arr;
}

function renderPillarOptions(counts) {
  if (!elPillarSel) return;
  const cur = (Array.isArray(pillarPick) ? (pillarPick.length===1 ? pillarPick[0] : '') : (pillarPick || '') ) || '';
  elPillarSel.innerHTML = '';
  const opt0 = document.createElement('option');
  opt0.value = '';
  opt0.textContent = 'Alle';
  elPillarSel.appendChild(opt0);
  for (const x of (counts || [])) {
    // keep all options visible even if zero to make the model explicit
    const opt = document.createElement('option');
    opt.value = x.k;
    opt.textContent = `${x.k} (${x.v})`;
    elPillarSel.appendChild(opt);
  }
  elPillarSel.value = cur;
}

function renderPillarChips(counts) {
  if (!elPillars) return;
  const activeSet = uiState.selPillars;
  let html = '<span class="label">Säulen:</span>';
  
  // "Alle" chip
  const allActive = activeSet.size === 0;
  html += `<button type="button" class="chip kpi ${allActive ? 'blue active' : 'blue'}" data-chip="pillar" data-val="__ALL__" title="Alle Säulen anzeigen">Alle</button>`;
  
  if (activeSet.size) {
    const label = activeSet.size + ' selected';
    const tip = Array.from(activeSet).join(', ');
    html += `<button type="button" class="chip kpi warn active" data-chip="pillar" data-val="__CLEAR__" title="Säulen-Filter löschen: ${esc(tip)}"> ${esc(label)}</button>`;
  }
  for (const x of (counts || [])) {
    const isOn = activeSet.has(x.k);
    const kind = isOn ? 'warn active' : 'blue';
    const dis = (x.v || 0) <= 0 ? 'disabled aria-disabled="true"' : '';
    html += `<button type="button" class="chip kpi ${kind}" data-chip="pillar" data-val="${esc(x.k)}" ${dis} title="Filter: nur Säule ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
  }
  elPillars.innerHTML = html;
}

function applyPillarFilter(rows) {
  if (uiState.selPillars.size === 0) return rows;
  return (rows || []).filter(r => uiState.selPillars.has(pillarLabel(r)));
}

    function percentileRank(sorted, v) {
      // sorted: ascending array, v: number
      if (!sorted || sorted.length === 0 || v === null || v === undefined) return null;
      const n = sorted.length;
      if (n === 1) return 100.0;
      // upper bound index
      let lo = 0, hi = n;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (sorted[mid] <= v) lo = mid + 1;
        else hi = mid;
      }
      const idx = Math.max(0, Math.min(n - 1, lo - 1));
      return (idx / (n - 1)) * 100.0;
    }

    function riskRaw(r) {
      const v = asNum(r.volatility);
      if (v !== null) return Math.abs(v);
      const dsd = asNum(r.downside_dev);
      if (dsd !== null) return Math.abs(dsd);
      const dd = asNum(r.max_drawdown);
      if (dd !== null) return Math.abs(dd);
      return null;
    }

    // Precompute score / risk percentiles for stable buckets & signal codes
    const SCORE_SORTED = DATA.map(r => asNum(r.score)).filter(v => v !== null).sort((a,b) => a-b);
    const RISK_SORTED = DATA.map(r => riskRaw(r)).filter(v => v !== null).sort((a,b) => a-b);

    for (const r of DATA) {
      r.score_pctl = percentileRank(SCORE_SORTED, asNum(r.score));
      r.risk_raw = riskRaw(r);
      r.risk_pctl = percentileRank(RISK_SORTED, r.risk_raw);
    }

    function scoreBucket(score) {
      const s = Math.max(0, Math.min(100, asNum(score) ?? 0));
      return Math.min(4, Math.floor(s / 20));
    }
    function riskBucket(pctl) {
      const p = Math.max(0, Math.min(100, asNum(pctl) ?? 0));
      return Math.min(4, Math.floor(p / 20));
    }

    function bucketRange(i) {
      const a = i * 20;
      const b = (i === 4) ? 100 : (i + 1) * 20;
      return `${a}${b}`;
    }
    function scoreBucketText(i) {
      return { range: bucketRange(i), hint: 'Score' };
    }
    function riskBucketText(i) {
      const hints = ['niedrig', 'moderat', 'mittel', 'hoch', 'sehr hoch'];
      return { range: bucketRange(i), hint: `Risk ${hints[i] || ''}`.trim() };
    }


    function fmtPrice(n) {
      if (n === null || n === undefined) return '';
      const ax = Math.abs(n);
      let maxFrac = 2;
      if (ax < 1) maxFrac = 4;
      if (ax < 0.1) maxFrac = 6;
      try {
        return n.toLocaleString('de-DE', { maximumFractionDigits: maxFrac });
      } catch (e) {
        return n.toFixed(maxFrac);
      }
    }

    function perfLine(p) {
      if (p === null || p === undefined) return '<div class="sub muted"></div>';
      const dir = (p > 0) ? 'pos' : (p < 0) ? 'neg' : 'flat';
      const arrow = (p > 0) ? '' : (p < 0) ? '' : '';
      return `<div class="sub chg ${dir}">${arrow} ${p.toFixed(2)}%</div>`;
    }

    function looksLikeISIN(s) {
      s = normStr(s);
      return /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(s);
    }

    function pickYahooSymbol(r) {
      // Used as fetch/link key for Yahoo Finance.
      // For crypto we prefer the trading pair (e.g. BTC-USD, ETH-EUR).
      const isC = asBool(r.is_crypto) === true;
      const cand = [
        r.yahoo_symbol, r.YahooSymbol, r.yahooSymbol, r.Yahoo, r.yahoo,
        r.symbol, r.Symbol, r.ticker_display, r.ticker
      ];
      let best = '';
      for (const c of cand) {
        const s = normStr(c);
        if (!s) continue;
        if (looksLikeISIN(s)) continue;
        if (isC) {
          if (s.includes('-')) return s; // perfect
          best = best || s;
        } else {
          return s;
        }
      }
      if (isC) {
        const base = normStr(r.crypto_base) || normStr(r.ticker_display) || best;
        const qc = normStr(r.quote_currency) || normStr(r.currency) || '';
        if (base && qc && !base.includes('-')) return `${base}-${qc}`;
        return best;
      }
      return '';
    }

    function pickDisplayTicker(r) {
      // Prefer human-friendly display symbol.
      // - crypto: ticker_display is usually the base asset (ADA)
      // - stocks: ticker_display is the refined ticker (Symbol/YahooSymbol fallback)
      let td = normStr(r.ticker_display);
      if (!td || looksLikeISIN(td)) {
        const s = normStr(r.yahoo_symbol) || normStr(r.YahooSymbol) || normStr(r.Yahoo) || normStr(r.symbol) || normStr(r.Symbol) || normStr(r.ticker);
        if (s && !looksLikeISIN(s)) td = s;
      }
      const t = normStr(r.ticker);
      if (td) return td;
      return looksLikeISIN(t) ? '' : t;
    }

    function pickDisplaySymbol(r) {
      const isC = asBool(r.is_crypto) === true;
      if (isC) {
        const pair = pickYahooSymbol(r);
        const base = normStr(r.crypto_base) || normStr(r.ticker_display);
        if (base && !looksLikeISIN(base)) return base;
        if (pair && pair.includes('-')) return pair.split('-')[0];
        const s = normStr(r.symbol) || normStr(r.Symbol);
        if (s && !looksLikeISIN(s)) return s;
        return pair || pickDisplayTicker(r) || '';
      }
      const s = normStr(r.symbol) || normStr(r.Symbol);
      if (s && !looksLikeISIN(s)) return s;
      const td = normStr(r.ticker_display);
      if (td && !looksLikeISIN(td)) return td;
      const yh = pickYahooSymbol(r);
      if (yh && !looksLikeISIN(yh)) return yh;
      const t = normStr(r.ticker);
      if (t && !looksLikeISIN(t)) return t;
      return '';
    }


    function yahooHref(sym) {
      const s = normStr(sym);
      if (!s || looksLikeISIN(s)) return '';
      return 'https://finance.yahoo.com/quote/' + encodeURIComponent(s);
    }

    function esc(v) {
      return (v ?? '').toString()
        .replaceAll('&','&amp;')
        .replaceAll('<','&lt;')
        .replaceAll('>','&gt;')
        .replaceAll('"','&quot;')
        .replaceAll("'",'&#39;');
    }

    function chip(text, kind, title) {
      const cls = kind ? `chip ${kind}` : 'chip';
      const t = title ? ` title="${esc(title)}"` : '';
      return `<span class="${cls}"${t}>${text}</span>`;
    }

    function kpiChip(text, kind, title, key, active) {
      const cls = `chip kpi ${kind || ''} ${active ? 'active' : ''}`.trim().replace(/\\s+/g,' ');
      const t = title ? ` title="${esc(title)}"` : '';
      const k = key ? ` data-kpi="${esc(key)}"` : '';
      return `<button type="button" class="${cls}"${k}${t} aria-pressed="${active ? 'true' : 'false'}">${text}</button>`;
    }

    function recFor(r) {
      const st = normStr(r.score_status);
      if (st === 'NA' || st === 'ERROR') return {code: 'R?', cls: 'bad'};
      if (st && st.startsWith('AVOID')) return {code: 'R0', cls: 'warn'};

      const p = asNum(r.score_pctl);
      const tr = asBool(r.trend_ok) === true;
      const liq = asBool(r.liquidity_ok) === true;

      if (p !== null && p >= 90 && tr && liq) return {code: 'R5', cls: 'good'};
      if (p !== null && p >= 75 && liq) return {code: 'R4', cls: 'good'};
      if (p !== null && p >= 45) return {code: 'R3', cls: 'blue'};
      if (p !== null && p >= 20) return {code: 'R2', cls: 'warn'};
      return {code: 'R1', cls: 'bad'};
    }

    function scoreCell(r) {
      const s = Math.max(0, Math.min(100, asNum(r.score) ?? 0));
      const rec = recFor(r);
      const sig = rec ? `<span class="sig ${rec.cls}" title="SignalCode">${esc(rec.code)}</span>` : '';
      return `<div class="scorecell"><div class="scorebar"><div style="width:${s}%;"></div></div><span class="mono">${s.toFixed(2)}</span>${sig}</div>`;
    }

    function dScoreCell(r) {
      const d = asNum(r.dscore_1d);
      if (d === null) return '<span class="muted"></span>';
      const sign = d > 0 ? '+' : '';
      const cls = d > 0 ? 'deltaUp' : (d < 0 ? 'deltaDown' : 'deltaFlat');
      return `<span class="mono ${cls}">${sign}${d.toFixed(2)}</span>`;
    }

    // ---- preset logic (mirrors scanner.presets.apply) ----
    function applyFilters(rows, preset) {
      const filters = preset.filters || [];
      if (!Array.isArray(filters) || filters.length === 0) return rows;

      return rows.filter(r => {
        for (const f of filters) {
          const field = f.field || f.key || f.name;
          if (!field) continue;
          const onMissing = (f.on_missing || 'skip').toLowerCase();
          const v = r[field];
          const missing = (v === null || v === undefined || v === '');
          if (missing) {
            if (onMissing === 'skip') continue;
            return false;
          }

          if (f.min !== undefined || f.max !== undefined) {
            const n = asNum(v);
            if (n === null) {
              if (onMissing === 'skip') continue;
              return false;
            }
            if (f.min !== undefined && n < Number(f.min)) return false;
            if (f.max !== undefined && n > Number(f.max)) return false;
          }

          if (f.eq !== undefined) {
            const target = f.eq;
            // bool-aware compare
            if (typeof target === 'boolean') {
              const b = asBool(v);
              if (b === null) return false;
              if (b !== target) return false;
            } else {
              if (v !== target) return false;
            }
          }

          if (Array.isArray(f.in)) {
            if (!f.in.includes(v)) return false;
          }
        }
        return true;
      });
    }

    function parseSortSpecs(specs) {
      if (!Array.isArray(specs)) return [];
      return specs.map(s => {
        const parts = (s || '').split(':');
        const k = parts[0];
        const dir = (parts[1] || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';
        return {k, dir};
      }).filter(x => x.k);
    }

    function compareBy(specs) {
      return (a, b) => {
        for (const s of specs) {
          const ka = a[s.k];
          const kb = b[s.k];

          // number first
          const na = asNum(ka);
          const nb = asNum(kb);
          let c = 0;
          if (na !== null && nb !== null) {
            c = na === nb ? 0 : (na < nb ? -1 : 1);
          } else {
            // bool
            const ba = asBool(ka);
            const bb = asBool(kb);
            if (ba !== null && bb !== null) {
              c = (ba === bb) ? 0 : (ba ? 1 : -1);
            } else {
              // string
              const sa = normStr(ka).toLowerCase();
              const sb = normStr(kb).toLowerCase();
              c = sa === sb ? 0 : (sa < sb ? -1 : 1);
            }
          }

          if (c !== 0) return s.dir === 'asc' ? c : -c;
        }
        return 0;
      };
    }

    function applyPreset(rows, presetName) {
      const preset = PRESETS[presetName] || PRESETS.CORE || {filters:[], sort:[], limit:200};
      let out = rows.slice();
      out = applyFilters(out, preset);

      const specs = parseSortSpecs(preset.sort || []);
      const eff = (specs.length > 0) ? specs : DEFAULT_SORT;
      out.sort(compareBy(eff));

      const limit = Number(preset.limit || 0);
      if (Number.isFinite(limit) && limit > 0) out = out.slice(0, limit);

      return {rows: out, preset};
    }

    function applySearch(rows, q) {
      q = (q || '').trim().toLowerCase();
      if (!q) return rows;
      const tokens = q.split(/\\s+/).filter(Boolean);
      return rows.filter(r => {
        const hay = [r.ticker, r.ticker_display, r.yahoo_symbol, r.YahooSymbol, r.symbol, r.isin, r.name, r.sector, r.Sector, r.category, r.Sektor, r.Kategorie, r.Industry, r.industry, r.country, r.currency, r["Währung"], r.quote_currency, r.score_status]
          .map(normStr).join(' ').toLowerCase();
        return tokens.every(t => hay.includes(t));
      });
    }

    function isAvoidStatus(status) {
      const s = normStr(status);
      return s === 'AVOID' || s.startsWith('AVOID_');
    }

    function summarize(rows) {
      const out = {total:0, ok:0, avoid:0, na:0, error:0, trendFail:0, liqFail:0, crypto:0, stock:0};
      out.total = rows.length;
      for (const r of rows) {
        const st = normStr(r.score_status);
        if (st === 'OK') out.ok++;
        else if (isAvoidStatus(st)) out.avoid++;
        else if (st === 'NA') out.na++;
        else if (st === 'ERROR') out.error++;

        if (asBool(r.trend_ok) === false) out.trendFail++;
        if (asBool(r.liquidity_ok) === false) out.liqFail++;

        const isC = asBool(r.is_crypto) === true;
        if (isC) out.crypto++; else out.stock++;
      }
      return out;
    }

    function renderKpis(allRows, visibleRows) {
      if (!elKpis) return;
      const a = summarize(allRows);
      const v = summarize(visibleRows);

      // KPI chips double as quick filters (intuitive). Preset applies first, then search, then quick filters.
      elKpis.innerHTML =
        `<span class="label">KPI</span>`
        + kpiChip(`Sichtbar ${v.total}/${a.total}`, 'blue', 'Sichtbar nach Preset  Suche  Quick-Filter / Gesamt', '', false)
        + kpiChip(`OK ${v.ok}`, 'good', 'Filter: nur score_status == OK', 'ok', !!quick.onlyOK)
        + kpiChip(`AVOID ${v.avoid}`, 'warn', 'Filter: nur score_status beginnt mit AVOID_', 'avoid', !!quick.onlyAvoid)
        + kpiChip(`NA ${v.na}`, 'bad', 'Filter: nur score_status == NA', 'na', !!quick.onlyNA)
        + kpiChip(`ERR ${v.error}`, 'bad', 'Filter: nur score_status == ERROR', 'err', !!quick.onlyERR)
        + kpiChip(`TrendFail ${v.trendFail}`, v.trendFail ? 'warn' : 'good', 'Filter: nur trend_ok == false', 'trendFail', !!quick.onlyTrendFail)
        + kpiChip(`LiqFail ${v.liqFail}`, v.liqFail ? 'warn' : 'good', 'Filter: nur liquidity_ok == false', 'liqFail', !!quick.onlyLiqFail)
        + kpiChip(`Aktien ${v.stock}`, 'blue', 'Filter: nur is_crypto == false', 'stock', !!quick.onlyStock)
        + kpiChip(`Krypto ${v.crypto}`, 'warn', 'Filter: nur is_crypto == true', 'crypto', !!quick.onlyCrypto);
    }

    
    function toggleKpi(key, multiSelect = false) {
      key = (key || '').toString();
      
      // For multi-select mode, don't clear other filters in the same group
      if (!multiSelect) {
        // mutually exclusive groups (normal click)
        const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
        const clearTrend = () => { quick.trendOK=false; quick.onlyTrendFail=false; };
        const clearLiq = () => { quick.liqOK=false; quick.onlyLiqFail=false; };
      }

      if (key === 'ok') {
        if (multiSelect) {
          quick.onlyOK = !quick.onlyOK;
        } else {
          const nv = !quick.onlyOK;
          const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
          clearStatus();
          quick.onlyOK = nv;
        }
      } else if (key === 'avoid') {
        if (multiSelect) {
          quick.onlyAvoid = !quick.onlyAvoid;
          if (quick.onlyAvoid) quick.hideAvoid = false; // show avoid when filtering for it
        } else {
          const nv = !quick.onlyAvoid;
          const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
          clearStatus();
          quick.onlyAvoid = nv;
          if (nv) quick.hideAvoid = false; // show avoid when filtering for it
        }
      } else if (key === 'na') {
        if (multiSelect) {
          quick.onlyNA = !quick.onlyNA;
        } else {
          const nv = !quick.onlyNA;
          const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
          clearStatus();
          quick.onlyNA = nv;
        }
      } else if (key === 'err') {
        if (multiSelect) {
          quick.onlyERR = !quick.onlyERR;
        } else {
          const nv = !quick.onlyERR;
          const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
          clearStatus();
          quick.onlyERR = nv;
        }
      } else if (key === 'trendFail') {
        if (multiSelect) {
          quick.onlyTrendFail = !quick.onlyTrendFail;
        } else {
          const nv = !quick.onlyTrendFail;
          const clearTrend = () => { quick.trendOK=false; quick.onlyTrendFail=false; };
          clearTrend();
          quick.onlyTrendFail = nv;
        }
      } else if (key === 'liqFail') {
        if (multiSelect) {
          quick.onlyLiqFail = !quick.onlyLiqFail;
        } else {
          const nv = !quick.onlyLiqFail;
          const clearLiq = () => { quick.liqOK=false; quick.onlyLiqFail=false; };
          clearLiq();
          quick.onlyLiqFail = nv;
        }
      } else if (key === 'stock') {
        quick.onlyStock = !quick.onlyStock;
        if (quick.onlyStock) quick.onlyCrypto = false;
      } else if (key === 'crypto') {
        quick.onlyCrypto = !quick.onlyCrypto;
        if (quick.onlyCrypto) quick.onlyStock = false;
      }

      syncFilterButtons();
      saveState();
      refresh();
    }

function applyQuickFilters(rows) {
      return rows.filter(r => {
        const status = normStr(r.score_status);
        const isAvoid = isAvoidStatus(status);

        // status-only filters (from KPI chips)
        if (uiState.quick.onlyOK && status !== 'OK') return false;
        if (uiState.quick.onlyAvoid && !isAvoid) return false;
        if (uiState.quick.onlyNA && status !== 'NA') return false;
        if (uiState.quick.onlyERR && status !== 'ERROR') return false;

        // Hide AVOID applies only when we're not explicitly filtering for AVOID
        if (uiState.quick.hideAvoid && !uiState.quick.onlyAvoid && isAvoid) return false;

        // pass/fail filters
        if (uiState.quick.trendOK && asBool(r.trend_ok) !== true) return false;
        if (uiState.quick.onlyTrendFail && asBool(r.trend_ok) !== false) return false;

        if (uiState.quick.liqOK && asBool(r.liquidity_ok) !== true) return false;
        if (uiState.quick.onlyLiqFail && asBool(r.liquidity_ok) !== false) return false;

        // class filters
        const isCrypto = asBool(r.is_crypto) === true;
        if (uiState.quick.onlyCrypto && !isCrypto) return false;
        if (uiState.quick.onlyStock && isCrypto) return false;
        return true;
      });
    }

    function applyMatrixFilter(rows) {
      const sb = (matrix && matrix.sb !== undefined) ? matrix.sb : null;
      const rb = (matrix && matrix.rb !== undefined) ? matrix.rb : null;
      if (sb === null && rb === null) return rows;
      return rows.filter(r => {
        const okS = (sb === null) ? true : (scoreBucket(r.score) === sb);
        const okR = (rb === null) ? true : (riskBucket(r.risk_pctl) === rb);
        return okS && okR;
      });
    }

    function renderMatrix(rows) {
      if (!elMatrix) return;

      // Grid layout: rows = Risk buckets (y), cols = Score buckets (x)
      const counts = Array.from({length:5}, () => Array(5).fill(0)); // [rb][sb]
      for (const r of rows) {
        const sb = scoreBucket(r.score);
        const rb = riskBucket(r.risk_pctl);
        counts[rb][sb] += 1;
      }

      const parts = [];
      // header row: Score buckets (x). Corner shows axis directions.
      parts.push(`<div class="matrixAxis" title="Achsen: Risk (y)  Score (x)"><div class="lbl">Risk </div><div class="hint">Score </div></div>`);
      for (let sb = 0; sb < 5; sb++) {
        const s = scoreBucketText(sb);
        parts.push(`<div class="matrixLabel" title="ScoreBucket"><div class="lbl">${esc(s.range)}</div><div class="hint">${esc(s.hint)}</div></div>`);
      }

      for (let rb = 0; rb < 5; rb++) {
        const rtxt = riskBucketText(rb);
        parts.push(`<div class="matrixLabel" title="RiskBucket (Perzentil; höher = riskanter)"><div class="lbl">${esc(rtxt.range)}</div><div class="hint">${esc(rtxt.hint)}</div></div>`);
        for (let sb = 0; sb < 5; sb++) {
          const c = counts[rb][sb] || 0;
          const active = (matrix && matrix.sb === sb && matrix.rb === rb) ? 'active' : '';
          const zero = c === 0 ? 'zero' : '';

          // Subtle gradient: best area (high score / low risk) tends green; worst tends red.
          const val = sb - rb; // -4..+4
          const hue = Math.max(15, Math.min(150, Math.round(80 + val * 15)));
          const alpha = 0.10;
          const bg = `background: hsla(${hue}, 70%, 45%, ${alpha});`;

          const st = `style="${bg}"`;
          const s = scoreBucketText(sb);
          const rr = riskBucketText(rb);

          parts.push(`<div class="cell ${active} ${zero}" ${st} data-sb="${sb}" data-rb="${rb}" title="Score ${esc(s.range)} · Risk ${esc(rr.hint)}">${c ? `<span class="cnt">${c}</span>` : `<span class="cnt">·</span>`}</div>`);
        }
      }

      elMatrix.innerHTML = parts.join('');

      // Click  toggle matrix filter
      elMatrix.querySelectorAll('.cell').forEach(cell => {
        cell.addEventListener('click', () => {
          if (cell.classList.contains('zero')) return;
          const sb = Number(cell.getAttribute('data-sb'));
          const rb = Number(cell.getAttribute('data-rb'));
          if (matrix.sb === sb && matrix.rb === rb) {
            matrix = Object.assign({}, DEFAULT_MATRIX);
          } else {
            matrix = {sb, rb};
          }
          refresh();
          saveState();
        });
      });

      const sb = (matrix && matrix.sb !== undefined) ? matrix.sb : null;
      const rb = (matrix && matrix.rb !== undefined) ? matrix.rb : null;
      if (elMatrixNote) {
        const metric = (RISK_SORTED && RISK_SORTED.length) ? 'RiskProxy aus volatility/downside_dev/max_drawdown (Perzentil)' : 'RiskProxy fehlt (keine RiskSpalten im CSV)';
        const sel = (sb !== null && rb !== null) ? ` · aktiv: Score ${bucketRange(sb)}  ${riskBucketText(rb).hint}` : '';
        elMatrixNote.textContent = metric + sel;
      }
    }

// ---- Market Context (passive; derived from current universe; no scoring influence) ----
function parsePct(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace('%','').replace(/\\s+/g,'').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function perfPct(r) {
  return parsePct(
    r.perf_pct ?? r['Perf %'] ?? r['Change %'] ?? r.change_pct ?? r.changePercent ?? r.PerfPct
  );
}

function fmtPct(v) {
  if (v === null || v === undefined || !Number.isFinite(v)) return '';
  const s = (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
  return s;
}

function renderBreadth(rows) {
  if (!elBreadthBox) return;

  const list = rows || [];
  const n = list.length;
  let adv = 0, dec = 0, flat = 0, miss = 0;
  let basisPerfPct = 0;

  for (const r of list) {
    const raw = (r.perf_pct ?? r['Perf %'] ?? r['Change %'] ?? r.change_pct ?? r.changePercent ?? r.PerfPct);
    if (raw !== null && raw !== undefined && raw !== '') basisPerfPct += 1;
    const p = perfPct(r);
    if (p === null) { miss++; continue; }
    if (p > 0) adv++;
    else if (p < 0) dec++;
    else flat++;
  }

  const tot = adv + dec + flat;
  const pct = (x, d) => d ? Math.round((x / d) * 10000) / 100 : 0;
  const advPct = pct(adv, tot);

  let regime = 'Gemischt';
  let regimeClass = 'mixed';
  let sayToday = 'Heute ist das Bild gemischt, weder Gewinner noch Verlierer dominieren klar.';
  if (tot > 0 && advPct >= 60) {
    regime = 'Risk-On';
    regimeClass = 'riskOn';
    sayToday = 'Heute berwiegen Gewinner, das Umfeld wirkt freundlich.';
  } else if (tot > 0 && advPct <= 40) {
    regime = 'Risk-Off';
    regimeClass = 'riskOff';
    sayToday = 'Heute berwiegen Verlierer, das Umfeld wirkt vorsichtig.';
  } else if (tot === 0) {
    sayToday = 'Heute fehlen genug Perf%-Daten fr ein klares Tagesbild.';
  }

  function quantile(sorted, q) {
    if (!sorted.length) return null;
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sorted[base + 1] === undefined) return sorted[base];
    return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
  }
  function medIqr(values) {
    const a = values.filter(Number.isFinite).slice().sort((x, y) => x - y);
    if (!a.length) return { med: null, iqr: null };
    const q25 = quantile(a, 0.25);
    const q50 = quantile(a, 0.50);
    const q75 = quantile(a, 0.75);
    return { med: q50, iqr: (q75 !== null && q25 !== null) ? (q75 - q25) : null };
  }

  const scores = list.map(r => asNum(r.score)).filter(Number.isFinite);
  const confs = list.map(r => asNum(r.confidence ?? r.conf ?? r.konf)).filter(Number.isFinite);
  const sm = medIqr(scores);
  const cm = medIqr(confs);

  let trendOk = 0, liqOk = 0, topPctl = 0;
  for (const r of list) {
    if (asBool(r.trend_ok) === true) trendOk++;
    if (asBool(r.liquidity_ok) === true) liqOk++;
    const sp = asNum(r.score_pctl);
    if (Number.isFinite(sp) && sp >= 90) topPctl++;
  }

  function topKey(getLabel) {
    const m = new Map();
    for (const r of list) {
      const k = normStr(getLabel(r));
      if (!k) continue;
      m.set(k, (m.get(k) || 0) + 1);
    }
    let best = null;
    let bestN = 0;
    for (const [k, v] of m.entries()) {
      if (v > bestN) { best = k; bestN = v; }
    }
    return best ? `${best} (${bestN})` : null;
  }
  const topPillar = topKey(r => pillarLabel(r));
  const topCluster = topKey(r => clusterLabel(r));

  elBreadthBox.innerHTML = `
    <div class="breadthLvl1">
      <div class="breadthHeadline">
        <span class="ampel ${regimeClass}" aria-hidden="true"></span>
        <span>${esc(regime)}</span>
        <span class="breadthPct">Adv ${tot ? advPct.toFixed(2) : '0.00'}%</span>
      </div>
      <div class="breadthRow">
        ${chip(`Gewinner ${adv}`, adv ? 'good' : 'blue')}
        ${chip(`Verlierer ${dec}`, dec ? 'bad' : 'blue')}
        ${chip(`Neutral ${flat}`, 'blue')}
        ${miss ? chip(`n/a ${miss}`, 'warn') : ''}
      </div>
      <div class="muted small">Was heit das heute? ${esc(sayToday)}</div>
    </div>

    <details class="breadthDetails">
      <summary class="breadthSummary">Mehr Details</summary>
      <div class="breadthDetailsBody">
        <div>
          <div class="muted small">Warum/Kontext</div>
          <div class="breadthRow">
            ${chip(`Strkste Sule ${topPillar || ''}`, 'blue')}
            ${chip(`Strkstes Cluster ${topCluster || ''}`, 'blue')}
          </div>
        </div>

        <div>
          <div class="muted small">Handelbarkeit</div>
          <div class="breadthRow">
            ${chip(`Trend OK ${pct(trendOk, n).toFixed(2)}%`, 'blue')}
            ${chip(`Liq OK ${pct(liqOk, n).toFixed(2)}%`, 'blue')}
            ${chip(`N ${n}`, 'blue')}
          </div>
        </div>

        <div>
          <div class="muted small">Kandidaten</div>
          <div class="breadthRow">
            ${chip(`Top-Anteil ${pct(topPctl, n).toFixed(2)}%`, 'blue')}
            ${chip(`Basis Perf% ${basisPerfPct}/${n}`, 'blue')}
          </div>
        </div>

        <div>
          <div class="muted small">Qualitt</div>
          <div class="breadthRow">
            ${chip(`Typische Qualitt ${sm.med !== null ? sm.med.toFixed(1) : ''}`, 'blue')}
            ${chip(`Streuung ${sm.iqr !== null ? sm.iqr.toFixed(1) : ''}`, 'blue')}
            ${chip(`Daten-Vertrauen typisch ${cm.med !== null ? cm.med.toFixed(1) : ''}`, 'blue')}
            ${chip(`Vertrauen Streuung ${cm.iqr !== null ? cm.iqr.toFixed(1) : ''}`, 'blue')}
          </div>
        </div>
      </div>
    </details>
  `;
}
function renderMovers(rows) {
  if (!elMoversUp || !elMoversDown) return;

  function perf1yPct(r) {
    return parsePct(
      r.perf_1y ?? r.perf1y ?? r.perf_1yr ?? r.perf_1year ?? r['Perf 1Y %'] ?? r['Perf 1Y'] ??
      r.change_1y_pct ?? r.change1y_pct ?? r.yoy_pct ?? r.perf_year
    );
  }

  const arr = [];
  for (const r of rows || []) {
    const p = perfPct(r);
    if (p === null) continue;
    arr.push({ r, p, y: perf1yPct(r) });
  }

  if (!arr.length) {
    elMoversUp.innerHTML = `<span class="muted"></span>`;
    elMoversDown.innerHTML = `<span class="muted"></span>`;
    return;
  }

  const up = arr.slice().sort((a,b) => b.p - a.p).filter(x => x.p > 0).slice(0, 10);
  const dn = arr.slice().sort((a,b) => a.p - b.p).filter(x => x.p < 0).slice(0, 10);

  function segShort(r) {
    const full = normStr(clusterLabel(r)) || (asBool(r.is_crypto) === true ? 'Krypto' : '');
    if (!full) return { short: '', full: '' };
    
    // Kürzere Abkürzungen für schmale Pills (ca. 8-10 Zeichen)
    const words = full.split(' ');
    let short = '';
    
    if (words.length >= 2) {
      // Erste Buchstaben der ersten 2 Worte (z.B. "Internet Content" -> "IC")
      short = words.slice(0, 2).map(w => w.charAt(0)).join('');
    } else if (words.length === 1) {
      // Bei einem Wort: erste 8-10 Zeichen
      const word = words[0];
      short = word.length > 8 ? word.slice(0, 8) + '' : word;
    }
    
    // Fallback falls zu kurz
    if (short.length < 2) {
      short = full.slice(0, 8);
    }
    
    return { short, full };
  }

  function itemHtml(x) {
    const r = x.r;
    const sym = pickDisplaySymbol(r);
    const yh  = pickYahooSymbol(r) || sym;
    const href = yahooHref(yh);
    const symHtml = href
      ? `<a class="yf" href="${href}" target="_blank" rel="noopener">${esc(sym)}</a>` 
      : esc(sym);

    const cls1d = x.p > 0 ? 'pos' : (x.p < 0 ? 'neg' : 'flat');
    const cls1y = (x.y === null) ? 'flat' : (x.y > 0 ? 'pos' : (x.y < 0 ? 'neg' : 'flat'));

    const line1 = `<span class="mvLine ${cls1d}">1D ${esc(fmtPct(x.p))}</span>`;
    const line2 = (x.y === null)
      ? `<span class="mvLine flat">1Y n/a</span>` 
      : `<span class="mvLine ${cls1y}">1Y ${esc(fmtPct(x.y))}</span>`;

    const seg = segShort(r);
    const segHtml = `<span class="mvSeg" title="${esc(seg.full || '')}">${esc(seg.short)}</span>`;

    return `<div class="moversItem">
      <div class="mvSym">${symHtml}</div>
      <div class="mvVals">${line1}${line2}</div>
      ${segHtml}
    </div>`;
  }

  // Wichtig: Container sollte .moversList tragen (falls du das schon im HTML hast, ok; sonst bleibt es trotzdem sichtbar)
  elMoversUp.innerHTML = up.length ? `<div class="moversList">${up.map(itemHtml).join('')}</div>` : `<span class="muted"></span>`;
  elMoversDown.innerHTML = dn.length ? `<div class="moversList">${dn.map(itemHtml).join('')}</div>` : `<span class="muted"></span>`;
}

function renderHeatmap(rows) {
  if (!elHeatmap) return;
  const fn = (heatMode === 'cluster') ? clusterLabel : pillarLabel;

  // counts[cat][sb] -> number
  const m = new Map();
  for (const r of rows || []) {
    const cat = (fn(r) || '').toString().trim();
    if (!cat) continue;
    const sb = scoreBucket(r.score);
    if (!m.has(cat)) m.set(cat, [0,0,0,0,0]);
    m.get(cat)[sb] += 1;
  }
  if (!m.size) {
    elHeatmap.innerHTML = `<div class="muted">Keine Daten für Heatmap (keine Kategorie im aktuellen Universe).</div>`;
    return;
  }

  // choose top categories
  const all = Array.from(m.entries()).map(([k, arr]) => ({k, arr, tot: arr.reduce((a,b)=>a+b,0)}));
  all.sort((a,b) => b.tot - a.tot || a.k.localeCompare(b.k));
  const limit = (heatMode === 'cluster') ? 8 : 6;
  const top = all.slice(0, limit);

  let vmax = 0;
  for (const x of top) for (const v of x.arr) vmax = Math.max(vmax, v);

  const hdr = Array.from({length:5}, (_,sb) => scoreBucketText(sb).range);
  const th = hdr.map(h => `<th class="heatCell">${esc(h)}</th>`).join('');

  const rowsHtml = top.map(x => {
    const rowOn = heatFilter.mode === heatMode && heatFilter.cat === x.k;

    const tds = x.arr.map((v, sb) => {
      const zero = v === 0 ? ' zero' : '';
      const rel = vmax ? (v / vmax) : 0;
      const alpha = 0.06 + rel * 0.28; // subtle
      const hue = 205; // blue-ish
      const bg = `background: hsla(${hue}, 70%, 50%, ${alpha});`;
      const cellOn = rowOn && heatFilter.sb !== null && Number(heatFilter.sb) === sb;
      const act = cellOn ? ' active' : '';
      return `<td class="heatCell${zero}${act}" data-hcat="${esc(x.k)}" data-sb="${sb}" style="${bg}" title="${esc(x.k)} · Score ${esc(hdr[sb])} = ${v}">${v ? v : '·'}</td>`;
    }).join('');

    const rowAct = rowOn ? ' active' : '';
    return `<tr class="heatRow${rowOn ? ' heatRowOn' : ''}"><td class="mono heatRowLabel${rowAct}" data-hcat="${esc(x.k)}" title="Filter: ${esc(x.k)}">${esc(x.k)}</td>${tds}</tr>`;
  }).join('');

  elHeatmap.innerHTML = `
    <div class="matrixGrid" style="grid-template-columns: 84px repeat(5, 1fr);">
      <!-- Corner cell with mode labels -->
      <div class="matrixAxis" style="display:flex; flex-direction:column; align-items:center; justify-content:center; gap:2px; padding: 6px 4px;">
        <div class="lbl" style="font-weight: 700;">${heatMode === 'cluster' ? 'Cluster' : 'Säule'} </div>
        <div class="hint" style="font-size: 9px;">Score </div>
      </div>
      <!-- Score bucket headers -->
      ${Array.from({length:5}, (_,sb) => {
        const s = scoreBucketText(sb);
        return `<div class="matrixLabel" title="ScoreBucket"><div class="lbl">${esc(s.range)}</div><div class="hint">${esc(s.hint)}</div></div>`;
      }).join('')}
      <!-- Category rows -->
      ${top.map(x => {
        const rowOn = heatFilter.mode === heatMode && heatFilter.cat === x.k;
        const parts = [];
        parts.push(`<div class="matrixLabel${rowOn ? ' active' : ''}" data-hcat="${esc(x.k)}" title="Filter: ${esc(x.k)}"><div class="lbl">${esc(x.k)}</div></div>`);
        for (let sb = 0; sb < 5; sb++) {
          const v = x.arr[sb] || 0;
          const rel = vmax ? (v / vmax) : 0;
          const alpha = 0.06 + rel * 0.28;
          const bg = `background: hsla(205, 70%, 50%, ${alpha});`;
          const cellOn = rowOn && heatFilter.sb !== null && Number(heatFilter.sb) === sb;
          const act = cellOn ? ' active' : '';
          const zero = v === 0 ? ' zero' : '';
          parts.push(`<div class="cell${zero}${act}" style="${bg}" data-hcat="${esc(x.k)}" data-sb="${sb}"><span class="cnt">${v ? v : '·'}</span></div>`);
        }
        return parts.join('');
      }).join('')}
    </div>
    <div class="muted small" style="margin-top:6px;">Zahl = Anzahl Werte pro ScoreBucket (Top ${limit} nach Häufigkeit).</div>
  `;

  elHeatmap.querySelectorAll('[data-hcat]').forEach(node => {
    node.addEventListener('click', () => {
      const cat = (node.getAttribute('data-hcat') || '').toString();
      const sbAttr = node.getAttribute('data-sb');
      const sb = (sbAttr === null || sbAttr === undefined) ? null : Number(sbAttr);
      if (!cat) return;

      if (sb === null) {
        if (heatFilter.mode === heatMode && heatFilter.cat === cat && heatFilter.sb === null) {
          heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER);
        } else {
          heatFilter = { cat, sb: null, mode: heatMode };
        }
      } else {
        if (heatFilter.mode === heatMode && heatFilter.cat === cat && heatFilter.sb === sb) {
          heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER);
        } else {
          heatFilter = { cat, sb, mode: heatMode };
        }
      }
      refresh();
      saveState();
    });
  });
}

function renderMarketContext(rows) {
  if (!elMarketPanel) return;
  renderBreadth(rows);
  renderMovers(rows);
  renderHeatmap(rows);
  renderHistoryDeltaPanel(rows);
}

function applyHeatFilter(rows) {
  if (!heatFilter || !heatFilter.cat) return rows;
  return (rows || []).filter(r => {
    const label = (heatFilter.mode === 'cluster') ? clusterLabel(r) : pillarLabel(r);
    if (label !== heatFilter.cat) return false;
    if (heatFilter.sb === null || heatFilter.sb === undefined) return true;
    return scoreBucket(r.score) === Number(heatFilter.sb);
  });
}


    function render(rows) {
      tbody.innerHTML = '';
      const frag = document.createDocumentFragment();

      for (const r of rows) {
        const tr = document.createElement('tr');

        const tRaw = normStr(r.ticker);
        const isC = asBool(r.is_crypto) === true;

        const disp = pickDisplaySymbol(r);
        const yh = pickYahooSymbol(r) || disp;
        const href = yahooHref(yh);

        const isinRaw = normStr(r.isin);
        const isin = (!isC) ? (isinRaw || (looksLikeISIN(tRaw) ? tRaw : '')) : '';

        const curr = normStr(r.quote_currency) || normStr(r.currency) || normStr(r["Währung"]);
        const currChip = curr ? `<span class="tinychip" title="Währung">${esc(curr)}</span>` : '';

        const main = href ? `<a class="yf" href="${href}" target="_blank" rel="noopener">${esc(disp)}</a>` : esc(disp);
        // subline for the left "Symbol/ISIN" cell: for crypto show the Yahoo pair (e.g. BTC-USD),
        // for stocks show ISIN. Use a distinct variable name so we don't collide with other "sub" vars.
        const subTicker = isC ? (yh || '') : (isin || '');
        const subLabel = isC ? 'YahooSymbol' : 'ISIN';
        const subLine = `<div class="sub mono" title="${subLabel}">${esc(subTicker)}</div>`;
        const tCell = `<div class="tickerCell"><div class="tickerMain">${main}${currChip}</div>${subLine}</div>`;

        const n = normStr(r.name);

        // Official taxonomy (prefer industry, fallback sector). Manual fantasy sectors are not shown here.
        const sectorOfficial = normStr(r.sector) || normStr(r.Sector);
        const industryOfficial = normStr(r.industry) || normStr(r.Industry) || normStr(r.cluster_official);

        // Private pillars (5-säulen + playground) are metadata only (never affect scoring)
        // Use UI fallback derivation so older universes still show the concept.
        const pillar = pillarLabel(r);
        const bucketType = normStr(r.bucket_type);

        let taxLabel = '';
        let taxTitle = '';
        if (asBool(r.is_crypto)) {
          taxLabel = 'Krypto';
          taxTitle = 'Assetklasse (Krypto)';
        } else if (industryOfficial) {
          taxLabel = industryOfficial;
          taxTitle = 'Industrie (offiziell, Yahoo)';
        } else if (sectorOfficial) {
          taxLabel = sectorOfficial;
          taxTitle = 'Sektor (offiziell, Yahoo)';
        }

        const ctry = normStr(r.country);
        const subParts = [];
        if (taxLabel) subParts.push(`<span title="${esc(taxTitle)}">${esc(taxLabel)}</span>`);
        if (pillar) subParts.push(`<span class="muted" title="Säule (privat, Metadaten)">Säule: ${esc(pillar)}</span>`);
        if (bucketType && bucketType !== 'pillar' && bucketType !== 'none') subParts.push(`<span class="muted" title="Bucket-Type (privat)">(${esc(bucketType)})</span>`);
        if (ctry) subParts.push(esc(ctry));
        const subName = subParts.join(' · ');

        const price = asNum(r.price) ?? asNum(r["Akt. Kurs"]);
        const perf = asNum(r.perf_pct) ?? asNum(r["Perf %"]);
        const priceMain = (price === null) ? '' : `${fmtPrice(price)}${curr ? ' ' + esc(curr) : ''}`;
        const pCell = `<div class="priceCell"><div class="priceMain">${priceMain}</div>${perfLine(perf)}</div>`;

        const trend = asBool(r.trend_ok) ? chip('OK', 'good') : chip('NO', 'bad');
        const liq = asBool(r.liquidity_ok) ? chip('OK', 'good') : chip('LOW', 'warn');

        const status = normStr(r.score_status);
        let statusKind = 'blue';
        if (status === 'OK') statusKind = 'good';
        if (status.startsWith('AVOID')) statusKind = 'warn';
        if (status === 'ERROR' || status === 'NA') statusKind = 'bad';

        const cls = asBool(r.is_crypto) ? chip('Krypto', 'warn') : chip('Aktie', 'blue');

        tr.innerHTML = `
          <td class="mono">${tCell}</td>
          <td>
            <div class="row-title">
              <div class="name">${n}</div>
              <div class="sub">${subName || ''}</div>
            </div>
          </td>
          <td class="right">${pCell}</td>
          <td>${scoreCell(r)}</td>
          <td class="hide-sm right mono">${dScoreCell(r)}</td>
          <td class="hide-sm right mono">${(asNum(r.confidence) ?? 0).toFixed(1)}</td>
          <td class="hide-sm right mono">${(asNum(r.cycle) ?? 0).toFixed(0)}%</td>
          <td>${trend}</td>
          <td>${liq}</td>
          <td>${chip(status || '', statusKind)}</td>
          <td class="hide-sm">${cls}</td>
        `;

        const a = tr.querySelector('a.yf');
        if (a) {
          a.addEventListener('click', (e) => { e.stopPropagation(); });
        }

        tr.style.cursor = 'pointer';
        tr.addEventListener('click', () => openDrawer(r));

        frag.appendChild(tr);
      }

      tbody.appendChild(frag);
    }

    function closeDrawer() {
      drawerOverlay.classList.remove('show');
      drawerOverlay.setAttribute('aria-hidden', 'true');
      document.body.style.overflow = '';
    }

    function openDrawer(r) {
      const t = pickDisplayTicker(r) || normStr(r.ticker);
      const n = normStr(r.name);
      drawerTitle.textContent = `${t}  ${n}`.trim();
      const sectorOfficial = normStr(r.sector) || normStr(r.Sector);
      const categoryManual = normStr(r.category) || normStr(r.Sektor) || normStr(r.Kategorie);
      const cat = asBool(r.is_crypto) ? 'Krypto' : (categoryManual ? `Cluster: ${categoryManual}` : (sectorOfficial || ''));
      const curr = normStr(r.quote_currency) || normStr(r.currency) || normStr(r["Währung"]);
      const sub = [cat, normStr(r.country), curr, normStr(r.isin)].filter(Boolean).join(' · ');
      drawerSub.textContent = sub || '';

      // Quick action: open on Yahoo Finance if we can determine a valid symbol
      if (drawerActions) {
        const sym = pickYahooSymbol(r);
        const href = yahooHref(sym);
        drawerActions.innerHTML = href ? `<a class="btn" href="${href}" target="_blank" rel="noopener" title="Auf Yahoo Finance öffnen">Yahoo</a>` : '';
      }

      const items = [
        ['Score', (asNum(r.score) ?? 0).toFixed(2)],
        ['Confidence', (asNum(r.confidence) ?? 0).toFixed(1)],
        ['Cycle', `${(asNum(r.cycle) ?? 0).toFixed(0)}%`],
        ['ScoreStatus', normStr(r.score_status) || ''],
        ['Trend OK', String(asBool(r.trend_ok))],
        ['Liquidity OK', String(asBool(r.liquidity_ok))],
        ['AssetClass', asBool(r.is_crypto) ? 'Krypto' : 'Aktie'],
      ];

      // optional interesting fields
      const opt = [
        ['Price', r.price],
        ['Currency', curr],
        ['Perf %', r.perf_pct],
        ['RS3M', r.rs3m],
        ['CRV', r.crv],
        ['MC Chance', r.mc_chance],
        ['Elliott', r.elliott_signal],
        ['CycleStatus', r.cycle_status],
        ['DollarVolume', r.dollar_volume],
        ['Volatility', r.volatility],
        ['MaxDrawdown', r.max_drawdown],
        ['MarketDate', r.market_date],
      ];
      for (const [k, v] of opt) {
        const s = normStr(v);
        if (s) items.push([k, s]);
      }

      const status = normStr(r.score_status);
      const why = [];
      if (status === 'OK') why.push('Score>0 & keine harten Filter verletzt.');
      if (status === 'AVOID_CRYPTO_BEAR') why.push('Crypto im Bear-Trend  Score=0 (bewusstes Avoid).');
      if (status === 'AVOID') why.push('Score==0  Avoid (non-crypto).');
      if (status === 'NA') why.push('Zu wenig / nicht konsistente Daten  NA.');
      if (status === 'ERROR') why.push('Scoring hat einen Fehler gemeldet (ScoreError).');
      if (asBool(r.trend_ok) === false) why.push('Trend-Filter: trend_ok=false.');
      if (asBool(r.liquidity_ok) === false) why.push('Liquidity-Filter: liquidity_ok=false.');
      if (why.length === 0) why.push('Noch kein detaillierter Factor-Breakdown (kommt in Phase B3).');

      drawerBody.innerHTML = `
        <div class="kv">
          ${items.map(([k,v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(v)}</div>`).join('')}
        </div>
        <div class="why">
          <div style="font-weight:700; margin-top: 12px;">Why Score</div>
          <ul>
            ${why.map(x => `<li>${esc(x)}</li>`).join('')}
          </ul>
        </div>
      `;

      drawerOverlay.classList.add('show');
      drawerOverlay.setAttribute('aria-hidden', 'false');
      document.body.style.overflow = 'hidden';
    }

    function refresh() {
      const base = DATA;
      const {rows: presetRows, preset} = applyPreset(base, activePreset);

      const q = elSearch.value;
      let rowsSQ = applySearch(presetRows, q);
      rowsSQ = applyQuickFilters(rowsSQ);

      // cluster counts reflect the current universe (after Preset+Search+Quick)
      const cc = computeClusterCounts(rowsSQ);
      renderClusterOptions(cc);
      renderClusterChips(cc);

      // pillar counts (5-säulen + playground) reflect the same universe
      const pc = computePillarCounts(rowsSQ);
      renderPillarOptions(pc);
      renderPillarChips(pc);

      // cluster + pillar filters (string)
      let rows = applyClusterFilter(rowsSQ);
      rows = applyPillarFilter(rows);
      rows = attachDScore(rows);

      // matrix counts always reflect the current (pre-matrix) universe (after cluster filter)
      renderMatrix(rows);
      renderMarketContext(rows);
      renderSegmentMonitor(rows);

      rows = applyHeatFilter(rows);

      // matrix filter (if active)
      rows = applyMatrixFilter(rows);

      // user override sort
      if (userSort && userSort.k) {
        rows = rows.slice().sort(compareBy([userSort]));
      }

      render(rows);
      renderKpis(base, rows);
      const f = [];
      if (quick.hideAvoid) f.push('hideAvoid');
      if (quick.onlyOK) f.push('onlyOK');
      if (quick.trendOK) f.push('trendOK');
      if (quick.liqOK) f.push('liqOK');
      if (quick.onlyStock) f.push('stock');
      if (quick.onlyCrypto) f.push('crypto');
      if (matrix && matrix.sb !== null && matrix.rb !== null) f.push(`matrix:${matrix.sb}x${matrix.rb}`);
      if (heatFilter && heatFilter.cat) {
        const suffix = (heatFilter.sb === null || heatFilter.sb === undefined) ? '' : `:${heatFilter.sb}`;
        f.push(`heat:${heatFilter.mode || heatMode}:${heatFilter.cat}${suffix}`);
      }
      const _cps = Array.isArray(clusterPick) ? clusterPick : ((clusterPick || '') ? [String(clusterPick)] : []);
      if (_cps.length) f.push(`cluster:${_cps.join('|')}`);
      const _pps = Array.isArray(pillarPick) ? pillarPick : ((pillarPick || '') ? [String(pillarPick)] : []);
      if (_pps.length) f.push(`pillar:${_pps.join('|')}`);

      elCount.textContent = `${rows.length} / ${base.length}` + (f.length ? `  ·  filters: ${f.join(',')}` : '');
      if (btnMatrixClear) btnMatrixClear.disabled = !(matrix && matrix.sb !== null && matrix.rb !== null);

      const override = userSort ? ` | override: ${userSort.k}:${userSort.dir}` : '';
      elSortHint.textContent = `preset: ${presetLabel(activePreset)}${override}`;
    }

    // ---- init ----
    const PRESET_LABELS = {
      CORE: 'bersicht',
      SCORED: 'Bewertet',
      TOP: 'Top',
      TOP_RELAXED: 'Top (entspannt)',
      AVOID: 'Vermeiden',
      BROKEN: 'Fehler/NA',
    };

    function presetLabel(key) {
      return PRESET_LABELS[key] || key;
    }

    function initPresets() {
      if (elPreset) elPreset.innerHTML = '';
      const names = Object.keys(PRESETS);
      // keep CORE first if present
      names.sort((a,b) => (a === 'CORE' ? -1 : b === 'CORE' ? 1 : a.localeCompare(b)));

      for (const n of names) {
        const opt = document.createElement('option');
        opt.value = n;
        const desc = (PRESETS[n].description || '').toString();
        opt.textContent = `${presetLabel(n)} (${n})` + (desc ? `  ${desc}` : '');
        elPreset.appendChild(opt);
      }
      elPreset.value = activePreset;
    }

    initPresets();
    // quick filters
    // Toggle helper for multi-select sets
    function toggleSet(set, val) {
      if (val === '__ALL__') { 
        set.clear(); 
        return; 
      }
      if (val === '__CLEAR__') { 
        set.clear(); 
        return; 
      }
      if (set.has(val)) set.delete(val); 
      else set.add(val);
    }

    // Unified Event Delegation for all chips and buttons
    console.log('delegation ready');
    document.addEventListener('click', (e) => {
      // Handle help buttons (i-Buttons)
      const helpBtn = e.target.closest('[data-action="help"]');
      if (helpBtn) {
        console.log('help', helpBtn.getAttribute('data-help-title'));
        openHelp(helpBtn);
        return;
      }

      // Handle chips (pillar/cluster)
      const chip = e.target.closest('[data-chip]');
      if (chip) {
        const type = chip.getAttribute('data-chip');
        const val = chip.getAttribute('data-val');
        console.log('chip', type, val);
        
        if (type === 'pillar') {
          toggleSet(uiState.selPillars, val);
          syncSelectionArrays();
          const base = DATA;
          const {rows: presetRows} = applyPreset(base, activePreset);
          const q = elSearch.value;
          let rowsSQ = applySearch(presetRows, q);
          rowsSQ = applyQuickFilters(rowsSQ);
          renderPillarChips(computePillarCounts(rowsSQ));
        } else if (type === 'cluster') {
          toggleSet(uiState.selClusters, val);
          syncSelectionArrays();
          const base = DATA;
          const {rows: presetRows} = applyPreset(base, activePreset);
          const q = elSearch.value;
          let rowsSQ = applySearch(presetRows, q);
          rowsSQ = applyQuickFilters(rowsSQ);
          renderClusterChips(computeClusterCounts(rowsSQ));
        }
        
        refresh();
        return;
      }

      const btnToggle = e.target.closest('.btnToggle[data-toggle]');
      if (btnToggle) {
        const panel = btnToggle.getAttribute('data-toggle');
        if (panel) {
          const card = document.querySelector(`.card[data-panel="${panel}"]`);
          if (card) {
            const collapsed = card.classList.toggle('is-collapsed');
            btnToggle.textContent = collapsed ? 'Einblenden' : 'Ausblenden';
          }
        }
        return;
      }

      if (e.target.closest('#heatmapClear')) {
        heatFilter = Object.assign({}, DEFAULT_HEAT_FILTER);
        refresh();
        saveState();
        return;
      }

      // Handle action buttons (quick filters, reset, etc.)
      const act = e.target.closest('[data-action]:not([data-action="help"])');
      if (act) {
        const action = act.getAttribute('data-action');
        const key = act.getAttribute('data-key');
        const target = act.getAttribute('data-target');
        
        console.log('delegation action', action, key, target);
        
        if (action === 'toggle' && key) {
          if (key === 'resetAll') {
            resetAll();
          } else if (key === 'resetSort') {
            userSort = null;
            refresh();
            saveState();
          } else if (key === 'onlyCrypto') {
            uiState.quick.onlyCrypto = !uiState.quick.onlyCrypto;
            if (uiState.quick.onlyCrypto) uiState.quick.onlyStock = false;
          } else if (key === 'onlyStock') {
            uiState.quick.onlyStock = !uiState.quick.onlyStock;
            if (uiState.quick.onlyStock) uiState.quick.onlyCrypto = false;
          } else {
            uiState.quick[key] = !uiState.quick[key];
          }
          
          syncFilterButtons();
          refresh();
          saveState();
        } else if (action === 'togglePanel' && target) {
          if (target === 'market') {
            marketVisible = !marketVisible;
            setMarketVisible(marketVisible);
            saveState();
          } else if (target === 'briefing') {
            briefingVisible = !briefingVisible;
            setBriefingVisible(briefingVisible);
          }
        } else if (action === 'resetSort') {
          userSort = null;
          refresh();
          saveState();
        } else if (action === 'resetAll') {
          resetAll();
        }
        return;
      }
    });
    syncFilterButtons();

    // cluster select wiring
    if (elClusterSel) {
      elClusterSel.addEventListener('change', () => {
        const v = (elClusterSel.value || '').toString().trim();
        uiState.selClusters.clear();
        if (v) uiState.selClusters.add(v);
        syncSelectionArrays();
        refresh();
        saveState();
      });
    }

    // pillar select wiring
    if (elPillarSel) {
      elPillarSel.addEventListener('change', () => {
        const v = (elPillarSel.value || '').toString().trim();
        uiState.selPillars.clear();
        if (v) uiState.selPillars.add(v);
        syncSelectionArrays();
        refresh();
        saveState();
      });
    }
    
    
    
    
    // ---- Unified Help Popover System ----
    function getHelpEls() {
      return {
        pop: document.getElementById('helpPop'),
        title: document.getElementById('helpPopTitle'),
        body: document.getElementById('helpPopBody'),
      };
    }

    // Null-safe helper for contains checks
    function _in(el, t) {
      if (!el) return false;
      try { return el === t || el.contains(t); } catch (e) { return false; }
    }

    function openHelp(button) {
      const { pop: helpPop, title: helpPopTitle, body: helpPopBody } = getHelpEls();
      if (!helpPop || !helpPopTitle || !helpPopBody || !button) return;

      // Get content from button
      const title = button.getAttribute('data-help-title') || 'Hilfe';
      const html = button.getAttribute('data-help-html') || button.getAttribute('data-help') || '';
      // Set content
      helpPopTitle.textContent = title;
      helpPopBody.innerHTML = html;

      // Position popover relative to button
      const rect = button.getBoundingClientRect();
      const popoverWidth = 360; // max-width from CSS
      const viewportWidth = window.innerWidth;
      const viewportHeight = window.innerHeight;
      // Work in viewport coordinates first, then convert once to page coordinates.
      let left = rect.right + 8;
      let top = rect.bottom + 8;

      if (left + popoverWidth > viewportWidth - 12) {
        left = rect.left - popoverWidth - 8;
      }
      if (left < 12) left = 12;

      const estimatedHeight = Math.min(260, Math.max(140, helpPop.offsetHeight || 200));
      if (top + estimatedHeight > viewportHeight - 12) {
        top = rect.top - estimatedHeight - 8;
      }
      if (top < 12) top = 12;

      helpPop.style.left = `${left}px`;
      helpPop.style.top = `${top}px`;
      
      // Show popover
      helpPop.classList.remove('hidden');
      helpPop.classList.add('show');
      helpPop.setAttribute('aria-hidden', 'false');
      button.setAttribute('aria-expanded', 'true');
    }

    function closeHelp() {
      const { pop: helpPop } = getHelpEls();
      if (!helpPop) return;
      
      helpPop.classList.add('hidden');
      helpPop.classList.remove('show');
      helpPop.setAttribute('aria-hidden', 'true');
      
      // Reset all button states
      document.querySelectorAll('.iBtn[aria-expanded="true"]').forEach(btn => {
        btn.setAttribute('aria-expanded', 'false');
      });
    }

    // Event delegation for all i-buttons
    document.addEventListener('click', (e) => {
      const button = e.target.closest('.iBtn');
      if (button) {
        e.stopPropagation();
        openHelp(button);
      } else if (!_in(getHelpEls().pop, e.target)) {
        closeHelp();
      }
    });

    // ESC key closes popover
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        closeHelp();
      }
    });

    // Close popover on scroll/resize (optional UX)
    let activeAnchor = null;
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        mutation.removedNodes.forEach((node) => {
          if (activeAnchor && (node === activeAnchor || node.contains && node.contains(activeAnchor))) {
            activeAnchor = null;
            closeHelp();
          }
        });
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });

    // Close on resize/scroll
    window.addEventListener('resize', closeHelp);
    window.addEventListener('scroll', closeHelp, true);

    if (elBriefing) {
      const hasTop = Array.isArray(briefing.top) && briefing.top.length > 0;
      const debugInfo = 'briefing: text=' + (briefing.text ? 'yes' : 'no') + ' | meta=' + (briefing.meta ? 'yes' : 'no') + ' | top=' + (hasTop ? briefing.top.length : 0);
      const isinRe = /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/;
      const escRe = (s) => String(s || '').replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&');
      function normalizeBriefHeader(rawSymbol, rawName) {
        const sym = normStr(rawSymbol);
        let name = normStr(rawName) || '?';
        if (sym) name = name.replace(new RegExp('^' + escRe(sym) + '\\s*[\\-–—:]\\s*', 'i'), '');
        name = name.replace(/^([A-Z]{2}[A-Z0-9]{9}[0-9])\\s*[\\-–—:]\\s*/i, '');
        let showSym = sym;
        const nameUpper = name.toUpperCase();
        const symUpper = sym.toUpperCase();
        if (sym && (nameUpper === symUpper || nameUpper.includes('(' + symUpper + ')'))) showSym = '';
        if (sym && isinRe.test(symUpper) && /\\([A-Z]{2}[A-Z0-9]{9}[0-9]\\)/.test(nameUpper)) showSym = '';
        return { displaySymbol: esc(showSym), displayName: esc(name || '?') };
      }
      
      if (!briefing.text && !hasTop) {
        elBriefing.innerHTML = '<div class="debugInfo">' + debugInfo + '</div><div class="briefingMissing">Warnung: Briefing Report fehlt / nicht generiert</div>';
      } else if (hasTop) {
        // Use structured data if available
        const meta = briefing.meta || {};
        const top = briefing.top || [];
        
        // Meta line
        const metaLine = '<div class="briefingMeta">' + esc(meta.date || '?') + ' · ' + esc(meta.generated_at ? new Date(meta.generated_at).toLocaleString('de-DE', {day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit'}) : '?') + ' · ' + esc(meta.universe_count || '?') + '/' + esc(meta.scored_count || '?') + ' scored</div>';
        
        // Top 3 picks
        const picksHtml = top.slice(0, 3).map((pick, idx) => {
          const symbol = pick.symbol || pick.isin || '?';
          const name = pick.name || '?';
          const score = pick.score ? pick.score.toFixed(2) : '?';
          const scorePctl = pick.score_pctl ? (pick.score_pctl).toFixed(1) + '%' : '?';
          const riskBucket = pick.risk_bucket != null ? 'R' + pick.risk_bucket : '?';
          const trendOk = pick.trend_ok ? 'OK' : 'Nein';
          const liqOk = pick.liq_ok ? 'OK' : 'Nein';
          const cluster = esc(pick.cluster || '?');
          const pillar = esc(pick.pillar_primary || '?');
          const reasons = Array.isArray(pick.reasons) ? pick.reasons.slice(0, 4) : [];
          
          const normHead = normalizeBriefHeader(symbol, name);
          const displayName = normHead.displayName;
          const displaySymbol = normHead.displaySymbol;
          
          return '<div class="briefingPick">' +
            '<div class="briefingPickHeader">' +
              '<span class="briefingPickRank">#' + (idx + 1) + '</span>' +
              '<span class="briefingPickSymbol">' + displaySymbol + '</span>' +
              '<span class="briefingPickName">' + displayName + '</span>' +
            '</div>' +
            '<div class="briefingPickBadges">' +
              '<span class="briefingBadge">Score: ' + score + ' (' + scorePctl + ')</span>' +
              '<span class="briefingBadge">Risk: ' + riskBucket + '</span>' +
              '<span class="briefingBadge">Trend: ' + trendOk + '</span>' +
              '<span class="briefingBadge">Liq: ' + liqOk + '</span>' +
              '<span class="briefingBadge">' + cluster + '</span>' +
              '<span class="briefingBadge">' + pillar + '</span>' +
            '</div>' +
            (reasons.length ? '<div class="briefingPickReasons">' + reasons.map(r => '<div class="briefingReason">• ' + esc(r) + '</div>').join('') + '</div>' : '') +
          '</div>';
        }).join('');
        
        elBriefing.innerHTML = '<div class="debugInfo">' + debugInfo + '</div><div class="renderProof">BRIEFING_RENDER: structured-data picks=' + top.length + '</div><div class="briefingStructured">' + metaLine + picksHtml + '</div>';
      } else {
        // Parse briefing text into picks
        function parseBriefingTextToPicks(text) {
          const lines = text.split('\\n').filter(line => line.trim());
          const picks = [];
          let currentPick = null;
          let foundFirstPick = false;
          
          for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            
            // Skip separator lines
            if (/^(—+|_+|-+)$/.test(line)) continue;
            
            // Check for pick header patterns
            const hashMatch = line.match(/^#\\s*(\\d+)\\s*(.+)$/);
            const numMatch = line.match(/^(\\d+)[).:]\\s*(.+)$/);
            const match = hashMatch || numMatch;
            
            if (match) {
              foundFirstPick = true;
              // Save previous pick if exists
              if (currentPick) picks.push(currentPick);
              
              // Parse symbol and name
              const header = match[2];
              const symbolMatch = header.match(/^([A-Z]{2}[A-Z0-9]{8,12}|[A-Z]{1,5})\\s*[\\-–—]\\s*(.+)$/);
              const symbol = symbolMatch ? symbolMatch[1] : header.split(/\\s+[\\-–—]\\s+/)[0] || '?';
              const name = symbolMatch ? symbolMatch[2] : header.split(/\\s+[\\-–—]\\s+/)[1] || header;
              
              currentPick = {
                rank: parseInt(match[1]),
                symbol: symbol,
                name: name,
                reasons: []
              };
            } else if (foundFirstPick && currentPick) {
              // Add as reason if we have a current pick
              const isBullet = line.startsWith('-') || line.startsWith('•') || line.startsWith('*');
              const reasonText = isBullet ? line.substring(1).trim() : line;
              if (reasonText && currentPick.reasons.length < 6) {
                currentPick.reasons.push(reasonText);
              }
            }
          }
          
          // Add last pick
          if (currentPick) picks.push(currentPick);
          
          return picks;
        }
        
        const picks = parseBriefingTextToPicks(briefing.text || '');
        
        // Meta line from briefing.meta or fallback extraction
        let metaLine = '';
        const meta = briefing.meta || {};
        if (meta.date || meta.generated_at || meta.universe_count) {
          metaLine = '<div class="briefingMeta">' + 
            (meta.date || '?') + ' · ' + 
            (meta.generated_at ? new Date(meta.generated_at).toLocaleString('de-DE', {day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit'}) : '?') + ' · ' +
            (meta.universe_count || '?') + '/' + (meta.scored_count || '?') + ' scored' +
          '</div>';
        } else {
          // Try to extract meta from briefing.text using regex
          const text = briefing.text || '';
          const dateMatch = text.match(/Datum:\\s*(\\d{2}\\.\\d{2}\\.\\d{4}|\\d{4}-\\d{2}-\\d{2})/i);
          const generatedMatch = text.match(/Generiert:\\s*(\\d{2}:\\d{2}|\\d{2}\\.\\d{2}\\.\\d{4}\\s+\\d{2}:\\d{2})/i);
          const universeMatch = text.match(/Universe:\\s*(\\d+)/i);
          const scoredMatch = text.match(/Scored:\\s*(\\d+)/i);
          
          if (dateMatch || generatedMatch || universeMatch) {
            const parts = [];
            if (dateMatch) parts.push(dateMatch[1]);
            if (generatedMatch) parts.push(generatedMatch[1]);
            if (universeMatch && scoredMatch) parts.push(universeMatch[1] + '/' + scoredMatch[1] + ' scored');
            else if (universeMatch) parts.push('Universe: ' + universeMatch[1]);
            
            if (parts.length > 0) {
              metaLine = '<div class="briefingMeta">' + parts.join(' · ') + '</div>';
            }
          }
        }
        
        // Render picks (max 3) with badge parsing
        const picksHtml = picks.slice(0, 3).map((pick, idx) => {
          // Parse badges from reasons
          const badgeKeywords = ['Score', 'Percentil', 'Bucket', 'Confidence', 'Trend', 'Liq', 'Liquidität', 'Cluster', 'Säule', 'Pillar'];
          const badges = [];
          const reasons = [];
          
          pick.reasons.forEach(reason => {
            const trimmed = reason.trim();
            // Check if reason starts with a badge keyword
            const isBadge = badgeKeywords.some(keyword => 
              trimmed.toLowerCase().startsWith(keyword.toLowerCase())
            );
            
            if (isBadge && !trimmed.toLowerCase().includes('gründe')) {
              badges.push(trimmed);
            } else if (trimmed && !trimmed.toLowerCase().includes('gründe')) {
              reasons.push(trimmed);
            }
          });
          
          const normHead = normalizeBriefHeader(pick.symbol, pick.name);
          const displayName = normHead.displayName;
          const displaySymbol = normHead.displaySymbol;
          
          return '<div class="briefingPick">' +
            '<div class="briefingPickHeader">' +
              '<span class="briefingPickRank">#' + pick.rank + '</span>' +
              '<span class="briefingPickSymbol">' + displaySymbol + '</span>' +
              '<span class="briefingPickName">' + displayName + '</span>' +
            '</div>' +
            (badges.length > 0 ? '<div class="briefingPickBadges">' + badges.map(b => '<span class="briefingBadge">' + esc(b) + '</span>').join('') + '</div>' : '') +
            (reasons.length > 0 ? '<ul class="briefingPickReasons">' + reasons.slice(0, 6).map(r => '<li>' + esc(r) + '</li>').join('') + '</ul>' : '') +
          '</div>';
        }).join('');
        
        const content = picks.length ? picksHtml : '<div class="briefingFallback">' + briefing.text.split('\\n').map(l => '<div class="briefingLine">' + esc(l) + '</div>').join('') + '</div>';
        
        const cardsRendered = picks.length;
        elBriefing.innerHTML = '<div class="debugInfo">' + debugInfo + '</div><div class="renderProof">BRIEFING_RENDER: parsed-text picks=' + picks.length + ' | briefing cards rendered: ' + cardsRendered + '</div><div class="briefingStructured">' + metaLine + content + '</div>';
      }
    }

    // Passive report panels (precomputed server-side)
    function renderHistoryDeltaPanel(rows) {
      if (!elHistory) return;
      try {
        const d = (HISTORY_DELTA || {});
        const latest = normStr(d.latest_date || d.latest || d.date || '');
        const prev   = normStr(d.prev_date   || d.prev   || d.prevDate || '');
        const snaps  = (latest && prev) ? 2 : 1;

        // Build items from CURRENT filtered rows + HD_BY mapping
        const items = [];
        for (const r of rows || []) {
          const k = historyKey(r);
          const rec = (k && HD_BY) ? HD_BY[k] : null;
          if (!rec) continue;

          const sd = asNum(rec.score_delta ?? rec.scoreDelta ?? rec.delta ?? rec.dscore_1d ?? rec.dscore);
          const rd = asNum(rec.rank_delta  ?? rec.rankDelta  ?? rec.dr    ?? rec.rank_change);
          if (sd === null && rd === null) continue;

          const sym  = pickDisplaySymbol(r);
          const yh   = pickYahooSymbol(r) || sym;
          const href = yahooHref(yh);

          const segFull  = normStr(clusterLabel(r)) || (asBool(r.is_crypto) === true ? 'Krypto' : '');
          const segShort = segFull
            ? (segFull.split(' ').slice(0, 2).join(' ').slice(0, 10) + (segFull.length > 10 ? '' : ''))
            : '';

          items.push({ sym, href, sd, rd, segFull, segShort });
        }

        function fmtDelta(n, digits) {
          if (n === null || n === undefined || !Number.isFinite(n)) return 'n/a';
          const sign = n > 0 ? '+' : '';
          return sign + n.toFixed(digits);
        }

        // Prefer score_delta lists, fallback to rank_delta if needed
        let pos = items.filter(x => x.sd !== null && x.sd > 0).sort((a,b) => b.sd - a.sd).slice(0, 12);
        let neg = items.filter(x => x.sd !== null && x.sd < 0).sort((a,b) => a.sd - b.sd).slice(0, 12);

        if (!pos.length && !neg.length) {
          pos = items.filter(x => x.rd !== null && x.rd > 0).sort((a,b) => b.rd - a.rd).slice(0, 12);
          neg = items.filter(x => x.rd !== null && x.rd < 0).sort((a,b) => a.rd - b.rd).slice(0, 12);
        }

        const header = `<div class="hdMeta">Snapshot: ${esc(prev || '')}  ${esc(latest || '')} · Universe: ${(rows||[]).length} · with : ${items.length}</div>`;

        // Deine schönen 4 Pills bleiben, plus Top/Weak Zähler als Bonus
        const controls = `<div class="breadthRow" style="margin-top:6px;">
          ${chip(`Snapshots ${snaps}`, 'blue')}
          ${chip(`1D`, 'blue')}
          ${chip(`1W n/a`, 'warn')}
          ${chip(`1M n/a`, 'warn')}
          ${chip(`Top ${pos.length}`, pos.length ? 'good' : 'blue')}
          ${chip(`Weak ${neg.length}`, neg.length ? 'bad' : 'blue')}
        </div>`;

        function itemRow(x) {
          const symHtml = x.href
            ? `<a class="yf hdSym" href="${x.href}" target="_blank" rel="noopener">${esc(x.sym)}</a>` 
            : `<span class="hdSym">${esc(x.sym)}</span>`;

          const sd = (x.sd === null || x.sd === undefined) ? null : Number(x.sd);
          const rd = (x.rd === null || x.rd === undefined) ? null : Number(x.rd);

          const sdCls = (sd === null) ? 'flat' : (sd > 0 ? 'pos' : (sd < 0 ? 'neg' : 'flat'));
          const rdCls = (rd === null) ? 'flat' : (rd > 0 ? 'pos' : (rd < 0 ? 'neg' : 'flat'));

          const line1 = `<span class="hdLine ${sdCls}">S ${esc(fmtDelta(sd, 2))}</span>`;
          const line2 = `<span class="hdLine ${rdCls}">R ${esc(fmtDelta(rd, 0))}</span>`;

          const seg = `<span class="hdSeg" title="${esc(x.segFull || '')}">${esc(x.segShort || '')}</span>`;

          return `<div class="hdItem">${symHtml}<div class="hdVals">${line1}${line2}</div>${seg}</div>`;
        }

        const topHtml  = pos.length ? pos.map(itemRow).join('') : `<div class="muted small"></div>`;
        const weakHtml = neg.length ? neg.map(itemRow).join('') : `<div class="muted small"></div>`;

        const grid = `<div class="hdGrid">
          <div><div class="hdColTitle">Top </div><div class="hdList">${topHtml}</div></div>
          <div><div class="hdColTitle">Weak </div><div class="hdList">${weakHtml}</div></div>
        </div>`;

        const explain = `<div class="muted small" style="margin-top:8px;">
          Positiv bedeutet: im Ranking/Score gestiegen. Anzeige ist <b>passiv</b> (kein Einfluss auf Scoring) und wird aus <span class="mono">history_delta.json</span> + aktuellem Filter gebaut.
        </div>`;

        elHistory.innerHTML = `<div class="hdWrap">${header}${controls}${grid}${explain}</div>`;
      } catch (e) {
        elHistory.textContent = '';
      }
    }

    function renderReality(r) {
      try {
        const hasStats = !!(r && r.stats && Object.keys(r.stats).length);
        const hasIssues = Array.isArray(r && r.top_issues) && r.top_issues.length > 0;
        const issuesCount = hasIssues ? r.top_issues.length : 0;
        
        // Debug info - show actual keys
        let debugInfo = `reality: stats=${hasStats ? 'yes' : 'no'} issues=${issuesCount}`;
        if (hasIssues && r.top_issues.length > 0) {
          debugInfo += ' | reality keys: ' + Object.keys(r.top_issues[0]).join(', ');
        }
        
        if (!r || (!hasStats && !hasIssues)) {
          return '<div class="debugInfo">' + debugInfo + '</div><div class="realityMissing">️ Reality Check Report fehlt / nicht generiert</div>';
        }
        
        const st = r.stats || {};
        const summary = '<div class="realitySummary">' +
          '<div class="summaryChip ok">ok: ' + esc(st.ok || 0) + '</div>' +
          '<div class="summaryChip warn">warn: ' + esc(st.warn || 0) + '</div>' +
          '<div class="summaryChip error">error: ' + esc(st.error || 0) + '</div>' +
        '</div>';
        
        if (hasIssues) {
          // Proper HTML table with robust key mapping
          const tableHtml = '<table class="realityTable">' +
            '<thead>' +
              '<tr>' +
                '<th>Intern</th>' +
                '<th>Offiziell</th>' +
                '<th>Scanner</th>' +
                '<th>Markt</th>' +
                '<th>Signal</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' +
            r.top_issues.slice(0, 12).map(issue => {
              // Robust key mapping with fallbacks
              const intern = esc(issue.intern || issue.internal || issue.pillar_primary || issue.scanner_pillar || '—');
              const official = esc(issue.offiziell || issue.official || issue.official_sector || issue.official_industry || '—');
              const scanner = esc(issue.scanner || issue.cluster || issue.scanner_cluster || issue.pillar_primary || '—');
              const market = esc(issue.market || issue.yahoo_sector || issue.yahoo_industry || issue.market_sector || '—');
              
              // Signal badge from severity/verdict
              let signalClass = 'neutral';
              let signalText = 'OK';
              const severity = (issue.severity || '').toLowerCase();
              const verdict = (issue.verdict || '').toLowerCase();
              const signal = (issue.signal || '').toLowerCase();
              
              if (severity === 'error' || severity === 'high' || verdict === 'contra' || signal === 'contra') {
                signalClass = 'contra';
                signalText = issue.signal || 'Kontra';
              } else if (severity === 'warn' || severity === 'medium' || verdict === 'warn' || signal === 'warn') {
                signalClass = 'neutral';
                signalText = issue.signal || 'Warn';
              } else {
                signalClass = 'positive';
                signalText = issue.signal || 'OK';
              }
              
              // Show problems as tooltip in signal cell if available
              let signalCell = '<span class="signalBadge ' + signalClass + '">' + signalText + '</span>';
              if (Array.isArray(issue.problems) && issue.problems.length > 0) {
                signalCell = '<span class="signalBadge ' + signalClass + '" title="' + esc(issue.problems.join('; ')) + '">' + signalText + '</span>';
              }
              
              return '<tr>' +
                '<td>' + intern + '</td>' +
                '<td>' + official + '</td>' +
                '<td>' + scanner + '</td>' +
                '<td>' + market + '</td>' +
                '<td>' + signalCell + '</td>' +
              '</tr>';
            }).join('') +
            '</tbody>' +
          '</table>' +
          (issuesCount > 12 ? '<div class="muted small" style="margin-top: 6px;"> weitere ' + (issuesCount - 12) + ' Einträge</div>' : '');
          
          const rowsRendered = Math.min(issuesCount, 12);
          return '<div class="debugInfo">' + debugInfo + '</div><div class="renderProof">REALITY_RENDER: top_issues rows=' + issuesCount + ' | reality rows rendered: ' + rowsRendered + '</div>' + summary + tableHtml;
        } else {
          // Fallback: render as list if no structured issues
          const fallbackHtml = '<div class="realityTable">' +
            '<div class="realityRow">' +
              '<div class="realityCell" style="grid-column: 1/-1;">' +
                '<div style="font-weight: 600; margin-bottom: 8px;">Reality Check Status</div>' +
                '<div>ok: ' + esc(st.ok || 0) + ' · warn: ' + esc(st.warn || 0) + ' · error: ' + esc(st.error || 0) + '</div>' +
              '</div>' +
            '</div>' +
          '</div>';
          
          return '<div class="debugInfo">' + debugInfo + '</div><div class="renderProof">REALITY_RENDER: fallback</div>' + summary + fallbackHtml;
        }
      } catch (e) { return ''; }
    }

    
    function renderSegmentMonitor(rows) {
      if (!elSegment) return;
      try {
        const s = (SEGMENT_MONITOR || {});
        const latest = esc(normStr(s.latest_date) || '');
        const prev = esc(normStr(s.prev_date) || '');

        // Group helper: total + valid (dScore) + sum + pos
        function buildGroups(getKey) {
          const map = new Map();
          for (const r of rows || []) {
            const k = normStr(getKey(r)) || '';
            const rec = map.get(k) || { total: 0, valid: 0, sum: 0, pos: 0 };
            rec.total += 1;
            const d = r.dscore_1d;
            if (Number.isFinite(d)) {
              rec.valid += 1;
              rec.sum += d;
              if (d > 0) rec.pos += 1;
            }
            map.set(k, rec);
          }
          const out = [];
          for (const [k, v] of map.entries()) {
            const cov = v.total > 0 ? (v.valid / v.total) : 0;
            const avg = v.valid > 0 ? (v.sum / v.valid) : null;
            const pp = v.valid > 0 ? (v.pos / v.valid) : null;
            out.push({ key: k, total: v.total, valid: v.valid, avg, posPct: pp, cov });
          }
          return out;
        }

        function fmtAvg(x) { return (x === null || x === undefined) ? '' : (x >= 0 ? '+' : '') + x.toFixed(2); }
        function fmtPct(x) { return (x === null || x === undefined) ? '' : (x * 100).toFixed(1) + '%'; }
        function fmtCov(x) { return (x === null || x === undefined) ? '' : (x * 100).toFixed(1) + '%'; }

        function stableBadge(valid) {
          if (!Number.isFinite(valid)) return '';
          const stable = valid >= 5;
          return stable ? '<span class="stableSample">stable</span>' : '<span class="stableSample" style="background: rgba(251,191,36,.08); border-color: rgba(251,191,36,.25); color: #fde68a;">thin</span>';
        }

        function nBadge(valid) {
          if (!Number.isFinite(valid)) return '';
          if (valid <= 2) return '<span class="stableSample" style="background: rgba(251,191,36,.08); border-color: rgba(251,191,36,.25); color: #fde68a;">' + valid + '</span>';
          return String(valid);
        }

        function renderTable(title, groups) {
          groups = groups.slice().sort((a,b) => {
            const av = (a.avg === null) ? -9999 : a.avg;
            const bv = (b.avg === null) ? -9999 : b.avg;
            if (bv !== av) return bv - av;
            return (b.valid - a.valid);
          }).slice(0, 12);

          const rowsHtml = groups.map(g => (
            '<tr>' +
              '<td title="' + esc(g.key) + '">' + esc(g.key) + '</td>' +
              '<td class="right">' + fmtAvg(g.avg) + '</td>' +
              '<td class="right">' + fmtPct(g.posPct) + '</td>' +
              '<td class="right">' + stableBadge(g.valid) + '</td>' +
              '<td class="right">' + fmtCov(g.cov) + '</td>' +
              '<td class="right">' + nBadge(g.valid) + '</td>' +
            '</tr>'
          )).join('');

          return (
            '<table class="segmentTable">' +
              '<thead>' +
                '<tr><th colspan="6">' + esc(title) + '</th></tr>' +
                '<tr>' +
                  '<th>Segment</th><th class="right"> dScore</th><th class="right">Pos%</th><th class="right">Stable</th><th class="right">Cov%</th><th class="right">N</th>' +
                '</tr>' +
              '</thead>' +
              '<tbody>' + (rowsHtml || '<tr><td colspan="6" class="muted"></td></tr>') + '</tbody>' +
            '</table>'
          );
        }

        // IMPORTANT: Use the same label logic as chips/filters so Segment Monitor matches the visible UI.
        const intern = buildGroups(r => pillarLabel(r));     // internal: 5-säulen (scanner-owned)
        const official = buildGroups(r => clusterLabel(r));  // official: market cluster/sector/industry

        const metaLine = '<div class="muted small">Snapshot: ' + (prev || '') + '  ' + (latest || '') + ' | Universe: ' + (rows ? rows.length : 0) + '</div>';

        elSegment.innerHTML = metaLine + '<div class="segmentTables">' +
          renderTable('Intern (Scanner)', intern) +
          renderTable('Offiziell (Markt)', official) +
        '</div>';
      } catch (e) {
        elSegment.textContent = '';
      }
    }


    if (elBriefReal) {
      const t = normStr((BRIEFING_REALITIES || {}).text);
      elBriefReal.textContent = t || '';
    }
    // History Delta wird jetzt dynamisch in renderMarketContext gerendert
    if (elReality) {
      elReality.innerHTML = renderReality(REALITY_CHECK);
    }

    refresh();
    try { document.documentElement.dataset.jsok = '1'; } catch (e) {}

    drawerClose.addEventListener('click', closeDrawer);
    drawerOverlay.addEventListener('click', (e) => {
      if (e.target === drawerOverlay) closeDrawer();
    });

    elPreset.addEventListener('change', () => {
      activePreset = elPreset.value;
      userSort = null;
      refresh();
      saveState();
    });

    let _searchTimer = null;
    elSearch.addEventListener('input', () => {
      if (_searchTimer) clearTimeout(_searchTimer);
      _searchTimer = setTimeout(() => {
        refresh();
        saveState();
      }, 80);
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        const { pop: helpPop } = getHelpEls();
        if (helpPop && helpPop.classList.contains('show')) {
          closeHelp();
          return;
        }
        if (drawerOverlay.classList.contains('show')) {
          closeDrawer();
        } else {
          elSearch.value = '';
          refresh();
          saveState();
        }
      }
    });

    // header click sort
    document.querySelectorAll('#tbl thead th').forEach(th => {
      th.addEventListener('click', () => {
        const k = th.getAttribute('data-k');
        if (!k) return;
        if (!userSort || userSort.k !== k) {
          userSort = {k, dir: 'desc'};
        } else {
          userSort.dir = (userSort.dir === 'desc') ? 'asc' : 'desc';
        }
        refresh();
        saveState();
      });
    });
    } catch (err) {
      const msg = (err && err.message) ? err.message : String(err);
      const el = document.getElementById('count');
      if (window.__showJsError) window.__showJsError('UIFehler (JS): ' + msg);
      if (el) el.textContent = `JS error: ${msg}`;
      const k = document.getElementById('kpis');
      if (k) k.textContent = 'JS error  siehe Konsole';
      try { console.error(err); } catch(e) {}
    }
  })();
  </script>
<!-- Unified Help Popover System -->
<div id="helpPop" class="helpPop hidden" role="dialog" aria-hidden="true">
  <div class="helpPopInner">
    <div class="helpPopTitle" id="helpPopTitle"></div>
    <div class="helpPopBody" id="helpPopBody"></div>
  </div>
</div>
</body>
</html>
"""

    return (
        template
        .replace("__DATA_JSON__", data_json)
        .replace("__PRESETS_JSON__", presets_json)
        .replace("__PRESET_OPTIONS__", preset_options_html)
        .replace("__BRIEFING_JSON__", briefing_json)
        .replace("__HISTORY_DELTA_JSON__", history_delta_json)
        .replace("__SEGMENT_MONITOR_JSON__", segment_monitor_json)
        .replace("__REALITY_CHECK_JSON__", reality_check_json)
        .replace("__BRIEFING_REALITIES_JSON__", briefing_realities_json)
        .replace("__FALLBACK_TBODY__", fallback_tbody_html)
        .replace("__VERSION__", str(version))
        .replace("__BUILD__", str(build))
        .replace("__RUN_AT__", str(run_at or ""))
        .replace("__RUN_SRC__", str(run_src or ""))
        .replace("__RUN_UNIVERSE__", str(run_universe or ""))
        .replace("__SOURCE_CSV__", str(source_csv))
    )


def _render_help_html_legacy_inline(*, version: str, build: str) -> str:
    """Generate a static help / project description page.

    This page is intentionally a living document: it describes what exists today
    and keeps placeholders for upcoming features (Portfolio, KIBriefing, etc.).
    """

    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Scanner_vNext  Hilfe & Projektbeschreibung</title>
  <style>
    :root{{
      --bg:#0b1020;
      --card:#0f172a;
      --border:rgba(148,163,184,.15);
      --muted:#94a3b8;
      --text:#e2e8f0;
      --accent:#60a5fa;
      --good:#34d399;
      --warn:#fbbf24;
      --bad:#fb7185;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }}
    *{{box-sizing:border-box}}
    body{{margin:0; font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial; background: radial-gradient(1000px 600px at 10% 0%, rgba(96,165,250,.12), transparent 60%), radial-gradient(800px 500px at 90% 10%, rgba(52,211,153,.10), transparent 55%), var(--bg); color:var(--text);}}
    a{{color:var(--accent); text-decoration:none}}
    a:hover{{text-decoration:underline}}
    header{{padding:18px 0; border-bottom:1px solid var(--border); background: rgba(15,23,42,.72); backdrop-filter: blur(10px); position: sticky; top:0; z-index: 10;}}
    .wrap{{max-width: 980px; margin: 0 auto; padding: 0 18px;}}
    .top{{display:flex; align-items:flex-end; justify-content:space-between; gap:12px; flex-wrap:wrap;}}
    h1{{margin:0; font-size:18px;}}
    .meta{{color:var(--muted); font-family: var(--mono); font-size:12px;}}
    .pill{{display:inline-block; border:1px solid var(--border); border-radius:999px; padding:4px 10px; font-size:12px; background: rgba(148,163,184,.06);}}
    .card{{background: rgba(15,23,42,.78); border:1px solid var(--border); border-radius: 16px; padding: 14px; margin: 14px 0;}}
    h2{{margin: 0 0 8px 0; font-size: 16px;}}
    h3{{margin: 14px 0 6px 0; font-size: 14px;}}
    p{{margin: 8px 0; line-height: 1.55; color: rgba(226,232,240,.95);}}
    ul{{margin: 8px 0 8px 18px; color: rgba(226,232,240,.95);}}
    code, pre{{font-family: var(--mono);}}
    pre{{background: rgba(2,6,23,.65); border:1px solid rgba(148,163,184,.15); padding:10px; border-radius: 12px; overflow:auto;}}
    .toc a{{display:block; padding:4px 0;}}
    .callout{{border-left: 4px solid var(--accent); padding: 8px 10px; background: rgba(96,165,250,.08); border-radius: 10px;}}
    .grid2{{display:grid; grid-template-columns: 1fr; gap:12px;}}
    @media(min-width:860px){{ .grid2{{grid-template-columns: 1fr 1fr;}} }}
    .tag{{font-family: var(--mono); font-size: 12px; color: var(--muted);}}
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <div class="top">
        <div>
          <h1>Scanner_vNext  Hilfe & Projektbeschreibung</h1>
          <div class="meta">version {version} · build {build} · <a href="index.html">zurück zum Dashboard</a></div>
        </div>
        <div class="pill">Living Doc · wird laufend erweitert</div>
      </div>
    </div>
  </header>

  <main class="wrap">

    <div class="card" style="border-color: rgba(251,191,36,.35); background: rgba(251,191,36,.06);">
      <h2>Disclaimer</h2>
      <p><b>Privates, experimentelles Projekt.</b> Keine Anlageberatung, keine Empfehlung, keine Gewähr. Inhalte können unvollständig, falsch oder veraltet sein. Nutzung ausschlielich auf eigene Verantwortung.</p>
    </div>

    <div class="card">
      <h2 id="toc">Inhalt</h2>
      <div class="toc">
        <a href="#ueberblick">1) berblick</a>
        <a href="#pipeline">2) Datenfluss (Pipeline)</a>
        <a href="#scoring">3) Scoring: Score, Opportunity, Risk, Regime, Confidence</a>
        <a href="#recommendation">4) Empfehlungscode (R0R5)</a>
        <a href="#dashboard">5) Dashboard-Funktionen</a>
        <a href="#portfolio">6) Portfolio (geplant)</a>
        <a href="#briefing">7) Briefing / KI</a>
        <a href="#autopilot">8) GitHub Autopilot</a>
        <a href="#notifications">9) Benachrichtigungen (Telegram)</a>
        <a href="#troubleshooting">10) Troubleshooting</a>
        <a href="#roadmap">11) Roadmap & Konzept</a>
      </div>
    </div>

    <div class="card" id="ueberblick">
      <h2>1) berblick</h2>
      <p><b>Scanner_vNext</b> ist ein privates TradingResearchSystem für Watchlists und PortfolioIdeen. Es bündelt Kennzahlen, Signale und MarktKontext zu einem <b>multifaktoriellen Score</b>  mit dem Ziel, Entscheidungen schneller, konsistenter und nachvollziehbar zu machen.</p>
      <div class="callout">
        <p style="margin:0"><b>Wichtig:</b> Presets sind reine <i>Ansichten</i> (ViewLayer)  sie verändern das Scoring nicht. KITexte sind reine <i>Briefings</i> und dürfen keinen Einfluss auf den Score haben.</p>
      </div>
      <p class="tag">Hinweis: Dieses Projekt ist kein Finanzrat. Es ist ein Werkzeug zur eigenen Strukturierung und Dokumentation von Entscheidungen.</p>
    </div>

    <div class="card" id="pipeline">
      <h2>2) Datenfluss (Pipeline)</h2>
      <p>Die täglichen Schritte sind bewusst getrennt  damit Scoring, Daten und UI sauber entkoppelt bleiben.</p>
      <pre>python -m scanner.app.run_daily
python -m scanner.ui.generator</pre>
      <ul>
        <li><code>run_daily</code> erzeugt/aktualisiert CSVs in <code>artifacts/watchlist/</code> und (optional) Reports in <code>artifacts/reports/</code>.</li>
        <li><code>ui.generator</code> liest eine CSV (z.B. <code>watchlist_CORE.csv</code>) und schreibt statisches HTML nach <code>artifacts/ui/</code>.</li>
      </ul>
      <p>Damit kannst du die Pipeline testen, versionieren und reproduzierbar ausführen  ohne dass das UI heimlich irgendwas berechnet, was die Ergebnisse verändern würde.</p>
    </div>

    <div class="card" id="scoring">
      <h2>3) Scoring (Score, Opportunity, Risk, Regime, Confidence)</h2>
      <p>Das Scoring läuft zentral im DomainLayer (<code>scanner.domain.scoring_engine</code>). Dort wird aus einer WatchlistZeile ein Satz aus <b>OpportunityFaktoren</b> und <b>RiskFaktoren</b> gebildet und anschlieend zu einem finalen Score zusammengeführt.</p>

      <div class="grid2">
        <div>
          <h3>Opportunity (0..1, höher = besser)</h3>
          <p>Beispiele der aktuell verwendeten Faktoren (wenn in den CSVs vorhanden):</p>
          <ul>
            <li><b>Growth %</b>, <b>ROE %</b>, <b>Margin %</b></li>
            <li><b>MCChance</b> (MonteCarloChance)</li>
            <li><b>Trend200</b> (200TageTrend) und <b>RS3M</b> (relative Stärke 3M)</li>
            <li><b>ElliottQuality</b> (abhängig vom ElliottSignal)</li>
            <li><b>Upside</b> (nur wenn ElliottSignal BUY und Target/Preis vorhanden; 30% Upside = voller Faktor)</li>
          </ul>
          <p class="tag">Hinweis: einzelne Faktoren sind bewusst als Platzhalter gesetzt (z.B. AnalystFaktor), bis Spalten dafür existieren.</p>
        </div>
        <div>
          <h3>Risk (0..1, höher = riskanter)</h3>
          <p>Beispiele der aktuell verwendeten Faktoren:</p>
          <ul>
            <li><b>Debt/Equity</b></li>
            <li><b>CRVFragility</b> (CRV wird in eine Fragilität umgerechnet; bei fehlendem CRV neutral)</li>
            <li><b>Volatility</b>, <b>DownsideDev</b>, <b>MaxDrawdown</b></li>
            <li><b>LiquidityRisk</b> (bevorzugt DollarVolume; Fallback AvgVolume)</li>
          </ul>
          <p class="tag">Die Idee: Opportunity alleine reicht nicht  ein hoher Score soll bei fragiler Liquidität oder extremem Drawdown nicht blind nach oben schieen.</p>
        </div>
      </div>

      <h3>Normalisierung (UniverseScaling)</h3>
      <p>Viele Rohwerte werden über ein Universe (Verteilung der Werte im aktuellen Datensatz) auf 0..1 skaliert. Dadurch wird der Score <b>relativ zum aktuellen MarktUniversum</b> interpretierbar (statt absolute Schwellen zu erzwingen).</p>

      <h3>Regime (MarktKontext)</h3>
      <p>Der Score kann je nach Marktregime anders gewichtet werden. Dafür werden vorhandene Spalten genutzt (z.B. <code>MarketRegimeStock</code>/<code>MarketRegimeCrypto</code> und Trend200Kontext). Wenn das RegimeLabel fehlt, wird es aus Trend200 grob als bull/neutral/bear abgeleitet.</p>
      <ul>
        <li><b>opp_w</b> / <b>risk_w</b>: wie stark Opportunity vs. Risk in den finalen Score einfliet</li>
        <li><b>risk_mult</b>: wie hart Risiko bestraft wird</li>
      </ul>

      <h3>Confidence (0..100)</h3>
      <p>Zusätzlich wird eine <b>Confidence</b> berechnet, die z.B. Datenabdeckung, Konfluenz, RisikoSauberkeit, RegimeAusrichtung und Liquidität berücksichtigt. Ziel: du erkennst schneller, ob ein hoher Score auf stabilen Inputs steht  oder auf dünnem DatenEis.</p>
    </div>

    <div class="card" id="recommendation">
      <h2>4) Empfehlungscode (R0R5)</h2>
      <p>Im Dashboard erscheint im ScoreBereich ein privater Code <b>R0R5</b>. Das ist <b>kein TradingSignal</b>, sondern eine knappe Zusammenfassung für deinen Workflow.</p>
      <ul>
        <li><b>R0</b>: AVOIDZeilen (score_status beginnt mit <code>AVOID_</code>)</li>
        <li><b>R5</b>: ScorePerzentil  90 <i>und</i> Trend OK <i>und</i> Liquidity OK</li>
        <li><b>R4</b>: ScorePerzentil  75 <i>und</i> Liquidity OK</li>
        <li><b>R3</b>: ScorePerzentil  45</li>
        <li><b>R2</b>: ScorePerzentil  20</li>
        <li><b>R1</b>: Rest</li>
      </ul>
      <p class="tag">Technik: das UI berechnet das ScorePerzentil aus allen Zeilen der geladenen Tabelle.</p>
    </div>

    <div class="card" id="dashboard">
      <h2>5) DashboardFunktionen</h2>
      <h3>Presets</h3>
      <p>Presets sind Filter/Sichten (CORE, TOP, AVOID ). Sie bestimmen, <i>was du siehst</i>, nicht <i>wie gescored wird</i>.</p>

      <h3>Suche</h3>
      <p>Suche filtert quer über Symbol/Name/Kategorie/Land (und weitere Felder, sofern vorhanden).</p>

      <h3>QuickFilter & KPIChips</h3>
      <p>QuickFilter sind schnelle boolesche/StatusSchalter (z.B. Nur OK, Trend OK, Liq OK). KPIChips sind klickbare Zusammenfassungen, die ebenfalls als Filter wirken.</p>

      <h3>Cluster (offiziell) vs. Säulen (privat)</h3>
      <p>Es gibt zwei unterschiedliche Kategorien im UI  mit unterschiedlicher Bedeutung:</p>
      <ul>
        <li><b>Cluster/Sektor (offiziell)</b>: kommt aus YahooTaxonomie (Sector/Industry) und ist dafür gedacht, echte MarktCluster sichtbar zu machen.</li>
        <li><b>Säulen (5Säulen/Playground, privat)</b>: deine thematische MetadatenZuordnung (Gehirn, Hardware, Energie, Fundament, Recycling, Playground). Sie dient nur der Navigation/Explainability und <b>ändert niemals</b> den Score.</li>
      </ul>
      <p class="tag">Hinweis: ältere PhantasieSektoren können weiterhin in der Quelle vorkommen, werden aber nicht als offizieller Sektor interpretiert. Die UI kann daraus optional eine Säule ableiten, damit das Konzept sichtbar bleibt.</p>

      <h3>BucketMatrix (Score  Risk)</h3>
      <p>Die Matrix verdichtet das Universum: <b>Score</b> auf der XAchse, <b>Risk</b> auf der YAchse. Klick auf ein Feld aktiviert einen zusätzlichen MatrixFilter.</p>

      <h3>WhyScore Drawer</h3>
      <p>Der Drawer erklärt, <i>warum</i> ein Wert so aussieht: StatusFlags, wichtige Kennzahlen und  falls vorhanden  ein Breakdown (z.B. ConfidenceBreakdown).</p>

      <h3>TickerZelle (2zeilig)</h3>
      <p>Aktien: oben Symbol, unten ISIN. Krypto: oben Pair/ID, unten YahooPair (oder was vorhanden ist). Ziel: du siehst Identität + Key sofort, ohne zusätzliche Spalten.</p>

      <h3>FinvizInspiration (eigene Umsetzung)</h3>
      <p>Die Marketbersicht (IndexCharts, Breadth, Heatmap, Movers/News) ist als eigene Seite geplant/teilweise vorhanden (<a href="../dashboard/index.html" target="_blank" rel="noopener">MarketDashboard</a>). LayoutIdeen dürfen inspiriert sein, aber Inhalte/Code werden nicht 1:1 kopiert  Scanner_vNext bleibt eine eigenständige Logik/UX.</p>
    </div>

    <div class="card" id="portfolio">
      <h2>6) Portfolio (geplant)</h2>
      <p>Hier kommt eine PortfolioSektion hin (Bestände, Einstand, Gewichtung, RisikoBeitrag, ZielAllokation, Alerts). Aktuell ist das bewusst noch Platzhalter, damit wir es sauber an dein Konzept andocken können.</p>
      <div class="callout"><p style="margin:0"><b>TODO:</b> PortfolioKonzept einfügen, sobald du es wieder parat hast (oder als Datei/Notiz lieferst).</p></div>
    </div>

    <div class="card" id="briefing">
      <h2>7) Briefing / KI</h2>
      <p>Das Briefing ist ein <b>passiver ExplainabilityReport</b> für die TopWerte. Es wird ausschlielich aus bereits vorhandenen Feldern der WatchlistCSV abgeleitet (kein ReScoring).</p>
      <h3>Outputs</h3>
      <ul>
        <li><code>artifacts/reports/briefing.json</code>  strukturierte Daten (TopN + Gründe/Risiken/Checks).</li>
        <li><code>artifacts/reports/briefing.txt</code>  deterministische TextVersion (immer vorhanden, offline).</li>
        <li><code>artifacts/reports/briefing_ai.txt</code>  optionale sprachliche Glättung via OpenAI API (FeatureFlag, default OFF).</li>
      </ul>
      <h3>Erzeugung</h3>
      <p>Briefing generieren (Stage A, deterministisch):</p>
      <pre><code>python scripts/generate_briefing.py</code></pre>
      <p>AIEnhancement (Stage B, optional):</p>
      <pre><code>set OPENAI_API_KEY=...  # Windows
python scripts/generate_briefing.py --enable-ai</code></pre>
      <ul>
        <li>Es ist <b>rein erklärend</b> (Notizen/Explainability).</li>
        <li>Es darf <b>niemals</b> das Scoring oder Ranking beeinflussen.</li>
        <li><b>Keine Anlageberatung</b>: das Briefing enthält einen kurzen Disclaimer.</li>
      </ul>
      <p class="tag">Dashboard: Das UI zeigt bevorzugt <code>briefing_ai.txt</code>, sonst <code>briefing.txt</code>. Wenn nichts vorhanden ist: Noch kein Briefing generiert.</p>
    </div>



    <div class="card" id="autopilot">
      <h2>8) GitHub Autopilot (ohne laufenden PC)</h2>
      <p>Wenn Scanner_vNext in einem GitHubRepo liegt, kann ein geplanter Workflow (GitHub Actions) die Pipeline automatisch ausführen. Damit läuft der Scanner serverlos in der Cloud  dein Rechner muss dafür nicht an sein.</p>
      <h3>Was macht der Autopilot?</h3>
      <ul>
        <li>Installiert das Projekt (<code>pip install -e .</code>).</li>
        <li>Führt <code>python -m scanner.app.run_daily</code> aus (CSVOutputs nach <code>artifacts/watchlist/</code>).</li>
        <li>Erzeugt das deterministische Briefing (<code>scripts/generate_briefing.py</code>  <code>artifacts/reports/</code>).</li>
        <li>Generiert die UI (<code>python -m scanner.ui.generator</code>  <code>artifacts/ui/</code>).</li>
        <li>Committet die Outputs (standardmäig <code>artifacts/</code>) zurück ins Repo.</li>
      </ul>
      <h3>Warum committen wir <code>artifacts/</code>?</h3>
      <p>Für den Einstieg ist das der simpelste Weg: du siehst im Repo und/oder über GitHub Pages sofort die aktuellen HTML/CSVOutputs. Später kann man das auf einen reinen DeployBranch umstellen, wenn das Repo zu gro wird.</p>
      <h3>Benachrichtigungen</h3>
      <p>GitHub kann dir EMails senden, wenn ein Workflow gelaufen ist. Das kommt von GitHub (nicht vom Projekt). Wenn du das reduzieren willst: Repo  <i>Watch</i> Einstellungen bzw. GitHub Notifications anpassen.</p>
      <p class="tag">Technik: WorkflowDatei liegt unter <code>.github/workflows/run_scanner.yml</code>.</p>
    </div>

    <div class="card" id="notifications">
      <h2>9) Benachrichtigungen (Telegram)</h2>
      <p>Telegram ist optional und standardmäig deaktiviert. Es hat keinen Einfluss auf Scoring oder Ranking  es ist nur ein zusätzlicher Kanal für Hinweise. Da du es aktuell nicht brauchst, bleibt es aus.</p>
      <h3>Aktivieren (falls du es später wieder willst)</h3>
      <pre><code>TELEGRAM_ENABLED=1
TELEGRAM_BOT_TOKEN=...   # oder TELEGRAM_TOKEN (Legacy)
TELEGRAM_CHAT_ID=...</code></pre>
      <p class="tag">Hinweis: Ohne <code>TELEGRAM_ENABLED</code> (oder bei fehlenden Tokens) wird nichts gesendet.</p>
    </div>

    <div class="card" id="troubleshooting">
      <h2>10) Troubleshooting</h2>
      <h3>UI zeigt Keine Daten</h3>
      <ul>
        <li>Prüfe, ob <code>artifacts/watchlist/watchlist_CORE.csv</code> (oder ALL) existiert.</li>
        <li>Führe zuerst <code>python -m scanner.app.run_daily</code> aus.</li>
      </ul>
      <h3>Contract validation failed</h3>
      <ul>
        <li>Contract: <code>configs/watchlist_contract.json</code></li>
        <li>Die UI bricht absichtlich ab, wenn Pflichtspalten fehlen  das verhindert stilles UI zeigt Mist.</li>
        <li>Lösung: WatchlistCSV neu generieren oder Migration/NormalizeScripts nutzen.</li>
      </ul>
      <h3>Briefing fehlt</h3>
      <ul>
        <li>Erzeuge es mit <code>python scripts/generate_briefing.py</code>.</li>
        <li>UI lädt bevorzugt <code>briefing_ai.txt</code>, sonst <code>briefing.txt</code>. Wenn beide fehlen: Noch kein Briefing generiert.</li>
      </ul>
      <h3>GOOGLE_CREDENTIALS fehlt (GitHub Action)</h3>
      <ul>
        <li>Das Secret muss in GitHub als <code>GOOGLE_CREDENTIALS</code> hinterlegt sein (JSONServiceAccount).</li>
        <li>Ohne Credentials kann der Scanner keine Sheets/DatenQuellen lesen (je nach Setup).</li>
      </ul>
      <p class="tag">Wenn du nicht weiterkommst: Logs aus GitHub Actions oder die konkrete Fehlermeldung hier rein kopieren.</p>
    </div>
    <div class="card" id="roadmap">
      <h2>11) Roadmap & Konzept (Platzhalter)</h2>
      <ul>
        <li>Matrix Labels/Logik weiter finalisieren (RiskProxy).</li>
        <li>RecommendationLogik bei Bedarf schärfen (Regeln bleiben transparent).</li>
        <li>WatchlistHygiene: Spaltenmigration & DedupeStrategie.</li>
        <li>PortfolioBlock ergänzen.</li>
        <li>BriefingLogik weiter schärfen (Texte/Mapping), AI bleibt optional und ohne Einfluss auf Score.</li>
      </ul>
      <p class="tag">Diese Seite ist absichtlich nicht fertig  sie ist deine Dokumentation, die mit dem Projekt mitwächst.</p>
    </div>

    <div class="card">
      <p style="margin:0"><a href="#toc"> zurück zum Anfang</a></p>
    </div>

  </main>
</body>
</html>
"""

def _render_help_html(*, version: str, build: str) -> str:
    template_path = Path(__file__).resolve().parent / "templates" / "help.html"
    try:
        template = template_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise RuntimeError(f"Help template fehlt: {template_path}") from exc

    return (
        template
        .replace("__VERSION__", html.escape(str(version)))
        .replace("__BUILD__", html.escape(str(build)))
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_ALL.csv")
    ap.add_argument("--contract", default=r"configs/watchlist_contract.json")
    ap.add_argument("--out", default=r"artifacts/ui/index.html")
    args = ap.parse_args()

    out = build_ui(csv_path=args.csv, out_html=args.out, contract_path=args.contract)
    print(f" UI wrote: {out.as_posix()}")
    print(f" Help wrote: {(out.parent / 'help.html').as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


