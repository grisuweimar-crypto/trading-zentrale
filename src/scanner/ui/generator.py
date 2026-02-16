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
from pathlib import Path
from typing import Any

import pandas as pd

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
    "diversification_penalty",
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

    html = _render_html(
        data_records=data_records,
        presets=presets,
        source_csv=str(csv_path),
        version=__version__,
        build=__build__,
        briefing_text=briefing_text,
        briefing_source=briefing_source,
        fallback_tbody_html=fallback_tbody_html,
    )

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")

    # Help / project description page (static)
    help_path = out_html.parent / "help.html"
    help_html = _render_help_html(version=__version__, build=__build__)
    help_path.write_text(help_html, encoding="utf-8")

    return out_html


def _render_html(*, data_records: list[dict[str, Any]], presets: dict[str, Any], source_csv: str, version: str, build: str, briefing_text: str, briefing_source: str, fallback_tbody_html: str) -> str:
    data_json = json.dumps(data_records, ensure_ascii=False)
    presets_json = json.dumps(presets, ensure_ascii=False)
    briefing_json = json.dumps({"text": briefing_text, "source": briefing_source}, ensure_ascii=False)

    # Server-side preset <option> fallback (so UI isn't empty if JS fails)
    preset_labels = {
        "ALL": "Alle Werte",
        "CORE": "Übersicht",
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
        txt = f"{label} ({n})" + (f" — {desc}" if desc else "")
        opts.append(f'<option value="{html.escape(n)}">{html.escape(txt)}</option>')
    preset_options_html = "\n".join(opts)

    # NOTE: We intentionally avoid Python f-strings for the HTML template because the
    # embedded CSS/JS contains many curly braces. We inject values via simple tokens.
    template = """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Scanner_vNext — Research Dashboard</title>
  <style>
    :root {
      --bg: #0b0f14;
      --card: #111827;
      --muted: #94a3b8;
      --text: #e5e7eb;
      --accent: #60a5fa;
      --good: #34d399;
      --warn: #fbbf24;
      --bad: #fb7185;
      --chip: #1f2937;
      --border: #243244;
      --shadow: 0 10px 30px rgba(0,0,0,.35);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      --w-ticker: 110px;
      --w-name: 220px;
      --w-price: 130px;
      --w-score: 170px;
      --w-conf: 70px;
      --w-cycle: 70px;
      --w-trend: 70px;
      --w-liq: 70px;
      --w-status: 120px;
      --w-class: 80px;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: var(--sans); background: var(--bg); color: var(--text); }
    header { padding: 16px 18px; border-bottom: 1px solid var(--border); background: rgba(17,24,39,.55); backdrop-filter: blur(8px); position: sticky; top: 0; z-index: 50; }
    .title { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
    .title h1 { margin: 0; font-size: 18px; font-weight: 700; }
    .meta { color: var(--muted); font-family: var(--mono); font-size: 12px; }
    .helpLink { color: var(--accent); text-decoration: none; }
    .helpLink:hover { text-decoration: underline; }

    .wrap { max-width: 1680px; margin: 0 auto; padding: 18px; }
    .panel { background: var(--card); border: 1px solid var(--border); border-radius: 14px; box-shadow: var(--shadow); }

    /* Briefing box (passive text; must not influence scoring) */
    .briefingBox { border: 1px solid rgba(148,163,184,.15); background: rgba(15,23,42,.35); border-radius: 12px; padding: 10px; }
    .briefHead { display:flex; align-items:center; justify-content: space-between; gap: 10px; margin-bottom: 6px; }
    .briefingBox .muted { margin-bottom: 8px; }
    .briefingText { margin: 0; padding: 10px; border-radius: 10px; border: 1px solid rgba(148,163,184,.12); background: rgba(15,23,42,.55); white-space: pre-wrap; max-height: 300px; overflow: auto; font-family: inherit; line-height: 1.55; font-size: 10px; }
    @media (min-width: 980px) { .briefingText { max-height: 520px; } }

    /* Briefing: Newlines + Bullets IMMER erhalten */
    #briefingText, #briefingBody, .briefing-body, [data-briefing] {
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
      line-height: 1.35;
    }

    /* Bullet-Styles bombensicher machen */
    .briefingText ul { margin: 6px 0 10px 18px; padding-left: 14px; list-style: disc; }
    .briefingText li { margin: 4px 0; }
    .briefing-asset { margin: 10px 0 6px; font-weight: 700; }
    .briefing-label { margin: 10px 0 6px; opacity: .9; font-weight: 700; }

    /* Mobile: Panels/Drawer/Modals dürfen nicht über den Viewport schießen */
    * { box-sizing: border-box; }
    html, body { max-width: 100%; overflow-x: hidden; }

    @media (max-width: 680px) {
      .modal, .drawer, .panel, dialog, [role="dialog"] {
        width: 96vw !important;
        max-width: 96vw !important;
      }
    }

    .controls { display: grid; grid-template-columns: 220px 1fr 220px 220px auto; gap: 12px; padding: 14px; align-items: center; }
    .controls label { font-size: 12px; color: var(--muted); }
    select, input { width: 100%; background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 10px; outline: none; }
    input::placeholder { color: #64748b; }
    .count { justify-self: end; color: var(--muted); font-size: 12px; font-family: var(--mono); }

    .kpis { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .kpis .label { color: var(--muted); font-size: 12px; font-family: var(--mono); margin-right: 4px; }

.clusters { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.clusters .label { color: var(--muted); font-size: 12px; font-family: var(--mono); margin-right: 4px; }
.clusters .chip { padding: 2px 6px; font-size: 10px; line-height: 1.1; }

.pillars { padding: 0 14px 10px 14px; display:flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.pillars .label { color: var(--muted); font-size: 12px; font-family: var(--mono); margin-right: 4px; }
.pillars .chip { padding: 2px 6px; font-size: 10px; line-height: 1.1; }

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
    .sig.grad.good { background: linear-gradient(90deg, rgba(52,211,153,.18), rgba(52,211,153,.06)); }
    .sig.grad.blue { background: linear-gradient(90deg, rgba(96,165,250,.18), rgba(96,165,250,.06)); }
    .sig.grad.warn { background: linear-gradient(90deg, rgba(251,191,36,.20), rgba(251,191,36,.06)); }
    .sig.grad.bad  { background: linear-gradient(90deg, rgba(251,113,133,.18), rgba(251,113,133,.06)); }

    /* Bucket matrix (Score × Risk) */
    .matrixPanel { padding: 12px 14px 14px; border-top: 1px solid var(--border); }
    .matrixHead { display:flex; justify-content: space-between; align-items:flex-end; gap: 12px; margin-bottom: 10px; }
    .matrixTitle { font-weight: 700; }
    .matrixLayout { display: grid; grid-template-columns: 1fr; gap: 12px; }
    @media (min-width: 980px) { .matrixLayout { grid-template-columns: .70fr 1.30fr; align-items: start; } }

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


/* Market Context (Finviz-inspired patterns, scanner-owned data & logic) */
.marketPanel { padding: 12px 14px 14px; border-top: 1px solid var(--border); }
.marketHead { display:flex; justify-content: space-between; align-items:flex-end; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
.marketHead select { width: auto; min-width: 170px; padding: 8px 10px; border-radius: 10px; }
.marketGrid { display:grid; grid-template-columns: 1fr; gap: 12px; }
@media (min-width: 980px) { .marketGrid { grid-template-columns: 1fr 1fr 1.2fr; align-items: stretch; } }

.marketCard { border: 1px solid rgba(148,163,184,.15); background: rgba(15,23,42,.35); border-radius: 12px; padding: 10px; }
.marketCardTitle { font-weight: 700; margin-bottom: 8px; }
.breadthRow { display:flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 6px; }

.moversGrid { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
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
.heatCell { text-align: center; font-family: var(--mono); }
.heatCell.zero { color: rgba(148,163,184,.55); }

    /* KPI chips are clickable quick-filters */
    button.chip { appearance: none; -webkit-appearance: none; border: 1px solid rgba(148,163,184,.15); background: var(--chip); color: inherit; font: inherit; }
    button.chip.kpi { cursor: pointer; }
    button.chip.kpi:hover { border-color: rgba(96,165,250,.45); }
    button.chip.kpi.active { box-shadow: 0 0 0 2px rgba(96,165,250,.25) inset; }

    /* KPI chips slightly more compact than normal chips */
    button.chip.kpi { padding: 2px 6px; font-size: 10px; line-height: 1.1; }

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
    .overlay.show { display: flex; }
    .drawer { width: min(720px, 96vw); max-height: 88vh; overflow: auto; }
    .drawer-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; padding: 14px; border-bottom: 1px solid var(--border); }
    .drawer-actions { display: flex; gap: 8px; align-items: center; }
    .drawer-title { font-weight: 700; }
    .drawer-body { padding: 14px; }
    .btn { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 10px; cursor: pointer; }
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

    /* Let the briefing breathe a bit more on larger screens */
    @media (min-width: 980px) {
      .briefingText { max-height: 520px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <div class="title">
        <h1>Scanner_vNext — Research Dashboard</h1>
        <div class="meta">version __VERSION__ · build __BUILD__ · <a class=\"helpLink\" href=\"help.html\" target=\"_blank\" rel=\"noopener\">Hilfe / Projektbeschreibung</a></div>
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
          <label for="search">Suche (Symbol/Name/Kategorie/Land…)</label>
          <input id="search" placeholder="z.B. NVDA, Gold, Deutschland, Krypto…"/>
        </div>
        <div>
          <label for="clusterSel">Cluster/Sektor</label>
          <select id="clusterSel"><option value="">Alle</option></select>
        </div>
        <div>
          <label for="pillarSel">Säule (5‑Säulen/Playground)</label>
          <select id="pillarSel"><option value="">Alle</option></select>
        </div>
        <div class="count" id="count">—</div>
      </div>

      <div class="kpis" id="kpis">—</div>
      <div class="pillars" id="pillars">—</div>
      <div class="clusters" id="clusters">—</div>

      <div class="filters" id="filters">
        <button type="button" class="fbtn active" data-f="hideAvoid" title="AVOID-Zeilen ausblenden (score_status beginnt mit AVOID_)">AVOID ausblenden</button>
        <button type="button" class="fbtn" data-f="onlyOK" title="Nur score_status = OK anzeigen">Nur OK</button>
        <button type="button" class="fbtn" data-f="trendOK" title="Nur trend_ok = true anzeigen">Trend OK</button>
        <button type="button" class="fbtn" data-f="liqOK" title="Nur liquidity_ok = true anzeigen">Liq OK</button>
        <span class="fsep"></span>
        <button type="button" class="fbtn" data-f="onlyStock" title="Nur Aktien (is_crypto = false)">Aktien</button>
        <button type="button" class="fbtn" data-f="onlyCrypto" title="Nur Krypto (is_crypto = true)">Krypto</button>
        <button type="button" class="fbtn" data-f="resetSort" title="Nur Sort-Override löschen (Preset-Sort bleibt)">Sortierung zurück</button>
        <button type="button" class="fbtn" data-f="reset" title="Alles zurücksetzen (Preset, Suche, Filter, Sort & Persistenz)">Reset</button>
        <button type="button" class="hintbtn" id="infoFlow" aria-haspopup="dialog" aria-expanded="false" title="Erklärung anzeigen"><span class="i">i</span>Preset → Quick-Filter</button>
      </div>

      <div class="popover" id="infoPopover" role="dialog" aria-modal="false" aria-hidden="true">
        <button type="button" class="btn close" id="infoClose" title="Schließen">✕</button>
        <div class="title">Wie wirken Preset, Suche und Quick-Filter?</div>
        <ul>
          <li><b>Preset</b> filtert und sortiert zuerst (View-Layer, verändert kein Scoring).</li>
          <li>Danach wirkt die <b>Suche</b> (Ticker/Name/Kategorie/Land…).</li>
          <li>Zuletzt greifen die <b>Quick-Filter</b> (z.B. Trend OK, Liq OK, Nur OK).</li>
        </ul>
        <div class="title" style="margin-top:10px;">Signal‑Codes (privat)</div>
        <ul>
          <li><b>R5</b> = <span class="sig good grad">Top Setup</span></li>
          <li><b>R4</b> = <span class="sig good grad">Good Setup</span></li>
          <li><b>R3</b> = <span class="sig blue grad">Neutral</span></li>
          <li><b>R2</b> = <span class="sig warn grad">Weak</span></li>
          <li><b>R1</b> = <span class="sig bad grad">Low Priority</span></li>
          <li><b>R0</b> = <span class="sig warn grad">Avoid</span> (z.B. AVOID_CRYPTO_BEAR)</li>
        </ul>
      </div>

      <div class="matrixPanel" id="matrixPanel">
        <div class="matrixLayout">
          <div>
            <div class="matrixHead">
              <div>
                <div class="matrixTitle">Bucket‑Matrix (Score × Risk)</div>
                <div class="muted small">Klick auf ein Feld = Matrix‑Filter (zusätzlich zu Preset/Suche/Quick‑Filter).</div>
              </div>
              <div style="display:flex; gap:8px; align-items:center;">
                <button type="button" class="btn" id="matrixClear" title="Matrix‑Filter zurücksetzen">Matrix zurück</button>
              </div>
            </div>
            <div class="matrixGrid" id="matrix"></div>
            <div class="matrixNote" id="matrixNote">—</div>
          </div>

          <div class="briefingBox" id="briefingBox">
            <div class="briefHead">
              <div class="matrixTitle">Briefing</div>
              <button type="button" class="btn" id="briefingToggle" title="Briefing ein-/ausblenden">Ausblenden</button>
            </div>
            <div class="muted small">Privat/experimentell · keine Anlageberatung · ohne Einfluss aufs Scoring.</div>
            <div id="briefingText" class="briefingText">—</div>
          </div>
        </div>
      </div>


<div class="marketPanel" id="marketPanel">
  <div class="marketHead">
    <div>
      <div class="matrixTitle">Market Context</div>
      <div class="muted small">Passiv aus deiner Watchlist (kein Einfluss auf Scoring) · Basis: gefiltertes Universe (Preset/Suche/Quick/Cluster/Säule)</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap: wrap;">
      <select id="heatMode" title="Heatmap-Modus">
        <option value="pillar">Heatmap: Säulen</option>
        <option value="cluster">Heatmap: Cluster</option>
      </select>
      <button type="button" class="btn" id="marketToggle" title="Market Context ein-/ausblenden">Ausblenden</button>
    </div>
  </div>
  <div id="marketBody" class="marketGrid">
    <div class="marketCard" id="breadthCard">
      <div class="marketCardTitle">Breadth</div>
      <div id="breadthBox">—</div>
      <div id="diversBox" class="muted small" style="margin-top:8px;">—</div>
      <div id="qualityBox" class="muted small" style="margin-top:8px;">—</div>
    </div>
    <div class="marketCard" id="moversCard">
      <div class="marketCardTitle">Movers</div>
      <div class="moversGrid">
        <div>
          <div class="muted small">Top ↑</div>
          <div id="moversUp" class="moversList">—</div>
        </div>
        <div>
          <div class="muted small">Weak ↓</div>
          <div id="moversDown" class="moversList">—</div>
        </div>
      </div>
    </div>
    <div class="marketCard" id="heatCard">
      <div class="marketCardTitle">Heatmap</div>
      <div id="heatmap" class="heatWrap">—</div>
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
        <div class="mono" id="sortHint">—</div>
      </div>
    </div>
  </div>

  <div id="drawerOverlay" class="overlay" aria-hidden="true">
    <div class="drawer panel" role="dialog" aria-modal="true" aria-label="Why Score">
      <div class="drawer-head">
        <div>
          <div id="drawerTitle" style="font-weight:700;">—</div>
          <div id="drawerSub" class="muted small">—</div>
        </div>
        <div class="drawer-actions">
          <div id="drawerActions"></div>
          <button class="btn" id="drawerClose">Schließen</button>
        </div>
      </div>
      <div class="drawer-body" id="drawerBody"></div>
    </div>
  </div>

  <script id="DATA" type="application/json">__DATA_JSON__</script>
  <script id="PRESETS" type="application/json">__PRESETS_JSON__</script>
  <script id="BRIEFING" type="application/json">__BRIEFING_JSON__</script>

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
      show('UI‑Fehler (JS): ' + m);
    });
    window.addEventListener('unhandledrejection', (ev) => {
      const r = ev && ev.reason;
      const m = (r && r.message) ? r.message : String(r);
      show('UI‑Fehler (Promise): ' + m);
    });
    // If the main UI never sets jsok, show a helpful message (covers parse errors)
    setTimeout(() => {
      const ok = document.documentElement && document.documentElement.dataset && document.documentElement.dataset.jsok;
      if (!ok) show('UI konnte nicht initialisiert werden (JS lädt nicht). Öffne die Konsole (F12) für Details.');
    }, 700);
  })();
  </script>

  <script>
  (function() {
    try {
    const DATA = JSON.parse((document.getElementById('DATA')?.textContent) || '[]');
    const PRESETS = JSON.parse((document.getElementById('PRESETS')?.textContent) || '{}');
    const BRIEFING = JSON.parse((document.getElementById('BRIEFING')?.textContent) || '{"text":""}');

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
const elDiversBox = document.getElementById('diversBox');
const elQualityBox = document.getElementById('qualityBox');
const elMoversUp = document.getElementById('moversUp');
const elMoversDown = document.getElementById('moversDown');
const elHeatmap = document.getElementById('heatmap');
const elHeatMode = document.getElementById('heatMode');


    const elBriefing = document.getElementById('briefingText');
    const btnBriefingToggle = document.getElementById('briefingToggle');
    const drawerOverlay = document.getElementById('drawerOverlay');
    const drawerClose = document.getElementById('drawerClose');
    const drawerTitle = document.getElementById('drawerTitle');
    const drawerSub = document.getElementById('drawerSub');
    const drawerBody = document.getElementById('drawerBody');
    const drawerActions = document.getElementById('drawerActions');

    // Flow info popover (Preset → Quick-Filter)
    const infoFlow = document.getElementById('infoFlow');
    const infoPopover = document.getElementById('infoPopover');
    const infoClose = document.getElementById('infoClose');

    // ---- briefing toggle (UI-only; must not affect scoring) ----
    let briefingVisible = true;
    function setBriefingVisible(on) {
      try {
        if (!elBriefing) return;
        elBriefing.style.display = on ? 'block' : 'none';
        if (btnBriefingToggle) btnBriefingToggle.textContent = on ? 'Ausblenden' : 'Einblenden';
      } catch (e) {}
    }
    if (btnBriefingToggle) {
      btnBriefingToggle.addEventListener('click', () => {
        briefingVisible = !briefingVisible;
        setBriefingVisible(briefingVisible);
      });
    }



    // ---- state ----
    let activePreset = 'ALL';
    let userSort = null; // {k, dir} dir: 'asc'|'desc'
    let clusterPick = ''; // Cluster/Sektor filter (string)
    let pillarPick = '';  // Säulen-Filter (string; private metadata)

// Market Context UI state (passive)
let marketVisible = true;
let heatMode = 'pillar'; // 'pillar' | 'cluster'


    const DEFAULT_SORT = [{k:'score', dir:'desc'},{k:'confidence', dir:'desc'},{k:'name', dir:'asc'}];

    let quick = {
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

    // Bucket-matrix filter (Score × Risk)
    let matrix = { sb: null, rb: null };
    const DEFAULT_MATRIX = { sb: null, rb: null };

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
          quick: quick,
          matrix: matrix,
          sort: userSort,
          cluster: clusterPick,
          pillar: pillarPick,
          marketVisible: marketVisible,
          heatMode: heatMode,
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
        const kk = b.getAttribute('data-f');
        if (!kk || kk === 'reset') return;
        const on = !!quick[kk];
        if (on) b.classList.add('active'); else b.classList.remove('active');
      });
    }

    

    // KPI chips: click to toggle quick-filters (intuitive)
    if (elKpis) {
      elKpis.addEventListener('click', (ev) => {
        const t = ev.target;
        if (!(t instanceof HTMLElement)) return;
        const k = t.getAttribute('data-kpi');
        if (!k) return;
        ev.preventDefault();
        toggleKpi(k);
      });
      elKpis.addEventListener('keydown', (ev) => {
        const t = ev.target;
        if (!(t instanceof HTMLElement)) return;
        const k = t.getAttribute('data-kpi');
        if (!k) return;
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault();
          toggleKpi(k);
        }
      });
    }
function resetAll() {
      activePreset = (PRESETS && PRESETS.ALL) ? 'ALL' : ((PRESETS && PRESETS.CORE) ? 'CORE' : (Object.keys(PRESETS || {})[0] || 'CORE'));
      userSort = null;
      quick = Object.assign({}, DEFAULT_QUICK);
      matrix = Object.assign({}, DEFAULT_MATRIX);
      if (elSearch) elSearch.value = '';
      clusterPick = '';
      if (elClusterSel) elClusterSel.value = '';
      pillarPick = '';
      if (elPillarSel) elPillarSel.value = '';
      if (elPreset) elPreset.value = activePreset;
      syncFilterButtons();
      clearState();
      refresh();
    }

    // ---- info popover (Preset → Quick-Filter) ----
    function closeInfoPopover() {
      if (!infoPopover) return;
      infoPopover.classList.remove('show');
      infoPopover.setAttribute('aria-hidden', 'true');
      if (infoFlow) infoFlow.setAttribute('aria-expanded', 'false');
    }

    function openInfoPopover() {
      if (!infoPopover || !infoFlow) return;
      infoPopover.classList.add('show');
      infoPopover.setAttribute('aria-hidden', 'false');
      infoFlow.setAttribute('aria-expanded', 'true');

      // position under the button
      const r = infoFlow.getBoundingClientRect();
      const pad = 12;
      // ensure we can measure width
      const w = infoPopover.offsetWidth || 360;
      const leftMax = window.scrollX + window.innerWidth - w - pad;
      const left = Math.max(window.scrollX + pad, Math.min(window.scrollX + r.left, leftMax));
      const top = window.scrollY + r.bottom + 8;
      infoPopover.style.left = `${left}px`;
      infoPopover.style.top = `${top}px`;
    }

    function toggleInfoPopover() {
      if (!infoPopover) return;
      const open = infoPopover.classList.contains('show');
      if (open) closeInfoPopover();
      else openInfoPopover();
    }

    // Try restoring last UI state
    (function restoreState() {
      const st = loadState();
      if (!st) return;

      const p = (st.preset || '').toString();
      if (p && PRESETS && PRESETS[p]) activePreset = p;

      if (elSearch && st.search !== undefined) {
        elSearch.value = (st.search || '').toString();
      }

      if (st.cluster !== undefined && st.cluster !== null) {
        clusterPick = (st.cluster || '').toString();
      }

      if (st.pillar !== undefined && st.pillar !== null) {
        pillarPick = (st.pillar || '').toString();
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
      }

      if (st.matrix && typeof st.matrix === 'object') {
        const sb = (st.matrix.sb === null || st.matrix.sb === undefined) ? null : Number(st.matrix.sb);
        const rb = (st.matrix.rb === null || st.matrix.rb === undefined) ? null : Number(st.matrix.rb);
        matrix = Object.assign({}, DEFAULT_MATRIX, {sb: Number.isFinite(sb) ? sb : null, rb: Number.isFinite(rb) ? rb : null});
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

if (btnMarketToggle) {
  btnMarketToggle.addEventListener('click', () => {
    marketVisible = !marketVisible;
    setMarketVisible(marketVisible);
    saveState();
  });
}

if (elHeatMode) {
  elHeatMode.addEventListener('change', () => {
    const v = (elHeatMode.value || '').toString();
    heatMode = (v === 'cluster' || v === 'pillar') ? v : heatMode;
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

    // HTML-Listen-Konvertierung für Briefing (bessere Mobile Darstellung)
    function briefingToHtml(text){
      const lines = (text || "").split(/\\r?\\n/);
      let out = "";
      let inUl = false;

      const closeUl = ()=>{ if(inUl){ out += "</ul>"; inUl=false; } };

      for(const raw of lines){
        const line = raw.replace(/\\s+$/,"");
        if(!line){ closeUl(); out += "<div class='spacer'></div>"; continue; }

        const m = line.match(/^\\s*-\\s+(.*)$/);
        if(m){
          if(!inUl){ out += "<ul>"; inUl=true; }
          out += `<li>${String(m[1]).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c)}</li>`;
          continue;
        }

        closeUl();

        if(/^\\d+\\)\\s/.test(line)){
          out += `<h4 class="briefing-asset">${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c)}</h4>`;
        } else if(/^(Gründe|Risiken\\/Flags|Nächste Checks|Kontext-Hinweise)/.test(line)){
          out += `<div class="briefing-label">${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c).replace(/:$/,"")}</div>`;
        } else {
          out += `<p>${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c)}</p>`;
        }
      }
      closeUl();
      return out;
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
  const cur = clusterPick || '';
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
  const active = clusterPick || '';
  let html = '<span class="label">Cluster:</span>';
  if (active) {
    html += `<button type="button" class="chip kpi warn active" data-clr="1" title="Cluster-Filter löschen">✕ ${esc(active)}</button>`;
  }
  for (const x of top) {
    const isOn = active && x.k === active;
    const kind = isOn ? 'warn active' : 'blue';
    html += `<button type="button" class="chip kpi ${kind}" data-cl="${esc(x.k)}" title="Filter: nur Cluster ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
  }
  if (!top.length) html += '<span class="muted">—</span>';
  elClusters.innerHTML = html;
}

function applyClusterFilter(rows) {
  const sel = (clusterPick || '').toString().trim();
  if (!sel) return rows;
  return (rows || []).filter(r => clusterLabel(r) === sel);
}

// ---- 5‑Säulen / Playground helpers (UI-only; private metadata; never affects scoring) ----
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
  // legacy mining buckets → Fundament
  if (s.includes('mining') || s.includes('mine') || s.includes('edelmetall') || s.includes('metall') || s.includes('rohstoff')) return 'Fundament';
  // ambiguous tech buckets → Gehirn (default), hardware-specific keywords → Hardware
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
  const cur = pillarPick || '';
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
  const active = pillarPick || '';
  let html = '<span class="label">Säulen:</span>';
  if (active) {
    html += `<button type="button" class="chip kpi warn active" data-pr="1" title="Säulen-Filter löschen">✕ ${esc(active)}</button>`;
  }
  for (const x of (counts || [])) {
    const isOn = active && x.k === active;
    const kind = isOn ? 'warn active' : 'blue';
    const dis = (x.v || 0) <= 0 ? 'disabled aria-disabled="true"' : '';
    html += `<button type="button" class="chip kpi ${kind}" data-p="${esc(x.k)}" ${dis} title="Filter: nur Säule ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
  }
  elPillars.innerHTML = html;
}

function applyPillarFilter(rows) {
  const sel = (pillarPick || '').toString().trim();
  if (!sel) return rows;
  return (rows || []).filter(r => pillarLabel(r) === sel);
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
      return `${a}–${b}`;
    }
    function scoreBucketText(i) {
      return { range: bucketRange(i), hint: 'Score' };
    }
    function riskBucketText(i) {
      const hints = ['niedrig', 'moderat', 'mittel', 'hoch', 'sehr hoch'];
      return { range: bucketRange(i), hint: `Risk ${hints[i] || ''}`.trim() };
    }


    function fmtPrice(n) {
      if (n === null || n === undefined) return '—';
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
      if (p === null || p === undefined) return '<div class="sub muted">—</div>';
      const dir = (p > 0) ? 'pos' : (p < 0) ? 'neg' : 'flat';
      const arrow = (p > 0) ? '▲' : (p < 0) ? '▼' : '•';
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
        return pair || pickDisplayTicker(r) || '—';
      }
      const s = normStr(r.symbol) || normStr(r.Symbol);
      if (s && !looksLikeISIN(s)) return s;
      const td = normStr(r.ticker_display);
      if (td && !looksLikeISIN(td)) return td;
      const yh = pickYahooSymbol(r);
      if (yh && !looksLikeISIN(yh)) return yh;
      const t = normStr(r.ticker);
      if (t && !looksLikeISIN(t)) return t;
      return '—';
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
      const sig = rec ? `<span class="sig ${rec.cls}" title="Signal‑Code">${esc(rec.code)}</span>` : '';
      return `<div class="scorecell"><div class="scorebar"><div style="width:${s}%;"></div></div><span class="mono">${s.toFixed(2)}</span>${sig}</div>`;
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
      const tokens = q.split(/\s+/).filter(Boolean);
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
        + kpiChip(`Sichtbar ${v.total}/${a.total}`, 'blue', 'Sichtbar nach Preset → Suche → Quick-Filter / Gesamt', '', false)
        + kpiChip(`OK ${v.ok}`, 'good', 'Filter: nur score_status == OK', 'ok', !!quick.onlyOK)
        + kpiChip(`AVOID ${v.avoid}`, 'warn', 'Filter: nur score_status beginnt mit AVOID_', 'avoid', !!quick.onlyAvoid)
        + kpiChip(`NA ${v.na}`, 'bad', 'Filter: nur score_status == NA', 'na', !!quick.onlyNA)
        + kpiChip(`ERR ${v.error}`, 'bad', 'Filter: nur score_status == ERROR', 'err', !!quick.onlyERR)
        + kpiChip(`TrendFail ${v.trendFail}`, v.trendFail ? 'warn' : 'good', 'Filter: nur trend_ok == false', 'trendFail', !!quick.onlyTrendFail)
        + kpiChip(`LiqFail ${v.liqFail}`, v.liqFail ? 'warn' : 'good', 'Filter: nur liquidity_ok == false', 'liqFail', !!quick.onlyLiqFail)
        + kpiChip(`Aktien ${v.stock}`, 'blue', 'Filter: nur is_crypto == false', 'stock', !!quick.onlyStock)
        + kpiChip(`Krypto ${v.crypto}`, 'warn', 'Filter: nur is_crypto == true', 'crypto', !!quick.onlyCrypto);
    }

    
    function toggleKpi(key) {
      key = (key || '').toString();
      // mutually exclusive groups
      const clearStatus = () => { quick.onlyOK=false; quick.onlyAvoid=false; quick.onlyNA=false; quick.onlyERR=false; };
      const clearTrend = () => { quick.trendOK=false; quick.onlyTrendFail=false; };
      const clearLiq = () => { quick.liqOK=false; quick.onlyLiqFail=false; };

      if (key === 'ok') {
        const nv = !quick.onlyOK;
        clearStatus();
        quick.onlyOK = nv;
      } else if (key === 'avoid') {
        const nv = !quick.onlyAvoid;
        clearStatus();
        quick.onlyAvoid = nv;
        if (nv) quick.hideAvoid = false; // show avoid when filtering for it
      } else if (key === 'na') {
        const nv = !quick.onlyNA;
        clearStatus();
        quick.onlyNA = nv;
      } else if (key === 'err') {
        const nv = !quick.onlyERR;
        clearStatus();
        quick.onlyERR = nv;
      } else if (key === 'trendFail') {
        const nv = !quick.onlyTrendFail;
        clearTrend();
        quick.onlyTrendFail = nv;
      } else if (key === 'liqFail') {
        const nv = !quick.onlyLiqFail;
        clearLiq();
        quick.onlyLiqFail = nv;
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
        if (quick.onlyOK && status !== 'OK') return false;
        if (quick.onlyAvoid && !isAvoid) return false;
        if (quick.onlyNA && status !== 'NA') return false;
        if (quick.onlyERR && status !== 'ERROR') return false;

        // Hide AVOID applies only when we're not explicitly filtering for AVOID
        if (quick.hideAvoid && !quick.onlyAvoid && isAvoid) return false;

        // pass/fail filters
        if (quick.trendOK && asBool(r.trend_ok) !== true) return false;
        if (quick.onlyTrendFail && asBool(r.trend_ok) !== false) return false;

        if (quick.liqOK && asBool(r.liquidity_ok) !== true) return false;
        if (quick.onlyLiqFail && asBool(r.liquidity_ok) !== false) return false;

        // class filters
        const isCrypto = asBool(r.is_crypto) === true;
        if (quick.onlyCrypto && !isCrypto) return false;
        if (quick.onlyStock && isCrypto) return false;
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
      const counts = Array.from({length: 5}, () => Array(5).fill(0)); // [rb][sb]
      for (const r of rows) {
        const sb = scoreBucket(r.score);
        const rb = riskBucket(r.risk_pctl);
        counts[rb][sb] += 1;
      }

      const parts = [];
      // header row: Score buckets (x). Corner shows axis directions.
      parts.push(`<div class="matrixAxis" title="Achsen: Risk (y) × Score (x)"><div class="lbl">Risk ↓</div><div class="hint">Score →</div></div>`);
      for (let sb = 0; sb < 5; sb++) {
        const s = scoreBucketText(sb);
        parts.push(`<div class="matrixLabel" title="Score‑Bucket"><div class="lbl">${esc(s.range)}</div><div class="hint">${esc(s.hint)}</div></div>`);
      }

      for (let rb = 0; rb < 5; rb++) {
        const rtxt = riskBucketText(rb);
        parts.push(`<div class="matrixLabel" title="Risk‑Bucket (Perzentil; höher = riskanter)"><div class="lbl">${esc(rtxt.range)}</div><div class="hint">${esc(rtxt.hint)}</div></div>`);
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

      // Click → toggle matrix filter
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
        const metric = (RISK_SORTED && RISK_SORTED.length) ? 'Risk‑Proxy aus volatility/downside_dev/max_drawdown (Perzentil)' : 'Risk‑Proxy fehlt (keine Risk‑Spalten im CSV)';
        const sel = (sb !== null && rb !== null) ? ` · aktiv: Score ${bucketRange(sb)} × ${riskBucketText(rb).hint}` : '';
        elMatrixNote.textContent = metric + sel;
      }
    }

// ---- Market Context (passive; derived from current universe; no scoring influence) ----
function parsePct(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace('%','').replace(/\s+/g,'').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function perfPct(r) {
  return parsePct(
    r.perf_pct ?? r['Perf %'] ?? r['Change %'] ?? r.change_pct ?? r.changePercent ?? r.PerfPct
  );
}

function fmtRatioPct(v) {
  const n = asNum(v);
  if (n === null) return '—';
  return `${(n * 100).toFixed(2)}%`;
}

function fmtPct(v) {
  if (v === null || v === undefined || !Number.isFinite(v)) return '—';
  const s = (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
  return s;
}

function quantile(sortedVals, q) {
  if (!Array.isArray(sortedVals) || !sortedVals.length) return null;
  if (sortedVals.length === 1) return sortedVals[0];
  const qq = Math.max(0, Math.min(1, Number(q) || 0));
  const pos = (sortedVals.length - 1) * qq;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sortedVals[lo];
  const w = pos - lo;
  return sortedVals[lo] * (1 - w) + sortedVals[hi] * w;
}

function renderBreadth(rows) {
  if (!elBreadthBox) return;
  let adv = 0, dec = 0, flat = 0, miss = 0;
  for (const r of rows || []) {
    const p = perfPct(r);
    if (p === null) { miss++; continue; }
    if (p > 0) adv++;
    else if (p < 0) dec++;
    else flat++;
  }
  const tot = adv + dec + flat;
  if (!tot) {
    elBreadthBox.innerHTML = `<div class="muted">Keine verwertbare Tagesänderung (Perf %) im aktuellen Universe.</div>`;
    return;
  }
  const pct = (x) => (tot ? Math.round((x / tot) * 100) : 0);

  const row = `
    <div class="breadthRow">
      ${chip(`Adv ${adv} (${pct(adv)}%)`, adv ? 'good' : 'blue')}
      ${chip(`Dec ${dec} (${pct(dec)}%)`, dec ? 'bad' : 'blue')}
      ${chip(`Flat ${flat}`, flat ? 'blue' : 'blue')}
      ${miss ? chip(`n/a ${miss}`, 'warn') : ''}
    </div>
    <div class="muted small">Basis: <span class="mono">perf_pct / Perf %</span> (nur Anzeige/Context).</div>
  `;
  elBreadthBox.innerHTML = row;
}

function renderDiversification(rows) {
  if (!elDiversBox) return;
  const vals = (rows || []).map(r => asNum(r.diversification_penalty)).filter(v => v !== null);
  if (!vals.length) {
    elDiversBox.innerHTML = `<div class="muted">Diversifikation: keine Penalty-Daten im aktuellen CSV.</div>`;
    return;
  }

  const s = vals.slice().sort((a, b) => a - b);
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
  const med = s[Math.floor(s.length / 2)];
  const hi = vals.filter(v => v >= 6).length;
  const lo = vals.filter(v => v <= 2).length;

  const sectorCounts = {};
  for (const r of rows || []) {
    const p = asNum(r.diversification_penalty);
    if (p === null || p < 6) continue;
    const sec = normStr(r.cluster_official) || normStr(r.sector) || normStr(r.Sector) || 'n/a';
    sectorCounts[sec] = (sectorCounts[sec] || 0) + 1;
  }
  const topSec = Object.entries(sectorCounts).sort((a, b) => b[1] - a[1]).slice(0, 2);
  const secInfo = topSec.length
    ? `Top Crowding: ${topSec.map(([k, v]) => `${esc(k)} (${v})`).join(' · ')}`
    : 'Top Crowding: —';

  elDiversBox.innerHTML = `
    <div class="breadthRow">
      ${chip(`Div Ø ${avg.toFixed(2)}`, avg >= 4 ? 'warn' : 'blue')}
      ${chip(`Median ${med.toFixed(2)}`, med >= 4 ? 'warn' : 'blue')}
      ${chip(`High ${hi}`, hi ? 'bad' : 'blue')}
      ${chip(`Low ${lo}`, lo ? 'good' : 'blue')}
    </div>
    <div class="muted small">${secInfo}</div>
  `;
}

function renderQualityPanel(rows) {
  if (!elQualityBox) return;
  const n = Array.isArray(rows) ? rows.length : 0;
  if (!n) {
    elQualityBox.innerHTML = `<div class="muted">Preset-Qualität: keine Daten.</div>`;
    return;
  }

  const scores = rows.map(r => asNum(r.score)).filter(v => v !== null).sort((a, b) => a - b);
  const confs = rows.map(r => asNum(r.confidence)).filter(v => v !== null).sort((a, b) => a - b);

  const scoreMed = quantile(scores, 0.5);
  const scoreQ1 = quantile(scores, 0.25);
  const scoreQ3 = quantile(scores, 0.75);
  const scoreIqr = (scoreQ1 !== null && scoreQ3 !== null) ? (scoreQ3 - scoreQ1) : null;

  const confMed = quantile(confs, 0.5);
  const confQ1 = quantile(confs, 0.25);
  const confQ3 = quantile(confs, 0.75);
  const confIqr = (confQ1 !== null && confQ3 !== null) ? (confQ3 - confQ1) : null;

  let trendOk = 0;
  let liqOk = 0;
  let topPctl = 0;
  let pctlDen = 0;
  for (const r of rows) {
    if (asBool(r.trend_ok) === true) trendOk++;
    if (asBool(r.liquidity_ok) === true) liqOk++;
    const p = asNum(r.score_pctl);
    if (p !== null) {
      pctlDen++;
      if (p >= 90) topPctl++;
    }
  }

  const trendShare = trendOk / n;
  const liqShare = liqOk / n;
  const topShare = pctlDen ? (topPctl / pctlDen) : 0;

  elQualityBox.innerHTML = `
    <div class="marketCardTitle" style="margin-top:2px;">Preset Quality</div>
    <div class="breadthRow">
      ${chip(`Score Med ${scoreMed === null ? '—' : scoreMed.toFixed(1)}`, (scoreMed !== null && scoreMed >= 34) ? 'good' : 'blue')}
      ${chip(`Score IQR ${scoreIqr === null ? '—' : scoreIqr.toFixed(1)}`, (scoreIqr !== null && scoreIqr >= 10) ? 'good' : 'warn')}
      ${chip(`Conf Med ${confMed === null ? '—' : confMed.toFixed(1)}`, (confMed !== null && confMed >= 60) ? 'good' : 'warn')}
      ${chip(`Conf IQR ${confIqr === null ? '—' : confIqr.toFixed(1)}`, (confIqr !== null && confIqr >= 12) ? 'blue' : 'warn')}
    </div>
    <div class="breadthRow">
      ${chip(`Trend OK ${fmtRatioPct(trendShare)}`, trendShare >= 0.7 ? 'good' : (trendShare >= 0.45 ? 'blue' : 'warn'))}
      ${chip(`Liq OK ${fmtRatioPct(liqShare)}`, liqShare >= 0.7 ? 'good' : (liqShare >= 0.45 ? 'blue' : 'warn'))}
      ${chip(`Top Pctl ${fmtRatioPct(topShare)}`, topShare >= 0.25 ? 'good' : (topShare >= 0.1 ? 'blue' : 'warn'))}
      ${chip(`N ${n}`, 'blue')}
    </div>
    <div class="muted small">Basis: aktives Preset (vor Suche/Quick/Matrix); Top Pctl = Anteil mit Score-Perzentil >= 90.</div>
  `;
}

function renderMovers(rows) {
  if (!elMoversUp || !elMoversDown) return;

  const arr = [];
  for (const r of rows || []) {
    const p = perfPct(r);
    if (p === null) continue;
    arr.push({r, p});
  }
  if (!arr.length) {
    elMoversUp.innerHTML = `<span class="muted">—</span>`;
    elMoversDown.innerHTML = `<span class="muted">—</span>`;
    return;
  }

  const up = arr.slice().sort((a,b) => b.p - a.p).filter(x => x.p > 0).slice(0, 8);
  const dn = arr.slice().sort((a,b) => a.p - b.p).filter(x => x.p < 0).slice(0, 8);

  function itemHtml(x) {
    const r = x.r;
    const sym = pickDisplaySymbol(r);
    const yh = pickYahooSymbol(r) || sym;
    const href = yahooHref(yh);
    const s = href ? `<a class="yf sym" href="${href}" target="_blank" rel="noopener">${esc(sym)}</a>` : `<span class="sym">${esc(sym)}</span>`;
    const cls = x.p > 0 ? 'pos' : (x.p < 0 ? 'neg' : 'flat');
    return `<div class="moversItem"><span class="sym">${s}</span><span class="val ${cls}">${esc(fmtPct(x.p))}</span></div>`;
  }

  elMoversUp.innerHTML = up.length ? up.map(itemHtml).join('') : `<span class="muted">—</span>`;
  elMoversDown.innerHTML = dn.length ? dn.map(itemHtml).join('') : `<span class="muted">—</span>`;
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
    const tds = x.arr.map((v, sb) => {
      const zero = v === 0 ? ' zero' : '';
      const rel = vmax ? (v / vmax) : 0;
      const alpha = 0.06 + rel * 0.28; // subtle
      const hue = 205; // blue-ish
      const bg = `background: hsla(${hue}, 70%, 50%, ${alpha});`;
      return `<td class="heatCell${zero}" style="${bg}" title="${esc(x.k)} · Score ${esc(hdr[sb])} = ${v}">${v ? v : '·'}</td>`;
    }).join('');
    return `<tr><td class="mono">${esc(x.k)}</td>${tds}</tr>`;
  }).join('');

  elHeatmap.innerHTML = `
    <div class="heatWrap">
      <table class="heatTbl">
        <thead><tr><th>${heatMode === 'cluster' ? 'Cluster' : 'Säule'}</th>${th}</tr></thead>
        <tbody>${rowsHtml}</tbody>
      </table>
      <div class="muted small" style="margin-top:6px;">Zahl = Anzahl Werte pro Score‑Bucket (Top ${limit} nach Häufigkeit).</div>
    </div>
  `;
}

function renderMarketContext(rows, presetRows) {
  if (!elMarketPanel) return;
  renderBreadth(rows);
  renderDiversification(rows);
  renderQualityPanel(presetRows || rows);
  renderMovers(rows);
  renderHeatmap(rows);
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
        const subTicker = isC ? (yh || '—') : (isin || '—');
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
        const priceMain = (price === null) ? '—' : `${fmtPrice(price)}${curr ? ' ' + esc(curr) : ''}`;
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
          <td class="hide-sm right mono">${(asNum(r.confidence) ?? 0).toFixed(1)}</td>
          <td class="hide-sm right mono">${(asNum(r.cycle) ?? 0).toFixed(0)}%</td>
          <td>${trend}</td>
          <td>${liq}</td>
          <td>${chip(status || '—', statusKind)}</td>
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
      drawerTitle.textContent = `${t} — ${n}`.trim();
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
        ['ScoreStatus', normStr(r.score_status) || '—'],
        ['Trend OK', String(asBool(r.trend_ok))],
        ['Liquidity OK', String(asBool(r.liquidity_ok))],
        ['AssetClass', asBool(r.is_crypto) ? 'Krypto' : 'Aktie'],
      ];

      // optional interesting fields
      const opt = [
        ['Price', r.price],
        ['Currency', curr],
        ['Perf %', fmtPct(asNum(r.perf_pct) ?? asNum(r["Perf %"]))],
        ['DiversPenalty', (asNum(r.diversification_penalty) ?? 0).toFixed(2)],
        ['RS3M', r.rs3m],
        ['CRV', r.crv],
        ['MC Chance', r.mc_chance],
        ['Elliott', r.elliott_signal],
        ['CycleStatus', r.cycle_status],
        ['DollarVolume', r.dollar_volume],
        ['Volatility', fmtRatioPct(r.volatility)],
        ['MaxDrawdown', fmtRatioPct(r.max_drawdown)],
        ['Trend200', fmtRatioPct(r.trend200)],
        ['MarketDate', r.market_date],
      ];
      for (const [k, v] of opt) {
        const s = normStr(v);
        if (s) items.push([k, s]);
      }

      const status = normStr(r.score_status);
      const why = [];
      if (status === 'OK') why.push('Score>0 & keine harten Filter verletzt.');
      if (status === 'AVOID_CRYPTO_BEAR') why.push('Crypto im Bear-Trend → Score=0 (bewusstes Avoid).');
      if (status === 'AVOID') why.push('Score==0 → Avoid (non-crypto).');
      if (status === 'NA') why.push('Zu wenig / nicht konsistente Daten → NA.');
      if (status === 'ERROR') why.push('Scoring hat einen Fehler gemeldet (ScoreError).');
      if (asBool(r.trend_ok) === false) why.push('Trend-Filter: trend_ok=false.');
      if (asBool(r.liquidity_ok) === false) why.push('Liquidity-Filter: liquidity_ok=false.');
      const dpen = asNum(r.diversification_penalty);
      if (dpen !== null && dpen >= 6) why.push(`Diversifikations-Penalty hoch (${dpen.toFixed(2)}): Setup ist im aktuellen Universum eher klumpig.`);
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

      // matrix counts always reflect the current (pre-matrix) universe (after cluster filter)
      renderMatrix(rows);
      renderMarketContext(rows, presetRows);

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
      if (clusterPick) f.push(`cluster:${clusterPick}`);
      if (pillarPick) f.push(`pillar:${pillarPick}`);
      if (pillarPick) f.push(`pillar:${pillarPick}`);

      elCount.textContent = `${rows.length} / ${base.length}` + (f.length ? `  ·  filters: ${f.join(',')}` : '');
      if (btnMatrixClear) btnMatrixClear.disabled = !(matrix && matrix.sb !== null && matrix.rb !== null);

      const override = userSort ? ` | override: ${userSort.k}:${userSort.dir}` : '';
      elSortHint.textContent = `preset: ${presetLabel(activePreset)}${override}`;
    }

    // ---- init ----
    const PRESET_LABELS = {
      CORE: 'Übersicht',
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
        opt.textContent = `${presetLabel(n)} (${n})` + (desc ? ` — ${desc}` : '');
        elPreset.appendChild(opt);
      }
      elPreset.value = activePreset;
    }

    initPresets();
    // quick filters
    const filterBar = document.getElementById('filters');
    if (filterBar) {
      filterBar.querySelectorAll('button.fbtn').forEach(btn => {
        btn.addEventListener('click', () => {
          const k = btn.getAttribute('data-f');
          if (!k) return;

          if (k === 'reset') {
            resetAll();
            return;
          } else if (k === 'resetSort') {
            userSort = null;
            refresh();
            saveState();
            return;
          } else if (k === 'onlyCrypto') {
            quick.onlyCrypto = !quick.onlyCrypto;
            if (quick.onlyCrypto) quick.onlyStock = false;
          } else if (k === 'onlyStock') {
            quick.onlyStock = !quick.onlyStock;
            if (quick.onlyStock) quick.onlyCrypto = false;
          } else {
            quick[k] = !quick[k];
          }

          syncFilterButtons();
          refresh();
          saveState();
        });
      });
    }
    syncFilterButtons();

    // cluster select wiring
    if (elClusterSel) {
      elClusterSel.addEventListener('change', () => {
        clusterPick = elClusterSel.value || '';
        refresh();
        saveState();
      });
    }

    // pillar select wiring
    if (elPillarSel) {
      elPillarSel.addEventListener('change', () => {
        pillarPick = elPillarSel.value || '';
        refresh();
        saveState();
      });
    }
    if (elClusters) {
      elClusters.addEventListener('click', (ev) => {
        const t = ev.target;
        if (!(t instanceof HTMLElement)) return;
        const clr = t.getAttribute('data-clr');
        if (clr) {
          clusterPick = '';
          if (elClusterSel) elClusterSel.value = '';
          refresh();
          saveState();
          return;
        }
        const cl = t.getAttribute('data-cl');
        if (!cl) return;
        clusterPick = cl;
        if (elClusterSel) elClusterSel.value = cl;
        refresh();
        saveState();
      });
    }

    if (elPillars) {
      elPillars.addEventListener('click', (ev) => {
        const t = ev.target;
        if (!(t instanceof HTMLElement)) return;
        const pr = t.getAttribute('data-pr');
        if (pr) {
          pillarPick = '';
          if (elPillarSel) elPillarSel.value = '';
          refresh();
          saveState();
          return;
        }
        const p = t.getAttribute('data-p');
        if (!p) return;
        pillarPick = p;
        if (elPillarSel) elPillarSel.value = p;
        refresh();
        saveState();
      });
    }

    if (btnMatrixClear) {
      btnMatrixClear.addEventListener('click', () => {
        matrix = Object.assign({}, DEFAULT_MATRIX);
        refresh();
        saveState();
      });
    }

    // info popover wiring
    if (infoFlow && infoPopover) {
      infoFlow.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleInfoPopover();
      });
      if (infoClose) {
        infoClose.addEventListener('click', (e) => {
          e.stopPropagation();
          closeInfoPopover();
        });
      }
      document.addEventListener('click', (e) => {
        if (!infoPopover.classList.contains('show')) return;
        const t = e.target;
        if (t === infoFlow || infoFlow.contains(t) || infoPopover.contains(t)) return;
        closeInfoPopover();
      });
      window.addEventListener('resize', () => closeInfoPopover());
      window.addEventListener('scroll', () => closeInfoPopover(), true);
    }
    if (elBriefing) {
      const t = normStr((BRIEFING || {}).text);
      elBriefing.innerHTML = briefingToHtml(t) || '— (noch kein Briefing generiert)';
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
        if (infoPopover && infoPopover.classList.contains('show')) {
          closeInfoPopover();
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
      if (window.__showJsError) window.__showJsError('UI‑Fehler (JS): ' + msg);
      if (el) el.textContent = `JS error: ${msg}`;
      const k = document.getElementById('kpis');
      if (k) k.textContent = 'JS error – siehe Konsole';
      try { console.error(err); } catch(e) {}
    }
  })();
  </script>
</body>
</html>
"""

    return (
        template
        .replace("__DATA_JSON__", data_json)
        .replace("__PRESETS_JSON__", presets_json)
        .replace("__PRESET_OPTIONS__", preset_options_html)
        .replace("__BRIEFING_JSON__", briefing_json)
        .replace("__FALLBACK_TBODY__", fallback_tbody_html)
        .replace("__VERSION__", str(version))
        .replace("__BUILD__", str(build))
        .replace("__SOURCE_CSV__", str(source_csv))
    )


def _render_help_html(*, version: str, build: str) -> str:
    """Generate a static help / project description page.

    This page is intentionally a living document: it describes what exists today
    and keeps placeholders for upcoming features (Portfolio, KI‑Briefing, etc.).
    """

    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Scanner_vNext — Hilfe & Projektbeschreibung</title>
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
          <h1>Scanner_vNext — Hilfe & Projektbeschreibung</h1>
          <div class="meta">version {version} · build {build} · <a href="index.html">zurück zum Dashboard</a></div>
        </div>
        <div class="pill">Living Doc · wird laufend erweitert</div>
      </div>
    </div>
  </header>

  <main class="wrap">

    <div class="card" style="border-color: rgba(251,191,36,.35); background: rgba(251,191,36,.06);">
      <h2>Disclaimer</h2>
      <p><b>Privates, experimentelles Projekt.</b> Keine Anlageberatung, keine Empfehlung, keine Gewähr. Inhalte können unvollständig, falsch oder veraltet sein. Nutzung ausschließlich auf eigene Verantwortung.</p>
    </div>

    <div class="card">
      <h2 id="toc">Inhalt</h2>
      <div class="toc">
        <a href="#ueberblick">1) Überblick</a>
        <a href="#pipeline">2) Datenfluss (Pipeline)</a>
        <a href="#scoring">3) Scoring: Score, Opportunity, Risk, Regime, Confidence</a>
        <a href="#recommendation">4) Empfehlungscode (R0–R5)</a>
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
      <h2>1) Überblick</h2>
      <p><b>Scanner_vNext</b> ist ein privates Trading‑Research‑System für Watchlists und Portfolio‑Ideen. Es bündelt Kennzahlen, Signale und Markt‑Kontext zu einem <b>multi‑faktoriellen Score</b> – mit dem Ziel, Entscheidungen schneller, konsistenter und nachvollziehbar zu machen.</p>
      <div class="callout">
        <p style="margin:0"><b>Wichtig:</b> Presets sind reine <i>Ansichten</i> (View‑Layer) – sie verändern das Scoring nicht. KI‑Texte sind reine <i>Briefings</i> und dürfen keinen Einfluss auf den Score haben.</p>
      </div>
      <p class="tag">Hinweis: Dieses Projekt ist kein Finanzrat. Es ist ein Werkzeug zur eigenen Strukturierung und Dokumentation von Entscheidungen.</p>
    </div>

    <div class="card" id="pipeline">
      <h2>2) Datenfluss (Pipeline)</h2>
      <p>Die täglichen Schritte sind bewusst getrennt – damit Scoring, Daten und UI sauber entkoppelt bleiben.</p>
      <pre>python -m scanner.app.run_daily
python -m scanner.ui.generator</pre>
      <ul>
        <li><code>run_daily</code> erzeugt/aktualisiert CSVs in <code>artifacts/watchlist/</code> und (optional) Reports in <code>artifacts/reports/</code>.</li>
        <li><code>ui.generator</code> liest eine CSV (z.B. <code>watchlist_CORE.csv</code>) und schreibt statisches HTML nach <code>artifacts/ui/</code>.</li>
      </ul>
      <p>Damit kannst du die Pipeline testen, versionieren und reproduzierbar ausführen – ohne dass das UI „heimlich“ irgendwas berechnet, was die Ergebnisse verändern würde.</p>
    </div>

    <div class="card" id="scoring">
      <h2>3) Scoring (Score, Opportunity, Risk, Regime, Confidence)</h2>
      <p>Das Scoring läuft zentral im Domain‑Layer (<code>scanner.domain.scoring_engine</code>). Dort wird aus einer Watchlist‑Zeile ein Satz aus <b>Opportunity‑Faktoren</b> und <b>Risk‑Faktoren</b> gebildet und anschließend zu einem finalen Score zusammengeführt.</p>

      <div class="grid2">
        <div>
          <h3>Opportunity (0..1, höher = besser)</h3>
          <p>Beispiele der aktuell verwendeten Faktoren (wenn in den CSVs vorhanden):</p>
          <ul>
            <li><b>Growth %</b>, <b>ROE %</b>, <b>Margin %</b></li>
            <li><b>MC‑Chance</b> (Monte‑Carlo‑Chance)</li>
            <li><b>Trend200</b> (200‑Tage‑Trend) und <b>RS3M</b> (relative Stärke 3M)</li>
            <li><b>Elliott‑Quality</b> (abhängig vom Elliott‑Signal)</li>
            <li><b>Upside</b> (nur wenn Elliott‑Signal BUY und Target/Preis vorhanden; 30% Upside = „voller“ Faktor)</li>
          </ul>
          <p class="tag">Hinweis: einzelne Faktoren sind bewusst als Platzhalter gesetzt (z.B. Analyst‑Faktor), bis Spalten dafür existieren.</p>
        </div>
        <div>
          <h3>Risk (0..1, höher = riskanter)</h3>
          <p>Beispiele der aktuell verwendeten Faktoren:</p>
          <ul>
            <li><b>Debt/Equity</b></li>
            <li><b>CRV‑Fragility</b> (CRV wird in eine „Fragilität“ umgerechnet; bei fehlendem CRV neutral)</li>
            <li><b>Volatility</b>, <b>DownsideDev</b>, <b>MaxDrawdown</b></li>
            <li><b>Liquidity‑Risk</b> (bevorzugt DollarVolume; Fallback AvgVolume)</li>
          </ul>
          <p class="tag">Die Idee: Opportunity alleine reicht nicht – ein hoher Score soll bei fragiler Liquidität oder extremem Drawdown nicht „blind“ nach oben schießen.</p>
        </div>
      </div>

      <h3>Normalisierung (Universe‑Scaling)</h3>
      <p>Viele Rohwerte werden über ein Universe (Verteilung der Werte im aktuellen Datensatz) auf 0..1 skaliert. Dadurch wird der Score <b>relativ zum aktuellen Markt‑Universum</b> interpretierbar (statt absolute Schwellen zu erzwingen).</p>

      <h3>Regime (Markt‑Kontext)</h3>
      <p>Der Score kann je nach Marktregime anders gewichtet werden. Dafür werden vorhandene Spalten genutzt (z.B. <code>MarketRegimeStock</code>/<code>MarketRegimeCrypto</code> und Trend200‑Kontext). Wenn das Regime‑Label fehlt, wird es aus Trend200 grob als bull/neutral/bear abgeleitet.</p>
      <ul>
        <li><b>opp_w</b> / <b>risk_w</b>: wie stark Opportunity vs. Risk in den finalen Score einfließt</li>
        <li><b>risk_mult</b>: wie „hart“ Risiko bestraft wird</li>
      </ul>

      <h3>Confidence (0..100)</h3>
      <p>Zusätzlich wird eine <b>Confidence</b> berechnet, die z.B. Datenabdeckung, Konfluenz, Risiko‑Sauberkeit, Regime‑Ausrichtung und Liquidität berücksichtigt. Ziel: du erkennst schneller, ob ein hoher Score auf stabilen Inputs steht – oder auf dünnem Daten‑Eis.</p>
    </div>

    <div class="card" id="recommendation">
      <h2>4) Empfehlungscode (R0–R5)</h2>
      <p>Im Dashboard erscheint im Score‑Bereich ein privater Code <b>R0–R5</b>. Das ist <b>kein Trading‑Signal</b>, sondern eine knappe Zusammenfassung für deinen Workflow.</p>
      <ul>
        <li><b>R0</b>: <span class="sig warn grad">Avoid</span> (score_status beginnt mit <code>AVOID_</code>)</li>
        <li><b>R5</b>: <span class="sig good grad">Top Setup</span> (Score-Perzentil >= 90 <i>und</i> Trend OK <i>und</i> Liquidity OK)</li>
        <li><b>R4</b>: <span class="sig good grad">Good Setup</span> (Score-Perzentil >= 75 <i>und</i> Liquidity OK)</li>
        <li><b>R3</b>: <span class="sig blue grad">Neutral</span> (Score-Perzentil >= 45)</li>
        <li><b>R2</b>: <span class="sig warn grad">Weak</span> (Score-Perzentil >= 20)</li>
        <li><b>R1</b>: <span class="sig bad grad">Low Priority</span> (Rest)</li>
      </ul>
      <p class="tag">Technik: das UI berechnet das Score‑Perzentil aus allen Zeilen der geladenen Tabelle.</p>
    </div>

    <div class="card" id="dashboard">
      <h2>5) Dashboard‑Funktionen</h2>
      <h3>Presets</h3>
      <p>Presets sind Filter/Sichten (CORE, TOP, AVOID …). Sie bestimmen, <i>was du siehst</i>, nicht <i>wie gescored wird</i>.</p>

      <h3>Suche</h3>
      <p>Suche filtert quer über Symbol/Name/Kategorie/Land (und weitere Felder, sofern vorhanden).</p>

      <h3>Quick‑Filter & KPI‑Chips</h3>
      <p>Quick‑Filter sind schnelle boolesche/Status‑Schalter (z.B. „Nur OK“, „Trend OK“, „Liq OK“). KPI‑Chips sind klickbare Zusammenfassungen, die ebenfalls als Filter wirken.</p>

      <h3>Cluster (offiziell) vs. Säulen (privat)</h3>
      <p>Es gibt zwei unterschiedliche „Kategorien“ im UI – mit unterschiedlicher Bedeutung:</p>
      <ul>
        <li><b>Cluster/Sektor (offiziell)</b>: kommt aus Yahoo‑Taxonomie (Sector/Industry) und ist dafür gedacht, echte Markt‑Cluster sichtbar zu machen.</li>
        <li><b>Säulen (5‑Säulen/Playground, privat)</b>: deine thematische Metadaten‑Zuordnung (Gehirn, Hardware, Energie, Fundament, Recycling, Playground). Sie dient nur der Navigation/Explainability und <b>ändert niemals</b> den Score.</li>
      </ul>
      <p class="tag">Hinweis: ältere „Phantasie‑Sektoren“ können weiterhin in der Quelle vorkommen, werden aber nicht als offizieller Sektor interpretiert. Die UI kann daraus optional eine Säule ableiten, damit das Konzept sichtbar bleibt.</p>

      <h3>Bucket‑Matrix (Score × Risk)</h3>
      <p>Die Matrix verdichtet das Universum: <b>Score</b> auf der X‑Achse, <b>Risk</b> auf der Y‑Achse. Klick auf ein Feld aktiviert einen zusätzlichen Matrix‑Filter.</p>

      <h3>Why‑Score Drawer</h3>
      <p>Der Drawer erklärt, <i>warum</i> ein Wert so aussieht: Status‑Flags, wichtige Kennzahlen und – falls vorhanden – ein Breakdown (z.B. Confidence‑Breakdown).</p>

      <h3>Ticker‑Zelle (2‑zeilig)</h3>
      <p>Aktien: oben Symbol, unten ISIN. Krypto: oben Pair/ID, unten Yahoo‑Pair (oder was vorhanden ist). Ziel: du siehst Identität + „Key“ sofort, ohne zusätzliche Spalten.</p>

      <h3>Finviz‑Inspiration (eigene Umsetzung)</h3>
      <p>Die Market‑Übersicht (Index‑Charts, Breadth, Heatmap, Movers/News) ist als eigene Seite geplant/teilweise vorhanden (<a href="../dashboard/index.html" target="_blank" rel="noopener">Market‑Dashboard</a>). Layout‑Ideen dürfen inspiriert sein, aber Inhalte/Code werden nicht 1:1 kopiert – Scanner_vNext bleibt eine eigenständige Logik/UX.</p>
    </div>

    <div class="card" id="portfolio">
      <h2>6) Portfolio (geplant)</h2>
      <p>Hier kommt eine Portfolio‑Sektion hin (Bestände, Einstand, Gewichtung, Risiko‑Beitrag, Ziel‑Allokation, Alerts). Aktuell ist das bewusst noch Platzhalter, damit wir es sauber an dein Konzept andocken können.</p>
      <div class="callout"><p style="margin:0"><b>TODO:</b> Portfolio‑Konzept einfügen, sobald du es wieder parat hast (oder als Datei/Notiz lieferst).</p></div>
    </div>

    <div class="card" id="briefing">
      <h2>7) Briefing / KI</h2>
      <p>Das Briefing ist ein <b>passiver Explainability‑Report</b> für die Top‑Werte. Es wird ausschließlich aus bereits vorhandenen Feldern der Watchlist‑CSV abgeleitet (kein Re‑Scoring).</p>
      <h3>Outputs</h3>
      <ul>
        <li><code>artifacts/reports/briefing.json</code> – strukturierte Daten (Top‑N + Gründe/Risiken/Checks).</li>
        <li><code>artifacts/reports/briefing.txt</code> – deterministische Text‑Version (immer vorhanden, offline).</li>
        <li><code>artifacts/reports/briefing_ai.txt</code> – optionale sprachliche Glättung via OpenAI API (Feature‑Flag, default OFF).</li>
      </ul>
      <h3>Erzeugung</h3>
      <p>Briefing generieren (Stage A, deterministisch):</p>
      <pre><code>python scripts/generate_briefing.py</code></pre>
      <p>AI‑Enhancement (Stage B, optional):</p>
      <pre><code>set OPENAI_API_KEY=...  # Windows
python scripts/generate_briefing.py --enable-ai</code></pre>
      <ul>
        <li>Es ist <b>rein erklärend</b> (Notizen/Explainability).</li>
        <li>Es darf <b>niemals</b> das Scoring oder Ranking beeinflussen.</li>
        <li><b>Keine Anlageberatung</b>: das Briefing enthält einen kurzen Disclaimer.</li>
      </ul>
      <p class="tag">Dashboard: Das UI zeigt bevorzugt <code>briefing_ai.txt</code>, sonst <code>briefing.txt</code>. Wenn nichts vorhanden ist: „Noch kein Briefing generiert“.</p>
    </div>



    <div class="card" id="autopilot">
      <h2>8) GitHub Autopilot (ohne laufenden PC)</h2>
      <p>Wenn Scanner_vNext in einem GitHub‑Repo liegt, kann ein geplanter Workflow (GitHub Actions) die Pipeline automatisch ausführen. Damit läuft der Scanner „serverlos“ in der Cloud – dein Rechner muss dafür nicht an sein.</p>
      <h3>Was macht der Autopilot?</h3>
      <ul>
        <li>Installiert das Projekt (<code>pip install -e .</code>).</li>
        <li>Führt <code>python -m scanner.app.run_daily</code> aus (CSV‑Outputs nach <code>artifacts/watchlist/</code>).</li>
        <li>Erzeugt das deterministische Briefing (<code>scripts/generate_briefing.py</code> → <code>artifacts/reports/</code>).</li>
        <li>Generiert die UI (<code>python -m scanner.ui.generator</code> → <code>artifacts/ui/</code>).</li>
        <li>Committet die Outputs (standardmäßig <code>artifacts/</code>) zurück ins Repo.</li>
      </ul>
      <h3>Warum committen wir <code>artifacts/</code>?</h3>
      <p>Für den Einstieg ist das der simpelste Weg: du siehst im Repo und/oder über GitHub Pages sofort die aktuellen HTML/CSV‑Outputs. Später kann man das auf einen reinen Deploy‑Branch umstellen, wenn das Repo zu groß wird.</p>
      <h3>Benachrichtigungen</h3>
      <p>GitHub kann dir E‑Mails senden, wenn ein Workflow gelaufen ist. Das kommt von GitHub (nicht vom Projekt). Wenn du das reduzieren willst: Repo → <i>Watch</i> Einstellungen bzw. GitHub Notifications anpassen.</p>
      <p class="tag">Technik: Workflow‑Datei liegt unter <code>.github/workflows/run_scanner.yml</code>.</p>
    </div>

    <div class="card" id="notifications">
      <h2>9) Benachrichtigungen (Telegram)</h2>
      <p>Telegram ist optional und standardmäßig deaktiviert. Es hat keinen Einfluss auf Scoring oder Ranking – es ist nur ein zusätzlicher Kanal für Hinweise. Da du es aktuell nicht brauchst, bleibt es aus.</p>
      <h3>Aktivieren (falls du es später wieder willst)</h3>
      <pre><code>TELEGRAM_ENABLED=1
TELEGRAM_BOT_TOKEN=...   # oder TELEGRAM_TOKEN (Legacy)
TELEGRAM_CHAT_ID=...</code></pre>
      <p class="tag">Hinweis: Ohne <code>TELEGRAM_ENABLED</code> (oder bei fehlenden Tokens) wird nichts gesendet.</p>
    </div>

    <div class="card" id="troubleshooting">
      <h2>10) Troubleshooting</h2>
      <h3>UI zeigt „Keine Daten“</h3>
      <ul>
        <li>Prüfe, ob <code>artifacts/watchlist/watchlist_CORE.csv</code> (oder ALL) existiert.</li>
        <li>Führe zuerst <code>python -m scanner.app.run_daily</code> aus.</li>
      </ul>
      <h3>Contract validation failed</h3>
      <ul>
        <li>Contract: <code>configs/watchlist_contract.json</code></li>
        <li>Die UI bricht absichtlich ab, wenn Pflichtspalten fehlen – das verhindert stilles „UI zeigt Mist“.</li>
        <li>Lösung: Watchlist‑CSV neu generieren oder Migration/Normalize‑Scripts nutzen.</li>
      </ul>
      <h3>Briefing fehlt</h3>
      <ul>
        <li>Erzeuge es mit <code>python scripts/generate_briefing.py</code>.</li>
        <li>UI lädt bevorzugt <code>briefing_ai.txt</code>, sonst <code>briefing.txt</code>. Wenn beide fehlen: „Noch kein Briefing generiert“.</li>
      </ul>
      <h3>GOOGLE_CREDENTIALS fehlt (GitHub Action)</h3>
      <ul>
        <li>Das Secret muss in GitHub als <code>GOOGLE_CREDENTIALS</code> hinterlegt sein (JSON‑Service‑Account).</li>
        <li>Ohne Credentials kann der Scanner keine Sheets‑/Daten‑Quellen lesen (je nach Setup).</li>
      </ul>
      <p class="tag">Wenn du nicht weiterkommst: Logs aus GitHub Actions oder die konkrete Fehlermeldung hier rein kopieren.</p>
    </div>
    <div class="card" id="roadmap">
      <h2>11) Roadmap & Konzept (Platzhalter)</h2>
      <ul>
        <li>Matrix Labels/Logik weiter finalisieren (Risk‑Proxy).</li>
        <li>Recommendation‑Logik bei Bedarf schärfen (Regeln bleiben transparent).</li>
        <li>Watchlist‑Hygiene: Spaltenmigration & Dedupe‑Strategie.</li>
        <li>Portfolio‑Block ergänzen.</li>
        <li>Briefing‑Logik weiter schärfen (Texte/Mapping), AI bleibt optional und ohne Einfluss auf Score.</li>
      </ul>
      <p class="tag">Diese Seite ist absichtlich nicht „fertig“ – sie ist deine Dokumentation, die mit dem Projekt mitwächst.</p>
    </div>

    <div class="card">
      <p style="margin:0"><a href="#toc">↑ zurück zum Anfang</a></p>
    </div>

  </main>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_ALL.csv")
    ap.add_argument("--contract", default=r"configs/watchlist_contract.json")
    ap.add_argument("--out", default=r"artifacts/ui/index.html")
    args = ap.parse_args()

    out = build_ui(csv_path=args.csv, out_html=args.out, contract_path=args.contract)
    print(f"✅ UI wrote: {out.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
