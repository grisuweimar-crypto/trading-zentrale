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
import os
from pathlib import Path
from typing import Any

import pandas as pd

from scanner._version import __version__, __build__
from scanner.data.io.paths import project_root
from scanner.data.schema.contract import validate_csv
from scanner.presets.load import load_presets
from scanner.data.io.paths import artifacts_dir

TRUE_SET = {"true", "t", "yes", "y", "1"}
FALSE_SET = {"false", "f", "no", "n", "0"}

_BROKEN_TEXT_MAP = {
    "\u00c3\u00bc": "ue",
    "\u00c3\u00b6": "oe",
    "\u00c3\u00a4": "ae",
    "\u00c3\u009c": "Ue",
    "\u00c3\u0096": "Oe",
    "\u00c3\u0084": "Ae",
    "\u00c3\u009f": "ss",
    "\u00e2\u20ac\u201d": " - ",
    "\u00e2\u20ac\u201c": "-",
    "\u00e2\u20ac\u2018": "-",
    "\u00e2\u20ac\u00a6": "...",
    "\u00e2\u20ac\u00a2": "*",
    "\u00e2\u20ac\u02dc": "\"",
    "\u00e2\u20ac\u2122": "'",
    "\u00e2\u2020\u2019": "->",
    "\u00e2\u2020\u2018": "^",
    "\u00e2\u2020\u201c": "v",
    "\u00e2\u0153\u2022": "x",
    "\u00c2\u00b7": " - ",
    "\u2011": "-",
    "\u2013": "-",
    "\u2014": " - ",
    "\u00d7": "x",
    "\u00d8": "O",
    "\u00f8": "o",
    "\u00d6": "Oe",
    "\u00f6": "oe",
    "\u00dc": "Ue",
    "\u00fc": "ue",
    "\u00c4": "Ae",
    "\u00e4": "ae",
    "\u00df": "ss",
    "\u25b2": "^",
    "\u25bc": "v",
}



def _clean_ui_text(text: str) -> str:
    out = text
    for bad, good in _BROKEN_TEXT_MAP.items():
        out = out.replace(bad, good)
    return out


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
    "WÃ¤hrung",
    # pricing / momentum
    "price",
    "Akt. Kurs",
    "price_eur",
    "perf_pct",
    "perf_1d_pct",
    "perf_1y_pct",
    "Perf %",
    "Perf 1D %",
    "Perf 1Y %",
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


def _coerce_bool_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.astype("boolean")
    if pd.api.types.is_numeric_dtype(s):
        out = pd.Series(pd.NA, index=s.index, dtype="boolean")
        out[s == 1] = True
        out[s == 0] = False
        return out
    ss = s.astype("string").str.strip().str.lower()
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out[ss.isin(TRUE_SET)] = True
    out[ss.isin(FALSE_SET)] = False
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

    # Failsafe: ensure bool fields are real booleans before JSON export.
    for c in ("trend_ok", "liquidity_ok", "is_crypto"):
        if c in df.columns:
            df[c] = _coerce_bool_series(df[c]).fillna(False).astype(bool)

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
    out_html.write_text(_clean_ui_text(html), encoding="utf-8")

    # Help / project description page (static)
    help_path = out_html.parent / "help.html"
    help_html = _render_help_html(version=__version__, build=__build__)
    help_path.write_text(_clean_ui_text(help_html), encoding="utf-8")

    return out_html


def _render_html(*, data_records: list[dict[str, Any]], presets: dict[str, Any], source_csv: str, version: str, build: str, briefing_text: str, briefing_source: str, fallback_tbody_html: str) -> str:
    def _json_for_script(obj: Any) -> str:
        s = json.dumps(obj, ensure_ascii=False)
        return s.replace("</script", "<\\/script").replace("</SCRIPT", "<\\/SCRIPT")

    data_json = _json_for_script(data_records)
    presets_json = _json_for_script(presets)
    briefing_json = _json_for_script({"text": briefing_text, "source": briefing_source})
    sha = (os.getenv("GITHUB_SHA") or "local").strip()
    sha_short = sha[:7] if sha and sha.lower() != "local" else "local"
    branch = (os.getenv("GITHUB_REF_NAME") or "").strip()
    branch_label = f" | branch {branch}" if branch else ""

    # Server-side preset <option> fallback (so UI isn't empty if JS fails)
    preset_labels = {
        "ALL": "Alle Werte",
        "CORE": "Ãœbersicht",
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
        txt = f"{label} ({n})" + (f" â€” {desc}" if desc else "")
        opts.append(f'<option value="{html.escape(n)}">{html.escape(txt)}</option>')
    preset_options_html = "\n".join(opts)

    # NOTE: We intentionally avoid Python f-strings for the HTML template because the
    # embedded CSS/JS contains many curly braces. We inject values via simple tokens.
    template = """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
  <meta name="build-sha" content="__SHA__"/>
  <title>Scanner_vNext â€” Research Dashboard</title>
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

    /* Mobile: Panels/Drawer/Modals dÃ¼rfen nicht Ã¼ber den Viewport schieÃŸen */
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

    /* Bucket matrix (Score Ã— Risk) */
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
@media (min-width: 980px) { .marketGrid { grid-template-columns: 1fr 1fr; align-items: stretch; } }

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

.heatMatrixPanel { margin-top: 10px; border-top: 1px solid var(--border); padding-top: 10px; }
.heatMatrixHead { display:flex; justify-content: space-between; align-items:flex-end; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }
.heatMatrixHead select { width: auto; min-width: 170px; padding: 8px 10px; border-radius: 10px; }
.heatMatrixGrid { display: grid; grid-template-columns: 84px repeat(5, 1fr); gap: 4px; }
.heatCellGrid { background: rgba(148,163,184,.06); border: 1px solid rgba(148,163,184,.15); border-radius: 9px; min-height: 28px; display:flex; align-items:center; justify-content:center; font-family: var(--mono); font-size: 11px; }
.heatCellGrid.zero { opacity: .55; }

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
<body data-build-sha="__SHA__" data-build-branch="__BRANCH__">
  <header>
    <div class="wrap">
      <div class="title">
        <h1>Scanner_vNext â€” Research Dashboard</h1>
        <div class="meta">version __VERSION__ ? build __BUILD__ ? sha __SHA____BRANCH_LABEL__ ? <a class=\"helpLink\" href=\"help.html\" target=\"_blank\" rel=\"noopener\">Hilfe / Projektbeschreibung</a></div>
      </div>
    </div>
  </header>

  <div class="wrap">
    <div class="disclaimer" id="disclaimer">
      <div class="txt"><b>Privates, experimentelles Projekt.</b> Keine Anlageberatung, keine GewÃ¤hr. Nutzung auf eigene Verantwortung.</div>
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
          <label for="search">Suche (Symbol/Name/Kategorie/Landâ€¦)</label>
          <input id="search" placeholder="z.B. NVDA, Gold, Deutschland, Kryptoâ€¦"/>
        </div>
        <div>
          <label for="clusterSel">Cluster/Sektor</label>
          <select id="clusterSel"><option value="">Alle</option></select>
        </div>
        <div>
          <label for="pillarSel">SÃ¤ule (5â€‘SÃ¤ulen/Playground)</label>
          <select id="pillarSel"><option value="">Alle</option></select>
        </div>
        <div class="count" id="count">â€”</div>
      </div>

      <div class="kpis" id="kpis">â€”</div>
      <div class="pillars" id="pillars">â€”</div>
      <div class="clusters" id="clusters">â€”</div>

      <div class="filters" id="filters">
        <button type="button" class="fbtn active" data-f="hideAvoid" title="AVOID-Zeilen ausblenden (score_status beginnt mit AVOID_)">AVOID ausblenden</button>
        <button type="button" class="fbtn" data-f="onlyOK" title="Nur score_status = OK anzeigen">Nur OK</button>
        <button type="button" class="fbtn" data-f="trendOK" title="Nur trend_ok = true anzeigen">Trend OK</button>
        <button type="button" class="fbtn" data-f="liqOK" title="Nur liquidity_ok = true anzeigen">Liq OK</button>
        <span class="fsep"></span>
        <button type="button" class="fbtn" data-f="onlyStock" title="Nur Aktien (is_crypto = false)">Aktien</button>
        <button type="button" class="fbtn" data-f="onlyCrypto" title="Nur Krypto (is_crypto = true)">Krypto</button>
        <button type="button" class="fbtn" data-f="resetSort" title="Nur Sort-Override lÃ¶schen (Preset-Sort bleibt)">Sortierung zurÃ¼ck</button>
        <button type="button" class="fbtn" data-f="reset" title="Alles zurÃ¼cksetzen (Preset, Suche, Filter, Sort & Persistenz)">Reset</button>
        <button type="button" class="hintbtn" id="infoFlow" aria-haspopup="dialog" aria-expanded="false" title="ErklÃ¤rung anzeigen"><span class="i">i</span>Preset â†’ Quick-Filter</button>
      </div>

      <div class="popover" id="infoPopover" role="dialog" aria-modal="false" aria-hidden="true">
        <button type="button" class="btn close" id="infoClose" title="SchlieÃŸen">âœ•</button>
        <div class="title">Wie wirken Preset, Suche und Quick-Filter?</div>
        <ul>
          <li><b>Preset</b> filtert und sortiert zuerst (View-Layer, verÃ¤ndert kein Scoring).</li>
          <li>Danach wirkt die <b>Suche</b> (Ticker/Name/Kategorie/Landâ€¦).</li>
          <li>Zuletzt greifen die <b>Quick-Filter</b> (z.B. Trend OK, Liq OK, Nur OK).</li>
        </ul>
        <div class="title" style="margin-top:10px;">Signalâ€‘Codes (privat)</div>
        <ul>
          <li>R0-R5 sind interne Workflow-Labels. Details in der Hilfe.</li>
        </ul>
      </div>

      <div class="matrixPanel" id="matrixPanel">
        <div class="matrixLayout">
          <div>
            <div class="matrixHead">
              <div>
                <div class="matrixTitle">Bucketâ€‘Matrix (Score Ã— Risk)</div>
                <div class="muted small">Klick auf ein Feld = Matrixâ€‘Filter (zusÃ¤tzlich zu Preset/Suche/Quickâ€‘Filter).</div>
              </div>
              <div style="display:flex; gap:8px; align-items:center;">
                <button type="button" class="btn" id="matrixClear" title="Matrixâ€‘Filter zurÃ¼cksetzen">Matrix zurÃ¼ck</button>
              </div>
            </div>
            <div class="matrixGrid" id="matrix"></div>
            <div class="matrixNote" id="matrixNote">â€”</div>
            <div class="heatMatrixPanel" id="heatMatrixPanel">
              <div class="heatMatrixHead">
                <div>
                  <div class="matrixTitle" title="Verteilung der Werte nach Kategorie und Score-Buckets.">Heatmap</div>
                  <div class="muted small">Direktvergleich je Score-Bucket im gleichen Matrix-Stil.</div>
                </div>
                <select id="heatMode" title="Heatmap-Modus">
                  <option value="pillar">Heatmap: Saeulen</option>
                  <option value="cluster">Heatmap: Cluster</option>
                </select>
              </div>
              <div id="heatmap" class="heatMatrixGrid">-</div>
              <div id="heatmapNote" class="matrixNote">-</div>
            </div>
          </div>

          <div class="briefingBox" id="briefingBox" title="Kurz-Erklaerung zu den Top-Werten aus vorhandenen Feldern, ohne Einfluss auf das Scoring.">
            <div class="briefHead">
              <div class="matrixTitle" title="Deterministische Zusammenfassung der aktuellen Top-Werte.">Briefing</div>
              <button type="button" class="btn" id="briefingToggle" title="Briefing ein-/ausblenden">Ausblenden</button>
            </div>
            <div class="muted small">Privat/experimentell Â· keine Anlageberatung Â· ohne Einfluss aufs Scoring.</div>
            <div id="briefingText" class="briefingText">â€”</div>
          </div>
        </div>
      </div>


<div class="marketPanel" id="marketPanel" title="Marktumfeld aus deinem aktuellen Universe - nur Kontext, kein Score-Einfluss.">
  <div class="marketHead">
    <div>
      <div class="matrixTitle" title="Passiver Markt-Kontext aus den aktuell sichtbaren Werten.">Market Context</div>
      <div class="muted small">Passiv aus deiner Watchlist (kein Einfluss auf Scoring) Â· Basis: gefiltertes Universe (Preset/Suche/Quick/Cluster/SÃ¤ule)</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap: wrap;">
      <button type="button" class="btn" id="marketToggle" title="Market Context ein-/ausblenden">Ausblenden</button>
    </div>
  </div>
  <div id="marketBody" class="marketGrid">
    <div class="marketCard" id="breadthCard">
      <div class="marketCardTitle" title="Marktbreite: Anteil Gewinner, Verlierer und neutrale Werte im aktuellen Universe.">Breadth</div>
      <div id="breadthBox">â€”</div>
      <div id="diversBox" class="muted small" style="margin-top:8px;">â€”</div>
      <div id="qualityBox" class="muted small" style="margin-top:8px;">â€”</div>
    </div>
    <div class="marketCard" id="moversCard">
      <div class="marketCardTitle" title="Staerkste Auf- und Abbewegungen nach Tagesbewegung (1D) im aktuellen Universe.">Movers</div>
      <div class="moversGrid">
        <div>
          <div class="muted small">Top â†‘</div>
          <div id="moversUp" class="moversList">â€”</div>
        </div>
        <div>
          <div class="muted small">Weak â†“</div>
          <div id="moversDown" class="moversList">â€”</div>
        </div>
      </div>
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
              <th data-k="ticker" title="Anzeige-Symbol (oben) + ISIN (unten) und ggf. Quote-WÃ¤hrung">Symbol/ISIN</th>
              <th data-k="name" title="Name + Kategorie/Land/WÃ¤hrung">Name</th>
              <th data-k="price" class="right" title="Aktueller Kurs (Originalwaehrung) + Tagesbewegung (1D) und Performance (1Y)">Kurs</th>
              <th data-k="score" class="right" title="Gesamtscore (hÃ¶her = besser)">Score</th>
              <th data-k="confidence" class="hide-sm right" title="Confidence/Vertrauen in das Scoring">Konf</th>
              <th data-k="cycle" class="hide-sm right" title="Zyklus in % (ca. 50 = neutral)">Zyklus</th>
              <th data-k="trend_ok" title="Trend-Filter (z.B. Trend200 > 0)">Trend</th>
              <th data-k="liquidity_ok" title="LiquiditÃ¤ts-Filter (z.B. DollarVolume/AvgVolume)">Liq</th>
              <th data-k="score_status" title="OK / AVOID / AVOID_CRYPTO_BEAR / NA / ERROR">Status</th>
              <th data-k="is_crypto" class="hide-sm" title="Assetklasse">Art</th>
            </tr>
          </thead>
          <tbody>__FALLBACK_TBODY__</tbody>
        </table>
      </div>

      <div class="footer">
        <div>Tipp: <span class="kbd">Klick</span> Header = Sortierung Â· <span class="kbd">Esc</span> = Suche leeren</div>
        <div class="mono" id="sortHint">â€”</div>
      </div>
    </div>
  </div>

  <div id="drawerOverlay" class="overlay" aria-hidden="true">
    <div class="drawer panel" role="dialog" aria-modal="true" aria-label="Why Score">
      <div class="drawer-head">
        <div>
          <div id="drawerTitle" style="font-weight:700;">â€”</div>
          <div id="drawerSub" class="muted small">â€”</div>
        </div>
        <div class="drawer-actions">
          <div id="drawerActions"></div>
          <button class="btn" id="drawerClose">SchlieÃŸen</button>
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
      show('UIâ€‘Fehler (JS): ' + m);
    });
    window.addEventListener('unhandledrejection', (ev) => {
      const r = ev && ev.reason;
      const m = (r && r.message) ? r.message : String(r);
      show('UIâ€‘Fehler (Promise): ' + m);
    });
    // If the main UI never sets jsok, show a helpful message (covers parse errors)
    setTimeout(() => {
      const ok = document.documentElement && document.documentElement.dataset && document.documentElement.dataset.jsok;
      if (!ok) show('UI konnte nicht initialisiert werden (JS lÃ¤dt nicht). Ã–ffne die Konsole (F12) fÃ¼r Details.');
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
const elHeatmapNote = document.getElementById('heatmapNote');
const elHeatMode = document.getElementById('heatMode');


    const elBriefing = document.getElementById('briefingText');
    const btnBriefingToggle = document.getElementById('briefingToggle');
    const drawerOverlay = document.getElementById('drawerOverlay');
    const drawerClose = document.getElementById('drawerClose');
    const drawerTitle = document.getElementById('drawerTitle');
    const drawerSub = document.getElementById('drawerSub');
    const drawerBody = document.getElementById('drawerBody');
    const drawerActions = document.getElementById('drawerActions');

    // Flow info popover (Preset â†’ Quick-Filter)
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
    let pillarPick = '';  // SÃ¤ulen-Filter (string; private metadata)

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

    // Bucket-matrix filter (Score Ã— Risk)
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

    // ---- info popover (Preset â†’ Quick-Filter) ----
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

    // HTML-Listen-Konvertierung fÃ¼r Briefing (bessere Mobile Darstellung)
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
          out += `<li>${String(m[1]).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c))}</li>`;
          continue;
        }

        closeUl();

        if(/^\\d+\\)\\s/.test(line)){
          out += `<h4 class="briefing-asset">${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c))}</h4>`;
        } else {
          const marker = line
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\\u0300-\\u036f]/g, '');
          if (
            marker.startsWith('grunde') ||
            marker.startsWith('risiken/flags') ||
            marker.startsWith('nachste checks') ||
            marker.startsWith('kontext-hinweise')
          ) {
            out += `<div class="briefing-label">${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c)).replace(/:$/,"")}</div>`;
          } else {
            out += `<p>${String(line).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c] || c))}</p>`;
          }
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
    html += `<button type="button" class="chip kpi warn active" data-clr="1" title="Cluster-Filter lÃ¶schen">âœ• ${esc(active)}</button>`;
  }
  for (const x of top) {
    const isOn = active && x.k === active;
    const kind = isOn ? 'warn active' : 'blue';
    html += `<button type="button" class="chip kpi ${kind}" data-cl="${esc(x.k)}" title="Filter: nur Cluster ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
  }
  if (!top.length) html += '<span class="muted">â€”</span>';
  elClusters.innerHTML = html;
}

function applyClusterFilter(rows) {
  const sel = (clusterPick || '').toString().trim();
  if (!sel) return rows;
  return (rows || []).filter(r => clusterLabel(r) === sel);
}

// ---- 5â€‘SÃ¤ulen / Playground helpers (UI-only; private metadata; never affects scoring) ----
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
  // legacy mining buckets â†’ Fundament
  if (s.includes('mining') || s.includes('mine') || s.includes('edelmetall') || s.includes('metall') || s.includes('rohstoff')) return 'Fundament';
  // ambiguous tech buckets â†’ Gehirn (default), hardware-specific keywords â†’ Hardware
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
  let html = '<span class="label">SÃ¤ulen:</span>';
  if (active) {
    html += `<button type="button" class="chip kpi warn active" data-pr="1" title="SÃ¤ulen-Filter lÃ¶schen">âœ• ${esc(active)}</button>`;
  }
  for (const x of (counts || [])) {
    const isOn = active && x.k === active;
    const kind = isOn ? 'warn active' : 'blue';
    const dis = (x.v || 0) <= 0 ? 'disabled aria-disabled="true"' : '';
    html += `<button type="button" class="chip kpi ${kind}" data-p="${esc(x.k)}" ${dis} title="Filter: nur SÃ¤ule ${esc(x.k)}">${esc(x.k)} <span class="mono">(${x.v})</span></button>`;
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
      return `${a}â€“${b}`;
    }
    function scoreBucketText(i) {
      return { range: bucketRange(i), hint: 'Score' };
    }
    function riskBucketText(i) {
      const hints = ['niedrig', 'moderat', 'mittel', 'hoch', 'sehr hoch'];
      return { range: bucketRange(i), hint: `Risk ${hints[i] || ''}`.trim() };
    }


    function fmtPrice(n) {
      if (n === null || n === undefined) return 'â€”';
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

    function perfLines(p1d, p1y) {
      const parts = [];
      if (p1d !== null && p1d !== undefined && Number.isFinite(p1d)) {
        const dir1 = (p1d > 0) ? 'pos' : (p1d < 0) ? 'neg' : 'flat';
        const s1 = (p1d >= 0 ? '+' : '') + p1d.toFixed(2) + '%';
        parts.push(`<div class="sub chg ${dir1}" title="Tagesbewegung (1D)">1D: ${s1}</div>`);
      } else {
        parts.push(`<div class="sub muted" title="Tagesbewegung (1D)">1D: n/a</div>`);
      }
      if (p1y !== null && p1y !== undefined && Number.isFinite(p1y)) {
        const dir2 = (p1y > 0) ? 'pos' : (p1y < 0) ? 'neg' : 'flat';
        const s2 = (p1y >= 0 ? '+' : '') + p1y.toFixed(2) + '%';
        parts.push(`<div class="sub chg ${dir2}" title="Performance (1Y)">1Y: ${s2}</div>`);
      } else {
        parts.push(`<div class="sub muted" title="Performance (1Y)">1Y: n/a</div>`);
      }
      return parts.join('');
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
        return pair || pickDisplayTicker(r) || 'â€”';
      }
      const s = normStr(r.symbol) || normStr(r.Symbol);
      if (s && !looksLikeISIN(s)) return s;
      const td = normStr(r.ticker_display);
      if (td && !looksLikeISIN(td)) return td;
      const yh = pickYahooSymbol(r);
      if (yh && !looksLikeISIN(yh)) return yh;
      const t = normStr(r.ticker);
      if (t && !looksLikeISIN(t)) return t;
      return 'â€”';
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
      const sig = rec ? `<span class="sig ${rec.cls}" title="Signalâ€‘Code">${esc(rec.code)}</span>` : '';
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
      const tokens = q.split(/\\s+/).filter(Boolean);
      return rows.filter(r => {
        const hay = [r.ticker, r.ticker_display, r.yahoo_symbol, r.YahooSymbol, r.symbol, r.isin, r.name, r.sector, r.Sector, r.category, r.Sektor, r.Kategorie, r.Industry, r.industry, r.country, r.currency, r["WÃ¤hrung"], r.quote_currency, r.score_status]
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
        + kpiChip(`Sichtbar ${v.total}/${a.total}`, 'blue', 'Sichtbar nach Preset â†’ Suche â†’ Quick-Filter / Gesamt', '', false)
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
      parts.push(`<div class="matrixAxis" title="Achsen: Risk (y) Ã— Score (x)"><div class="lbl">Risk â†“</div><div class="hint">Score â†’</div></div>`);
      for (let sb = 0; sb < 5; sb++) {
        const s = scoreBucketText(sb);
        parts.push(`<div class="matrixLabel" title="Scoreâ€‘Bucket"><div class="lbl">${esc(s.range)}</div><div class="hint">${esc(s.hint)}</div></div>`);
      }

      for (let rb = 0; rb < 5; rb++) {
        const rtxt = riskBucketText(rb);
        parts.push(`<div class="matrixLabel" title="Riskâ€‘Bucket (Perzentil; hÃ¶her = riskanter)"><div class="lbl">${esc(rtxt.range)}</div><div class="hint">${esc(rtxt.hint)}</div></div>`);
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

          parts.push(`<div class="cell ${active} ${zero}" ${st} data-sb="${sb}" data-rb="${rb}" title="Score ${esc(s.range)} Â· Risk ${esc(rr.hint)}">${c ? `<span class="cnt">${c}</span>` : `<span class="cnt">Â·</span>`}</div>`);
        }
      }

      elMatrix.innerHTML = parts.join('');

      // Click â†’ toggle matrix filter
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
        const metric = (RISK_SORTED && RISK_SORTED.length) ? 'Riskâ€‘Proxy aus volatility/downside_dev/max_drawdown (Perzentil)' : 'Riskâ€‘Proxy fehlt (keine Riskâ€‘Spalten im CSV)';
        const sel = (sb !== null && rb !== null) ? ` Â· aktiv: Score ${bucketRange(sb)} Ã— ${riskBucketText(rb).hint}` : '';
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

function perf1dPct(r) {
  return parsePct(
    r.perf_1d_pct ?? r['Perf 1D %'] ?? r.perf_1d ?? r.perf1d ?? r['Change %'] ?? r.change_pct ?? r.changePercent
  );
}

function perf1yPct(r) {
  return parsePct(
    r.perf_1y_pct ?? r['Perf 1Y %'] ?? r.perf_1y ?? r.perf1y ?? r.perf_pct ?? r['Perf %']
  );
}

function fmtRatioPct(v) {
  const n = asNum(v);
  if (n === null) return 'â€”';
  return `${(n * 100).toFixed(2)}%`;
}

function fmtPct(v) {
  if (v === null || v === undefined || !Number.isFinite(v)) return 'â€”';
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
    const p = perf1dPct(r);
    if (p === null) { miss++; continue; }
    if (p > 0) adv++;
    else if (p < 0) dec++;
    else flat++;
  }
  const tot = adv + dec + flat;
  if (!tot) {
    elBreadthBox.innerHTML = `<div class="muted">Keine verwertbare Tagesbewegung (1D) im aktuellen Universe.</div>`;
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
    <div class="muted small">Basis: <span class="mono">perf_1d_pct / Perf 1D %</span> (Fallback: perf_pct / Perf %; nur Anzeige/Context).</div>
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
    ? `Top Crowding: ${topSec.map(([k, v]) => `${esc(k)} (${v})`).join(' Â· ')}`
    : 'Top Crowding: â€”';

  elDiversBox.innerHTML = `
    <div class="breadthRow">
      ${chip(`Div Ã˜ ${avg.toFixed(2)}`, avg >= 4 ? 'warn' : 'blue')}
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
    elQualityBox.innerHTML = `<div class="muted">Preset-QualitÃ¤t: keine Daten.</div>`;
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
      ${chip(`Score Med ${scoreMed === null ? 'â€”' : scoreMed.toFixed(1)}`, (scoreMed !== null && scoreMed >= 34) ? 'good' : 'blue')}
      ${chip(`Score IQR ${scoreIqr === null ? 'â€”' : scoreIqr.toFixed(1)}`, (scoreIqr !== null && scoreIqr >= 10) ? 'good' : 'warn')}
      ${chip(`Conf Med ${confMed === null ? 'â€”' : confMed.toFixed(1)}`, (confMed !== null && confMed >= 60) ? 'good' : 'warn')}
      ${chip(`Conf IQR ${confIqr === null ? 'â€”' : confIqr.toFixed(1)}`, (confIqr !== null && confIqr >= 12) ? 'blue' : 'warn')}
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
    const p = perf1dPct(r);
    if (p === null) continue;
    arr.push({r, p});
  }
  if (!arr.length) {
    elMoversUp.innerHTML = `<span class="muted">â€”</span>`;
    elMoversDown.innerHTML = `<span class="muted">â€”</span>`;
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

  elMoversUp.innerHTML = up.length ? up.map(itemHtml).join('') : `<span class="muted">â€”</span>`;
  elMoversDown.innerHTML = dn.length ? dn.map(itemHtml).join('') : `<span class="muted">â€”</span>`;
}

function renderHeatmap(rows) {
  if (!elHeatmap) return;
  const fn = (heatMode === 'cluster') ? clusterLabel : pillarLabel;
  const kindLabel = (heatMode === 'cluster') ? 'Cluster' : 'Saeule';

  const m = new Map();
  for (const r of rows || []) {
    const cat = (fn(r) || '').toString().trim();
    if (!cat) continue;
    const sb = scoreBucket(r.score);
    if (!m.has(cat)) m.set(cat, [0,0,0,0,0]);
    m.get(cat)[sb] += 1;
  }

  if (!m.size) {
    elHeatmap.innerHTML = `<div class="muted">Keine Daten fuer Heatmap (keine Kategorie im aktuellen Universe).</div>`;
    if (elHeatmapNote) elHeatmapNote.textContent = '-';
    return;
  }

  const all = Array.from(m.entries()).map(([k, arr]) => ({k, arr, tot: arr.reduce((a,b)=>a+b,0)}));
  all.sort((a,b) => b.tot - a.tot || a.k.localeCompare(b.k));
  const limit = (heatMode === 'cluster') ? 8 : 6;
  const top = all.slice(0, limit);

  let vmax = 0;
  for (const x of top) for (const v of x.arr) vmax = Math.max(vmax, v);

  const hdr = Array.from({length:5}, (_,sb) => scoreBucketText(sb).range);
  const parts = [];
  parts.push(`<div class="matrixAxis" title="Achsen: Kategorie (y) x Score (x)"><div class="lbl">${kindLabel} v</div><div class="hint">Score -></div></div>`);
  for (let sb = 0; sb < 5; sb++) {
    const s = scoreBucketText(sb);
    parts.push(`<div class="matrixLabel" title="Score-Bucket"><div class="lbl">${esc(s.range)}</div><div class="hint">${esc(s.hint)}</div></div>`);
  }

  for (const x of top) {
    parts.push(`<div class="matrixLabel" title="${kindLabel}"><div class="lbl">${esc(x.k)}</div><div class="hint">N ${x.tot}</div></div>`);
    for (let sb = 0; sb < 5; sb++) {
      const v = x.arr[sb] || 0;
      const rel = vmax ? (v / vmax) : 0;
      const alpha = 0.08 + rel * 0.30;
      const bg = `background: hsla(205, 72%, 50%, ${alpha});`;
      const zero = v === 0 ? ' zero' : '';
      parts.push(`<div class="heatCellGrid${zero}" style="${bg}" title="${esc(x.k)} | Score ${esc(hdr[sb])} = ${v}">${v ? v : '-'}</div>`);
    }
  }

  elHeatmap.innerHTML = parts.join('');
  if (elHeatmapNote) {
    elHeatmapNote.textContent = `Zahl = Anzahl Werte pro Score-Bucket (Top ${limit} nach Haeufigkeit).`;
  }
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

        const curr = normStr(r.quote_currency) || normStr(r.currency) || normStr(r["WÃ¤hrung"]);
        const currChip = curr ? `<span class="tinychip" title="WÃ¤hrung">${esc(curr)}</span>` : '';

        const main = href ? `<a class="yf" href="${href}" target="_blank" rel="noopener">${esc(disp)}</a>` : esc(disp);
        // subline for the left "Symbol/ISIN" cell: for crypto show the Yahoo pair (e.g. BTC-USD),
        // for stocks show ISIN. Use a distinct variable name so we don't collide with other "sub" vars.
        const subTicker = isC ? (yh || 'â€”') : (isin || 'â€”');
        const subLabel = isC ? 'YahooSymbol' : 'ISIN';
        const subLine = `<div class="sub mono" title="${subLabel}">${esc(subTicker)}</div>`;
        const tCell = `<div class="tickerCell"><div class="tickerMain">${main}${currChip}</div>${subLine}</div>`;

        const n = normStr(r.name);

        // Official taxonomy (prefer industry, fallback sector). Manual fantasy sectors are not shown here.
        const sectorOfficial = normStr(r.sector) || normStr(r.Sector);
        const industryOfficial = normStr(r.industry) || normStr(r.Industry) || normStr(r.cluster_official);

        // Private pillars (5-sÃ¤ulen + playground) are metadata only (never affect scoring)
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
        if (pillar) subParts.push(`<span class="muted" title="SÃ¤ule (privat, Metadaten)">SÃ¤ule: ${esc(pillar)}</span>`);
        if (bucketType && bucketType !== 'pillar' && bucketType !== 'none') subParts.push(`<span class="muted" title="Bucket-Type (privat)">(${esc(bucketType)})</span>`);
        if (ctry) subParts.push(esc(ctry));
        const subName = subParts.join(' Â· ');

        const price = asNum(r.price) ?? asNum(r["Akt. Kurs"]);
        const perf1d = perf1dPct(r);
        const perf1y = perf1yPct(r);
        const priceMain = (price === null) ? 'â€”' : `${fmtPrice(price)}${curr ? ' ' + esc(curr) : ''}`;
        const pCell = `<div class="priceCell"><div class="priceMain">${priceMain}</div>${perfLines(perf1d, perf1y)}</div>`;

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
          <td>${chip(status || 'â€”', statusKind)}</td>
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
      drawerTitle.textContent = `${t} â€” ${n}`.trim();
      const sectorOfficial = normStr(r.sector) || normStr(r.Sector);
      const categoryManual = normStr(r.category) || normStr(r.Sektor) || normStr(r.Kategorie);
      const cat = asBool(r.is_crypto) ? 'Krypto' : (categoryManual ? `Cluster: ${categoryManual}` : (sectorOfficial || ''));
      const curr = normStr(r.quote_currency) || normStr(r.currency) || normStr(r["WÃ¤hrung"]);
      const sub = [cat, normStr(r.country), curr, normStr(r.isin)].filter(Boolean).join(' Â· ');
      drawerSub.textContent = sub || '';

      // Quick action: open on Yahoo Finance if we can determine a valid symbol
      if (drawerActions) {
        const sym = pickYahooSymbol(r);
        const href = yahooHref(sym);
        drawerActions.innerHTML = href ? `<a class="btn" href="${href}" target="_blank" rel="noopener" title="Auf Yahoo Finance Ã¶ffnen">Yahoo</a>` : '';
      }

      const items = [
        ['Score', (asNum(r.score) ?? 0).toFixed(2)],
        ['Confidence', (asNum(r.confidence) ?? 0).toFixed(1)],
        ['Cycle', `${(asNum(r.cycle) ?? 0).toFixed(0)}%`],
        ['ScoreStatus', normStr(r.score_status) || 'â€”'],
        ['Trend OK', String(asBool(r.trend_ok))],
        ['Liquidity OK', String(asBool(r.liquidity_ok))],
        ['AssetClass', asBool(r.is_crypto) ? 'Krypto' : 'Aktie'],
      ];

      // optional interesting fields
      const opt = [
        ['Price', r.price],
        ['Currency', curr],
        ['Tagesbewegung (1D)', fmtPct(perf1dPct(r))],
        ['Performance (1Y)', fmtPct(perf1yPct(r))],
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
      if (status === 'AVOID_CRYPTO_BEAR') why.push('Crypto im Bear-Trend â†’ Score=0 (bewusstes Avoid).');
      if (status === 'AVOID') why.push('Score==0 â†’ Avoid (non-crypto).');
      if (status === 'NA') why.push('Zu wenig / nicht konsistente Daten â†’ NA.');
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

      // pillar counts (5-sÃ¤ulen + playground) reflect the same universe
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

      elCount.textContent = `${rows.length} / ${base.length}` + (f.length ? `  Â·  filters: ${f.join(',')}` : '');
      if (btnMatrixClear) btnMatrixClear.disabled = !(matrix && matrix.sb !== null && matrix.rb !== null);

      const override = userSort ? ` | override: ${userSort.k}:${userSort.dir}` : '';
      elSortHint.textContent = `preset: ${presetLabel(activePreset)}${override}`;
    }

    // ---- init ----
    const PRESET_LABELS = {
      CORE: 'Ãœbersicht',
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
        opt.textContent = `${presetLabel(n)} (${n})` + (desc ? ` â€” ${desc}` : '');
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
      elBriefing.innerHTML = briefingToHtml(t) || 'â€” (noch kein Briefing generiert)';
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
      if (window.__showJsError) window.__showJsError('UIâ€‘Fehler (JS): ' + msg);
      if (el) el.textContent = `JS error: ${msg}`;
      const k = document.getElementById('kpis');
      if (k) k.textContent = 'JS error â€“ siehe Konsole';
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
        .replace("__SHA__", sha_short)
        .replace("__BRANCH__", branch)
        .replace("__BRANCH_LABEL__", branch_label)
        .replace("__SOURCE_CSV__", str(source_csv))
    )


def _render_help_html(*, version: str, build: str) -> str:
    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
  <title>Scanner_vNext - Hilfe</title>
  <style>
    :root{{--bg:#0b1020;--card:#0f172a;--border:rgba(148,163,184,.18);--muted:#94a3b8;--text:#e2e8f0;--accent:#60a5fa;}}
    *{{box-sizing:border-box}}
    body{{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}}
    .wrap{{max-width:980px;margin:0 auto;padding:16px}}
    .card{{background:rgba(15,23,42,.84);border:1px solid var(--border);border-radius:14px;padding:14px;margin:12px 0}}
    .meta{{color:var(--muted);font-size:12px}}
    a{{color:var(--accent);text-decoration:none}}
    details{{border:1px solid var(--border);border-radius:10px;padding:10px;margin:10px 0;background:rgba(2,6,23,.4)}}
    summary{{cursor:pointer;font-weight:700}}
    code{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}}
    ul{{margin:8px 0 8px 18px}}
    p{{line-height:1.5}}
  </style>
</head>
<body>
  <main class="wrap">
    <div class="card">
      <h1 style="margin:0 0 6px 0">Scanner_vNext - Hilfe</h1>
      <div class="meta">version {version} - build {build} - <a href="index.html">zurueck zum Dashboard</a></div>
      <p><b>Was ist das?</b> Scanner_vNext ist ein Research-Dashboard fuer Watchlist-Analyse. Die Anzeige kombiniert Rangfolge (Score), Datenqualitaet (Confidence), Filterstatus und Marktkontext in einer kompakten Ansicht.</p>
      <p><b>Wichtig:</b> Die Inhalte sind Analyse- und Priorisierungsinformationen. Die Darstellung ist kein Handelsaufruf und keine Anlageberatung.</p>
    </div>

    <div class="card">
      <h2 style="margin:0 0 8px 0">60 Sekunden: So nutzt du das Dashboard</h2>
      <ol>
        <li>Preset waehlen (ALL, CORE, TOP...), danach Suche und Quick-Filter setzen.</li>
        <li>Score, Confidence, Trend und Liq gemeinsam lesen.</li>
        <li>Bucket-Matrix fuer Score/Risk-Fokus verwenden.</li>
        <li>Details im Drawer pruefen und mit Market Context einordnen.</li>
      </ol>
    </div>

    <details open>
      <summary>Wie lese ich die Hauptspalten?</summary>
      <p>Die Hauptspalten bilden drei Ebenen ab: Rangfolge, Qualitaet und Filterstatus.</p>
      <ul>
        <li><b>Score</b>: relative Rangfolge im aktuell geladenen Universe. Ein hoher Wert bedeutet eine starke Position innerhalb der aktuellen Vergleichsmenge.</li>
        <li><b>Confidence</b>: Stabilitaet der Daten- und Signalbasis. Hohe Werte stehen fuer konsistentere Rahmenbedingungen.</li>
        <li><b>Trend / Liq</b>: einfache Plausibilitaetsfilter. Trend prueft Trendlage, Liq prueft Handelbarkeit/Volumennahe.</li>
        <li><b>Status</b>: kompakter Arbeitsstatus. <code>OK</code> = normal, <code>AVOID</code> = meiden, <code>NA</code> = unvollstaendig.</li>
      </ul>
      <p>Einzelwerte sollten nicht isoliert interpretiert werden. Aussagekraft entsteht aus der Kombination der Spalten.</p>
    </details>

    <details open>
      <summary>Was bedeuten R0-R5?</summary>
      <p>R0-R5 sind neutrale Workflow-Codes fuer die interne Priorisierung. Sie sind keine Kauf-/Verkaufssignale.</p>
      <ul>
        <li><b>R5/R4</b>: hohe Prioritaet fuer vertiefte Analyse.</li>
        <li><b>R3</b>: neutraler Beobachtungsstatus.</li>
        <li><b>R2/R1</b>: niedrige Prioritaet.</li>
        <li><b>R0</b>: Ausschluss-/Vermeidungsstatus (z.B. AVOID).</li>
      </ul>
      <p>Der Code verdichtet mehrere Informationen zu einem schnellen Arbeitslabel. Er ersetzt keine Detailpruefung.</p>
    </details>

    <details open>
      <summary>Market Context: Wozu ist das gut?</summary>
      <p>Der Market-Context-Block ergaenzt die Einzeltitelansicht um den Zustand des aktuell gefilterten Gesamtuniversums.</p>
      <ul>
        <li><b>Breadth</b>: Verteilung von Gewinnern, Verlierern und neutralen Werten. Zeigt, ob das Umfeld breit getragen oder eng ist.</li>
        <li><b>Movers</b>: staerkste positive und negative Tagesbewegungen im aktiven Filter.</li>
        <li><b>Heatmap</b>: Strukturansicht nach Cluster/Saeule und Score-Bucket. Macht Konzentrationen und Luecken sichtbar.</li>
      </ul>
      <p>Dieser Block liefert Kontext und Orientierung. Er veraendert den Score nicht.</p>
    </details>

    <details>
      <summary>Kurze Projektbeschreibung</summary>
      <p>Scanner_vNext verbindet Datenaufbereitung, Scoring und UI-Exploration in einem taeglichen Analyseablauf. Ziel ist eine reproduzierbare und transparente Entscheidungsgrundlage: relevante Werte schneller erkennen, Risiken frueh markieren und Interpretationen nachvollziehbar dokumentieren.</p>
    </details>
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
    print(f"[OK] UI wrote: {out.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

