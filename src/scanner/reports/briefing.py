from __future__ import annotations

"""Deterministic (rule-based) watchlist briefing + optional AI enhancement.

Design goals
------------
- Offline-first: deterministic briefing (Stage A) works without any network.
- Explainability only: reads existing CSV outputs; never recalculates scores.
- Robust: tolerant to missing columns via fallback mappings.
- Testable: stable output JSON + light validation.
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from scanner.data.io.paths import project_root


BRIEFING_SCHEMA_VERSION = 2


# ---------------------------
# Config loading (YAML-lite)
# ---------------------------


def _try_import_pyyaml():
    try:
        import yaml  # type: ignore

        return yaml
    except Exception:
        return None


def load_yaml_lite(path: Path) -> dict[str, Any]:
    """Load a *very small* YAML subset.

    Supports:
      key: value
    with optional quotes, booleans and numbers. Comments (# ...) are ignored.

    If PyYAML is available, we use it. Otherwise we use a minimal parser.
    """

    if not path.exists():
        return {}

    yaml = _try_import_pyyaml()
    if yaml is not None:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}

    out: dict[str, Any] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # strip inline comments (simple)
        if "#" in line:
            left, *_ = line.split("#", 1)
            line = left.strip()
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        if v.startswith(("'", '"')) and v.endswith(("'", '"')) and len(v) >= 2:
            v = v[1:-1]

        lv = v.lower()
        if lv in ("true", "false"):
            out[k] = lv == "true"
            continue
        if lv in ("null", "none", ""):
            out[k] = None
            continue
        # number?
        try:
            if re.fullmatch(r"-?\d+", v):
                out[k] = int(v)
                continue
            if re.fullmatch(r"-?\d+\.\d+", v):
                out[k] = float(v)
                continue
        except Exception:
            pass

        out[k] = v

    return out


@dataclass(frozen=True)
class BriefingConfig:
    source_csv: str = "CORE"  # ALL|CORE|FULL or filename
    top_n: int = 3
    language: str = "de"
    enable_ai: bool = False
    ai_provider: str = "openai"
    ai_model: str = "gpt-4.1"  # override via config or OPENAI_MODEL
    output_dir: str = "artifacts/reports"


def load_briefing_config(config_path: str | Path | None = None) -> BriefingConfig:
    root = project_root()
    cfg_path = Path(config_path) if config_path is not None else (root / "configs" / "briefing.yaml")
    d = load_yaml_lite(cfg_path)

    def _get(name: str, default: Any):
        return d.get(name, default)

    top_n = _get("top_n", 3)
    try:
        top_n = int(top_n)
    except Exception:
        top_n = 3
    top_n = max(1, min(50, top_n))

    enable_ai = bool(_get("enable_ai", False))
    # allow env override to simplify local runs
    if os.getenv("SCANNER_BRIEFING_ENABLE_AI"):
        enable_ai = os.getenv("SCANNER_BRIEFING_ENABLE_AI", "").strip().lower() in ("1", "true", "yes", "on")

    ai_model = str(_get("ai_model", "gpt-4.1")).strip() or "gpt-4.1"
    ai_model = os.getenv("OPENAI_MODEL", ai_model)

    def _text_or_default(v: Any, default: str) -> str:
        if v is None:
            return default
        if isinstance(v, str):
            t = v.strip()
            if not t:
                return default
            if t.lower() in {"nan", "none", "<na>", "na"}:
                return default
            return t
        try:
            if pd.isna(v):
                return default
        except Exception:
            pass
        t = str(v).strip()
        return t or default

    return BriefingConfig(
        source_csv=_text_or_default(_get("source_csv", "CORE"), "CORE"),
        top_n=top_n,
        language=_text_or_default(_get("language", "de"), "de"),
        enable_ai=enable_ai,
        ai_provider=_text_or_default(_get("ai_provider", "openai"), "openai"),
        ai_model=ai_model,
        output_dir=_text_or_default(_get("output_dir", "artifacts/reports"), "artifacts/reports"),
    )


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        t = v.strip().lower()
        return t in {"", "nan", "none", "<na>", "na"}
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _to_float_or_none(v: Any) -> float | None:
    if _is_blank(v):
        return None
    try:
        x = float(v)
    except Exception:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    return x


def resolve_source_csv(source: Any) -> Path:
    """Resolve configured CSV selector to an existing CSV path."""

    root = project_root()
    watch_dir = root / "artifacts" / "watchlist"
    s = "" if _is_blank(source) else str(source).strip()
    upper = s.upper()

    mapping = {
        "ALL": "watchlist_ALL.csv",
        "CORE": "watchlist_CORE.csv",
        "FULL": "watchlist_full.csv",
        "WATCHLIST_ALL": "watchlist_ALL.csv",
        "WATCHLIST_CORE": "watchlist_CORE.csv",
        "WATCHLIST_FULL": "watchlist_full.csv",
    }
    if upper in mapping:
        return watch_dir / mapping[upper]

    p = Path(s)
    if p.suffix.lower() != ".csv":
        p = Path(s + ".csv")
    if not p.is_absolute() and p.parts and p.parts[0] != "artifacts":
        # most likely a watchlist filename
        p = watch_dir / p.name
    if not p.is_absolute():
        p = root / p
    return p


# ---------------------------
# Field access + derivations
# ---------------------------


def _first_col(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _num_series(df: pd.DataFrame, col: str | None) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _bool_series(df: pd.DataFrame, col: str | None) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    s = df[col]
    # already bool?
    if s.dtype == bool:
        return s
    # numeric
    if pd.api.types.is_numeric_dtype(s):
        return s.fillna(0).astype(float).ne(0)
    # strings
    t = s.fillna("").astype(str).str.strip().str.lower()
    return t.isin(["1", "true", "yes", "y", "ok", "t"])


def _norm_str(v: Any) -> str:
    if _is_blank(v):
        return ""
    return str(v).strip()


def _percentile_rank(sorted_vals: list[float], v: float | None) -> float | None:
    if v is None:
        return None
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    if n == 1:
        return 100.0
    import bisect

    # mimic JS: upper bound index of <= v
    idx = bisect.bisect_right(sorted_vals, v) - 1
    idx = max(0, min(n - 1, idx))
    return (idx / (n - 1)) * 100.0


def _bucket_0_4(x: float | None) -> int | None:
    if x is None:
        return None
    p = max(0.0, min(100.0, float(x)))
    return min(4, int(p // 20.0))


def _score_bucket(score: float | None) -> int:
    if score is None:
        return 0
    s = max(0.0, min(100.0, float(score)))
    return min(4, int(s // 20.0))


def _risk_raw(df: pd.DataFrame) -> pd.Series:
    vol = _num_series(df, _first_col(df, ["volatility", "Volatility"]))
    dsd = _num_series(df, _first_col(df, ["downside_dev", "DownsideDev"]))
    dd = _num_series(df, _first_col(df, ["max_drawdown", "MaxDrawdown"]))

    out = vol.abs()
    out = out.where(~out.isna(), dsd.abs())
    out = out.where(~out.isna(), dd.abs())
    return out


def _is_crypto_row(row: pd.Series) -> bool:
    for col in ("is_crypto", "IsCrypto"):
        if col in row.index:
            try:
                return bool(row[col])
            except Exception:
                pass
    for col in ("asset_class", "ScoreAssetClass"):
        if col in row.index:
            if _norm_str(row[col]).lower() == "crypto":
                return True
    ys = _norm_str(row.get("yahoo_symbol")) or _norm_str(row.get("YahooSymbol"))
    tk = _norm_str(row.get("ticker")) or _norm_str(row.get("Ticker"))
    for s in (ys, tk):
        u = s.upper()
        if u.endswith("-USD"):
            return True
        if "-" in u:
            base, quote = u.rsplit("-", 1)
            if quote in {"USD", "EUR", "USDT", "USDC", "GBP", "CHF", "BTC", "ETH"} and len(base) >= 2:
                return True
    return False


def _rec_code(score_status: str, score_pctl: float | None, trend_ok: bool | None, liq_ok: bool | None) -> str:
    st = (score_status or "").strip().upper()
    if st in ("NA", "ERROR"):
        return "R?"
    if st.startswith("AVOID"):
        return "R0"
    p = score_pctl
    tr = bool(trend_ok) is True
    liq = bool(liq_ok) is True

    if p is not None and p >= 90 and tr and liq:
        return "R5"
    if p is not None and p >= 75 and liq:
        return "R4"
    if p is not None and p >= 45:
        return "R3"
    if p is not None and p >= 20:
        return "R2"
    return "R1"


def _pick_identity(row: pd.Series) -> dict[str, str]:
    """Best-effort identity fields (symbol/isin/yahoo/name)."""

    name = _norm_str(row.get("name")) or _norm_str(row.get("Name"))
    symbol = (
        _norm_str(row.get("ticker_display"))
        or _norm_str(row.get("symbol"))
        or _norm_str(row.get("Symbol"))
        or _norm_str(row.get("ticker"))
        or _norm_str(row.get("Ticker"))
    )
    isin = _norm_str(row.get("isin")) or _norm_str(row.get("ISIN"))
    yahoo = _norm_str(row.get("yahoo_symbol")) or _norm_str(row.get("YahooSymbol"))

    return {
        "name": name,
        "symbol": symbol,
        "isin": isin,
        "yahoo": yahoo,
    }


# ---------------------------
# Briefing generation
# ---------------------------


def build_briefing_from_csv(csv_path: Path, *, top_n: int = 3, language: str = "de") -> dict[str, Any]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"CSV is empty: {csv_path}")

    c_score = _first_col(df, ["score", "Score"])
    c_status = _first_col(df, ["score_status", "ScoreStatus", "Status"])
    c_trend = _first_col(df, ["trend_ok", "TrendOK", "Trend Ok", "Trend"])
    c_liq = _first_col(df, ["liquidity_ok", "LiquidityOK", "LiqOK", "Liq"])
    c_cycle = _first_col(df, ["cycle", "Zyklus %", "Zyklus", "cycle_pct"])
    c_perf_1d = _first_col(df, ["perf_1d_pct", "Perf 1D %", "perf_1d", "perf1d"])
    c_perf_1y = _first_col(df, ["perf_1y_pct", "Perf 1Y %", "perf_1y", "perf1y", "perf_pct", "Perf %", "Perf%", "perf"])
    c_conf = _first_col(df, ["confidence", "Confidence", "ConfidenceScore"])
    c_div = _first_col(df, ["diversification_penalty", "ScoreDiversificationPenalty"])
    c_cluster = _first_col(df, ["cluster_official", "Cluster", "Sektor", "sector", "Sector"])
    c_pillar = _first_col(df, ["pillar_primary", "Saeule", "S??ule", "Pillar"])
    c_symbol = _first_col(df, ["ticker_display", "symbol", "Symbol", "ticker", "Ticker"])

    score_vals = _num_series(df, c_score)
    score_sorted = sorted(float(x) for x in score_vals.dropna().tolist())
    risk_raw = _risk_raw(df)
    risk_sorted = sorted(float(x) for x in risk_raw.dropna().tolist())

    df = df.copy()
    df["__score__"] = score_vals
    df["__score_pctl__"] = [
        _percentile_rank(score_sorted, None if pd.isna(v) else float(v)) for v in score_vals.tolist()
    ]
    df["__risk_raw__"] = risk_raw
    df["__risk_pctl__"] = [
        _percentile_rank(risk_sorted, None if pd.isna(v) else float(v)) for v in risk_raw.tolist()
    ]

    status_s = df[c_status].fillna("").astype(str) if c_status and c_status in df.columns else pd.Series([""] * len(df))
    ok_mask = status_s.str.upper().eq("OK")
    cand = df.loc[ok_mask].copy() if ok_mask.any() else df.copy()
    if c_status and c_status in cand.columns:
        st = cand[c_status].fillna("").astype(str).str.upper()
        m = ~st.isin(["NA", "ERROR"])
        if m.any():
            cand = cand.loc[m].copy()

    cand["__conf__"] = _num_series(cand, c_conf)
    cand["__symbol__"] = (
        cand[c_symbol].fillna("").astype(str).str.strip().str.upper() if c_symbol and c_symbol in cand.columns else ""
    )
    cand = cand.sort_values(by=["__score__", "__conf__", "__symbol__"], ascending=[False, False, True], kind="mergesort")
    top_n = max(1, min(50, int(top_n)))
    top_df = cand.head(top_n)

    def _risk_bucket_1_4(risk_pctl: float | None) -> int:
        b0 = _bucket_0_4(risk_pctl)
        if b0 is None:
            return 2
        return min(4, max(1, int(b0) + 1))

    def _risk_label(bucket: int) -> str:
        return {1: "niedrig", 2: "moderat", 3: "hoch", 4: "sehr hoch"}.get(bucket, "moderat")

    def _market_regime_hint(row: pd.Series) -> str:
        if _is_crypto_row(row):
            return _norm_str(row.get("regime_crypto")) or _norm_str(row.get("MarketRegimeCrypto"))
        return _norm_str(row.get("regime_stock")) or _norm_str(row.get("MarketRegimeStock"))

    def _trend200_hint(row: pd.Series) -> float | None:
        if _is_crypto_row(row):
            return _to_float_or_none(row.get("trend200_crypto") if "trend200_crypto" in row.index else row.get("Trend200Crypto"))
        return _to_float_or_none(row.get("trend200_stock") if "trend200_stock" in row.index else row.get("Trend200Stock"))

    def _bool_or_none(col: str | None, row: pd.Series) -> bool | None:
        if not col or col not in row.index:
            return None
        try:
            return bool(row.get(col))
        except Exception:
            return None

    perf_window_1d = "unknown"
    if c_perf_1d:
        n = c_perf_1d.strip().lower()
        if n in {"perf_1d_pct", "perf 1d %", "perf_1d", "perf1d"}:
            perf_window_1d = "1d_close_to_close"
        else:
            perf_window_1d = "snapshot_to_snapshot"

    perf_window_1y = "unknown"
    if c_perf_1y:
        n = c_perf_1y.strip().lower()
        if n in {"perf_1y_pct", "perf 1y %", "perf_1y", "perf1y"}:
            perf_window_1y = "1y_total_return"
        else:
            perf_window_1y = "snapshot_to_snapshot"

    def _anomaly_for_perf_1d(perf_f: float | None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if perf_f is None:
            return out
        ap = abs(perf_f)
        if ap >= 40:
            sev = "alert"
            lbl = "Sehr auffaellige Tagesbewegung (1D)"
            exp = f"Sehr hoher 1D-Wert. Pruefe Datenbasis/Zeitraum ({perf_window_1d}) sowie Corporate Actions."
        elif ap >= 20:
            sev = "warn"
            lbl = "Ungewoehnliche Tagesbewegung (1D)"
            exp = f"Erhoehter 1D-Wert. Pruefe Definition/Zeitraum ({perf_window_1d}) und Datenkonsistenz."
        else:
            return out
        out.append(
            {
                "id": "anomaly_extreme_perf",
                "severity": sev,
                "label": lbl,
                "value": f"{perf_f:+.2f}%",
                "explain": exp,
                "suggested_check": "Vergleiche Close heute vs. vorherigen Referenzwert und pruefe Split/Corporate Actions.",
            }
        )
        return out

    notes: list[dict[str, str]] = []
    if c_pillar and c_pillar in top_df.columns:
        vc = top_df[c_pillar].fillna("").astype(str).str.strip()
        vc = vc[vc.ne("")]
        if not vc.empty:
            k = vc.value_counts().idxmax()
            v = int(vc.value_counts().max())
            if v >= max(2, (len(top_df) + 1) // 2):
                notes.append({"id": "note_column_concentration", "text": f"Top-{len(top_df)} enthalten {v}/{len(top_df)} Werte in Saeule '{k}'."})

    items: list[dict[str, Any]] = []
    extreme_perf_count = 0
    for rank, (_, r) in enumerate(top_df.iterrows(), start=1):
        ident = _pick_identity(r)
        score = _to_float_or_none(r.get("__score__"))
        score_pctl = _to_float_or_none(r.get("__score_pctl__"))
        risk_pctl = _to_float_or_none(r.get("__risk_pctl__"))
        rb = _risk_bucket_1_4(risk_pctl)
        trend_ok = _bool_or_none(c_trend, r)
        liq_ok = _bool_or_none(c_liq, r)
        score_status = _norm_str(r.get(c_status)) if c_status and c_status in r.index else ""
        conf_f = _to_float_or_none(None if c_conf is None else r.get(c_conf))
        cycle_f = _to_float_or_none(None if c_cycle is None else r.get(c_cycle))
        perf_1d_f = _to_float_or_none(None if c_perf_1d is None else r.get(c_perf_1d))
        perf_1y_f = _to_float_or_none(None if c_perf_1y is None else r.get(c_perf_1y))
        dpen_f = _to_float_or_none(None if c_div is None else r.get(c_div))
        regime = _market_regime_hint(r)
        trend200_f = _trend200_hint(r)
        r_code = _rec_code(score_status, score_pctl, trend_ok, liq_ok)
        pillar = _norm_str(r.get(c_pillar)) if c_pillar and c_pillar in r.index else ""

        anomalies = _anomaly_for_perf_1d(perf_1d_f)
        if anomalies:
            extreme_perf_count += 1

        headline = (
            "Starker Kandidat im aktuellen Universe (Trend und Liquiditaet OK, hohe Confidence)."
            if bool(trend_ok) and bool(liq_ok) and (conf_f is not None and conf_f >= 70)
            else "Relevanter Kandidat im aktuellen Universe (weitere Pruefung empfohlen)."
        )

        drivers: list[dict[str, str]] = [
            {
                "id": "driver_score_top",
                "label": "Rang im Universe",
                "value": f"#{rank} von {len(df)}",
                "why_it_matters": "Hohe relative Position in der aktuellen Vergleichsmenge.",
            },
            {
                "id": "driver_quality_filters",
                "label": "Qualitaetsfilter",
                "value": f"Trend {'OK' if trend_ok else 'NO'} | Liq {'OK' if liq_ok else 'NO'}",
                "why_it_matters": "Filterstatus zeigt, ob Mindestkriterien aktuell erfuellt sind.",
            },
            {
                "id": "driver_confidence",
                "label": "Confidence",
                "value": "-" if conf_f is None else f"{conf_f:.0f}/100",
                "why_it_matters": "Hinweis auf Datenkonsistenz und Konfluenz im Modellkontext.",
            },
        ]

        next_checks: list[dict[str, str]] = [
            {"id": "check_chart", "text": "Chartstruktur pruefen (Trend, markante Zonen, Bewegungskontext)."},
            {"id": "check_news", "text": "News/Events pruefen (Earnings, Guidance, Corporate Actions)."},
            {"id": "check_risk", "text": "Risikoprofil pruefen (Drawdown, Volatilitaet, Positionsrisiko)."},
        ]
        if anomalies:
            next_checks[0] = {
                "id": "check_perf_basis",
                "text": f"Datenbasis der Tagesbewegung (1D) pruefen ({perf_window_1d}, Referenzwerte, Zeitfenster).",
            }

        item = {
            "rank": rank,
            "symbol": ident["symbol"],
            "name": ident["name"],
            "isin": ident["isin"],
            "score": score,
            "score_percentile": score_pctl,
            "risk_bucket": rb,
            "risk_buckets_total": 4,
            "risk_label": _risk_label(rb),
            "quality": {
                "trend_ok": trend_ok,
                "liq_ok": liq_ok,
                "confidence": conf_f,
                "status": score_status or "NA",
            },
            "headline": headline,
            "drivers": drivers,
            "anomalies": anomalies,
            "next_checks": next_checks,
            "details": {
                "column": pillar,
                "code": r_code,
                "cycle_pct": cycle_f,
                "perf_1d_pct": perf_1d_f,
                "perf_1y_pct": perf_1y_f,
                "regime": regime,
                "trend200_proxy": trend200_f,
                "risk_percentile": risk_pctl,
                "diversification_penalty": dpen_f,
            },
        }
        items.append(item)

    now = datetime.now(timezone.utc)
    date_col = _first_col(df, ["market_date", "MarketDate", "Date"])
    market_date = None
    if date_col and date_col in df.columns:
        try:
            md = pd.to_datetime(df[date_col], errors="coerce", utc=True)
            if md.notna().any():
                market_date = md.max().date().isoformat()
        except Exception:
            market_date = None

    source_name = csv_path.name.upper()
    preset = "ALL" if "ALL" in source_name else ("CORE" if "CORE" in source_name else ("FULL" if "FULL" in source_name else "ALL"))

    meta = {
        "briefing_version": f"v{BRIEFING_SCHEMA_VERSION}",
        "generated_utc": now.isoformat().replace("+00:00", "Z"),
        "source_csv": str(csv_path.as_posix()),
        "preset": preset,
        "filters": {"query": "", "cluster": "Alle", "column": "Alle", "bucket": None},
        "universe": {
            "total": int(len(df)),
            "scored": int(score_vals.notna().sum()),
            "scoring_basis": "score desc, tie-break: confidence desc, then symbol asc",
        },
        "metrics_basis": {
            "perf_1d_pct": {
                "label": c_perf_1d or "perf_1d_pct",
                "window": perf_window_1d,
                "definition_hint": "Tagesbewegung (1D). Bei extremen Werten Referenz und Zeitfenster pruefen.",
            },
            "perf_1y_pct": {
                "label": c_perf_1y or "perf_1y_pct",
                "window": perf_window_1y,
                "definition_hint": "Performance ueber 1 Jahr auf Basis der verfuegbaren Datenhistorie.",
            }
        },
        "disclaimer": "Privat/experimentell | keine Anlageberatung | keine Empfehlung.",
        "date": market_date or now.date().isoformat(),
        "language": language or "de",
        "schema_version": BRIEFING_SCHEMA_VERSION,
    }

    summary = {
        "what_to_do_next": [
            "Top-Werte zuerst auf Chart/News/Risiko pruefen.",
            "Bei extremer Tagesbewegung (1D) zuerst Datenbasis und Zeitraum verifizieren.",
        ],
        "market_context_hint": "Market Context ist informativ und beeinflusst das Scoring nicht.",
    }

    diagnostics = {
        "data_quality": {
            "extreme_perf_count": int(extreme_perf_count),
            "missing_fields_count": 0,
            "suspicious_values": [{"field": "perf_1d_pct", "rule": "abs(perf_1d_pct) >= 20", "count": int(extreme_perf_count)}],
        },
        "build_info": {
            "git_sha": (os.getenv("GITHUB_SHA") or "local")[:7],
            "ui_sha": (os.getenv("GITHUB_SHA") or "local")[:7],
            "pipeline": "run_daily -> validate -> briefing -> ui",
        },
    }

    context = {"notes": notes}
    return {"meta": meta, "summary": summary, "top": items, "context": context, "diagnostics": diagnostics}


def render_briefing_txt(briefing: dict[str, Any]) -> str:
    meta = briefing.get("meta") or {}
    summary = briefing.get("summary") or {}
    items = briefing.get("top") or []

    date_str = _norm_str(meta.get("date")) or "-"
    generated_utc = _norm_str(meta.get("generated_utc")) or "-"
    source_csv = _norm_str(meta.get("source_csv")) or "-"
    u = meta.get("universe") or {}
    universe_total = u.get("total", "-")
    universe_scored = u.get("scored", "-")

    lines: list[str] = []
    lines.append("Scanner_vNext - Briefing (deterministisch, Research)")
    lines.append(_norm_str(meta.get("disclaimer")) or "Privat/experimentell - keine Anlageberatung - keine Empfehlung.")
    lines.append("")
    lines.append(f"Datum: {date_str} | Generiert (UTC): {generated_utc}")
    lines.append(f"Quelle: {source_csv}")
    lines.append(f"Universe: {universe_total} | Scored: {universe_scored}")
    lines.append("")
    lines.append("So nutzt du das (30 Sekunden):")
    for t in summary.get("what_to_do_next") or []:
        lines.append(f"- {t}")
    lines.append("")
    lines.append(f"TOP {len(items)} (relativ im aktuellen Universe)")
    lines.append("-" * 72)

    for it in items:
        rank = it.get("rank")
        symbol = _norm_str(it.get("symbol")) or "-"
        name = _norm_str(it.get("name"))
        isin = _norm_str(it.get("isin"))
        score = _to_float_or_none(it.get("score"))
        sp = _to_float_or_none(it.get("score_percentile"))
        rb = it.get("risk_bucket")
        rb_total = it.get("risk_buckets_total") or 4
        risk_label = _norm_str(it.get("risk_label")) or "-"
        d = it.get("details") or {}
        perf_1d = _to_float_or_none(d.get("perf_1d_pct"))
        perf_1y = _to_float_or_none(d.get("perf_1y_pct"))

        q = it.get("quality") or {}
        trend_ok = q.get("trend_ok")
        liq_ok = q.get("liq_ok")
        conf = _to_float_or_none(q.get("confidence"))

        title = f"#{rank} {symbol}"
        if name:
            title += f" - {name}"
        if isin:
            title += f" ({isin})"
        lines.append(title)

        parts = []
        if score is not None:
            parts.append(f"Score {score:.2f}")
        if sp is not None:
            parts.append(f"Top {sp:.1f}%")
        if rb is not None:
            parts.append(f"Risiko {risk_label} ({rb}/{rb_total})")
        lines.append("Rang: " + " | ".join(parts) if parts else "Rang: -")

        q_txt = f"Qualitaet: Trend {'OK' if trend_ok else 'NO'} | Liq {'OK' if liq_ok else 'NO'}"
        if conf is not None:
            q_txt += f" | Confidence {conf:.0f}/100"
        lines.append(q_txt)
        if perf_1d is not None or perf_1y is not None:
            p1d = "-" if perf_1d is None else f"{perf_1d:+.2f}%"
            p1y = "-" if perf_1y is None else f"{perf_1y:+.2f}%"
            lines.append(f"Performance: 1D {p1d} | 1Y {p1y}")

        lines.append("Warum oben:")
        for d in (it.get("drivers") or [])[:3]:
            lines.append(f"- {d.get('label', '-')}: {d.get('value', '-')} - {d.get('why_it_matters', '-')}")

        anomalies = it.get("anomalies") or []
        if anomalies:
            lines.append("Auffaellig:")
            for a in anomalies[:3]:
                lines.append(f"- {a.get('label', '-')}: {a.get('value', '-')} - {a.get('explain', '-')}")

        lines.append("Naechste Checks:")
        for c in (it.get("next_checks") or [])[:3]:
            lines.append(f"- {c.get('text', '-')}")
        lines.append("")

    notes = ((briefing.get("context") or {}).get("notes") or [])
    if notes:
        lines.append("Kontext-Hinweise")
        lines.append("-" * 72)
        for n in notes:
            lines.append(f"- {n.get('text', '-')}")
        lines.append("")

    lines.append("Disclaimer: Privat/experimentell. Keine Anlageberatung. Keine Empfehlung.")
    return "\n".join(lines).strip() + "\n"


def validate_briefing_json(briefing: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []

    if not isinstance(briefing, dict):
        return False, ["root must be an object"]
    meta = briefing.get("meta")
    summary = briefing.get("summary")
    top = briefing.get("top")
    context = briefing.get("context")
    diagnostics = briefing.get("diagnostics")
    if not isinstance(meta, dict):
        errs.append("meta must be an object")
        meta = {}
    if not isinstance(summary, dict):
        errs.append("summary must be an object")
    if not isinstance(context, dict):
        errs.append("context must be an object")
    if not isinstance(diagnostics, dict):
        errs.append("diagnostics must be an object")
    if not isinstance(top, list):
        errs.append("top must be an array")
        top = []

    for k in ("briefing_version", "generated_utc", "source_csv", "preset", "universe", "metrics_basis", "disclaimer"):
        if k not in meta:
            errs.append(f"meta.{k} missing")

    for i, it in enumerate(top):
        if not isinstance(it, dict):
            errs.append(f"top[{i}] must be an object")
            continue
        for k in ("rank", "symbol", "name", "score", "score_percentile", "quality", "drivers", "anomalies", "next_checks", "details"):
            if k not in it:
                errs.append(f"top[{i}].{k} missing")
        if "drivers" in it and not isinstance(it.get("drivers"), list):
            errs.append(f"top[{i}].drivers must be an array")
        if "anomalies" in it and not isinstance(it.get("anomalies"), list):
            errs.append(f"top[{i}].anomalies must be an array")
        if "next_checks" in it and not isinstance(it.get("next_checks"), list):
            errs.append(f"top[{i}].next_checks must be an array")
        if "quality" in it and not isinstance(it.get("quality"), dict):
            errs.append(f"top[{i}].quality must be an object")
        if "details" in it and not isinstance(it.get("details"), dict):
            errs.append(f"top[{i}].details must be an object")

    return len(errs) == 0, errs


def write_briefing_outputs(
    *,
    briefing: dict[str, Any],
    output_dir: Path,
    write_ai: bool = False,
    ai_text: str | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    p_json = output_dir / "briefing.json"
    p_txt = output_dir / "briefing.txt"
    p_ai = output_dir / "briefing_ai.txt"

    p_json.write_text(json.dumps(briefing, ensure_ascii=False, indent=2), encoding="utf-8")
    p_txt.write_text(render_briefing_txt(briefing), encoding="utf-8")

    out = {"json": p_json, "txt": p_txt}
    if write_ai and ai_text:
        p_ai.write_text(ai_text.strip() + "\n", encoding="utf-8")
        out["ai_txt"] = p_ai
    return out


# ---------------------------
# Optional AI enhancement
# ---------------------------


def _extract_output_text(resp: dict[str, Any]) -> str:
    # Try common fields first
    if isinstance(resp.get("output_text"), str) and resp["output_text"].strip():
        return resp["output_text"].strip()

    out = resp.get("output")
    if not isinstance(out, list):
        return ""
    parts: list[str] = []
    for item in out:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for c in content:
            if not isinstance(c, dict):
                continue
            if c.get("type") == "output_text" and isinstance(c.get("text"), str):
                parts.append(c.get("text"))
    return "\n".join([p.strip() for p in parts if p and p.strip()]).strip()


def generate_ai_briefing_text(
    briefing_json: dict[str, Any],
    *,
    model: str,
    api_key: str | None = None,
    api_base: str | None = None,
    timeout_s: int = 30,
) -> str:
    """Generate an AI-enhanced German briefing from briefing.json.

    Uses OpenAI API via plain HTTP (stdlib) to avoid adding dependencies.

    We try the newer Responses API first and fall back to Chat Completions if
    the endpoint isn't available in the user's account/environment.
    """

    api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    api_base = (api_base or os.getenv("OPENAI_API_BASE") or "https://api.openai.com/v1").rstrip("/")
    url_responses = f"{api_base}/responses"
    url_chat = f"{api_base}/chat/completions"

    system = (
        "Du bist ein Assistenzsystem für ein privates Trading-Research-Dashboard. "
        "WICHTIG: Keine Anlageberatung, keine Kauf-/Verkaufsempfehlungen. "
        "Nutze ausschließlich die Informationen aus dem JSON (briefing.json). "
        "Rechne keine Scores neu und erfinde keine Daten. "
        "Schreibe auf Deutsch, kurz, präzise, nachvollziehbar. "
        "Baue am Ende einen kurzen Disclaimer ein: privat/experimentell, keine Anlageberatung."
    )

    user = (
        "Bitte erstelle eine sprachlich glatte Kurz-Zusammenfassung (Briefing) für die Top-Werte.\n\n"
        "Format:\n"
        "- Titelzeile: 'Scanner_vNext Briefing (AI)'\n"
        "- Danach pro Asset: 3–6 Bulletpoints (Warum oben, Chancen/Risiken kurz, Datenqualität, Regime-Hinweis).\n"
        "- Optional: 3–6 generische 'Nächste Checks' (keine Beratung).\n\n"
        "Input JSON:\n" + json.dumps(briefing_json, ensure_ascii=False)
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # stdlib HTTP to keep deps minimal
    import urllib.error
    import urllib.request

    def _post_json(url: str, payload: dict[str, Any]) -> tuple[int, str]:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return int(getattr(resp, "status", 200)), raw
        except urllib.error.HTTPError as e:  # noqa: PERF203
            raw = e.read().decode("utf-8", errors="replace") if getattr(e, "fp", None) else ""
            return int(getattr(e, "code", 500)), raw
        except Exception as e:  # network errors
            raise RuntimeError(f"OpenAI API request failed: {e}")

    # 1) Try Responses API
    payload_responses = {
        "model": model,
        "instructions": system,
        "input": user,
        "temperature": 0.0,
        "top_p": 1.0,
        "max_output_tokens": 800,
        "text": {"format": {"type": "text"}},
    }
    status, raw = _post_json(url_responses, payload_responses)
    if status < 400:
        try:
            data = json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"OpenAI Responses API returned invalid JSON: {e}")
        text = _extract_output_text(data)
        if not text:
            raise RuntimeError("OpenAI response had no text output")
        return text.strip() + "\n"

    # 2) Fallback: Chat Completions API (older but widely supported)
    # Only fallback for endpoint-ish failures; otherwise raise.
    if status not in (404, 405):
        raise RuntimeError(f"OpenAI API error {status}: {raw[:400]}")

    payload_chat = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.0,
        "top_p": 1.0,
        "max_tokens": 800,
    }
    status2, raw2 = _post_json(url_chat, payload_chat)
    if status2 >= 400:
        raise RuntimeError(f"OpenAI API error {status2}: {raw2[:400]}")
    try:
        data2 = json.loads(raw2)
        choices = data2.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("missing choices")
        msg = choices[0].get("message") or {}
        text2 = msg.get("content")
        if not isinstance(text2, str) or not text2.strip():
            raise RuntimeError("no message.content")
        return text2.strip() + "\n"
    except Exception as e:
        raise RuntimeError(f"OpenAI Chat Completions returned unexpected JSON: {e}")
