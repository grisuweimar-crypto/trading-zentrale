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


BRIEFING_SCHEMA_VERSION = 1


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

    # canonical columns (fallback tolerant)
    c_score = _first_col(df, ["score", "Score"])
    c_status = _first_col(df, ["score_status", "ScoreStatus", "Status"])
    c_trend = _first_col(df, ["trend_ok", "TrendOK", "Trend Ok", "Trend"])
    c_liq = _first_col(df, ["liquidity_ok", "LiquidityOK", "LiqOK", "Liq"])
    c_cycle = _first_col(df, ["cycle", "Zyklus %", "Zyklus", "cycle_pct"])  # pct-ish
    c_perf = _first_col(df, ["perf_pct", "Perf %", "Perf%", "perf"])  # daily %
    c_conf = _first_col(df, ["confidence", "Confidence", "ConfidenceScore"])
    c_div = _first_col(df, ["diversification_penalty", "ScoreDiversificationPenalty"])

    c_cluster = _first_col(df, ["cluster_official", "Cluster", "Sektor", "sector", "Sector"])
    c_pillar = _first_col(df, ["pillar_primary", "Saeule", "Säule", "Pillar"])
    c_bucket_type = _first_col(df, ["bucket_type", "BUCKET", "bucket"])  # private taxonomy

    # percentiles
    score_vals = _num_series(df, c_score)
    score_sorted = [float(x) for x in score_vals.dropna().tolist()]
    score_sorted.sort()

    risk_raw = _risk_raw(df)
    risk_sorted = [float(x) for x in risk_raw.dropna().tolist()]
    risk_sorted.sort()

    score_pctl_list: list[float | None] = []
    risk_pctl_list: list[float | None] = []
    for i in range(len(df)):
        sv = score_vals.iloc[i]
        rv = risk_raw.iloc[i]
        sp = _percentile_rank(score_sorted, None if pd.isna(sv) else float(sv))
        rp = _percentile_rank(risk_sorted, None if pd.isna(rv) else float(rv))
        score_pctl_list.append(sp)
        risk_pctl_list.append(rp)

    df = df.copy()
    df["__score__"] = score_vals
    df["__score_pctl__"] = score_pctl_list
    df["__risk_raw__"] = risk_raw
    df["__risk_pctl__"] = risk_pctl_list

    # candidate filter: prefer OK rows
    status_s = df[c_status] if c_status and c_status in df.columns else pd.Series(["" for _ in range(len(df))])
    status_s = status_s.fillna("").astype(str)
    ok_mask = status_s.str.upper().eq("OK")
    cand = df.loc[ok_mask].copy() if ok_mask.any() else df.copy()

    # still filter out NA/ERROR if possible
    if c_status and c_status in cand.columns:
        st = cand[c_status].fillna("").astype(str).str.upper()
        m = ~st.isin(["NA", "ERROR"])
        if m.any():
            cand = cand.loc[m].copy()

    # sort by score desc; stable fallbacks
    cand["__conf__"] = _num_series(cand, c_conf)
    c_name = _first_col(cand, ["name", "Name", "ticker_display", "ticker", "Ticker", "symbol", "Symbol"])
    cand["__name__"] = (
        cand[c_name].fillna("").astype(str).str.strip().str.lower()
        if c_name and c_name in cand.columns
        else ""
    )
    cand = cand.sort_values(by=["__score__", "__conf__", "__name__"], ascending=[False, False, True], kind="mergesort")

    top_n = max(1, min(50, int(top_n)))
    top_df = cand.head(top_n)

    # concentration hints
    def _top_counts(series: pd.Series) -> dict[str, int]:
        s = series.fillna("").astype(str).str.strip()
        s = s[s.ne("")]
        return s.value_counts().to_dict() if not s.empty else {}

    cluster_counts = _top_counts(top_df[c_cluster]) if c_cluster and c_cluster in top_df.columns else {}
    pillar_counts = _top_counts(top_df[c_pillar]) if c_pillar and c_pillar in top_df.columns else {}

    concentration: list[str] = []
    if cluster_counts:
        k, v = next(iter(sorted(cluster_counts.items(), key=lambda x: (-x[1], x[0]))))
        if v >= max(2, (len(top_df) + 1) // 2):
            concentration.append(f"Klumpenrisiko: Top‑{len(top_df)} dominiert von Cluster '{k}' ({v}/{len(top_df)}).")
    if pillar_counts:
        k, v = next(iter(sorted(pillar_counts.items(), key=lambda x: (-x[1], x[0]))))
        if v >= max(2, (len(top_df) + 1) // 2):
            concentration.append(f"Kontext: Viele Top‑{len(top_df)} in Säule '{k}' ({v}/{len(top_df)}).")

    # build items
    items: list[dict[str, Any]] = []
    for _, r in top_df.iterrows():
        ident = _pick_identity(r)
        score = _to_float_or_none(r.get("__score__"))
        sp = _to_float_or_none(r.get("__score_pctl__"))
        rp = _to_float_or_none(r.get("__risk_pctl__"))

        trend_ok = None
        if c_trend and c_trend in r.index:
            try:
                trend_ok = bool(r.get(c_trend))
            except Exception:
                trend_ok = None
        liq_ok = None
        if c_liq and c_liq in r.index:
            try:
                liq_ok = bool(r.get(c_liq))
            except Exception:
                liq_ok = None

        score_status = _norm_str(r.get(c_status)) if c_status and c_status in r.index else ""
        r_code = _rec_code(score_status, sp, trend_ok, liq_ok)

        cycle_f = _to_float_or_none(None if c_cycle is None else r.get(c_cycle))
        perf_f = _to_float_or_none(None if c_perf is None else r.get(c_perf))
        conf_f = _to_float_or_none(None if c_conf is None else r.get(c_conf))
        dpen_f = _to_float_or_none(None if c_div is None else r.get(c_div))

        cluster = _norm_str(r.get(c_cluster)) if c_cluster and c_cluster in r.index else ""
        pillar = _norm_str(r.get(c_pillar)) if c_pillar and c_pillar in r.index else ""
        bucket_type = _norm_str(r.get(c_bucket_type)) if c_bucket_type and c_bucket_type in r.index else ""

        score_bucket = _score_bucket(score)
        risk_bucket = _bucket_0_4(rp)

        # regime hint (best effort)
        is_crypto = _is_crypto_row(r)
        if is_crypto:
            regime = _norm_str(r.get("regime_crypto")) or _norm_str(r.get("MarketRegimeCrypto"))
            trend200 = r.get("trend200_crypto") if "trend200_crypto" in r.index else r.get("Trend200Crypto")
        else:
            regime = _norm_str(r.get("regime_stock")) or _norm_str(r.get("MarketRegimeStock"))
            trend200 = r.get("trend200_stock") if "trend200_stock" in r.index else r.get("Trend200Stock")

        trend200_f = _to_float_or_none(trend200)

        reasons: list[str] = []
        risks: list[str] = []
        next_checks: list[str] = []

        if score is not None:
            if sp is not None:
                reasons.append(f"Hoher Score: {score:.2f} (Perzentil {sp:.1f}%, Bucket {score_bucket}/4).")
            else:
                reasons.append(f"Hoher Score: {score:.2f} (Bucket {score_bucket}/4).")

        if conf_f is not None:
            reasons.append(f"Confidence: {conf_f:.1f} (Daten-/Konfluenz‑Hinweis).")

        if dpen_f is not None:
            if dpen_f >= 6:
                risks.append(f"Diversifikation: Penalty {dpen_f:.2f} (erhoehtes Klumpenrisiko).")
            elif dpen_f <= 2:
                reasons.append(f"Diversifikation: Penalty {dpen_f:.2f} (breiteres Setup).")

        if cycle_f is not None:
            reasons.append(f"Zyklus: {cycle_f:.1f}% (≈50 neutral).")

        if perf_f is not None:
            reasons.append(f"Tagesbewegung (Perf %): {perf_f:+.2f}%." )

        if trend_ok is True:
            reasons.append("Trend‑Filter: OK.")
        elif trend_ok is False:
            risks.append("Trend‑Filter negativ (Trend OK = false).")

        if liq_ok is True:
            reasons.append("Liquidität: OK.")
        elif liq_ok is False:
            risks.append("Liquidität/Volumen schwach (Liquidity OK = false).")

        if score_status:
            if score_status.upper().startswith("AVOID"):
                risks.append(f"Status: {score_status} (System‑Flag).")
            elif score_status.upper() not in ("OK",):
                risks.append(f"Status: {score_status}.")

        if regime:
            reasons.append(f"Regime‑Hinweis: {regime}.")
        if trend200_f is not None:
            reasons.append(f"Trend200‑Proxy: {trend200_f:+.3f}.")

        if risk_bucket is not None:
            if rp is not None:
                (risks if risk_bucket >= 3 else reasons).append(
                    f"Risk‑Proxy: Bucket {risk_bucket}/4 (Perzentil {rp:.1f}%)."
                )
            else:
                (risks if risk_bucket >= 3 else reasons).append(f"Risk‑Proxy: Bucket {risk_bucket}/4.")

        if cluster:
            reasons.append(f"Cluster/Sektor: {cluster}.")
        if pillar:
            reasons.append(f"Säule (privat): {pillar}.")
        if bucket_type:
            reasons.append(f"BUCKET: {bucket_type}.")

        # Top-level concentration hints into per-item risks
        for msg in concentration:
            if "Cluster" in msg and cluster and (f"'{cluster}'" in msg or cluster in msg):
                risks.append(msg)

        # next checks (workflow, not advice)
        next_checks.extend(
            [
                "Datenqualität prüfen (Status/fehlende Felder/Confidence‑Breakdown).",
                "Chart‑Kontext prüfen (z.B. Trend200, Support/Resistance).",
                "Risk‑Metriken prüfen (Drawdown/Volatilität/Downside‑Dev).",
                "Cluster-/Säulen‑Exposure im eigenen Setup prüfen (Klumpenrisiko).",
            ]
        )
        if is_crypto:
            next_checks.append("Krypto‑Regime/Trend‑Kontext separat prüfen (Risk‑Management).")

        # de-dupe while preserving order
        def _dedupe(xs: list[str]) -> list[str]:
            seen: set[str] = set()
            out2: list[str] = []
            for x in xs:
                x = x.strip()
                if not x or x in seen:
                    continue
                seen.add(x)
                out2.append(x)
            return out2

        item = {
            "symbol": ident["symbol"],
            "isin": ident["isin"],
            "yahoo": ident["yahoo"],
            "name": ident["name"],
            "score": score,
            "score_pctl": sp,
            "r_code": r_code,
            "trend_ok": trend_ok,
            "liq_ok": liq_ok,
            "risk_bucket": risk_bucket,
            "risk_pctl": rp,
            "score_bucket": score_bucket,
            "diversification_penalty": dpen_f,
            "cluster": cluster,
            "pillar_primary": pillar,
            "bucket_type": bucket_type,
            "reasons": _dedupe(reasons),
            "risks": _dedupe(risks),
            "next_checks": _dedupe(next_checks),
        }
        items.append(item)

    now = datetime.now(timezone.utc)

    # detect market date if present
    date_col = _first_col(df, ["market_date", "MarketDate", "Date"])
    market_date = None
    if date_col and date_col in df.columns:
        try:
            md = pd.to_datetime(df[date_col], errors="coerce", utc=True)
            if md.notna().any():
                market_date = md.max().date().isoformat()
        except Exception:
            market_date = None

    meta = {
        "schema_version": BRIEFING_SCHEMA_VERSION,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "date": market_date or now.date().isoformat(),
        "language": language or "de",
        "source_csv": str(csv_path.as_posix()),
        "universe_count": int(len(df)),
        "scored_count": int(score_vals.notna().sum()),
        "top_n": int(top_n),
        "notes": concentration,
    }

    return {"meta": meta, "top": items}


def render_briefing_txt(briefing: dict[str, Any]) -> str:
    meta = briefing.get("meta") or {}
    items = briefing.get("top") or []
    lines: list[str] = []

    lines.append("Scanner_vNext – Briefing (deterministisch)")
    lines.append(f"Datum: {meta.get('date','—')}  |  Generiert: {meta.get('generated_at','—')}")
    lines.append(f"Quelle: {meta.get('source_csv','—')}")
    lines.append(f"Universe: {meta.get('universe_count','—')}  |  Scored: {meta.get('scored_count','—')}")
    lines.append("")

    n = len(items)
    lines.append(f"Top {n} – Warum diese Werte oben stehen (aus vorhandenen Feldern abgeleitet)")
    lines.append("—" * 72)

    def _short_why(it: dict) -> str:
        # Nimm 1–2 stärkste Gründe + Flags, keine neue Berechnung
        sym = it.get("symbol") or "—"
        score = it.get("score")
        sp = it.get("score_pctl")
        rb = it.get("risk_bucket")
        trend_ok = it.get("trend_ok")
        liq_ok = it.get("liq_ok")
        reasons = it.get("reasons") or []

        # 1–2 Gründe als "Treiber"
        treiber = "; ".join([str(x) for x in reasons[:2] if str(x).strip()])

        bits = []
        if score is not None:
            bits.append(f"Score {float(score):.2f}")
        if sp is not None:
            bits.append(f"Pctl {float(sp):.0f}%")
        if rb is not None:
            bits.append(f"RiskB {rb}/4")
        if trend_ok is True:
            bits.append("Trend OK")
        if liq_ok is True:
            bits.append("Liq OK")

        head = f"{sym}: " + " | ".join(bits) if bits else f"{sym}:"
        return f"{head} — Treiber: {treiber}." if treiber else f"{head}"

    lines.append("")
    lines.append("Top 3 – Kurzbegründung (pseudo-KI, deterministisch)")
    for i, it in enumerate(items[:3], 1):
        lines.append(f" {i}) {_short_why(it)}")
    lines.append("")
    lines.append("—" * 72)

    for i, it in enumerate(items, 1):
        sym = it.get("symbol") or "—"
        name = it.get("name") or ""
        isin = it.get("isin") or it.get("yahoo") or ""
        score = it.get("score")
        sp = it.get("score_pctl")
        rb = it.get("risk_bucket")
        rc = it.get("r_code") or "R?"
        cluster = it.get("cluster") or ""
        pillar = it.get("pillar_primary") or ""

        head = f"{i}) {sym}"
        if name:
            head += f" — {name}"
        if isin:
            head += f" ({isin})"
        lines.append(head)

        meta_line = []
        if score is not None:
            meta_line.append(f"Score {float(score):.2f}")
        if sp is not None:
            meta_line.append(f"Pctl {float(sp):.1f}%")
        if rb is not None:
            meta_line.append(f"RiskB {rb}/4")
        meta_line.append(f"Code {rc}")
        if cluster:
            meta_line.append(f"Cluster {cluster}")
        if pillar:
            meta_line.append(f"Säule {pillar}")
        lines.append("   " + " | ".join(meta_line))

        reasons = it.get("reasons") or []
        risks = it.get("risks") or []
        checks = it.get("next_checks") or []

        if reasons:
            lines.append("   Gründe:")
            for x in reasons[:10]:
                lines.append(f"     - {x}")
        if risks:
            lines.append("   Risiken/Flags:")
            for x in risks[:10]:
                lines.append(f"     - {x}")
        if checks:
            lines.append("   Nächste Checks (keine Beratung):")
            for x in checks[:10]:
                lines.append(f"     - {x}")
        lines.append("")

    notes = meta.get("notes") or []
    if notes:
        lines.append("Kontext-Hinweise")
        lines.append("—" * 72)
        for x in notes:
            lines.append(f"- {x}")
        lines.append("")

    lines.append("Disclaimer: Privat/experimentell. Keine Anlageberatung. Keine Empfehlung. Nutzung auf eigenes Risiko.")
    return "\n".join(lines).strip() + "\n"


def validate_briefing_json(briefing: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []

    if not isinstance(briefing, dict):
        return False, ["root must be an object"]
    meta = briefing.get("meta")
    top = briefing.get("top")
    if not isinstance(meta, dict):
        errs.append("meta must be an object")
        meta = {}
    if not isinstance(top, list):
        errs.append("top must be an array")
        top = []

    for k in ("schema_version", "generated_at", "date", "source_csv", "universe_count", "scored_count", "top_n"):
        if k not in meta:
            errs.append(f"meta.{k} missing")

    for i, it in enumerate(top):
        if not isinstance(it, dict):
            errs.append(f"top[{i}] must be an object")
            continue
        for k in ("symbol", "name", "score", "r_code", "reasons", "risks", "next_checks"):
            if k not in it:
                errs.append(f"top[{i}].{k} missing")
        if "reasons" in it and not isinstance(it.get("reasons"), list):
            errs.append(f"top[{i}].reasons must be an array")
        if "risks" in it and not isinstance(it.get("risks"), list):
            errs.append(f"top[{i}].risks must be an array")
        if "next_checks" in it and not isinstance(it.get("next_checks"), list):
            errs.append(f"top[{i}].next_checks must be an array")

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
