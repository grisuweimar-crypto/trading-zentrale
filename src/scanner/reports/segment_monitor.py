from __future__ import annotations

"""Segment Monitor for scanner-internal and official groupings.

Explainability-only. The module keeps segment analysis separate from History
Delta itself. It consumes the already computed 1D score deltas as comparison
facts, but never feeds them back into scoring.

Inputs
------
- current watchlist-derived segment snapshot
- local segment history (membership/taxonomy changes)
- History Delta report (canonical 1D score-delta basis)

Outputs
-------
- artifacts/reports/segment_monitor.json
- artifacts/reports/segment_monitor.csv
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scanner.data.io.paths import artifacts_dir, project_root
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 2
STABLE_SAMPLE_MIN = 5


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "<na>", "none"} else text


def resolve_segment_history_path() -> Path:
    a = artifacts_dir() / "snapshots" / "segment_history.csv"
    if a.exists():
        return a
    return project_root() / "data" / "snapshots" / "segment_history.csv"


def build_segment_snapshot(df_full: pd.DataFrame, date: str | None = None) -> pd.DataFrame:
    if df_full is None or df_full.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "pillar_primary",
                "cluster_official",
                "sector",
                "bucket_type",
            ]
        )

    dt = date
    if not dt and "market_date" in df_full.columns:
        md = df_full["market_date"].dropna().astype(str).str.strip()
        md = md[md != ""]
        if len(md) > 0:
            dt = str(md.iloc[0])[:10]
    dt = dt or _utc_today()

    def _col(name: str) -> pd.Series:
        if name not in df_full.columns:
            return pd.Series([pd.NA] * len(df_full), index=df_full.index)
        return df_full[name]

    sym = _col("asset_id")
    if sym.isna().all() and "symbol" in df_full.columns:
        sym = _col("symbol")
    if sym.isna().all() and "ticker" in df_full.columns:
        sym = _col("ticker")

    snap = pd.DataFrame(
        {
            "date": dt,
            "symbol": sym.astype(str),
            "pillar_primary": _col("pillar_primary").astype(str),
            "cluster_official": _col("cluster_official").astype(str),
            "sector": _col("sector").astype(str),
            "bucket_type": _col("bucket_type").astype(str),
        }
    )

    for column in (
        "symbol",
        "pillar_primary",
        "cluster_official",
        "sector",
        "bucket_type",
    ):
        snap[column] = snap[column].replace({"nan": ""}).fillna("").astype(str).str.strip()

    return snap[snap["symbol"] != ""].copy()


def upsert_segment_snapshot(path: Path, snapshot: pd.DataFrame) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    if snapshot is None or snapshot.empty:
        if path.exists():
            return pd.read_csv(path)
        return pd.DataFrame()

    dt = str(snapshot["date"].iloc[0])

    if path.exists():
        existing = pd.read_csv(path)
        for column in snapshot.columns:
            if column not in existing.columns:
                existing[column] = ""
    else:
        existing = pd.DataFrame()

    if not existing.empty:
        existing["date"] = existing["date"].astype(str)
        syms = set(snapshot["symbol"].astype(str).tolist())
        drop = (existing["date"] == dt) & (existing["symbol"].astype(str).isin(syms))
        existing = existing.loc[~drop].copy()

    combined = pd.concat([existing, snapshot], ignore_index=True)
    if "date" in combined.columns and "symbol" in combined.columns:
        combined["date"] = combined["date"].astype(str)
        combined["symbol"] = combined["symbol"].astype(str)
        combined = combined.sort_values(["date", "symbol"], ascending=[True, True])

    to_csv_safely(combined, path, index=False)
    return combined


def _history_delta_map(history_delta: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    if not isinstance(history_delta, Mapping):
        return {}
    raw = history_delta.get("by_symbol")
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(symbol): value
        for symbol, value in raw.items()
        if isinstance(value, Mapping)
    }


def _segment_metrics(
    current_snapshot: pd.DataFrame,
    history_delta: Mapping[str, Any] | None,
    *,
    view: str,
    primary_field: str,
    fallback_field: str | None = None,
) -> list[dict[str, Any]]:
    delta_by_symbol = _history_delta_map(history_delta)
    buckets: dict[str, dict[str, float | int]] = {}

    for _, row in current_snapshot.iterrows():
        key = _clean(row.get(primary_field))
        if not key and fallback_field:
            key = _clean(row.get(fallback_field))
        if not key:
            continue

        rec = buckets.setdefault(
            key,
            {"n_total": 0, "n_valid": 0, "sum_delta": 0.0, "n_positive": 0},
        )
        rec["n_total"] = int(rec["n_total"]) + 1

        symbol = _clean(row.get("symbol"))
        delta_row = delta_by_symbol.get(symbol)
        if not isinstance(delta_row, Mapping) or str(delta_row.get("status") or "") != "ok":
            continue
        try:
            delta = float(delta_row.get("score_delta"))
        except (TypeError, ValueError):
            continue
        if pd.isna(delta):
            continue

        rec["n_valid"] = int(rec["n_valid"]) + 1
        rec["sum_delta"] = float(rec["sum_delta"]) + delta
        if delta > 0:
            rec["n_positive"] = int(rec["n_positive"]) + 1

    rows: list[dict[str, Any]] = []
    for segment, rec in buckets.items():
        n_total = int(rec["n_total"])
        n_valid = int(rec["n_valid"])
        average = (float(rec["sum_delta"]) / n_valid) if n_valid else None
        positive_share = (int(rec["n_positive"]) / n_valid) if n_valid else None
        coverage = (n_valid / n_total) if n_total else 0.0
        sample_state = (
            "stable"
            if n_valid >= STABLE_SAMPLE_MIN
            else ("thin" if n_valid > 0 else "unavailable")
        )
        rows.append(
            {
                "view": view,
                "segment": segment,
                "average_dscore_1d": average,
                "positive_share": positive_share,
                "stable_sample": n_valid >= STABLE_SAMPLE_MIN,
                "sample_state": sample_state,
                "coverage": coverage,
                "n_valid": n_valid,
                "n_total": n_total,
            }
        )

    rows.sort(
        key=lambda row: (
            row["average_dscore_1d"] is None,
            -(float(row["average_dscore_1d"]) if row["average_dscore_1d"] is not None else 0.0),
            -int(row["n_valid"]),
            str(row["segment"]),
        )
    )
    return rows


def compute_segment_monitor(
    seg_hist: pd.DataFrame,
    current_snapshot: pd.DataFrame,
    history_delta: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    cur = current_snapshot.copy() if current_snapshot is not None else pd.DataFrame()
    if cur.empty:
        empty = pd.DataFrame(
            columns=[
                "view",
                "segment",
                "average_dscore_1d",
                "positive_share",
                "stable_sample",
                "sample_state",
                "coverage",
                "n_valid",
                "n_total",
            ]
        )
        return empty, {
            "schema_version": SCHEMA_VERSION,
            "latest_date": None,
            "prev_date": None,
            "stats": {"total": 0, "changed": 0, "score_delta_basis_count": 0},
            "internal_segments": [],
            "official_segments": [],
            "changes": [],
            "semantics": {
                "separate_from_history_delta": True,
                "mixed_super_metric_created": False,
            },
        }

    latest_date = str(cur["date"].iloc[0])

    def _dist(col: str, limit: int = 30) -> list[dict[str, Any]]:
        s = cur[col].fillna("").astype(str).replace({"nan": ""}).str.strip()
        s = s.replace({"": "∅"})
        vc = s.value_counts(dropna=False).head(limit)
        return [{"key": k, "count": int(v)} for k, v in vc.items()]

    pillar_dist = _dist("pillar_primary")
    cluster_dist = _dist("cluster_official")
    bucket_dist = _dist("bucket_type")

    prev_date = None
    changes: list[dict[str, Any]] = []
    if seg_hist is not None and not seg_hist.empty:
        work = seg_hist.copy()
        work["date"] = work["date"].astype(str)
        dates = sorted(
            d for d in work["date"].dropna().unique().tolist() if str(d).strip()
        )
        if len(dates) >= 2:
            prev_date = str(dates[-2])
            prev = (
                work[work["date"] == prev_date]
                .copy()
                .sort_values(["symbol"])
                .drop_duplicates(subset=["symbol"], keep="last")
                .set_index("symbol", drop=False)
            )
            now = (
                cur.sort_values(["symbol"])
                .drop_duplicates(subset=["symbol"], keep="last")
                .set_index("symbol", drop=False)
            )
            for symbol in sorted(set(prev.index.tolist()) & set(now.index.tolist())):
                before = prev.loc[symbol]
                after = now.loc[symbol]
                changed = []
                for field in (
                    "pillar_primary",
                    "cluster_official",
                    "sector",
                    "bucket_type",
                ):
                    previous_value = _clean(before.get(field))
                    current_value = _clean(after.get(field))
                    if previous_value != current_value:
                        changed.append(
                            {
                                "field": field,
                                "from": previous_value,
                                "to": current_value,
                            }
                        )
                if changed:
                    changes.append({"symbol": str(symbol), "changes": changed})

    internal_segments = _segment_metrics(
        cur,
        history_delta,
        view="internal",
        primary_field="pillar_primary",
    )
    official_segments = _segment_metrics(
        cur,
        history_delta,
        view="official",
        primary_field="cluster_official",
        fallback_field="sector",
    )
    metric_rows = internal_segments + official_segments
    out = pd.DataFrame(metric_rows)

    delta_by_symbol = _history_delta_map(history_delta)
    valid_basis = 0
    for symbol in cur["symbol"].astype(str):
        row = delta_by_symbol.get(symbol)
        if not isinstance(row, Mapping) or str(row.get("status") or "") != "ok":
            continue
        try:
            value = float(row.get("score_delta"))
        except (TypeError, ValueError):
            continue
        if not pd.isna(value):
            valid_basis += 1

    payload = {
        "schema_version": SCHEMA_VERSION,
        "latest_date": latest_date,
        "prev_date": prev_date,
        "stats": {
            "total": int(len(cur)),
            "changed": int(len(changes)),
            "score_delta_basis_count": int(valid_basis),
            "score_delta_basis_coverage": (valid_basis / len(cur)) if len(cur) else 0.0,
        },
        "pillar_dist": pillar_dist,
        "cluster_dist": cluster_dist,
        "bucket_dist": bucket_dist,
        "internal_segments": internal_segments,
        "official_segments": official_segments,
        "changes": changes[:50],
        "semantics": {
            "separate_from_history_delta": True,
            "history_delta_used_only_as_score_change_fact": True,
            "internal_and_official_views_kept_separate": True,
            "mixed_super_metric_created": False,
            "thin_samples_marked_not_overinterpreted": True,
        },
    }
    return out, payload


def write_segment_monitor_outputs(
    df: pd.DataFrame, payload: dict[str, Any]
) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "segment_monitor.json"
    p_csv = out_dir / "segment_monitor.csv"

    p_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    to_csv_safely(df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
