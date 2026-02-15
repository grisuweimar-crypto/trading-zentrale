from __future__ import annotations

"""Pillar (5-säulen) enrichment.

Same philosophy as yahoo_taxonomy:
- The pipeline only merges cached mapping data (no external calls).
- Mapping is stored under artifacts/mapping/pillars.csv and can be regenerated any time.

Expected mapping columns (case-insensitive, flexible):
- isin (preferred key)
- yahoo_symbol / ticker (fallback keys)
- pillar_primary (e.g. Gehirn/Hardware/Energie/Fundament/Recycling/Playground)
- bucket_type (pillar/concept2/playground/none)
- pillar_confidence (0..100)
- pillar_reason
- pillar_tags
"""

from pathlib import Path
import pandas as pd

from scanner.data.io.paths import artifacts_dir


DEFAULT_MAPPING_PATH = artifacts_dir() / "mapping" / "pillars.csv"

ALLOWED_PILLARS = {
    "Gehirn",
    "Hardware",
    "Energie",
    "Fundament",
    "Recycling",
    "Playground",
}

ALLOWED_BUCKET_TYPES = {"pillar", "concept2", "playground", "none"}


def _norm(s: object) -> str:
    if s is None:
        return ""
    try:
        x = str(s).strip()
    except Exception:
        return ""
    if x.lower() == "nan":
        return ""
    return x


def load_mapping(path: Path | None = None) -> pd.DataFrame | None:
    p = Path(path) if path is not None else DEFAULT_MAPPING_PATH
    if not p.exists():
        return None
    try:
        m = pd.read_csv(p)
    except Exception:
        return None
    if m.empty:
        return None

    # normalize columns
    ren = {}
    for c in m.columns:
        cn = str(c).strip().lower()
        if cn in {"isin", "isinnumber"}:
            ren[c] = "isin"
        elif cn in {"yahoo_symbol", "yahoosymbol", "yahoo"}:
            ren[c] = "yahoo_symbol"
        elif cn in {"ticker", "symbol"}:
            ren[c] = "ticker"
        elif cn in {"pillar", "pillar_primary", "saeule", "säule"}:
            ren[c] = "pillar_primary"
        elif cn in {"bucket_type", "bucket"}:
            ren[c] = "bucket_type"
        elif cn in {"pillar_confidence", "confidence"}:
            ren[c] = "pillar_confidence"
        elif cn in {"pillar_reason", "reason"}:
            ren[c] = "pillar_reason"
        elif cn in {"pillar_tags", "tags"}:
            ren[c] = "pillar_tags"
    m = m.rename(columns=ren)

    # normalize keys
    for k in ("isin", "yahoo_symbol", "ticker"):
        if k in m.columns:
            m[k] = m[k].fillna("").astype(str).str.strip()
    if "pillar_primary" in m.columns:
        m["pillar_primary"] = m["pillar_primary"].fillna("").astype(str).str.strip()
    if "bucket_type" in m.columns:
        m["bucket_type"] = m["bucket_type"].fillna("").astype(str).str.strip()

    # filter empty rows
    key_cols = [c for c in ("isin", "yahoo_symbol", "ticker") if c in m.columns]
    if not key_cols:
        return None
    mask_key = pd.Series(False, index=m.index)
    for c in key_cols:
        mask_key = mask_key | m[c].astype(str).str.len().gt(0)
    m = m[mask_key].copy()
    if m.empty:
        return None

    # sanitize values (don't hard-fail)
    if "pillar_primary" in m.columns:
        m.loc[~m["pillar_primary"].isin(ALLOWED_PILLARS), "pillar_primary"] = m["pillar_primary"]
    if "bucket_type" in m.columns:
        bt = m["bucket_type"].str.lower()
        bt = bt.where(bt.isin(ALLOWED_BUCKET_TYPES), "none")
        m["bucket_type"] = bt

    return m


def apply_mapping(df: pd.DataFrame, mapping: pd.DataFrame | None) -> pd.DataFrame:
    if df.empty or mapping is None or mapping.empty:
        return df

    out = df.copy()

    # Build keys
    out["_k_isin"] = out.get("isin", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    out["_k_yh"] = out.get("yahoo_symbol", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    out["_k_tk"] = out.get("ticker", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()

    m = mapping.copy()
    m["_k_isin"] = m.get("isin", pd.Series("", index=m.index)).fillna("").astype(str).str.strip() if "isin" in m.columns else ""
    m["_k_yh"] = m.get("yahoo_symbol", pd.Series("", index=m.index)).fillna("").astype(str).str.strip() if "yahoo_symbol" in m.columns else ""
    m["_k_tk"] = m.get("ticker", pd.Series("", index=m.index)).fillna("").astype(str).str.strip() if "ticker" in m.columns else ""

    cols = [c for c in ("pillar_primary", "bucket_type", "pillar_confidence", "pillar_reason", "pillar_tags") if c in m.columns]
    if not cols:
        return df

    # Merge priority: ISIN > Yahoo > Ticker
    merged = out.copy()

    def _merge_on(key: str) -> None:
        nonlocal merged
        mm = m[m[key].astype(str).str.len().gt(0)].copy()
        if mm.empty:
            return
        mm = mm.drop_duplicates(subset=[key], keep="last")
        merged = merged.merge(mm[[key] + cols], on=key, how="left", suffixes=("", "_m"))
        for c in cols:
            cm = c + "_m"
            if cm in merged.columns:
                if c in out.columns:
                    merged[c] = merged[c].where(merged[c].notna() & merged[c].astype(str).str.strip().ne(""), merged[cm])
                else:
                    merged[c] = merged[cm]
                merged = merged.drop(columns=[cm])

    _merge_on("_k_isin")
    _merge_on("_k_yh")
    _merge_on("_k_tk")

    merged = merged.drop(columns=["_k_isin", "_k_yh", "_k_tk"], errors="ignore")
    return merged


def derive_from_official_taxonomy(df: pd.DataFrame) -> pd.DataFrame:
    """Heuristic derivation of 5-säulen metadata from *official* taxonomy (sector/industry).

    Why:
    - User wants official sectors/industries (no fantasy sectors),
      but still wants the 5-säulen view as a separate metadata layer.
    - This is *not* a scoring factor. It only fills missing pillar fields.

    Strategy (conservative):
    - Use industry first (more specific), then sector as weak fallback.
    - Fill only when pillar_primary is missing/empty.
    - Set a modest confidence (heuristic), and a short reason.

    Expected input columns (case-insensitive across canonicalized DF):
    - sector, industry, cluster_official (optional)

    Notes:
    - This is intentionally simple and can be refined via pillars.csv mapping anytime.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # Ensure columns exist
    for c in ("pillar_primary", "bucket_type", "pillar_confidence", "pillar_reason", "pillar_tags"):
        if c not in out.columns:
            out[c] = pd.NA
    out["bucket_type"] = out["bucket_type"].fillna("none")

    def _col(name: str) -> pd.Series:
        # best-effort: accept multiple casings
        for cand in (name, name.lower(), name.title(), name.upper()):
            if cand in out.columns:
                return out[cand].fillna("").astype(str).str.strip()
        return pd.Series("", index=out.index)

    sector = _col("sector").str.lower()
    industry = _col("industry").str.lower()
    cluster = _col("cluster_official").str.lower()

    # Only fill if missing and not explicitly tagged as concept2/playground
    pillar_missing = out["pillar_primary"].isna() | (out["pillar_primary"].astype(str).str.strip() == "")
    safe_bucket = out["bucket_type"].fillna("none").astype(str).str.lower().isin({"none", "pillar", ""})
    fill_mask = pillar_missing & safe_bucket

    if not fill_mask.any():
        return out

    def _fill(mask, pillar: str, conf: int, tags: str) -> None:
        idx = fill_mask & mask
        if not idx.any():
            return
        out.loc[idx, "pillar_primary"] = pillar
        out.loc[idx, "bucket_type"] = "pillar"
        # fill confidence only if empty/na
        pc = out.loc[idx, "pillar_confidence"]
        out.loc[idx, "pillar_confidence"] = pc.where(pc.notna(), conf)
        # reason only if empty
        pr = out.loc[idx, "pillar_reason"]
        out.loc[idx, "pillar_reason"] = pr.where(
            pr.notna() & (pr.astype(str).str.strip() != ""),
            "heuristic from official taxonomy",
        )
        # tags only if empty
        pt = out.loc[idx, "pillar_tags"]
        out.loc[idx, "pillar_tags"] = pt.where(
            pt.notna() & (pt.astype(str).str.strip() != ""),
            tags,
        )

    # Gehirn (AI/software + chip chain)
    _fill(
        industry.str.contains("semiconductor|software|internet|cloud|cyber|security|data|database|ai|artificial intelligence|it services") |
        cluster.str.contains("semiconductor|software|internet|technology"),
        "Gehirn",
        55,
        "ai, software, semis",
    )

    # Hardware (automation/robotics/sensors/vision)
    _fill(
        industry.str.contains("robot|automation|industrial machinery|specialty industrial machinery|electrical equipment|sensor|vision|mechatronic|factory") |
        cluster.str.contains("industrial|machinery|electrical equipment"),
        "Hardware",
        50,
        "robotics, automation",
    )

    # Energie (electrification grid/storage/renewables + uranium regime)
    _fill(
        industry.str.contains("utility|utilities|renewable|solar|wind|battery|storage|grid|power|transmission|uranium") |
        sector.str.contains("utilities") |
        cluster.str.contains("utilities|renewable|uranium"),
        "Energie",
        45,
        "grid, storage, electrification",
    )

    # Fundament (materials/metals/mining)
    _fill(
        sector.str.contains("basic materials") |
        industry.str.contains("metals|mining|gold|silver|copper|steel|aluminum|lithium|nickel|uranium") |
        cluster.str.contains("metals|mining|gold|silver|copper"),
        "Fundament",
        55,
        "materials, mining",
    )

    # Recycling (waste/recycling/urban mining)
    _fill(
        industry.str.contains("recycling|waste management|environmental services|scrap") |
        cluster.str.contains("recycling|waste"),
        "Recycling",
        50,
        "recycling, urban mining",
    )

    return out



def derive_from_legacy_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback derivation for 5-säulen/playground metadata.

    Purpose:
    - Keep the *concept* visible even if pillars.csv is missing.
    - Never affect scoring (metadata only).
    - Only fill missing pillar fields; never overwrite an explicit mapping.

    Derivation source:
    - legacy user categories: 'Sektor' (DE) and 'category' (EN)

    Notes:
    - This is intentionally conservative: if we cannot map confidently, we leave it empty.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # Ensure columns exist
    if "pillar_primary" not in out.columns:
        out["pillar_primary"] = pd.NA
    if "bucket_type" not in out.columns:
        out["bucket_type"] = "none"
    if "pillar_reason" not in out.columns:
        out["pillar_reason"] = pd.NA

    legacy = (
        out.get("Sektor", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
        .where(lambda s: s.str.lower().ne("nan"), "")
    )
    legacy2 = (
        out.get("category", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
        .where(lambda s: s.str.lower().ne("nan"), "")
    )
    src = legacy.where(legacy.str.len().gt(0), legacy2)
    s = src.str.lower()

    # Only fill if pillar is missing
    pillar_missing = out["pillar_primary"].isna() | (out["pillar_primary"].astype(str).str.strip() == "")
    if not pillar_missing.any():
        return out

    def _set(mask, pillar: str, bucket: str) -> None:
        idx = pillar_missing & mask
        if not idx.any():
            return
        out.loc[idx, "pillar_primary"] = pillar
        out.loc[idx, "bucket_type"] = bucket
        out.loc[idx, "pillar_reason"] = out.loc[idx, "pillar_reason"].where(
            out.loc[idx, "pillar_reason"].notna() & (out.loc[idx, "pillar_reason"].astype(str).str.strip() != ""),
            "derived from legacy category",
        )

    _set(s.str.contains("experiment") | s.str.contains("spiel") | s.str.contains("playground"), "Playground", "playground")
    _set(s.str.contains("gehirn"), "Gehirn", "pillar")
    _set(s.str.contains("hardware"), "Hardware", "pillar")
    _set(s.str.contains("energie") | s.str.contains("uran"), "Energie", "pillar")
    _set(s.str.contains("recycling") | s.str.contains("urban"), "Recycling", "pillar")
    _set(s.str.contains("fundament"), "Fundament", "pillar")

    # Metals/mining often mapped to Fundament in the user's framework
    _set(
        s.str.contains("edelmetall")
        | s.str.contains("mining")
        | s.str.contains("mine")
        | s.str.contains("metall"),
        "Fundament",
        "pillar",
    )

    # Concept2 (Konsum) is intentionally *not* mapped to a pillar.
    # We still mark bucket_type so the UI can signal it.
    concept2_mask = pillar_missing & (out["bucket_type"].fillna("none").astype(str).str.lower() == "none") & (
        s.str.contains("konsum") | s.str.contains("lifestyle")
    )
    if concept2_mask.any():
        out.loc[concept2_mask, "bucket_type"] = "concept2"
        out.loc[concept2_mask, "pillar_reason"] = out.loc[concept2_mask, "pillar_reason"].where(
            out.loc[concept2_mask, "pillar_reason"].notna() & (out.loc[concept2_mask, "pillar_reason"].astype(str).str.strip() != ""),
            "concept2 (Konsum) – derived from legacy category",
        )

    return out
