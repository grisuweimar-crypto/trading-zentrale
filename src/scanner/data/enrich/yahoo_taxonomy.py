from __future__ import annotations

"""Official taxonomy enrichment (Yahoo Finance).

Cache-first design:
- The main pipeline never calls Yahoo/network.
- A separate script fetches + caches a mapping CSV under artifacts/mapping/.
- The pipeline merges that cache into the canonical watchlist.
"""

from pathlib import Path
import pandas as pd

from scanner.data.io.paths import artifacts_dir


DEFAULT_MAPPING_PATH = artifacts_dir() / "mapping" / "yahoo_taxonomy.csv"


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


def _first_present(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def load_mapping(path: Path | None = None) -> pd.DataFrame | None:
    """Load cached mapping CSV. Returns None if missing/empty."""
    p = Path(path) if path is not None else DEFAULT_MAPPING_PATH
    if not p.exists():
        return None
    try:
        m = pd.read_csv(p)
    except Exception:
        return None
    if m.empty:
        return None

    # normalize expected columns
    ren = {}
    for c in m.columns:
        cn = str(c).strip().lower()
        if cn in {"yahoo_symbol", "yahoosymbol", "symbol"}:
            ren[c] = "yahoo_symbol"
        elif cn == "sector":
            ren[c] = "sector"
        elif cn == "industry":
            ren[c] = "industry"
        elif cn == "country":
            ren[c] = "country"
        elif cn == "currency":
            ren[c] = "currency"
    m = m.rename(columns=ren)

    if "yahoo_symbol" not in m.columns:
        return None

    m["yahoo_symbol"] = m["yahoo_symbol"].fillna("").astype(str).str.strip()
    m = m[m["yahoo_symbol"].str.len().gt(0)].copy()
    if m.empty:
        return None

    # keep last occurrence
    m = m.drop_duplicates(subset=["yahoo_symbol"], keep="last")
    return m


def apply_mapping(df: pd.DataFrame, mapping: pd.DataFrame | None) -> pd.DataFrame:
    """Merge mapping columns into df (sector/industry/country/currency).

    We try to match by (in order):
    - yahoo_symbol
    - ticker
    - symbol
    """
    if df.empty or mapping is None or mapping.empty:
        return df

    work = df.copy()

    key_col = _first_present(work, ["yahoo_symbol", "YahooSymbol", "yahoo", "ticker", "Ticker", "symbol", "Symbol"])
    if key_col is None:
        return df

    work["_key"] = work[key_col].apply(_norm).astype(str).str.strip()
    map_df = mapping.copy()
    map_df["_key"] = map_df["yahoo_symbol"].apply(_norm).astype(str).str.strip()
    map_df = map_df.dropna(subset=["_key"])
    map_df = map_df[map_df["_key"].str.len().gt(0)]

    cols = [c for c in ["sector", "industry", "country", "currency"] if c in map_df.columns]
    if not cols:
        return df

    merged = work.merge(map_df[["_key"] + cols], on="_key", how="left", suffixes=("", "_yahoo"))
    merged = merged.drop(columns=["_key"])

    # only fill if missing
    for c in cols:
        if c in merged.columns and c + "_yahoo" in merged.columns:
            if c in work.columns:
                merged[c] = merged[c].where(merged[c].notna() & merged[c].astype(str).str.strip().ne(""), merged[c + "_yahoo"])
            else:
                merged[c] = merged[c + "_yahoo"]
            merged = merged.drop(columns=[c + "_yahoo"])

    return merged


def derive_cluster_official(df: pd.DataFrame) -> pd.DataFrame:
    """Derive cluster_official used in UI filters.

    Priority: industry > sector. Crypto gets 'Krypto'.
    """
    if df.empty:
        return df
    out = df.copy()
    is_crypto = out.get("is_crypto", False)
    try:
        is_crypto = is_crypto.fillna(False).astype(bool)
    except Exception:
        is_crypto = pd.Series(False, index=out.index)

    industry = out.get("industry", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    sector = out.get("sector", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()

    cl = industry.where(industry.ne(""), sector)
    cl = cl.where(cl.ne(""), "")
    cl = cl.where(~is_crypto, "Krypto")
    out["cluster_official"] = cl
    return out
