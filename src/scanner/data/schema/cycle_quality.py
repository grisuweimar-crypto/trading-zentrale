"""CY-01/02: preserve nullable cycle and versioned computation provenance.

'VALID' means numeric/range-valid *recorded input*, not proven fresh price-bar
lineage. The daily scanner does not recalculate this legacy field; CY-02 owns
the new as-of/provenance contract.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


QUALITY_CODES = frozenset({
    "VALID", "MISSING_SOURCE", "INVALID_VALUE", "INSUFFICIENT_HISTORY", "STALE"
})


def normalize_cycle_source(frame: pd.DataFrame) -> pd.DataFrame:
    """Return nullable cycle, quality and source fields, aligned to frame.index.

    Prefer the original legacy source when it exists. In the 2026-10-08
    source, the parallel 'cycle' field is zero-filled and contradicts 'Zyklus %'
    on many rows; it must not be used as a row-wise fallback.
    """
    index = frame.index
    if "Zyklus %" in frame.columns:
        field = "Zyklus %"
        source_name = "LEGACY_ZYKLUS_PCT"
    elif "cycle" in frame.columns:
        field = "cycle"
        source_name = "CANONICAL_CYCLE"
    else:
        field = None
        source_name = "NONE"

    if field is None:
        original = pd.Series(pd.NA, index=index)
    else:
        original = frame[field]
        if isinstance(original, pd.DataFrame):
            original = original.iloc[:, 0]

    text = original.astype("string").str.strip()
    missing = text.isna() | text.str.lower().isin(["", "nan", "none", "null", "na", "n/a"])
    missing = missing.fillna(True)
    numeric = pd.to_numeric(text.mask(missing), errors="coerce")
    finite = pd.Series(np.isfinite(numeric.to_numpy(dtype="float64", na_value=float("nan"))), index=index)
    valid = (~missing) & finite & numeric.between(0.0, 100.0).fillna(False)

    quality = pd.Series("INVALID_VALUE", index=index, dtype="string")
    quality.loc[missing] = "MISSING_SOURCE"
    quality.loc[valid] = "VALID"

    # A separately recorded upstream status may veto its own numeric value.
    # Unknown nonempty statuses are conservatively invalid, not silently VALID.
    if "cycle_quality" in frame.columns:
        prior = frame["cycle_quality"].astype("string").str.strip().str.upper()
        for code in ("STALE", "INSUFFICIENT_HISTORY", "MISSING_SOURCE", "INVALID_VALUE"):
            quality.loc[prior.eq(code).fillna(False)] = code
        unknown = prior.notna() & prior.ne("") & ~prior.isin(QUALITY_CODES)
        quality.loc[unknown.fillna(False)] = "INVALID_VALUE"
    numeric = numeric.where(quality.eq("VALID"), float("nan")).astype("float64")

    source = pd.Series(source_name, index=index, dtype="string")
    source.loc[quality.eq("MISSING_SOURCE")] = "NONE"
    # Keep the CY-02 calculation identity, rather than mislabelling computed
    # values as legacy simply because the compatible input column is named
    # 'Zyklus %'. Untrusted arbitrary source strings get no special status.
    if "cycle_source" in frame.columns:
        upstream = frame["cycle_source"].astype("string").str.strip()
        computed = upstream.eq("YAHOO_PIT_CYCLE_V1").fillna(False)
        source.loc[computed & quality.ne("MISSING_SOURCE")] = "YAHOO_PIT_CYCLE_V1"
    return pd.DataFrame(
        {"cycle": numeric, "cycle_quality": quality, "cycle_source": source},
        index=index,
    )
