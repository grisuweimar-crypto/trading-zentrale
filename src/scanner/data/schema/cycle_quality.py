"""CY-01: preserve cycle missingness without changing the oscillator formula.

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

    # A separately attested upstream status may veto the use of its own value.
    # Never upgrade STALE/INSUFFICIENT_HISTORY to VALID just because it is numeric.
    if "cycle_quality" in frame.columns:
        previous_quality = frame["cycle_quality"].astype("string").str.strip().str.upper()
        for code in ("STALE", "INSUFFICIENT_HISTORY"):
            quality.loc[previous_quality.eq(code).fillna(False)] = code
    numeric = numeric.where(quality.eq("VALID"), float("nan")).astype("float64")

    source = pd.Series(source_name, index=index, dtype="string")
    source.loc[quality.eq("MISSING_SOURCE")] = "NONE"
    return pd.DataFrame(
        {"cycle": numeric, "cycle_quality": quality, "cycle_source": source},
        index=index,
    )
