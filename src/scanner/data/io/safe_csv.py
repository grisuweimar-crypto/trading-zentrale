from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

import pandas as pd


def to_csv_safely(df: pd.DataFrame, path: str | Path, **kwargs: Any) -> None:
    """Write CSV without noisy numpy RuntimeWarnings.

    Newer numpy versions emit a RuntimeWarning ("invalid value encountered in cast")
    when float arrays containing NaN are cast to string during CSV formatting.
    This is benign for our use-case (NaN stays empty / "nan" depending on pandas),
    but it makes CLI output look like something is broken.

    We suppress only this specific warning during the write.
    """

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="invalid value encountered in cast",
            category=RuntimeWarning,
        )
        df.to_csv(path, **kwargs)
