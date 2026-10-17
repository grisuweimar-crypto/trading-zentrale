"""Read the existing validated historical OHLCV cache for a closed-session price gate.

Research-only adapter. No independent price archive, no silent backfill, and
no historical PIT reconstruction. Data freshness is checked by caller.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from scanner.data.price_history import MARKET_COLUMNS, validated_rows
from scanner.data.enrich.yahoo_prices import _completed_daily_series_for_venue


def load_closed_market_cache(path: str | Path, *, as_of, symbols: set[str] | None = None):
    path=Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    # Canonical CSV source, same file used by prefetch_market_history.
    raw=pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")
    valid, issues=validated_rows(raw)
    if issues:
        raise ValueError(f"invalid_or_conflicting_market_rows:{issues}")
    if any(key not in raw[0] for key in MARKET_COLUMNS) if raw else True:
        raise ValueError("missing_canonical_market_columns")
    grouped={}
    for row in valid:
        symbol=row["symbol"]
        if symbols is not None and symbol not in symbols:
            continue
        grouped.setdefault(symbol,[]).append(row)
    frames={};coverage={}
    for sym,rs in grouped.items():
        d=pd.DataFrame(rs).sort_values("date")
        if d.date.duplicated().any():
            raise ValueError(f"duplicate_session:{sym}")
        idx=pd.to_datetime(d.date)
        close=pd.Series(pd.to_numeric(d.close,errors="coerce").to_numpy(),index=idx)
        vol=pd.Series(pd.to_numeric(d.volume,errors="coerce").to_numpy(),index=idx)
        eligible,_=_completed_daily_series_for_venue(close,vol,as_of=as_of,symbol=sym)
        if eligible.empty:
            coverage[sym]={"status":"NO_ELIGIBLE_CLOSED_BAR","latest_session":None}
            continue
        last=str(eligible.index[-1].date())
        # Archive is not known PIT final from local clock; only an observed
        # closed-session candidate. Provider-published finality is separate.
        used=d.loc[d.date.le(last)].copy()
        ohlc=["open","high","low","close","adj_close","volume"]
        for col in ohlc:
            used[col]=pd.to_numeric(used[col],errors="coerce")
        if used[["open","high","low","close"]].isna().any().any():
            raise ValueError(f"incomplete_ohlc:{sym}")
        if used.adj_close.isna().any():
            coverage[sym]={"status":"ADJUSTED_HISTORY_INCOMPLETE","latest_session":last}
            continue
        used.index=pd.to_datetime(used.date)
        frames[sym]=used
        coverage[sym]={"status":"CLOSED_ARCHIVED_SESSION","latest_session":last,"bars":len(used)}
    if symbols:
        for sym in symbols:
            coverage.setdefault(sym,{"status":"SYMBOL_NOT_IN_ARCHIVE","latest_session":None})
    return frames,coverage
