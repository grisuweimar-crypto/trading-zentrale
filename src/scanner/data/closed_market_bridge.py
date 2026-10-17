"""Research-only bridge: canonical market archive -> frozen Yahoo-shaped frame.

The frame is derived from previously archived validated daily OHLCV and
adj_close, not a second provider download. It cannot repair old PIT history.
"""
import pandas as pd
from scanner.data.closed_market_cache import load_closed_market_cache

def market_archive_as_enrichment_frame(path, *, as_of, symbols):
    frames,coverage=load_closed_market_cache(path,as_of=as_of,symbols=set(symbols))
    columns={}
    for sym,d in frames.items():
        # Enrichment historically downloads auto_adjust=True. Reproduce its
        # adjusted close series and keep volumes; do not invent adjusted OHLC.
        columns[("Close",sym)]=pd.Series(d["adj_close"].to_numpy(dtype=float),index=d.index)
        columns[("Volume",sym)]=pd.Series(d["volume"].to_numpy(dtype=float),index=d.index)
    if not columns:
        raise ValueError("no_usable_closed_archived_bars")
    assembled=pd.DataFrame(columns)
    assembled.columns=pd.MultiIndex.from_tuples(assembled.columns)
    return assembled,coverage
