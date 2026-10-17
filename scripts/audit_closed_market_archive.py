"""Full-universe research gate: existing market archive, scanner closed-day eligibility,
and read-only Decision/Watch contract check. Does not alter any production artifacts."""
import json
from pathlib import Path
from datetime import datetime,timezone
import pandas as pd
from scanner.data.closed_market_cache import load_closed_market_cache

root=Path(__file__).resolve().parents[1]
latest=root/"artifacts/research/latest_scanner.csv"
cache=root/"artifacts/market_data/yahoo_ohlcv.csv"
out=root/"artifacts/research/closed_bar_shadow/archive_gate.json"
symbols=set(pd.read_csv(latest,dtype=str)["symbol"])
frames,coverage=load_closed_market_cache(cache,as_of=datetime.now(timezone.utc),symbols=symbols)
counts={}
for q in coverage.values():
 counts[q["status"]]=counts.get(q["status"],0)+1
# This only checks current archive applicability, not live price finality.
report={"symbols":len(symbols),"eligible":len(frames),"statuses":counts,
        "source":"artifacts/market_data/yahoo_ohlcv.csv",
        "research_only":True,"watch_publication_untouched":True,
        "blocked":[{"symbol":s,**v} for s,v in sorted(coverage.items()) if s not in frames],
        "sample_last_dates":{s:coverage[s]["latest_session"] for s in sorted(frames)[:12]}}
out.parent.mkdir(parents=True,exist_ok=True)
# Separate old archival gaps from gaps in the recent indicator window.
raw=pd.read_csv(cache, dtype=str, keep_default_na=False)
recent={}
for sym,g in raw.loc[raw.symbol.isin(symbols)].groupby("symbol"):
 g=g.sort_values("date").tail(300)
 vals=pd.to_numeric(g.get("adj_close",pd.Series(dtype=str)),errors="coerce")
 recent[sym]={"rows_last300":len(g),"missing_adj_last300":int((vals.isna() | vals.le(0)).sum())}
report["recent_adjusted_coverage"]={
 "last300_complete":sum(v["rows_last300"]>=200 and v["missing_adj_last300"]==0 for v in recent.values()),
 "last300_missing":sum(v["missing_adj_last300"]>0 for v in recent.values()),
 "sample_missing":[{"symbol":k,**v} for k,v in sorted(recent.items()) if v["missing_adj_last300"]>0][:15]}
out.write_text(json.dumps(report,indent=2))
print(json.dumps({"symbols":len(symbols),"eligible":len(frames),"status":counts}))
# A realistic readiness threshold; do not silently accept mass exclusions.
if len(frames)<int(len(symbols)*.95):
 raise SystemExit("ARCHIVED_CLOSED_MARKET_DATA_INSUFFICIENT")
