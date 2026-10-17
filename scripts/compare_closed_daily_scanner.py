"""Research-only full-universe shadow comparison; never publishes Scanner or Watch."""
import os,json,datetime,pathlib
import pandas as pd
from scanner.data.enrich.yahoo_prices import enrich_watchlist_with_yahoo
from scanner.app.score_step import apply_scoring
root=pathlib.Path(__file__).resolve().parents[1]
src=root/"artifacts/watchlist/watchlist.csv"
if not src.exists(): src=root/"data/inputs/watchlist.csv"
if not src.exists(): raise FileNotFoundError("No watchlist universe to compare")
base=pd.read_csv(src,low_memory=False)
assert len(base)>=100, "unexpectedly small watchlist"
out=root/"artifacts/research/closed_bar_shadow"
out.mkdir(parents=True,exist_ok=True)
reports={}
for name,closed in [("legacy",False),("closed",True)]:
 os.environ["SCANNER_CLOSED_DAILY_ONLY"]="1" if closed else "0"
 enriched,rep=enrich_watchlist_with_yahoo(base.copy(),enabled=True)
 scored=apply_scoring(enriched,universe_csv_path=str(src))
 scored.to_csv(out/(name+"_scored.csv"),index=False)
 reports[name]=rep.__dict__
 if closed:
  status=scored.get("price_bar_status",pd.Series(["MISSING"]*len(scored))).fillna("MISSING").value_counts(dropna=False).to_dict()
  reports[name]["bar_status_counts"]={str(k):int(v) for k,v in status.items()}
  reports[name]["bar_dates"]=scored.get("price_bar_date",pd.Series(dtype=str)).fillna("").astype(str).value_counts().head(15).to_dict()
  bad=scored.loc[scored["ScoreError"].fillna("").astype(str).ne(""),["ScoreError"]]
  reports[name]["error_counts"]=bad["ScoreError"].value_counts().to_dict()
key=next((x for x in ["YahooSymbol","Symbol","Ticker","symbol"] if x in base.columns),None)
a=pd.read_csv(out/"legacy_scored.csv",low_memory=False)
b=pd.read_csv(out/"closed_scored.csv",low_memory=False)
assert len(a)==len(b)
delta={}
for c in ["Score","OpportunityScore","RiskScore","ConfidenceScore","Akt. Kurs","Trend200","RS3M"]:
 if c not in a or c not in b:continue
 x=pd.to_numeric(a[c],errors="coerce");y=pd.to_numeric(b[c],errors="coerce")
 both=x.notna()&y.notna()
 change=(y-x).abs()[both]
 delta[c]={"comparable":int(both.sum()),"changed":int((change>1e-8).sum()),
           "median_abs_change":float(change.median()) if len(change) else None,
           "max_abs_change":float(change.max()) if len(change) else None}
summary={"run_at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
 "universe_rows":len(base),"identity_col":key,"reports":reports,"metrics":delta,
 "original_legacy_snapshot_reused":False,
 "note":"Two provider fetches at different times; exploratory shadow comparison, NOT strictly same-download paired test. No productive outputs modified."}
(out/"comparison.json").write_text(json.dumps(summary,indent=2,default=str))
print(json.dumps({"universe_rows":len(base),"bar_status":reports["closed"].get("bar_status_counts"),"metric_deltas":delta},default=str))
