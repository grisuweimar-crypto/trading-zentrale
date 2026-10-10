#!/usr/bin/env python3
"""Immutable scanner checkpoints for a separate, nonproductive external comparison."""
import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path


def capture(metadata, csv_bytes):
    if metadata.get("validation",{}).get("status")!="ok" or metadata.get("latest_run_complete") is not True:
        raise ValueError("scanner_publication_not_validated")
    snap=metadata.get("snapshot_id")
    if not snap or not metadata.get("generated_at") or not metadata.get("as_of"):
        raise ValueError("missing_scanner_publication_markers")
    published=datetime.fromisoformat(metadata["generated_at"].replace("Z","+00:00"))
    if published.tzinfo is None:
        raise ValueError("scanner_publication_timestamp_unzoned")
    rdr=csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
    required={"symbol","snapshot_id","observation_type","data_source",
              "score","opportunity","risk","confidence","currency"}
    if not rdr.fieldnames or not required.issubset(rdr.fieldnames):
        raise ValueError("scanner_columns_missing")
    symbols={}
    for r in rdr:
        if r["snapshot_id"]!=snap or r["observation_type"]!="observed_scanner" or r["data_source"]!="scanner_run":
            raise ValueError("scanner_snapshot_row_mismatch")
        s=r["symbol"]
        if not s or s in symbols: raise ValueError("scanner_symbol_missing_or_duplicate")
        symbols[s]={key:r[key] for key in ("score","opportunity","risk","confidence","currency")}
    if not symbols: raise ValueError("empty_scanner")
    if metadata.get("latest_scanner",{}).get("row_count",len(symbols))!=len(symbols):
        raise ValueError("scanner_count_not_matching")
    return {"schema_version":"argus_external_scanner_checkpoint_v1",
            "snapshot_id":snap,"as_of":metadata["as_of"],
            "published_at":published.isoformat(),
            "csv_sha256":hashlib.sha256(csv_bytes).hexdigest(),
            "symbols":symbols,"research_only":True}


def attach(report, snapshots):
    if report.get("schema_version")!="argus_external_thesis_followup_v1" or report.get("research_only") is not True:
        raise ValueError("invalid_external_thesis_report")
    for claim in report["claims"]:
        publication=claim["publication_date"]+"T00:00:00+00:00"
        eligible=[x for x in snapshots if x["published_at"] < publication and
                  x["as_of"] <= claim["publication_date"]]
        eligible.sort(key=lambda x:x["published_at"],reverse=True)
        if not eligible:
            claim["scanner_comparison"]="NO_VERIFIED_ASOF_SNAPSHOT"
            continue
        snapshot=eligible[0]
        fields=snapshot["symbols"].get(claim["symbol"])
        if not fields or fields["currency"]!=claim.get("currency"):
            claim["scanner_comparison"]="NOT_IN_VERIFIED_PRIOR_SCANNER"
            continue
        claim["scanner_comparison"]={
            "state":"PRIOR_SNAPSHOT_OBSERVED","snapshot_id":snapshot["snapshot_id"],
            "published_at":snapshot["published_at"],"scanner_as_of":snapshot["as_of"],
            "score":fields["score"],"opportunity":fields["opportunity"],
            "risk":fields["risk"],"confidence":fields["confidence"],
            "boundary":"not_a_retroactive_trade_decision_or_prediction"
        }
    return report


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--metadata",type=Path,required=True)
    p.add_argument("--scanner",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    result=capture(json.loads(args.metadata.read_text()),args.scanner.read_bytes())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+"\n")
    print(json.dumps({"scanner_as_of":result["as_of"],
                      "published_at":result["published_at"],
                      "symbols":len(result["symbols"])}))


if __name__=="__main__":
    main()
