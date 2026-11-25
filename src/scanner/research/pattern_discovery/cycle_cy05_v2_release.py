"""CY-03-v2 -> CY-05 explicit signed release, never a default or v1 override.

Release requires independent evidence and separate signed scope approval.
L1 freeze, outcomes, sufficiency, and production are outside this adapter.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path

from scanner.reports.cycle_history import ROOT, read_ledger
from scanner.research.pattern_discovery.cycle_cy03_v2_review import (
    _verify_signature, digest, read_bytes, row_digest, utc, verify_review,
)

SCHEMA = "cycle_cy03_v2_cy05_release_v1"


def _projection(ledger, review, release, *, now):
    if release.get("schema_version") != SCHEMA or release.get("scope") != "CY05_RESEARCH_ONLY":
        raise ValueError("cy05v2:release_scope")
    if release.get("review_sha256") != review["review_sha256"]:
        raise ValueError("cy05v2:review_hash_mismatch")
    if release.get("source_archive_manifest_hashes") != review["source_archive_manifest_hashes"]:
        raise ValueError("cy05v2:archive_hash_mismatch")
    if release.get("reviewer_id") != review["reviewer_id"]:
        raise ValueError("cy05v2:reviewer_mismatch")
    cutoff = utc(release["data_cutoff_utc"])
    signed = utc(release["reviewed_at"])
    if not cutoff <= signed <= now:
        raise ValueError("cy05v2:release_chronology")
    if release.get("independent_review_approved") is not True:
        raise ValueError("cy05v2:independent_review_missing")
    if release.get("production_enabled") is not False:
        raise ValueError("cy05v2:production_forbidden")
    rows = {(r["snapshot_id"],r["asset_id"]):r for r in ledger}
    if len(rows)!=len(ledger):
        raise ValueError("cy05v2:duplicate_ledger_rows")
    wanted = release["rows"]
    if not isinstance(wanted,list) or not wanted:
        raise ValueError("cy05v2:empty_release")
    approved = {(r["snapshot_id"],r["asset_id"]):r for r in review["rows"]}
    out=[]
    seen=set()
    for item in wanted:
        key=(item["snapshot_id"],item["asset_id"])
        r=rows.get(key)
        a=approved.get(key)
        if key in seen or r is None or a is None:
            raise ValueError("cy05v2:unreviewed_or_duplicate_row")
        seen.add(key)
        h=row_digest(r)
        if item["ledger_row_sha256"]!=h or a["ledger_row_sha256"]!=h:
            raise ValueError("cy05v2:row_hash_mismatch")
        if utc(r["generated_at"])>cutoff or r["quality"]!="VALID":
            raise ValueError("cy05v2:cutoff_or_quality")
        if item["verified_lags"]!=a["verified_lags"]:
            raise ValueError("cy05v2:lag_claim_mismatch")
        value=float(r["cycle"])
        if not math.isfinite(value) or not 0<=value<=100:
            raise ValueError("cy05v2:cycle_value_invalid")
        out.append({"symbol":r["asset_id"],"as_of":r["as_of"],"cycle":value,
                    "cycle_quality":r["quality"],"cycle_research_status":"ELIGIBLE",
                    "cycle_history_source":"CY03_VERIFIED_LEDGER",
                    "cycle_snapshot_id":r["snapshot_id"],"cycle_asset_id":r["asset_id"],
                    "cycle_formula":r["formula"],"cycle_currency":r["currency"],
                    "cycle_listing_symbol":r["listing_symbol"],"cycle_price_symbol":r["price_symbol"],
                    "cycle_release_review_sha256":review["review_sha256"],
                    **{f"cycle_lag_{n}obs":"RESEARCH_ELIGIBLE" if n in a["verified_lags"]
                       else "NOT_RELEASED" for n in (1,5,10)}})
    # The release may not drop intermediate rows from a verified history.
    # Release the exact eligible reviewed set up to the frozen cutoff.
    expected={key for key,a in approved.items() if utc(rows[key]["generated_at"])<=cutoff}
    if seen!=expected:
        raise ValueError("cy05v2:incomplete_reviewed_cohort")
    return sorted(out,key=lambda r:(r["as_of"],r["symbol"],r["cycle_snapshot_id"]))


def load_cycle_cy05_v2_rows(repo_root, review_bundle, release_bundle, *, now=None):
    root=Path(repo_root)
    release_bundle=Path(release_bundle)
    now=now or datetime.now(timezone.utc)
    review=verify_review(root,review_bundle,now=now)
    raw=read_bytes(release_bundle/"release.json")
    release=json.loads(raw)
    _verify_signature(root,release_bundle,raw,release)
    # Release cannot be dated before the independently signed observation review.
    original=json.loads(read_bytes(Path(review_bundle)/"review.json"))
    if utc(release["reviewed_at"])<utc(original["reviewed_at"]):
        raise ValueError("cy05v2:release_before_review")
    ledger=read_ledger(root/ROOT/"observations.csv")
    rows=_projection(ledger,review,release,now=now)
    # Require archive bytes still match exactly those validated by review.
    for name,h in review["source_archive_manifest_hashes"].items():
        if digest(read_bytes(root/ROOT/name))!=h:
            raise ValueError("cy05v2:archive_changed_during_release")
    return {"schema_version":SCHEMA,"research_only":True,"production_enabled":False,
            "l1_frozen":False,"outcomes_inspected":False,
            "release_sha256":digest(raw),"review_sha256":review["review_sha256"],
            "data_cutoff_utc":release["data_cutoff_utc"],"rows":rows}
