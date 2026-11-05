"""Candidate Radar ↔ ARGUS bridge invariants: no admission and no invented effect."""
import json
from pathlib import Path

import pytest
from scripts.audit_radar_zone_bridge import build,_load_watch,write


def candidate(symbol,isin=None,when=None):
    record={"symbol":symbol,"name":symbol,"source_family":"Test research","source_date":when,
            "source_evidence_status":"test_external","research_only":True,
            "scanner_admission_status":"not_requested"}
    if isin:
        record["isin"]=isin
    return record


def scanner(when="2026-11-04"):
    return {"XPEV":{"yahoo_symbol":"XPEV","isin":"US98422D1054",
                    "price_bar_status":"CLOSED_PREVIOUS_SESSION","price_bar_date":when}}


def zone(symbol="XPEV",when="2026-11-04"):
    return {"symbol":symbol,"asof":when,"status":"RESEARCH_ONLY",
            "zones":[{"lower":9.2,"upper":9.4}],
            "multiscale":{"monthly":{"status":"RESEARCH_ONLY","zones":[{}]},
                          "four_hour":{"status":"RESEARCH_ONLY","zones":[{},{},{}]}}}


def doc(items=None):
    return {"schema_version":"argus_external_thesis_public_candidates_v1",
            "items":items or [candidate("XPEV",isin="US98422D1054"),candidate("LULU")]}


def zones(entries=None):
    return {"research_only":True,"score_modified":False,
            "assets":[zone()] if entries is None else entries}


def test_exact_scanner_and_shadow_membership_without_promotion():
    out=build(doc(),scanner(),zones())
    assert out["count"]==2 and out["in_scanner"]==1 and out["radar_only"]==1
    assert out["daily_zones_available"]==1
    assert out["monthly_zones_available"]==1
    assert out["four_hour_zones_available"]==1
    assert out["auto_admission_allowed"] is False
    assert out["prospective_outcomes_confirmed"] is False
    assert out["redundancy_conclusion"].startswith("UNDETERMINED")
    assert all(x["research_only"] and x["empirical_effect"] is None for x in out["items"])
    outside=out["items"][1]
    assert outside["membership"]=="RADAR_ONLY"
    assert outside["daily_zone_status"]=="UNAVAILABLE"


def test_stale_asof_cannot_be_used_as_confirmation():
    z=zones([zone(when="2026-11-03")])
    out=build(doc([candidate("XPEV")]),scanner(),z)
    assert out["daily_zones_available"]==0
    assert out["items"][0]["daily_zones_count"]==0


def test_isin_conflict_blocks_join():
    out=build(doc([candidate("XPEV",isin="CHANGEME")]),scanner(),zones())
    assert out["items"][0]["identity_state"]=="ISIN_CONFLICT"
    assert out["daily_zones_available"]==0


def test_source_timestamp_after_scan_fails_prospective_eligibility():
    out=build(doc([candidate("XPEV",when="2026-11-07")]),scanner(),zones())
    assert out["items"][0]["sample_eligibility"]=="SOURCE_DATE_AFTER_SCANNER_SNAPSHOT"


def test_unknown_source_time_never_becomes_verified_by_join():
    out=build(doc([candidate("XPEV",when=None)]),scanner(),zones())
    assert out["items"][0]["sample_eligibility"]=="PENDING_PIT_SOURCE_AND_OUTCOME_REVIEW"


def test_source_governance_and_duplicate_fail_closed():
    bad=candidate("XPEV")
    bad["scanner_admission_status"]="admitted"
    with pytest.raises(ValueError,match="unapproved_membership"):
        build(doc([bad]),scanner(),zones())
    with pytest.raises(ValueError,match="duplicate_or_empty"):
        build(doc([candidate("XPEV"),candidate("XPEV")]),scanner(),zones())
    with pytest.raises(ValueError,match="governance"):
        build(doc(),scanner(),{"research_only":False,"score_modified":False,"assets":[]})


def test_reporting_escapes_html_and_freezes_snapshot(tmp_path):
    out=build(doc([candidate("XPEV")]),scanner(),zones())
    out["items"][0]["name"]="</script><script>alert(1)</script>"
    j=tmp_path/"report.json";h=tmp_path/"radar-zones.html";archive=tmp_path/"history"
    write(out,json_output=j,html_output=h,history_dir=archive)
    assert "</script><script>alert(1)</script>" not in h.read_text()
    assert r"\u003c/script>" in h.read_text()
    assert len(list(archive.glob("*.json")))==1
    write(out,json_output=j,html_output=h,history_dir=archive)
    assert len(list(archive.glob("*.json")))==1


def test_current_real_radar_comparison_no_watchlist_changes():
    root=Path(__file__).resolve().parents[1]
    radar=json.loads((root/"argus-preview/observations/external_thesis_watch_candidates.json").read_text())
    full=_load_watch(root/"artifacts/watchlist/watchlist_full.csv")
    assert "UEC" in full
    out=build(radar,full,zones([]))
    assert out["count"]==24
    assert out["in_scanner"]==12
    assert out["radar_only"]==12
    assert out["daily_zones_available"]==0
    assert out["scanner_universe_modified"] is False
    assert out["orders_allowed"] is False
