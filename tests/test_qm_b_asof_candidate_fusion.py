from copy import deepcopy

import pytest

from scanner.research.governance.qm_b_asof_candidate_fusion import (
    AsOfCandidateFusionError,
    fuse_asof_candidates,
)


def membership_snapshot():
    return {
        "schema_version": "qm_b_prospective_membership_snapshot_v1",
        "membership_snapshot_id": "qmbm_test",
        "universe_snapshot_id": "git:abc:data/inputs/universe_master.csv",
        "universe_observed_at": "2026-09-30T05:00:00+00:00",
        "membership_valid_from": "2026-09-30T05:00:00+00:00",
        "universe_sha256": "a" * 64,
        "historical_retrojection_permitted": False,
        "absence_interpreted_as_out_of_scope": False,
        "claims": [
            {
                "instrument_id": "urn:scanner:isin:US0378331005",
                "canonical_isin": "US0378331005",
                "membership_observation_status": "OBSERVED_IN_PROJECT_UNIVERSE",
                "membership_valid_from": "2026-09-30T05:00:00+00:00",
            },
            {
                "instrument_id": "urn:scanner:isin:US5949181045",
                "canonical_isin": "US5949181045",
                "membership_observation_status": "EXPLICIT_INACTIVE_ROWS_ONLY",
                "membership_valid_from": "2026-09-30T05:00:00+00:00",
            },
        ],
    }


def listing_snapshot():
    return {
        "schema_version": "qm_b_prospective_listing_snapshot_v1",
        "snapshot_id": "qmbls_test",
        "source_id": "nasdaq_symbol_directory",
        "retrieved_at": "2026-09-30T05:10:00+00:00",
        "valid_from": "2026-09-30T05:10:00+00:00",
        "historical_retrojection_permitted": False,
        "records": [
            {
                "source_symbol": "AAPL",
                "venue_namespace": "nasdaq_symbol_directory",
                "venue_code": "NASDAQ",
                "listing_state": "LISTED_IN_SNAPSHOT",
            }
        ],
    }


def identity_match():
    return {
        "schema_version": "qm_b_prospective_identity_match_result_v1",
        "source_snapshot_id": "qmbls_test",
        "source_id": "nasdaq_symbol_directory",
        "universe_snapshot_id": "git:abc:data/inputs/universe_master.csv",
        "universe_observed_at": "2026-09-30T05:00:00+00:00",
        "universe_sha256": "a" * 64,
        "identity_match_valid_from": "2026-09-30T05:10:00+00:00",
        "historical_retrojection_permitted": False,
        "matches": [
            {
                "source_record_index": 1,
                "match_status": "MATCHED",
                "candidate_instrument_id": "urn:scanner:isin:US0378331005",
                "canonical_isin": "US0378331005",
                "identity_valid_from": "2026-09-30T05:10:00+00:00",
            }
        ],
    }


def test_missing_listing_evidence_fails_closed():
    result = fuse_asof_candidates(membership_snapshot())
    assert result["fused_candidate_count"] == 0
    assert result["status_counts"] == {
        "BLOCKED_MEMBERSHIP_NOT_POSITIVE": 1,
        "BLOCKED_NO_LISTING_EVIDENCE": 1,
    }
    assert result["strict_bundle_promotion_performed"] is False
    assert result["project_investability_promotion_performed"] is False


def test_positive_three_way_fusion_uses_latest_evidence_time():
    result = fuse_asof_candidates(
        membership_snapshot(), listing_snapshot=listing_snapshot(), identity_match=identity_match()
    )
    fused = [row for row in result["candidates"] if row["candidate_status"] == "FUSED_LISTED_MEMBER_CANDIDATE"]
    assert len(fused) == 1
    assert fused[0]["instrument_id"] == "urn:scanner:isin:US0378331005"
    assert fused[0]["listing_venue"] == "NASDAQ"
    assert fused[0]["candidate_valid_from"] == "2026-09-30T05:10:00+00:00"
    assert fused[0]["identity_membership_listing_fused"] is True
    assert fused[0]["strict_bundle_promotion_ready"] is False
    assert fused[0]["project_investability_status"] == "UNKNOWN"


def test_later_membership_time_controls_candidate_boundary():
    membership = membership_snapshot()
    membership["universe_observed_at"] = "2026-09-30T06:00:00+00:00"
    membership["membership_valid_from"] = "2026-09-30T06:00:00+00:00"
    for claim in membership["claims"]:
        claim["membership_valid_from"] = "2026-09-30T06:00:00+00:00"
    match = identity_match()
    match["universe_observed_at"] = "2026-09-30T06:00:00+00:00"
    match["identity_match_valid_from"] = "2026-09-30T06:00:00+00:00"
    match["matches"][0]["identity_valid_from"] = "2026-09-30T06:00:00+00:00"
    result = fuse_asof_candidates(membership, listing_snapshot=listing_snapshot(), identity_match=match)
    fused = next(row for row in result["candidates"] if row["candidate_status"] == "FUSED_LISTED_MEMBER_CANDIDATE")
    assert fused["candidate_valid_from"] == "2026-09-30T06:00:00+00:00"


def test_universe_hash_mismatch_is_hard_error():
    match = identity_match()
    match["universe_sha256"] = "b" * 64
    with pytest.raises(AsOfCandidateFusionError, match="identity_membership_universe_hash_mismatch"):
        fuse_asof_candidates(membership_snapshot(), listing_snapshot=listing_snapshot(), identity_match=match)


def test_listing_source_mismatch_is_hard_error():
    listing = listing_snapshot()
    listing["snapshot_id"] = "different"
    with pytest.raises(AsOfCandidateFusionError, match="listing_identity_source_snapshot_mismatch"):
        fuse_asof_candidates(membership_snapshot(), listing_snapshot=listing, identity_match=identity_match())


def test_nonpositive_listing_state_is_blocked():
    listing = listing_snapshot()
    listing["records"][0]["listing_state"] = "UNKNOWN"
    result = fuse_asof_candidates(membership_snapshot(), listing_snapshot=listing, identity_match=identity_match())
    assert result["status_counts"]["BLOCKED_LISTING_NOT_POSITIVE"] == 1
    assert result["fused_candidate_count"] == 0


def test_missing_venue_is_blocked():
    listing = listing_snapshot()
    listing["records"][0]["venue_code"] = None
    result = fuse_asof_candidates(membership_snapshot(), listing_snapshot=listing, identity_match=identity_match())
    assert result["status_counts"]["BLOCKED_VENUE_MISSING"] == 1
    assert result["fused_candidate_count"] == 0


def test_duplicate_same_venue_requires_review():
    listing = listing_snapshot()
    listing["records"].append(deepcopy(listing["records"][0]))
    match = identity_match()
    match["matches"].append(deepcopy(match["matches"][0]))
    match["matches"][1]["source_record_index"] = 2
    result = fuse_asof_candidates(membership_snapshot(), listing_snapshot=listing, identity_match=match)
    assert result["status_counts"]["REVIEW_REQUIRED_DUPLICATE_LISTING_VENUE"] == 1
    assert result["fused_candidate_count"] == 0


def test_retrojection_guard_is_mandatory():
    membership = membership_snapshot()
    membership["historical_retrojection_permitted"] = True
    with pytest.raises(AsOfCandidateFusionError, match="membership_snapshot_retrojection_guard_missing"):
        fuse_asof_candidates(membership)
