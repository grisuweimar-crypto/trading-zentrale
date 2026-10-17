from scripts.pattern_discovery.collect_uran_ab_shadow import build_observation, persist_observation
import pytest


def fixture():
    daily = {
        "snapshot_id": "snapshot-new-0001", "source_snapshot_id": "snapshot-new-0001",
        "as_of": "2026-10-15",
        "generated_at": "2026-10-15T19:00:00Z",
        "symbols": {
            "UEC": {"current": {"r_code": "R1", "rs3m": -0.12, "trend200": -0.35, "close": 8.2}},
            "UUUU": {"current": {"r_code": "R1", "rs3m": -0.23, "trend200": -0.44, "close": 9.4}},
        },
    }
    w10 = {"snapshot_id": "snapshot-new-0001", "status": "sealed", "sealed_at": "2026-10-15T19:20:00Z"}
    packets = {
        s: {"source_snapshot_id": "snapshot-new-0001", "universal_stance": {"state": "insufficient_evidence"}}
        for s in ("UEC", "UUUU")
    }
    return daily, w10, packets


def test_matched_pair_is_unmatured_and_not_a_trade():
    report, status = build_observation(*fixture(), "2026-10-15T19:21:00Z", {"daily": "hash"})
    assert status == "CAPTURED"
    assert len(report["paired_symbols"]) == 2
    assert report["future_outcomes_used"] is False
    assert report["productive_decision_change"] is False
    assert report["L7_claim_created"] is False
    for row in report["paired_symbols"]:
        assert row["B_hypothesis_condition"] is True
        assert row["A_private_7h_action"] == "NOT_SUPPLIED"
        assert row["B_trade_signal"] == "NONE"
        assert row["outcome"] == "UNMATURED"


def test_pre_registration_snapshot_cannot_be_credited():
    daily, w10, packets = fixture()
    daily["generated_at"] = "2026-10-13T19:00:00Z"
    w10["sealed_at"] = "2026-10-13T19:20:00Z"
    report, status = build_observation(daily, w10, packets, "2026-10-14T18:00:00Z", {})
    assert report is None
    assert status == "SKIP_BEFORE_REGISTRATION"


def test_w10_snapshot_mismatch_is_error():
    daily, w10, packets = fixture()
    w10["snapshot_id"] = "other"
    with pytest.raises(ValueError, match="snapshot_not_identical"):
        build_observation(daily, w10, packets, "2026-10-15T19:21:00Z", {})


def test_stale_snapshot_not_backfilled():
    report, status = build_observation(*fixture(), "2026-10-18T19:21:00Z", {})
    assert report is None
    assert status == "SKIP_STALE_W10_NO_RETROSPECTIVE_CAPTURE"


def test_missing_public_baseline_fails_closed():
    daily, w10, packets = fixture()
    packets.pop("UEC")
    with pytest.raises(ValueError, match="paired_current_packets_missing"):
        build_observation(daily, w10, packets, "2026-10-15T19:21:00Z", {})


def test_missing_features_produce_unknown_not_negative_label():
    daily, w10, packets = fixture()
    daily["symbols"]["UEC"]["current"].pop("rs3m")
    report, _ = build_observation(daily, w10, packets, "2026-10-15T19:21:00Z", {})
    assert report["paired_symbols"][0]["B_hypothesis_condition"] is None


def test_persist_is_idempotent_and_never_overwrites(tmp_path):
    report, _ = build_observation(*fixture(), "2026-10-15T19:21:00Z", {"daily": "hash"})
    first = persist_observation(tmp_path, report)
    assert first["status"] == "CAPTURED"
    target = tmp_path / first["path"]
    before = target.read_bytes()
    second = persist_observation(tmp_path, report)
    assert second["status"] == "ALREADY_CAPTURED"
    assert target.read_bytes() == before
    other = dict(report)
    other["source_sha256"] = {"daily": "tampered"}
    with pytest.raises(ValueError, match="immutable_capture_collision"):
        persist_observation(tmp_path, other)
    assert target.read_bytes() == before
