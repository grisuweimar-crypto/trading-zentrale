from scripts.check_watch_snapshot_pair import status

def test_waits_for_new_w10_and_does_not_backdate():
    daily = {"snapshot_id": "new", "as_of": "2026-10-13"}
    sealed = {"snapshot_id": "old", "status": "sealed", "stages": {"final_7a": {"status": "available"}, "phase7a_archive": {"status": "available"}}}
    result = status(daily, sealed)
    assert result["ready"] is False
    assert result["reason"] == "awaiting_w10_for_latest_scanner"
    assert sealed["snapshot_id"] == "old"

def test_accepts_consistent_sealed_pair():
    daily = {"snapshot_id": "same", "as_of": "2026-10-13"}
    sealed = {"snapshot_id": "same", "status": "sealed", "stages": {"final_7a": {"status": "available"}, "phase7a_archive": {"status": "available"}}}
    assert status(daily, sealed)["ready"] is True

def test_rejects_unsealed_pair():
    daily = {"snapshot_id": "same", "as_of": "2026-10-13"}
    sealed = {"snapshot_id": "same", "status": "open", "stages": {"final_7a": {"status": "available"}, "phase7a_archive": {"status": "available"}}}
    assert status(daily, sealed)["ready"] is False
