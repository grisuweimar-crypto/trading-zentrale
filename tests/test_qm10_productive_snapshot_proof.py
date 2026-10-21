import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def test_qm10_historical_proof_is_frozen_and_current_snapshot_identity_is_coherent():
    p=json.loads((ROOT/"configs/qm10_productive_snapshot_proof_v1.json").read_text())
    meta=json.loads((ROOT/"artifacts/research/history_metadata.json").read_text())
    daily=json.loads((ROOT/"artifacts/research/daily_research.json").read_text())
    w10=json.loads((ROOT/"artifacts/research/decision_snapshot_w10.json").read_text())
    runtime=json.loads((ROOT/"artifacts/research/watch_runtime/public_long_reference.json").read_text())
    manifest=json.loads((ROOT/"artifacts/research/watch_runtime/manifest.json").read_text())

    # Historical QM10 acceptance evidence belongs to 2026-10-06 (213
    # instruments). Do not rewrite it to follow the changing live scanner.
    assert p["as_of"] == "2026-10-06"
    assert p["scanner_symbol_count"] == p["decision_symbol_count"] == 213
    assert p["runtime_shard_count"] == 64

    # The actual current scanner/W10/runtime must agree with one another,
    # independently of the immutable earlier quality-management receipt.
    sid=meta["snapshot_id"]
    assert sid==daily["snapshot_id"]==w10["snapshot_id"]==runtime["diagnostics"]["snapshot_id"]==manifest["snapshot_id"]
    assert p["as_of"] <= daily["as_of"]
    if p["as_of"] < daily["as_of"]:
        assert sid != p["snapshot_id"]
    assert meta["latest_run_complete"] is True and meta["validation"]["status"]=="ok"
    assert meta["validation"]["symbol_count"]==daily["universe_size"]==runtime["row_count"]==manifest["symbol_count"]
    assert runtime["diagnostics"]["scanner_scalar_fallback_used"] is False
    assert runtime["diagnostics"]["private_position_data_persisted"] is False
    assert runtime["diagnostics"]["missing_current_packet_symbols"]==[]
    assert w10["status"]=="sealed"
    # Old QM10 proof on 06.10 is immutable; the *current* Elliott source
    # may now be supplied, but it must remain review-only (never a vote).
    assert p["phase6_elliott_status"] == "not_supplied"
    assert p["phase6_decision_effect"] == "none"
    phase = w10["stages"]["phase6_elliott"]
    assert phase["status"] == runtime["diagnostics"]["elliott_6h_source_status"]
    assert phase["status"] in {"available", "not_supplied"}
    assert runtime["diagnostics"]["elliott_changed_universal_stance"] is False
    assert runtime["diagnostics"]["elliott_direction_used_as_vote"] is False
    assert runtime["semantics"]["elliott_direction_used_as_vote"] is False
    if phase["status"] == "available":
        assert phase["snapshot_identity_state"] == "verified"
        assert phase["elliott_direction_used_as_vote"] is False
        assert phase["multi_degree_reducer_used"] is False
        assert phase["artifact_sha256"] and len(phase["artifact_sha256"]) == 64
        assert phase["symbol_count"] > 0
        assert phase["decision_effect"] == "review_only_downstream_of_7d_7e"
    else:
        assert phase["decision_effect"] == "none"
    assert manifest["decision_logic_changed"] is False
    assert manifest["private_position_data_included"] is False

def test_qm10_transport_capacity_and_lag1_block_remain_intact():
    p=json.loads((ROOT/"configs/qm10_productive_snapshot_proof_v1.json").read_text())
    assert p["runtime_shard_count"]==64
    assert p["max_runtime_shard_bytes"] < p["hard_transport_limit_bytes"]
    assert p["transport_headroom_ratio"] >= 0.20
    qmj=json.loads((ROOT/"configs/ba_qm7_qm_j_closure_v1.json").read_text())
    capa=qmj["open_capa"]
    assert capa["finding_id"]==p["lag1_finding_id"]
    assert capa["status"]=="IMPLEMENTED"
    assert capa["evidence_impact"]=="PROMOTION_BLOCKED"
    assert capa["automatic_release_allowed"] is False
    assert p["automatic_promotion_performed"] is False
