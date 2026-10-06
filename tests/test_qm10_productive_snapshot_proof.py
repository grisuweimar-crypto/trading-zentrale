import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def test_qm10_new_productive_snapshot_identity_and_guards():
    p=json.loads((ROOT/"configs/qm10_productive_snapshot_proof_v1.json").read_text())
    meta=json.loads((ROOT/"artifacts/research/history_metadata.json").read_text())
    daily=json.loads((ROOT/"artifacts/research/daily_research.json").read_text())
    w10=json.loads((ROOT/"artifacts/research/decision_snapshot_w10.json").read_text())
    runtime=json.loads((ROOT/"artifacts/research/watch_runtime/public_long_reference.json").read_text())
    manifest=json.loads((ROOT/"artifacts/research/watch_runtime/manifest.json").read_text())
    sid=p["snapshot_id"]
    assert sid==meta["snapshot_id"]==daily["snapshot_id"]==w10["snapshot_id"]==runtime["diagnostics"]["snapshot_id"]==manifest["snapshot_id"]
    assert meta["latest_run_complete"] is True and meta["validation"]["status"]=="ok"
    assert meta["validation"]["symbol_count"]==p["scanner_symbol_count"]==213
    assert runtime["row_count"]==p["decision_symbol_count"]==213
    assert runtime["diagnostics"]["scanner_scalar_fallback_used"] is False
    assert runtime["diagnostics"]["private_position_data_persisted"] is False
    assert runtime["diagnostics"]["missing_current_packet_symbols"]==[]
    assert w10["status"]=="sealed"
    assert w10["stages"]["phase6_elliott"]["status"]=="not_supplied"
    assert w10["stages"]["phase6_elliott"]["decision_effect"]=="none"
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
