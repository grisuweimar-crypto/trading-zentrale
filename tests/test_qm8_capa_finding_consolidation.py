import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def _ledger_state():
    events=[json.loads(x) for x in (ROOT/"artifacts/research/qm/qm_h_capa_ledger.jsonl").read_text().splitlines() if x.strip()]
    state={}
    for event in events:
        fid=event["finding_id"]
        if event["event_type"]=="FINDING_REGISTERED":
            state[fid]={"status":"OPEN","evidence_impact":event["payload"]["evidence_impact"]}
        else:
            state[fid]["status"]=event["payload"]["to_status"]
            details=event["payload"].get("details",{})
            if "evidence_impact" in details:
                state[fid]["evidence_impact"]=details["evidence_impact"]
    return state

def test_qm8_authoritative_finding_states():
    c=json.loads((ROOT/"configs/qm8_capa_finding_consolidation_v1.json").read_text())
    state=_ledger_state()
    for fid, expected in c["required_findings"].items():
        assert state[fid]["status"]==expected["expected_state"]
        assert state[fid]["evidence_impact"]==expected["expected_evidence_impact"]
    assert c["automatic_promotion_allowed"] is False
    assert c["automatic_semantic_change_allowed"] is False

def test_qm8_lag1_cannot_be_closed_by_consolidation():
    c=json.loads((ROOT/"configs/qm8_capa_finding_consolidation_v1.json").read_text())
    lag=c["required_findings"]["QM-H-QMJ-PHASE1A-LAG1-001"]
    assert lag["expected_state"]=="IMPLEMENTED"
    assert lag["expected_evidence_impact"]=="PROMOTION_BLOCKED"
    assert lag["closure_allowed_without_prospective_effectiveness"] is False

def test_qm8_capacity_recurrence_does_not_rewrite_historical_f01():
    c=json.loads((ROOT/"configs/qm8_capa_finding_consolidation_v1.json").read_text())
    op=json.loads((ROOT/c["operational_recurrence_register"]["contract"]).read_text())
    findings={x["finding_id"]:x for x in op["findings"]}
    f1=findings["BA-QM10-F01"]; f5=findings["BA-QM10-F05"]
    assert f1["state"]=="CLOSED_EFFECTIVE"
    assert f1["effectiveness"]["evidence"]["published_shard_count"]==32
    assert f5["related_prior_finding_id"]=="BA-QM10-F01"
    assert f5["effectiveness"]["historical_f01_evidence_rewritten"] is False
