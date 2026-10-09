"""Independent closure contract for the 2026-10-09 Elliott 6H transport and PIT CAPAs.

This verifies only QM-H engineering effectiveness records; it is not
predictive validity, model approval, portfolio advice or execution.
"""
from __future__ import annotations

from pathlib import Path

from scanner.research.governance.qm_h_capa import CapaLedger


ROOT = Path(__file__).resolve().parents[1]
LEDGER_PATH = ROOT / "artifacts/research/qm/qm_h_capa_ledger.jsonl"

BASELINE_SEQUENCE = 47
BASELINE_HEAD_SHA256 = (
    "13637f9a60c087ff12207a67e91cdc095e3fd7c4d8c29367ca14c46a6ba5c942"
)

CLOSED_TECHNICAL_FINDINGS = {
    "QM-H-ELLIOTT-6H-PIT-242": "QM-H-CAPA-ELLIOTT-6H-PIT-242",
    "QM-H-ELLIOTT-6H-ARCHIVE-254": "QM-H-CAPA-ELLIOTT-6H-ARCHIVE-254",
}


def test_elliott_242_254_are_append_only_valid_technical_closures():
    ledger = CapaLedger(LEDGER_PATH)
    verification = ledger.verify_integrity()
    assert verification["valid"] is True
    assert verification["event_count"] >= BASELINE_SEQUENCE + 14

    # The original audited 47-event QM-H history is immutable even though
    # independent Elliott CAPAs were appended after it.
    events = ledger._read_raw_events()
    assert events[BASELINE_SEQUENCE - 1]["entry_hash"] == BASELINE_HEAD_SHA256
    assert events[BASELINE_SEQUENCE]["previous_event_hash"] == BASELINE_HEAD_SHA256

    for finding_id, capa_id in CLOSED_TECHNICAL_FINDINGS.items():
        finding = ledger.get_finding(finding_id)
        assert finding["category"] == "DEFECT"
        assert finding["status"] == "CLOSED"
        assert finding["capa_id"] == capa_id
        assert finding["evidence_impact"] == "EVIDENCE_REVIEW_REQUIRED"
        assert finding_id not in verification["open_findings"]

        matching = [
            event for event in events if event["finding_id"] == finding_id
        ]
        assert [event["event_type"] for event in matching] == [
            "FINDING_REGISTERED",
            *["FINDING_TRANSITION"] * 6,
        ]
        transitions = [
            event["payload"] for event in matching[1:]
        ]
        assert [transition["to_status"] for transition in transitions] == [
            "TRIAGED", "ROOT_CAUSE_IDENTIFIED", "ACTION_PLANNED",
            "IMPLEMENTED", "EFFECTIVENESS_VERIFIED", "CLOSED",
        ]
        effective = transitions[-2]["details"]
        assert effective["effectiveness_result"] == "EFFECTIVE"
        assert "37921435520" in effective["effectiveness_evidence_reference"]
        closed = transitions[-1]["details"]
        assert closed["evidence_disposition_reference"].startswith("docs/")
        assert (ROOT / closed["evidence_disposition_reference"].split("#")[0]).is_file()


def test_elliott_capa_closure_does_not_close_lag1_or_promote_research():
    ledger = CapaLedger(LEDGER_PATH)
    lag1 = ledger.get_finding("QM-H-QMJ-PHASE1A-LAG1-001")
    assert lag1["evidence_impact"] == "PROMOTION_BLOCKED"
    assert lag1["status"] != "CLOSED"

    pit_doc = (ROOT / "docs/QM_H_ELLIOTT_242_PIT_CAPA_CLOSURE_2026-10-09.md").read_text(encoding="utf-8")
    archive_doc = (ROOT / "docs/QM_H_ELLIOTT_254_ARCHIVE_CAPA_CLOSURE_2026-10-09.md").read_text(encoding="utf-8")
    for doc in (pit_doc, archive_doc):
        assert "EVIDENCE_REVIEW_REQUIRED" in doc
        assert "empirical" in doc.lower()
        assert "37921435520" in doc
