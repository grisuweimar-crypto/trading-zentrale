from pathlib import Path
import json

import pytest

from scanner.research.governance.qm_b_evidence_impact import (
    EvidenceImpactError,
    evaluate,
    load_contract,
    require_pass,
)

ROOT = Path(__file__).resolve().parents[1]


def test_real_contract_evaluates_with_scoped_restrictions():
    result = evaluate(ROOT)
    require_pass(result)
    assert result["audit_gate_status"] == "PASS_WITH_SCOPED_RESTRICTIONS"
    assert result["source_document_count"] == 5
    assert result["impact_count"] == 7
    assert result["global_research_invalidation_required"] is False
    assert result["phase1_selection_timing_invalidation_required"] is False
    assert result["daily_research_historical_matcher_revalidation_required"] is True
    assert result["strict_asof_backtest_claim_allowed"] is False
    assert result["crypto_cross_namespace_merge_allowed"] is False
    assert result["strict_qm_b_promotion_ready"] is False


def test_every_impact_is_non_global_and_explicitly_classified():
    result = evaluate(ROOT)
    allowed = set(load_contract()["impact_classes"])
    assert all(row["impact_class"] in allowed for row in result["impacts"])
    assert all(row["automatic_global_invalidation"] is False for row in result["impacts"])


def test_source_document_change_fails_closed(tmp_path: Path):
    contract = load_contract()
    source = next(iter(contract["source_documents"].values()))
    target = tmp_path / source["path"]
    target.parent.mkdir(parents=True)
    target.write_text("changed\n", encoding="utf-8")
    contract["source_documents"] = {"changed": source}
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(EvidenceImpactError, match="source_document_changed_without_impact_review"):
        evaluate(tmp_path, contract_path=contract_path)


def test_strict_promotion_cannot_be_silently_enabled(tmp_path: Path):
    contract = load_contract()
    contract["expected_conclusions"]["strict_asof_backtest_claim_allowed"] = True
    for spec in contract["source_documents"].values():
        src = ROOT / spec["path"]
        dst = tmp_path / spec["path"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(EvidenceImpactError, match="strict_backtest_claim_must_remain_blocked"):
        evaluate(tmp_path, contract_path=contract_path)
