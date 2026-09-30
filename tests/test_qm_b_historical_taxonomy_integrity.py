from __future__ import annotations

import json

import pytest

from scanner.research.governance.qm_b_historical_taxonomy_integrity import (
    HistoricalTaxonomyIntegrityError,
    evaluate,
    load_contract,
    require_pass,
)


def _scan(*paths):
    return {
        "schema_version": "qm_b_historical_taxonomy_scan_v1",
        "files_scanned": 10,
        "high_priority_count": 1,
        "candidates": [{"path": path} for path in paths],
    }


def _contract(*rows):
    return {
        "dispositions": list(rows),
        "confirmed_current_taxonomy_dependencies": 0,
        "confirmed_current_metadata_retrojections": 0,
    }


def test_all_reviewed_safe_candidates_pass():
    result = evaluate(
        _scan("a.py", "b.py"),
        _contract(
            {"path": "a.py", "status": "SAFE", "reason": "stored contemporaneously"},
            {"path": "b.py", "status": "SAFE", "reason": "current display only"},
        ),
    )
    assert result["gate_status"] == "PASS"
    assert result["safe_count"] == 2
    require_pass(result)


def test_new_unreviewed_candidate_fails_closed():
    result = evaluate(
        _scan("a.py", "new.py"),
        _contract({"path": "a.py", "status": "SAFE", "reason": "reviewed"}),
    )
    assert result["gate_status"] == "FAIL"
    assert result["unreviewed_candidates"] == ["new.py"]
    with pytest.raises(HistoricalTaxonomyIntegrityError):
        require_pass(result)


def test_stale_disposition_fails():
    result = evaluate(
        _scan("a.py"),
        _contract(
            {"path": "a.py", "status": "SAFE", "reason": "reviewed"},
            {"path": "gone.py", "status": "SAFE", "reason": "stale"},
        ),
    )
    assert result["gate_status"] == "FAIL"
    assert result["stale_dispositions"] == ["gone.py"]


def test_confirmed_dependency_cannot_pass_as_safe():
    contract = _contract({"path": "a.py", "status": "CURRENT_TAXONOMY_DEPENDENCY", "reason": "confirmed"})
    contract["confirmed_current_taxonomy_dependencies"] = 1
    result = evaluate(_scan("a.py"), contract)
    assert result["gate_status"] == "FAIL"
    assert result["confirmed_current_taxonomy_dependencies"] == ["a.py"]


def test_review_required_cannot_pass():
    result = evaluate(
        _scan("a.py"),
        _contract({"path": "a.py", "status": "REVIEW_REQUIRED", "reason": "needs review"}),
    )
    assert result["gate_status"] == "FAIL"


def test_contract_forbids_retrojection(tmp_path):
    path = tmp_path / "contract.json"
    path.write_text(json.dumps({
        "schema_version": "qm_b_historical_taxonomy_integrity_v1",
        "rules": {
            "current_taxonomy_may_define_historical_sample_membership": True,
            "current_taxonomy_may_retroactively_enrich_historical_rows": False,
            "missing_historical_taxonomy_may_be_filled_from_current_metadata": False,
            "new_candidate_requires_explicit_review": True,
        },
    }), encoding="utf-8")
    with pytest.raises(HistoricalTaxonomyIntegrityError):
        load_contract(path)
