"""CY-08 preflight regressions: read-only, fail closed, no evidence fabrication."""
from __future__ import annotations

import json
from pathlib import Path

from scripts.audit_cycle_cy08 import CONTRACTS, audit_cy08


ROOT = Path(__file__).resolve().parents[1]


def _seed(tmp_path: Path) -> Path:
    paths = [value[0] for value in CONTRACTS.values()] + [
        "configs/cycle_direction/cy05_preregistered_design_v1.json",
        "artifacts/cycle_history/manifest.json",
    ]
    for item in paths:
        target = tmp_path / item
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((ROOT / item).read_text(encoding="utf-8"), encoding="utf-8")
    return tmp_path


def _edit(root: Path, relative: str, change):
    target = root / relative
    payload = json.loads(target.read_text(encoding="utf-8"))
    change(payload)
    target.write_text(json.dumps(payload), encoding="utf-8")


def test_read_only_current_repository_never_promotes():
    result = audit_cy08(ROOT)
    assert result["read_only"] is True
    assert result["promotion_performed"] is False
    assert result["shadow_ablation_performed"] is False
    assert result["productive_decision_modified"] is False
    assert result["status"] in {"BLOCKED", "REVIEW_REQUIRED"}
    assert result["next_gate"].startswith("Exact independently verified")


def test_unsafe_l12_contract_fails_closed(tmp_path):
    root = _seed(tmp_path)
    _edit(root, CONTRACTS["L12"][0], lambda v: v.update({"productive_integration_enabled": True}))
    result = audit_cy08(root)
    assert result["status"] == "BLOCKED"
    assert "L12_CONTRACT_INVALID_OR_UNSAFE" in result["blockers"]
    assert result["promotion_performed"] is False


def test_artificial_archive_improvement_cannot_bypass_other_gates(tmp_path):
    root = _seed(tmp_path)
    _edit(root, "artifacts/cycle_history/manifest.json", lambda v: v.update({
        "snapshots": 100,
        "research_eligible": 1000,
        "provisional_lags": {"1": 500, "5": 400, "10": 100},
    }))
    result = audit_cy08(root)
    assert result["status"] == "BLOCKED"
    assert "CY03_NO_RESEARCH_ELIGIBLE_OBSERVATIONS" not in result["blockers"]
    assert "CY05_L1_RUN_FREEZE_NOT_DOCUMENTED" in result["blockers"]
    assert "L13_NET_COST_ABLATION_NOT_IMPLEMENTED" in result["blockers"]


def test_l14_generic_ready_receipt_is_not_cycle_promotion(tmp_path):
    root = _seed(tmp_path)
    latest = root / "artifacts/research/pattern_discovery/operations/latest.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps({"cycle_status": "READY_WITH_WORK"}), encoding="utf-8")
    result = audit_cy08(root)
    assert result["status"] == "BLOCKED"
    assert result["promotion_performed"] is False
    assert result["shadow_ablation_performed"] is False
