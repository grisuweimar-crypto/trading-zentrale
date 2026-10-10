"""CYCLE-DIR future-readiness watch: synthetic gates plus current real-archive smoke."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from scripts import watch_cycle_science as watch


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def _root(tmp_path: Path) -> Path:
    p = tmp_path / watch.PREREG_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text((ROOT / watch.PREREG_PATH).read_text(encoding="utf-8"), encoding="utf-8")
    return tmp_path


def _inspection(snapshots=1, lags=None):
    return {
        "snapshots": snapshots,
        "observations": snapshots * 215,
        "as_of": "2026-10-10",
        "internally_replayable": snapshots * 209,
        "excluded": snapshots * 6,
        "lag_status_counts": {key: {"PROVISIONAL_CHAIN": (lags or {}).get(key, 0)} for key in ("1", "5", "10")},
        "research_eligible": 0,
        "independent_external_pit_verified": False,
        "research_released": False,
        "research_gate": "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269",
    }


def _stub(monkeypatch, result=None, keep_anchor=True):
    monkeypatch.setattr(watch, "inspect_cycle_archive", lambda root: result or _inspection())
    import scanner.reports.cycle_history as mod
    original = {"snapshot_id": watch.ORIGINAL_SNAPSHOT if keep_anchor else "other", "as_of": "2026-10-10"}
    monkeypatch.setattr(mod, "read_ledger", lambda p: [original])


def test_real_current_archive_is_read_only_and_remains_source_blocked():
    actual = watch.audit_scientific_readiness(ROOT, observed_at=datetime.now(timezone.utc), issue_269_state="OPEN")
    assert actual["gates"]["CY03_internal_archive"] == "PASS_INTERNAL_ONLY"
    assert actual["observations"]["original_snapshot_retained"] is True
    assert actual["research_release_performed"] is False
    assert actual["promotion_performed"] is False
    assert actual["production_effect"] is False
    assert "CY03_V1_HAS_NO_RESEARCH_RELEASE_CONTRACT" in actual["blockers"]


def test_issue_closure_does_not_certify_provider_PIT(monkeypatch, tmp_path):
    _stub(monkeypatch)
    actual = watch.audit_scientific_readiness(_root(tmp_path), observed_at=NOW, issue_269_state="CLOSED")
    assert actual["status"] == "BLOCKED"
    assert actual["gates"]["CY02_external_source_PIT"] == "NOT_VERIFIED"
    assert "ISSUE_269_CLOSED_BUT_SOURCE_PIT_RELEASE_NOT_CERTIFIED" in actual["alerts"]
    assert actual["observations"]["research_eligible_count"] == 0


def test_lag_1_5_10_growth_is_visible_but_not_promoted(monkeypatch, tmp_path):
    _stub(monkeypatch, result=_inspection(snapshots=22, lags={"1": 400, "5": 150, "10": 50}))
    result = watch.audit_scientific_readiness(_root(tmp_path), observed_at=NOW, issue_269_state="OPEN")
    assert result["observations"]["snapshot_count"] == 22
    assert result["observations"]["provisional_lag_5obs"] == 150
    assert result["gates"]["CY03_lag_10obs"] == "PROVISIONAL_ONLY"
    assert "CY03_NO_5OBS_CHAIN" not in result["blockers"]
    assert result["status"] == "BLOCKED"
    assert result["gates"]["CY03_research_release"] == "BLOCKED_BY_VERSIONED_V1_CONTRACT"


def test_corrupt_archive_fails_closed(monkeypatch, tmp_path):
    def broken(_):
        raise ValueError("cy05:cycle_archive_integrity_failed")
    monkeypatch.setattr(watch, "inspect_cycle_archive", broken)
    result = watch.audit_scientific_readiness(tmp_path, observed_at=NOW)
    assert result["status"] == "INTEGRITY_FAILURE"
    assert "CY03_ARCHIVE_INTEGRITY_FAILURE" in result["blockers"]
    assert result["promotion_performed"] is False


def test_original_snapshot_loss_detected(monkeypatch, tmp_path):
    _stub(monkeypatch, keep_anchor=False)
    result = watch.audit_scientific_readiness(_root(tmp_path), observed_at=NOW)
    assert result["status"] == "INTEGRITY_FAILURE"
    assert "CY03_ORIGINAL_SNAPSHOT_REMOVED" in result["blockers"]


def test_fake_l1_files_do_not_complete_freeze(monkeypatch, tmp_path):
    _stub(monkeypatch)
    root = _root(tmp_path)
    fake = root / watch.ROOT / "discovery_runs" / "FAKE" / "manifest.json"
    fake.parent.mkdir(parents=True, exist_ok=True)
    fake.write_text(json.dumps({"FAKE": True}), encoding="utf-8")
    result = watch.audit_scientific_readiness(root, observed_at=NOW)
    assert result["gates"]["CY05_L1_real_run_freeze"] == "ARTIFACTS_REQUIRE_PIT_AUDIT"
    assert result["research_release_performed"] is False
    assert result["status"] == "BLOCKED"


def test_design_tampering_fails_closed(monkeypatch, tmp_path):
    _stub(monkeypatch)
    root = _root(tmp_path)
    p = root / watch.PREREG_PATH
    design = json.loads(p.read_text())
    design["execution_allowed"] = True
    p.write_text(json.dumps(design), encoding="utf-8")
    result = watch.audit_scientific_readiness(root, observed_at=NOW)
    assert result["status"] == "INTEGRITY_FAILURE"
    assert "CY05_DESIGN_CONTRACT_INVALID" in result["blockers"]


def test_old_publication_warns_not_invalidates_history(monkeypatch, tmp_path):
    _stub(monkeypatch)
    result = watch.audit_scientific_readiness(
        _root(tmp_path), observed_at=datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    )
    assert result["status"] == "BLOCKED"
    assert result["gates"]["CY03_freshness"] == "STALE"
    assert "CY03_NO_RECENT_ARCHIVE_PUBLICATION_OVER_3_DAYS" in result["alerts"]


def test_unproven_research_release_fails_closed(monkeypatch, tmp_path):
    altered = _inspection()
    altered["research_released"] = True
    _stub(monkeypatch, result=altered)
    result = watch.audit_scientific_readiness(_root(tmp_path), observed_at=NOW)
    assert result["status"] == "INTEGRITY_FAILURE"
    assert "CY03_V1_FALSE_RESEARCH_RELEASE" in result["blockers"]


def test_cy03_ci_checks_growing_history_and_keeps_v1_gate():
    workflow = (ROOT / ".github/workflows/cycle_dir_cy03.yml").read_text(encoding="utf-8")
    assert 'assert manifest["observations"] == len(ledger)' in workflow
    assert 'assert manifest["snapshots"] == len({r["snapshot_id"] for r in ledger})' in workflow
    assert 'assert manifest["snapshots"] == 1' not in workflow
    assert 'assert all(v == 0 for v in manifest["provisional_lags"].values())' not in workflow
    assert 'manifest["research_gate"] == "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269"' in workflow


def test_watch_action_is_triggered_by_real_scanner_and_daily_with_readonly_permissions():
    workflow = (ROOT / ".github/workflows/cycle_dir_science_watch.yml").read_text(encoding="utf-8")
    assert 'workflows: ["Scanner_vNext Autopilot"]' in workflow
    assert 'types: [completed]' in workflow
    assert 'schedule:' in workflow
    assert 'cron: "50 22 * * *"' in workflow
    assert 'contents: read' in workflow
    assert 'issues: read' in workflow
    assert 'actions/upload-artifact@v4' in workflow
    assert 'github.event.workflow_run.conclusion == \'success\'' in workflow
