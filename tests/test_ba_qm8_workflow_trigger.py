from pathlib import Path

import yaml


PROJECT = Path(__file__).resolve().parents[1]


def test_ba_qm8_runs_after_successful_main_decision_watch_and_is_consolidated() -> None:
    path = PROJECT / ".github" / "workflows" / "ba_qm8_scanner_e2e_audit.yml"
    workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)

    triggers = workflow["on"]
    assert triggers["workflow_run"]["workflows"] == ["Decision Watch Integration Pipeline"]
    assert triggers["workflow_run"]["types"] == ["completed"]

    job = workflow["jobs"]["ba-qm8-audit"]
    condition = job["if"]
    assert "github.event.workflow_run.conclusion == 'success'" in condition
    assert "github.event.workflow_run.head_branch == 'main'" in condition

    concurrency = workflow["concurrency"]
    assert concurrency["cancel-in-progress"] == "true"
    assert "ba-qm8-main-prospective-audit" in concurrency["group"]


def test_ba_qm8_workflow_accepts_historical_and_prospective_states() -> None:
    path = PROJECT / ".github" / "workflows" / "ba_qm8_scanner_e2e_audit.yml"
    workflow_text = path.read_text(encoding="utf-8")

    assert 'if data_status == "PARTIAL":' in workflow_text
    assert 'elif data_status == "PASS":' in workflow_text
    assert 'transitions["transition_pass_count"] == 9' in workflow_text
    assert 'transitions["transition_pass_count"] == 10' in workflow_text
    assert 'closure["status"] == "PENDING_PROSPECTIVE_SNAPSHOT"' in workflow_text
    assert (
        'closure["status"] == "ELIGIBLE_FOR_BA_QM8_ENGINEERING_CLOSURE"'
        in workflow_text
    )
    assert 'closure["lag1_evidence_impact"] == "PROMOTION_BLOCKED"' in workflow_text
