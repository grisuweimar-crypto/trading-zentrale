"""QM8 orchestration: pipeline success without an integrate job must defer."""
import pytest
from scanner.research.governance.ba_qm8_workflow_gate import classify_integration_jobs


def test_performed_decision_integration_unblocks_full_qm8():
    assert classify_integration_jobs({"jobs": [
        {"name": "readiness", "status": "completed", "conclusion": "success"},
        {"name": "integrate", "status": "completed", "conclusion": "success"},
    ]}) == "RUN"


def test_skipped_decision_integration_is_deferred_not_successful_audit():
    assert classify_integration_jobs({"jobs": [
        {"name": "readiness", "status": "completed", "conclusion": "success"},
        {"name": "integrate", "status": "completed", "conclusion": "skipped"},
    ]}) == "DEFER"


@pytest.mark.parametrize("jobs", [
    [],
    [{"name": "readiness", "status": "completed", "conclusion": "success"}],
    [{"name": "integrate", "status": "completed", "conclusion": "success"},
     {"name": "integrate", "status": "completed", "conclusion": "success"}],
    [{"name": "integrate", "status": "in_progress", "conclusion": None}],
    [{"name": "integrate", "status": "completed", "conclusion": "failure"}],
])
def test_ambiguous_or_failed_upstream_cannot_be_blessed_as_qm8_pass(jobs):
    with pytest.raises(ValueError, match="qm8_gate"):
        classify_integration_jobs({"jobs": jobs})
