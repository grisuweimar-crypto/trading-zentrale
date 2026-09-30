from __future__ import annotations

import pytest

from scanner.research.decision_layer.prospective_gate import (
    DEPLOYMENT_COMMIT,
    PROSPECTIVE_NOT_BEFORE,
    ProspectiveBoundaryError,
    assert_orchestration_snapshot_eligible,
)


def test_pre_deployment_snapshot_is_rejected():
    with pytest.raises(ProspectiveBoundaryError, match="snapshot_before_depot_watch_prospective_boundary"):
        assert_orchestration_snapshot_eligible({
            "generated_at": "2026-09-29T18:00:00+00:00",
        })


def test_exact_deployment_boundary_is_allowed_and_auditable():
    result = assert_orchestration_snapshot_eligible({
        "generated_at": PROSPECTIVE_NOT_BEFORE,
    })
    assert result["prospective_boundary_passed"] is True
    assert result["prospective_not_before"] == "2026-09-30T08:47:58+00:00"
    assert result["deployment_commit"] == DEPLOYMENT_COMMIT
    assert result["retroactive_backfill_allowed"] is False


def test_post_deployment_snapshot_is_allowed():
    result = assert_orchestration_snapshot_eligible({
        "generated_at": "2026-09-30T17:10:00+00:00",
    })
    assert result["prospective_boundary_passed"] is True


def test_naive_or_missing_generated_at_fails_closed():
    with pytest.raises(ProspectiveBoundaryError, match="daily_generated_at_timezone_required"):
        assert_orchestration_snapshot_eligible({"generated_at": "2026-09-30T17:10:00"})
    with pytest.raises(ProspectiveBoundaryError, match="daily_generated_at_required"):
        assert_orchestration_snapshot_eligible({})
