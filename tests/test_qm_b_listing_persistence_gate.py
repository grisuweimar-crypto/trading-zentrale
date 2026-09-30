import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_listing_persistence_gate import (
    ListingPersistenceGateError,
    assess_persistence,
    require_persistence_allowed,
)


def test_current_nasdaq_source_is_blocked_for_public_repository():
    result = assess_persistence(
        "nasdaq_symbol_directory",
        persistence_scope="public_repository",
    )
    assert result["allowed"] is False
    assert result["gate_status"] == "BLOCKED"
    assert result["license_status"] == "REVIEW_REQUIRED"
    assert result["explicit_redistribution_clearance"] is False
    assert "NO_EXPLICIT_REDISTRIBUTION_CLEARANCE" in result["reason_codes"]
    assert result["may_be_committed_or_redistributed"] is False


def test_current_nasdaq_source_is_allowed_only_for_local_ephemeral_qm_test():
    result = assess_persistence(
        "nasdaq_symbol_directory",
        persistence_scope="local_ephemeral",
    )
    assert result["allowed"] is True
    assert result["gate_status"] == "ALLOWED_QM_TEST_ONLY"
    assert result["license_clearance_inferred"] is False
    assert result["may_be_committed_or_redistributed"] is False


def test_public_requirement_raises_fail_closed():
    with pytest.raises(ListingPersistenceGateError, match="persistence_blocked"):
        require_persistence_allowed(
            "nasdaq_symbol_directory",
            persistence_scope="public_repository",
        )


def test_explicit_clearance_and_usable_license_are_both_required(tmp_path: Path):
    source = {
        "schema_version": "qm_b_listing_metadata_sources_v1",
        "sources": [
            {
                "source_id": "cleared_source",
                "license_status": "USABLE",
                "access_status": "OPEN",
            }
        ],
    }
    policy = {
        "schema_version": "qm_b_listing_persistence_policy_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "persistence_scopes": ["local_ephemeral", "public_repository"],
        "local_ephemeral": {
            "allowed_for_qm_test_execution": True,
            "implies_license_clearance": False,
            "may_be_committed_or_redistributed": False,
        },
        "public_repository": {
            "default_allowed": False,
            "required_source_license_status": "USABLE",
            "requires_explicit_redistribution_clearance": True,
            "explicitly_cleared_source_ids": ["cleared_source"],
        },
    }
    source_path = tmp_path / "source.json"
    policy_path = tmp_path / "policy.json"
    source_path.write_text(json.dumps(source), encoding="utf-8")
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    result = require_persistence_allowed(
        "cleared_source",
        persistence_scope="public_repository",
        source_assessment_path=source_path,
        policy_path=policy_path,
    )
    assert result["allowed"] is True
    assert result["explicit_redistribution_clearance"] is True
    assert result["may_be_committed_or_redistributed"] is True


def test_usable_license_without_explicit_clearance_still_blocks(tmp_path: Path):
    source = {
        "schema_version": "qm_b_listing_metadata_sources_v1",
        "sources": [
            {
                "source_id": "uncleared_source",
                "license_status": "USABLE",
                "access_status": "OPEN",
            }
        ],
    }
    policy = {
        "schema_version": "qm_b_listing_persistence_policy_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "persistence_scopes": ["local_ephemeral", "public_repository"],
        "local_ephemeral": {
            "allowed_for_qm_test_execution": True,
            "implies_license_clearance": False,
            "may_be_committed_or_redistributed": False,
        },
        "public_repository": {
            "default_allowed": False,
            "required_source_license_status": "USABLE",
            "requires_explicit_redistribution_clearance": True,
            "explicitly_cleared_source_ids": [],
        },
    }
    source_path = tmp_path / "source.json"
    policy_path = tmp_path / "policy.json"
    source_path.write_text(json.dumps(source), encoding="utf-8")
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    result = assess_persistence(
        "uncleared_source",
        persistence_scope="public_repository",
        source_assessment_path=source_path,
        policy_path=policy_path,
    )
    assert result["allowed"] is False
    assert result["reason_codes"] == ["NO_EXPLICIT_REDISTRIBUTION_CLEARANCE"]


def test_unknown_source_cannot_be_assumed_safe():
    with pytest.raises(ListingPersistenceGateError, match="source_not_assessed"):
        assess_persistence("unknown_source", persistence_scope="public_repository")


def test_invalid_scope_is_rejected():
    with pytest.raises(ListingPersistenceGateError, match="persistence_scope_invalid"):
        assess_persistence("nasdaq_symbol_directory", persistence_scope="anything")
