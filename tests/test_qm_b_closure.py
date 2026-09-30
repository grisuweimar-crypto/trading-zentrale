import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_closure import (
    EXPECTED_DISPLAY_STATUS,
    validate_closure_manifest,
)


MANIFEST = Path("configs/qm_b_closure_v1.json")


def load_manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_final_manifest_is_valid_and_fail_closed():
    payload = validate_closure_manifest(load_manifest())
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["research_governance_status"] == "COMPLETE"
    assert payload["strict_historical_promotion_status"] == "BLOCKED_EXTERNAL_EVIDENCE"
    assert payload["productive_strict_asof_investable_universe_promoted"] is False
    assert len(payload["external_blockers"]) == 3


@pytest.mark.parametrize(
    "field,value",
    [
        ("historical_retrojection_permitted", True),
        ("missing_is_neutral", True),
        ("unknown_is_promotable", True),
        ("productive_strict_asof_investable_universe_promoted", True),
        ("strict_historical_promotion_status", "COMPLETE"),
    ],
)
def test_unsafe_closure_states_fail(field, value):
    payload = load_manifest()
    payload[field] = value
    with pytest.raises(ValueError):
        validate_closure_manifest(payload)


def test_missing_required_completed_block_fails():
    payload = load_manifest()
    payload["completed_blocks"] = [
        item for item in payload["completed_blocks"]
        if item != "historical_matcher_session_alignment"
    ]
    with pytest.raises(ValueError):
        validate_closure_manifest(payload)


def test_external_blocker_cannot_be_relabelled_as_resolved_or_internal_task():
    payload = load_manifest()
    payload["external_blockers"] = copy.deepcopy(payload["external_blockers"])
    payload["external_blockers"][0]["state"] = "VERIFIED"
    payload["external_blockers"][0]["remaining_qm_b_engineering_task"] = True
    with pytest.raises(ValueError):
        validate_closure_manifest(payload)


def test_methodology_finding_must_be_resolved():
    payload = load_manifest()
    payload["resolved_internal_findings"][0]["status"] = "OPEN"
    with pytest.raises(ValueError):
        validate_closure_manifest(payload)
