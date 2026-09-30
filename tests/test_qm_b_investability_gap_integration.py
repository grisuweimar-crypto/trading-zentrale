from __future__ import annotations

import json
from pathlib import Path

from scanner.research.governance.qm_b_investability_gap_integration import audit_investability_as_of
from scanner.research.governance.qm_b_project_restrictions import build_project_restriction_evidence


def test_project_restrictions_close_exactly_one_current_gap_without_retrojection():
    latest = json.loads(Path('artifacts/research/qm/qm_b_membership/latest.json').read_text(encoding='utf-8'))
    membership = json.loads(Path(latest['normalized_snapshot_path']).read_text(encoding='utf-8'))
    policy_time = '2026-09-30T07:00:00Z'
    restrictions = build_project_restriction_evidence(
        membership,
        policy_commit_sha='a' * 40,
        policy_observed_at=policy_time,
    )
    result = audit_investability_as_of(
        membership,
        as_of=policy_time,
        additional_evidence=restrictions['evidence'],
    )
    assert result['instrument_count'] == 207
    assert result['status_counts'] == {'UNKNOWN': 207}
    assert result['strict_universe_promotion_ready_count'] == 0
    assert result['evidence_gap_counts']['stable_identity'] == 0
    assert result['evidence_gap_counts']['project_membership'] == 0
    assert result['evidence_gap_counts']['project_restrictions'] == 0
    assert result['evidence_gap_counts']['listing_state'] == 207
    assert result['evidence_gap_counts']['market_tradability'] == 207
    assert result['evidence_gap_counts']['execution_channel'] == 207
    assert result['as_of'] == '2026-09-30T07:00:00+00:00'
    assert result['historical_retrojection_permitted'] is False
