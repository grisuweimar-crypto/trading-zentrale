from __future__ import annotations

import json
from pathlib import Path

from scanner.research.governance.qm_b_project_restrictions import build_project_restriction_evidence


def test_current_membership_is_clear_under_current_internal_project_policy():
    latest = json.loads(Path('artifacts/research/qm/qm_b_membership/latest.json').read_text(encoding='utf-8'))
    snapshot = json.loads(Path(latest['normalized_snapshot_path']).read_text(encoding='utf-8'))
    expected = len(snapshot['claims'])
    assert expected == snapshot['stable_instrument_claim_count']
    result = build_project_restriction_evidence(
        snapshot,
        policy_commit_sha='a' * 40,
        policy_observed_at=latest['observed_at'],
    )
    assert result['instrument_count'] == expected
    assert result['status_counts'] == {'CLEAR': expected}
    assert result['historical_retrojection_permitted'] is False
    assert result['legal_eligibility_evaluated'] is False
    assert result['broker_availability_evaluated'] is False
    assert result['market_tradability_evaluated'] is False
    assert result['listing_state_evaluated'] is False
    assert all(row['dimension'] == 'project_restrictions' for row in result['evidence'])
    assert all(row['status'] == 'CLEAR' for row in result['evidence'])
    assert all(row['valid_from'] == latest['observed_at'] for row in result['evidence'])
