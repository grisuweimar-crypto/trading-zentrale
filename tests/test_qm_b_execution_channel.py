from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_execution_channel import (
    ExecutionChannelError,
    audit_membership_execution_gap,
    execution_status_as_of,
)


def rec(iid, channel, status, t, *, pit=True, source='test'):
    return {
        'instrument_id': iid,
        'channel_id': channel,
        'status': status,
        'valid_from': t,
        'pit_verified': pit,
        'source_id': source,
    }


def test_available_if_any_pit_channel_available():
    iid='urn:scanner:isin:US0378331005'
    result=execution_status_as_of(
        instrument_id=iid,
        as_of='2026-09-30T08:00:00Z',
        evidence_records=[
            rec(iid,'channel-a','UNAVAILABLE','2026-09-30T07:00:00Z'),
            rec(iid,'channel-b','AVAILABLE','2026-09-30T07:10:00Z'),
        ],
    )
    assert result['execution_channel_status']=='AVAILABLE'
    assert result['available_channel_ids']==['channel-b']


def test_missing_evidence_is_unknown():
    iid='urn:scanner:isin:US0378331005'
    result=execution_status_as_of(instrument_id=iid,as_of='2026-09-30T08:00:00Z',evidence_records=[])
    assert result['execution_channel_status']=='UNKNOWN'


def test_future_evidence_not_back_projected():
    iid='urn:scanner:isin:US0378331005'
    result=execution_status_as_of(
        instrument_id=iid,
        as_of='2026-09-30T08:00:00Z',
        evidence_records=[rec(iid,'channel-a','AVAILABLE','2026-10-01T08:00:00Z')],
    )
    assert result['execution_channel_status']=='UNKNOWN'


def test_restricted_preserved_when_no_available_channel():
    iid='urn:scanner:isin:US0378331005'
    result=execution_status_as_of(
        instrument_id=iid,
        as_of='2026-09-30T08:00:00Z',
        evidence_records=[rec(iid,'channel-a','RESTRICTED','2026-09-30T07:00:00Z')],
    )
    assert result['execution_channel_status']=='RESTRICTED'


def test_sensitive_account_fields_are_rejected():
    iid='urn:scanner:isin:US0378331005'
    row=rec(iid,'channel-a','AVAILABLE','2026-09-30T07:00:00Z')
    row['account_id']='forbidden'
    with pytest.raises(ExecutionChannelError, match='forbidden_sensitive_fields'):
        execution_status_as_of(instrument_id=iid,as_of='2026-09-30T08:00:00Z',evidence_records=[row])


def test_current_repository_has_no_execution_channel_evidence():
    latest=json.loads(Path('artifacts/research/qm/qm_b_membership/latest.json').read_text(encoding='utf-8'))
    membership=json.loads(Path(latest['normalized_snapshot_path']).read_text(encoding='utf-8'))
    expected=len(membership['claims'])
    assert expected==membership['stable_instrument_claim_count']
    result=audit_membership_execution_gap(
        membership,
        as_of=latest['observed_at'],
        evidence_records=[],
    )
    assert result['instrument_count']==expected
    assert result['status_counts']=={'UNKNOWN':expected}
    assert result['registered_channel_count']==0
    assert result['current_gap_status']=='BLOCKED_NO_EXECUTION_CHANNEL_EVIDENCE'
    assert result['historical_retrojection_permitted'] is False
