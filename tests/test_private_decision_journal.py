from copy import deepcopy
import pytest

from scanner.research.decision_layer.private_decision_journal import append, verify


def assessment():
    return {
        "schema_version": "argus_private_decision_journal_v1",
        "mode": "prospective", "kind": "argus_assessment",
        "symbol": "TEST", "subject_id": "TEST:case1",
        "recorded_at": "2026-10-12T11:00:00+02:00",
        "effective_at": "2026-10-12T11:00:00+02:00",
        "evidence_available_at": "2026-10-12T10:59:00+02:00",
        "snapshot_id": "sealed-w10-1", "argus_version": "7h",
        "assessment": {"stance": "insufficient_evidence"},
        "validation_eligible": True,
    }


def test_private_journal_append_verify_and_idempotence(tmp_path):
    out = tmp_path / "private" / "journal.jsonl"
    root = tmp_path / "repo"
    root.mkdir()
    entry = assessment()
    eid = append(out, entry, repo_root=root)
    assert append(out, entry, repo_root=root) == eid
    assert verify(out) == 1
    result = {
        "schema_version": "argus_private_decision_journal_v1",
        "mode": "prospective", "kind": "outcome",
        "subject_id": "TEST:case1", "symbol": "TEST",
        "recorded_at": "2026-11-12T11:00:00+01:00",
        "effective_at": "2026-11-12T11:00:00+01:00",
        "parent_event_id": eid,
        "validation_eligible": False,
        "result": {"horizon_sessions": 20, "alpha": None, "maturity": "pending_prices"}
    }
    append(out, result, repo_root=root)
    assert verify(out) == 2


def test_retroactive_prediction_and_future_evidence_rejected(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    item = assessment()
    item["mode"] = "retrospective"
    item["recorded_at"] = "2026-11-12T11:00:00+01:00"
    with pytest.raises(ValueError, match="retrospective_assessment"):
        append(tmp_path / "outside.jsonl", item, repo_root=root)
    item = assessment()
    item["evidence_available_at"] = "2026-10-13T11:00:00+02:00"
    with pytest.raises(ValueError, match="prospective_requires"):
        append(tmp_path / "outside.jsonl", item, repo_root=root)


def test_no_private_journal_inside_repo(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    with pytest.raises(ValueError, match="outside_repository"):
        append(root / "events.jsonl", assessment(), repo_root=root)


def test_detect_tampered_row(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    path = tmp_path / "outside.jsonl"
    append(path, assessment(), repo_root=root)
    path.write_text(path.read_text().replace("insufficient_evidence", "positive"))
    with pytest.raises(ValueError, match="integrity"):
        verify(path)
