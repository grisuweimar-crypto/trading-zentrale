import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.prospective_start_audit_8i import (
    ADMITTED_FIRST_PROSPECTIVE,
    AUDITED_PRESTART_SHADOW,
    STANDARD_PROSPECTIVE,
    ExternalEvidence8IProspectiveStartAuditError,
    classify_snapshot,
    digest,
    validate_audit_contract,
    validate_source_identity_correction,
)

ROOT = Path(__file__).resolve().parents[1]


def load(path: str):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


@pytest.fixture
def binding():
    return load("configs/external_evidence_8i_promotion_provenance_binding_v1.json")


@pytest.fixture
def correction():
    return load("configs/external_evidence_upstream_source_identity_correction_v1.json")


@pytest.fixture
def audit():
    return load("configs/external_evidence_8i_prospective_start_audit_v1.json")


def exact_snapshot(audit):
    e = audit["exact_first_prospective_exception"]
    keys = (
        "snapshot_id", "attempt_id", "started_at", "generated_at", "completed_tickers",
        "total_tickers", "scanner_commit_sha", "history_metadata_git_blob_sha",
        "latest_scanner_git_blob_sha", "run_source",
    )
    return {key: e[key] for key in keys}


def resign(row, field):
    row.pop(field, None)
    row[field] = digest(row)
    return row


def test_source_identity_correction_is_exact_outcome_blind_nonretroactive(binding, correction):
    assert validate_source_identity_correction(correction, binding) == correction["correction_sha256"]
    assert correction["alias_to_canonical"] == {
        "FED_H15": "federal_reserve_board_h15",
        "ECB_EXR": "ecb_data_portal",
    }
    assert correction["outcomes_read"] is False
    assert correction["retroactive_evidence_rewrite"] is False
    assert correction["identity_only"] is True


def test_canonical_8i_e_start_remains_sep29(audit, correction, binding):
    validate_audit_contract(audit, correction, binding)
    e = load("configs/external_evidence_8i_reliability_extension_research_v1.json")
    assert e["prospective_evidence"]["prospective_not_before_utc"] == "2026-09-29T00:00:00+00:00"
    assert audit["canonical_blanket_start_utc"] == "2026-09-29T00:00:00+00:00"
    assert audit["canonical_blanket_start_unchanged"] is True


def test_18_to_27_sep_is_shadow_only(audit, correction, binding):
    result = classify_snapshot(
        audit=audit, correction=correction, binding_contract=binding,
        snapshot={"snapshot_id": "shadow-20260920", "generated_at": "2026-09-20T18:00:00+00:00"},
    )
    assert result["classification"] == AUDITED_PRESTART_SHADOW
    assert result["counts_toward_8i_e_confirmatory_family_if_other_gates_pass"] is False


def test_arbitrary_sep28_snapshot_is_not_admitted(audit, correction, binding):
    result = classify_snapshot(
        audit=audit, correction=correction, binding_contract=binding,
        snapshot={"snapshot_id": "some-other-sep28-run", "generated_at": "2026-09-28T18:00:00+00:00"},
    )
    assert result["classification"] == AUDITED_PRESTART_SHADOW
    assert result["counts_toward_8i_e_confirmatory_family_if_other_gates_pass"] is False


def test_exact_late_sep28_snapshot_is_admitted_as_first_prospective(audit, correction, binding):
    result = classify_snapshot(
        audit=audit, correction=correction, binding_contract=binding,
        snapshot=exact_snapshot(audit),
    )
    assert result["classification"] == ADMITTED_FIRST_PROSPECTIVE
    assert result["counts_toward_8i_e_confirmatory_family_if_other_gates_pass"] is True
    assert result["reason"] == "exact_post_freeze_snapshot_exception"
    assert result["canonical_blanket_start_unchanged"] is True


@pytest.mark.parametrize("field,bad_value", [
    ("attempt_id", "wrong"),
    ("generated_at", "2026-09-28T16:17:30+00:00"),
    ("completed_tickers", 212),
    ("history_metadata_git_blob_sha", "0" * 40),
    ("latest_scanner_git_blob_sha", "1" * 40),
])
def test_exact_snapshot_fails_closed_on_provenance_drift(audit, correction, binding, field, bad_value):
    row = exact_snapshot(audit)
    row[field] = bad_value
    with pytest.raises(ExternalEvidence8IProspectiveStartAuditError):
        classify_snapshot(audit=audit, correction=correction, binding_contract=binding, snapshot=row)


def test_bad_source_identity_correction_fails_closed(audit, correction, binding):
    bad = copy.deepcopy(correction)
    bad["alias_to_canonical"]["FED_H15"] = "something_else"
    resign(bad, "correction_sha256")
    with pytest.raises(ExternalEvidence8IProspectiveStartAuditError):
        classify_snapshot(audit=audit, correction=bad, binding_contract=binding, snapshot=exact_snapshot(audit))


def test_sep29_and_later_use_standard_prospective_classification(audit, correction, binding):
    result = classify_snapshot(
        audit=audit, correction=correction, binding_contract=binding,
        snapshot={"snapshot_id": "standard", "generated_at": "2026-09-29T00:00:00+00:00"},
    )
    assert result["classification"] == STANDARD_PROSPECTIVE
    assert result["counts_toward_8i_e_confirmatory_family_if_other_gates_pass"] is True


def test_outcome_data_is_rejected(audit, correction, binding):
    row = exact_snapshot(audit)
    row["peer_excess"] = 0.123
    with pytest.raises(ExternalEvidence8IProspectiveStartAuditError, match="outcome_field_forbidden"):
        classify_snapshot(audit=audit, correction=correction, binding_contract=binding, snapshot=row)


def test_exception_is_eligibility_only_not_empirical_completion(audit, correction, binding):
    result = classify_snapshot(
        audit=audit, correction=correction, binding_contract=binding, snapshot=exact_snapshot(audit)
    )
    assert audit["exact_first_prospective_exception"]["does_not_itself_increase_empirical_sample"] is True
    assert audit["empirical_status"]["8i_e_empirically_complete"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["extended_reliability_enabled"] is False
    assert result["extended_stance_enabled"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"


def test_audit_contract_digest_and_exact_snapshot_are_frozen(audit, correction, binding):
    assert validate_audit_contract(audit, correction, binding) == audit["audit_sha256"]
    bad = copy.deepcopy(audit)
    bad["exact_first_prospective_exception"]["snapshot_id"] = "different"
    resign(bad, "audit_sha256")
    with pytest.raises(ExternalEvidence8IProspectiveStartAuditError):
        validate_audit_contract(bad, correction, binding)
