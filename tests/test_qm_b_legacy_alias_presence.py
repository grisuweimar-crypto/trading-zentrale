from __future__ import annotations

from scanner.research.governance.qm_b_legacy_alias_presence import audit_pre_boundary_legacy_alias_presence


def boundary_payload() -> dict:
    return {
        "schema_version": "qm_b_alias_listing_boundaries_audit_v1",
        "unverified_observations": [
            {
                "as_of_date": "2026-02-12",
                "observed_identifier": "AAPL",
                "source_row_number": 10,
                "observation_class": "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO",
            },
            {
                "as_of_date": "2026-02-10",
                "observed_identifier": "MSFT",
                "source_row_number": 11,
                "observation_class": "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO",
            },
            {
                "as_of_date": "2026-02-12",
                "observed_identifier": "BTC-USD",
                "source_row_number": 12,
                "observation_class": "CRYPTO_STABLE_OBJECT_UNRESOLVED",
            },
        ],
    }


def legacy_history() -> dict:
    return {
        "source_path": "watchlist.csv",
        "history_cutoff_commit": "cutoff",
        "commit_count": 2,
        "commit_date_min": "2026-02-09",
        "commit_date_max": "2026-02-11",
        "unique_identifier_count": 2,
        "by_identifier": {
            "AAPL": [
                {
                    "commit": "a",
                    "commit_date": "2026-02-11",
                    "pit_available_from": "2026-02-12",
                    "identifier": "AAPL",
                }
            ],
            "MSFT": [
                {
                    "commit": "m",
                    "commit_date": "2026-02-10",
                    "pit_available_from": "2026-02-11",
                    "identifier": "MSFT",
                }
            ],
        },
    }


def test_prior_legacy_presence_is_alias_evidence_only(tmp_path):
    payload = audit_pre_boundary_legacy_alias_presence(
        boundary_payload(),
        repo_root=tmp_path,
        history_payload=legacy_history(),
    )
    rows = {row["observed_identifier"]: row for row in payload["audited_observations"]}
    assert payload["pre_boundary_observation_count"] == 2
    assert rows["AAPL"]["legacy_alias_presence_status"] == "PIT_SUPPORTED_ALIAS_PRESENCE_ONLY"
    assert rows["AAPL"]["stable_identity_pit_supported"] is False
    assert payload["stable_identity_upgraded"] is False
    assert payload["strict_alias_ledger"] is False
    assert payload["strict_membership_ledger"] is False


def test_same_day_or_future_snapshot_does_not_back_project(tmp_path):
    payload = audit_pre_boundary_legacy_alias_presence(
        boundary_payload(),
        repo_root=tmp_path,
        history_payload=legacy_history(),
    )
    rows = {row["observed_identifier"]: row for row in payload["audited_observations"]}
    assert rows["MSFT"]["legacy_alias_presence_status"] == "NO_PRIOR_LEGACY_ALIAS_PRESENCE"
    assert rows["MSFT"]["legacy_alias_first_pit_available_from"] is None


def test_crypto_rows_are_not_in_pre_boundary_legacy_audit(tmp_path):
    payload = audit_pre_boundary_legacy_alias_presence(
        boundary_payload(),
        repo_root=tmp_path,
        history_payload=legacy_history(),
    )
    identifiers = {row["observed_identifier"] for row in payload["audited_observations"]}
    assert "BTC-USD" not in identifiers
