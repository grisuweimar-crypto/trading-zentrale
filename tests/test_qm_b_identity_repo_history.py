from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import subprocess

from scanner.research.governance.qm_b_identity_repo_history import (
    audit_repo_history_upgrades,
    collect_universe_history_events,
)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def git(repo: Path, *args: str, env: dict[str, str] | None = None) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    return proc.stdout.strip()


def commit_file(repo: Path, path: Path, rows: list[dict[str, str]], when: str, message: str) -> str:
    write_csv(path, rows)
    git(repo, "add", path.relative_to(repo).as_posix())
    env = os.environ.copy()
    env["GIT_AUTHOR_DATE"] = f"{when}T12:00:00+00:00"
    env["GIT_COMMITTER_DATE"] = f"{when}T12:00:00+00:00"
    git(repo, "commit", "-m", message, env=env)
    return git(repo, "rev-parse", "HEAD")


def make_contract(path: Path, cutoff: str) -> Path:
    payload = {
        "schema_version": "qm_b_identity_reconciliation_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "sources": {
            "historical_snapshots": [],
            "historical_git_history": {
                "path": "data/inputs/universe_master.csv",
                "schema": "universe_master",
                "history_until_commit": cutoff,
                "availability_rule": "next_calendar_day",
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def init_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.name", "QM Test")
    git(repo, "config", "user.email", "qm@example.test")
    universe = repo / "data" / "inputs" / "universe_master.csv"
    universe.parent.mkdir(parents=True)
    return repo, universe


def test_repo_history_upgrade_respects_next_day_pit_boundary_and_frozen_cutoff(tmp_path: Path):
    repo, universe = init_repo(tmp_path)
    cutoff = commit_file(
        repo,
        universe,
        [{"active": "1", "symbol": "NEW", "name": "New Co", "isin": "US0378331005", "asset_type": "stock"}],
        "2026-04-01",
        "add NEW",
    )
    # This later conflicting change exists in git, but is intentionally outside the frozen cutoff.
    commit_file(
        repo,
        universe,
        [{"active": "1", "symbol": "NEW", "name": "Other Co", "isin": "US5949181045", "asset_type": "stock"}],
        "2026-04-10",
        "future remap",
    )
    contract = make_contract(tmp_path / "contract.json", cutoff)
    history = tmp_path / "history.csv"
    write_csv(
        history,
        [
            {"date": "2026-04-01", "symbol": "NEW", "observation_type": "observed_scanner", "data_source": "scanner_run"},
            {"date": "2026-04-02", "symbol": "NEW", "observation_type": "observed_scanner", "data_source": "scanner_run"},
        ],
    )
    base_payload = {
        "reconciled_identifiers": [
            {
                "observed_identifier": "NEW",
                "candidate_class": "CURRENT_SYMBOL_ONLY_MATCH",
                "canonical_isin": "US0378331005",
                "identity_status": "PARTIAL",
            }
        ]
    }
    result = audit_repo_history_upgrades(
        history,
        universe,
        repo_root=repo,
        contract_path=contract,
        base_payload=base_payload,
    )
    row = result["audited_identifiers"][0]
    assert result["history_cutoff_commit"] == cutoff
    assert result["history_commit_count"] == 1
    assert row["repo_history_status"] == "HISTORICAL_REPO_MATCH"
    assert row["identity_status_after_repo_history"] == "VERIFIED"
    assert row["pit_alias_available_from"] == "2026-04-02"
    assert row["pit_verified_observation_count"] == 1
    assert row["pit_unverified_observation_count"] == 1
    assert result["upgraded_identifier_count"] == 1
    assert result["conflicting_identifier_count"] == 0


def test_repo_history_conflicting_symbol_mapping_fails_closed(tmp_path: Path):
    repo, universe = init_repo(tmp_path)
    commit_file(
        repo,
        universe,
        [{"active": "1", "symbol": "CLASH", "name": "One", "isin": "US0378331005", "asset_type": "stock"}],
        "2026-04-01",
        "first mapping",
    )
    cutoff = commit_file(
        repo,
        universe,
        [{"active": "1", "symbol": "CLASH", "name": "Two", "isin": "US5949181045", "asset_type": "stock"}],
        "2026-04-05",
        "conflicting mapping",
    )
    contract = make_contract(tmp_path / "contract.json", cutoff)
    history = tmp_path / "history.csv"
    write_csv(history, [{"date": "2026-04-06", "symbol": "CLASH", "observation_type": "observed_scanner", "data_source": "scanner_run"}])
    base_payload = {
        "reconciled_identifiers": [
            {
                "observed_identifier": "CLASH",
                "candidate_class": "CURRENT_SYMBOL_ONLY_MATCH",
                "canonical_isin": "US0378331005",
                "identity_status": "PARTIAL",
            }
        ]
    }
    result = audit_repo_history_upgrades(
        history,
        universe,
        repo_root=repo,
        contract_path=contract,
        base_payload=base_payload,
    )
    row = result["audited_identifiers"][0]
    assert row["repo_history_status"] == "CONFLICTING_HISTORICAL_REPO_MAPPING"
    assert row["identity_status_after_repo_history"] == "CONFLICTING"
    assert row["upgraded"] is False
    assert result["conflicting_identifier_count"] == 1


def test_collect_history_reads_only_active_valid_isin_rows(tmp_path: Path):
    repo, universe = init_repo(tmp_path)
    cutoff = commit_file(
        repo,
        universe,
        [
            {"active": "1", "symbol": "AAPL", "name": "Apple", "isin": "US0378331005", "asset_type": "stock"},
            {"active": "0", "symbol": "MSFT", "name": "Microsoft", "isin": "US5949181045", "asset_type": "stock"},
            {"active": "1", "symbol": "BAD", "name": "Bad", "isin": "NOTANISIN", "asset_type": "stock"},
        ],
        "2026-04-01",
        "snapshot",
    )
    contract = make_contract(tmp_path / "contract.json", cutoff)
    result = collect_universe_history_events(repo, contract_path=contract)
    assert "AAPL" in result["by_symbol"]
    assert "MSFT" not in result["by_symbol"]
    assert result["by_symbol"]["BAD"][0]["isin"] == ""
    assert "US0378331005" in result["by_isin"]
    assert "US5949181045" not in result["by_isin"]
