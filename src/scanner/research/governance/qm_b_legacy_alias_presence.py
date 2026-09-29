"""QM-B legacy alias-presence audit for pre-boundary observations.

This helper uses the historical root-level ``watchlist.csv`` only to establish that
an identifier string was present in the scanner configuration by a conservative
point-in-time boundary. It deliberately does not upgrade stable instrument identity,
listing venue, membership or investability.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import date, timedelta
import io
import json
from pathlib import Path
import subprocess
from typing import Any, Mapping


SCHEMA_VERSION = "qm_b_legacy_alias_presence_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_alias_listing_boundaries_v1.json"


class LegacyAliasPresenceError(ValueError):
    """Raised when legacy alias-presence evidence cannot be evaluated safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _parse_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_clean(value))
    except ValueError as exc:
        raise LegacyAliasPresenceError(f"invalid_date:{field}:{value}") from exc


def _source_spec(contract_path: str | Path | None = None) -> dict[str, Any]:
    target = Path(contract_path) if contract_path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacyAliasPresenceError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_alias_listing_boundaries_v1":
        raise LegacyAliasPresenceError("contract_schema_invalid")
    spec = payload.get("sources", {}).get("legacy_root_watchlist_history")
    if not isinstance(spec, Mapping):
        raise LegacyAliasPresenceError("legacy_watchlist_source_missing")
    return dict(spec)


def _git_text(repo_root: Path, args: list[str], *, error_code: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise LegacyAliasPresenceError(error_code) from exc
    try:
        return proc.stdout.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise LegacyAliasPresenceError(f"{error_code}:not_utf8") from exc


def collect_legacy_watchlist_history(
    repo_root: str | Path,
    *,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    spec = _source_spec(contract_path)
    path = str(spec["path"])
    cutoff = str(spec["history_until_commit"])
    fields = [str(value) for value in spec["identifier_fields"]]
    root = Path(repo_root)
    raw_log = _git_text(
        root,
        ["log", "--format=%H%x09%cs", cutoff, "--", path],
        error_code=f"legacy_watchlist_history_unreadable:{cutoff}:{path}",
    )
    commits: list[tuple[str, str]] = []
    for line in raw_log.splitlines():
        if not line.strip():
            continue
        try:
            commit, commit_date = line.split("\t", 1)
            _parse_date(commit_date, field=f"commit:{commit}")
        except ValueError as exc:
            raise LegacyAliasPresenceError(f"legacy_watchlist_log_line_invalid:{line}") from exc
        commits.append((commit, commit_date))

    by_identifier: dict[str, list[dict[str, Any]]] = defaultdict(list)
    snapshot_row_count = 0
    for commit, commit_date in commits:
        text = _git_text(
            root,
            ["show", f"{commit}:{path}"],
            error_code=f"legacy_watchlist_snapshot_unreadable:{commit}:{path}",
        )
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or any(field not in reader.fieldnames for field in fields):
            raise LegacyAliasPresenceError(f"legacy_watchlist_schema_invalid:{commit}:{path}")
        available_from = (_parse_date(commit_date, field=f"commit:{commit}") + timedelta(days=1)).isoformat()
        for row_number, row in enumerate(reader, start=2):
            snapshot_row_count += 1
            identifiers = sorted({_upper(row.get(field)) for field in fields if _upper(row.get(field))})
            for identifier in identifiers:
                by_identifier[identifier].append(
                    {
                        "commit": commit,
                        "commit_date": commit_date,
                        "pit_available_from": available_from,
                        "source_path": path,
                        "source_row_number": row_number,
                        "identifier": identifier,
                        "name": _clean(row.get("Name") or row.get("name")),
                    }
                )

    for identifier in by_identifier:
        by_identifier[identifier].sort(key=lambda row: (row["pit_available_from"], row["commit"], row["source_row_number"]))

    return {
        "source_path": path,
        "history_cutoff_commit": cutoff,
        "commit_count": len(commits),
        "commit_date_min": min((value for _, value in commits), default=None),
        "commit_date_max": max((value for _, value in commits), default=None),
        "snapshot_row_count": snapshot_row_count,
        "unique_identifier_count": len(by_identifier),
        "by_identifier": dict(by_identifier),
    }


def audit_pre_boundary_legacy_alias_presence(
    boundary_payload: Mapping[str, Any],
    *,
    repo_root: str | Path,
    contract_path: str | Path | None = None,
    history_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if boundary_payload.get("schema_version") != "qm_b_alias_listing_boundaries_audit_v1":
        raise LegacyAliasPresenceError("boundary_payload_schema_invalid")
    history = dict(history_payload) if history_payload is not None else collect_legacy_watchlist_history(
        repo_root,
        contract_path=contract_path,
    )
    by_identifier = history.get("by_identifier")
    if not isinstance(by_identifier, Mapping):
        raise LegacyAliasPresenceError("legacy_history_index_missing")

    pre_rows = [
        dict(row)
        for row in boundary_payload.get("unverified_observations", [])
        if row.get("observation_class") == "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO"
    ]
    audited: list[dict[str, Any]] = []
    for row in pre_rows:
        identifier = _upper(row.get("observed_identifier"))
        as_of = _parse_date(row.get("as_of_date"), field=f"observation:{identifier}")
        events = [dict(value) for value in by_identifier.get(identifier, [])]
        prior = [
            event for event in events
            if _parse_date(event.get("pit_available_from"), field=f"legacy_boundary:{identifier}") <= as_of
        ]
        if prior:
            status = "PIT_SUPPORTED_ALIAS_PRESENCE_ONLY"
            first_available = min(str(event["pit_available_from"]) for event in prior)
            latest_prior = max(prior, key=lambda event: (event["pit_available_from"], event["commit"]))
        else:
            status = "NO_PRIOR_LEGACY_ALIAS_PRESENCE"
            first_available = None
            latest_prior = None
        audited.append(
            {
                "as_of_date": row.get("as_of_date"),
                "observed_identifier": identifier,
                "source_row_number": row.get("source_row_number"),
                "stable_identity_pit_supported": False,
                "legacy_alias_presence_status": status,
                "legacy_alias_first_pit_available_from": first_available,
                "latest_prior_commit": latest_prior.get("commit") if latest_prior else None,
                "latest_prior_commit_date": latest_prior.get("commit_date") if latest_prior else None,
            }
        )

    counts = Counter(row["legacy_alias_presence_status"] for row in audited)
    supported = [row for row in audited if row["legacy_alias_presence_status"] == "PIT_SUPPORTED_ALIAS_PRESENCE_ONLY"]
    missing = [row for row in audited if row["legacy_alias_presence_status"] == "NO_PRIOR_LEGACY_ALIAS_PRESENCE"]
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "stable_identity_upgraded": False,
        "strict_alias_ledger": False,
        "strict_membership_ledger": False,
        "legacy_watchlist_source_path": history.get("source_path"),
        "legacy_watchlist_history_cutoff_commit": history.get("history_cutoff_commit"),
        "legacy_watchlist_commit_count": history.get("commit_count"),
        "legacy_watchlist_commit_date_min": history.get("commit_date_min"),
        "legacy_watchlist_commit_date_max": history.get("commit_date_max"),
        "legacy_watchlist_unique_identifier_count": history.get("unique_identifier_count"),
        "pre_boundary_observation_count": len(audited),
        "legacy_alias_presence_status_counts": dict(sorted(counts.items())),
        "pit_supported_alias_presence_only_observation_count": len(supported),
        "pit_supported_alias_presence_only_identifier_count": len({row["observed_identifier"] for row in supported}),
        "no_prior_legacy_alias_presence_observation_count": len(missing),
        "no_prior_legacy_alias_presence_identifier_count": len({row["observed_identifier"] for row in missing}),
        "audited_observations": audited,
    }
