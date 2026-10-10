#!/usr/bin/env python3
"""Prevent BA-QM8 from auditing a stale W10 when Decision integration was deferred.

A successful *workflow* can contain a skipped `integrate` job. That means
no new Decision W10/7A was published. This is an explicit DEFER, not a
successful current-snapshot evidence check or an excuse to weaken identity.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from urllib.request import Request, urlopen


def classify_integration_jobs(payload: dict) -> str:
    """Return RUN or DEFER, failing closed on ambiguous/unexpected job states."""
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("qm8_gate:jobs_list_missing")
    found = [x for x in jobs if isinstance(x, dict) and x.get("name") == "integrate"]
    if len(found) != 1:
        raise ValueError(f"qm8_gate:integrate_job_cardinality:{len(found)}")
    job = found[0]
    if job.get("status") != "completed":
        raise ValueError("qm8_gate:integrate_not_completed")
    result = job.get("conclusion")
    if result == "success":
        return "RUN"
    if result == "skipped":
        return "DEFER"
    raise ValueError("qm8_gate:integrate_not_successful:" + str(result))


def fetch_workflow_jobs(repo: str, run_id: str, token: str) -> dict:
    if not repo or not run_id.isdecimal() or not token:
        raise ValueError("qm8_gate:github_action_context_missing")
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"
    request = Request(url, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "scanner-vnext-ba-qm8-gate",
    })
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("qm8_gate:jobs_payload_invalid")
    if payload.get("total_count", 0) > 100:
        raise ValueError("qm8_gate:jobs_pagination_not_audited")
    return payload


def main() -> int:
    payload = fetch_workflow_jobs(
        os.environ.get("GITHUB_REPOSITORY", ""),
        os.environ.get("UPSTREAM_RUN_ID", ""),
        os.environ.get("GH_TOKEN", ""),
    )
    state = classify_integration_jobs(payload)
    ready = "true" if state == "RUN" else "false"
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as out:
        out.write(f"ready={ready}\n")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as summary:
        if state == "RUN":
            summary.write("BA-QM8 permitted: upstream `integrate` job actually succeeded. Full W10 snapshot identity validation still required.\n")
        else:
            summary.write("**BA-QM8 DEFERRED, not passed:** upstream Decision pipeline succeeded only in readiness; `integrate` was skipped and no fresh W10 was published. Wait for an actual integration or run the manual audit later.\n")
    if state == "DEFER":
        print("::notice::BA-QM8 DEFERRED: Decision integration skipped; stale W10 must not be audited or treated as successful.")
    else:
        print("BA-QM8 upstream integration completed; run full independent real snapshot audit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
