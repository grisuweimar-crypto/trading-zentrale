#!/usr/bin/env python3
"""Fail closed when the publication branch advanced during a long Scanner build."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


STALE_PUBLICATION_EXIT = 75


class PublicationBaseError(RuntimeError):
    """Raised when the publication base cannot be established unambiguously."""


def _git(root: Path, *args: str) -> str:
    try:
        return subprocess.check_output(
            ["git", *args],
            cwd=root,
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except subprocess.CalledProcessError as exc:
        raise PublicationBaseError(
            f"git_command_failed:{' '.join(args)}:{exc.output.strip()}"
        ) from exc


def local_head(root: str | Path) -> str:
    value = _git(Path(root), "rev-parse", "HEAD")
    if len(value) != 40:
        raise PublicationBaseError("local_head_invalid")
    return value


def remote_branch_head(
    root: str | Path,
    *,
    remote: str = "origin",
    branch: str,
) -> str:
    ref = f"refs/heads/{branch}"
    output = _git(Path(root), "ls-remote", "--heads", remote, ref)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != 1:
        raise PublicationBaseError(
            f"remote_branch_resolution_invalid:{remote}:{branch}:{len(lines)}"
        )
    parts = lines[0].split()
    if len(parts) != 2 or parts[1] != ref or len(parts[0]) != 40:
        raise PublicationBaseError(
            f"remote_branch_resolution_invalid:{remote}:{branch}:malformed"
        )
    return parts[0]


def check_publication_base(
    root: str | Path,
    *,
    remote: str = "origin",
    branch: str,
) -> dict[str, object]:
    local = local_head(root)
    remote_head = remote_branch_head(root, remote=remote, branch=branch)
    return {
        "local_base": local,
        "remote_base": remote_head,
        "branch": branch,
        "remote": remote,
        "unchanged": local == remote_head,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default=os.environ.get("GITHUB_REF_NAME"))
    args = parser.parse_args()
    branch = str(args.branch or "").strip()
    if not branch:
        raise PublicationBaseError("publication_branch_required")

    result = check_publication_base(
        args.root.resolve(),
        remote=str(args.remote),
        branch=branch,
    )
    print(json.dumps(result, sort_keys=True))
    if result["unchanged"] is not True:
        print(
            "::warning::main advanced or publication branch advanced during "
            "scanner build; stale publication refused"
        )
        return STALE_PUBLICATION_EXIT
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
