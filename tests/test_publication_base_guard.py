from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

from scripts.autorun_state import STATE, should_run
from scripts.check_publication_base import (
    STALE_PUBLICATION_EXIT,
    check_publication_base,
)


def _run(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


def _configure(repo: Path) -> None:
    _run(repo, "config", "user.email", "qm10@example.invalid")
    _run(repo, "config", "user.name", "BA-QM10 Test")


def _commit(repo: Path, name: str, content: str, message: str) -> None:
    (repo / name).write_text(content, encoding="utf-8")
    _run(repo, "add", name)
    _run(repo, "commit", "-m", message)


def test_publication_base_guard_rejects_real_remote_advance_and_preserves_retry(
    tmp_path: Path,
) -> None:
    remote = tmp_path / "remote.git"
    producer = tmp_path / "producer"
    runner = tmp_path / "runner"
    advancer = tmp_path / "advancer"

    subprocess.run(
        ["git", "init", "--bare", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )

    producer.mkdir()
    _run(producer, "init")
    _configure(producer)
    _commit(producer, "seed.txt", "seed\n", "seed")
    _run(producer, "branch", "-M", "main")
    _run(producer, "remote", "add", "origin", str(remote))
    _run(producer, "push", "-u", "origin", "main")

    subprocess.run(
        ["git", "clone", "--branch", "main", str(remote), str(runner)],
        check=True,
        capture_output=True,
        text=True,
    )
    initial = check_publication_base(runner, branch="main")
    assert initial["unchanged"] is True
    assert initial["local_base"] == initial["remote_base"]

    subprocess.run(
        ["git", "clone", "--branch", "main", str(remote), str(advancer)],
        check=True,
        capture_output=True,
        text=True,
    )
    _configure(advancer)
    _commit(advancer, "advance.txt", "advanced\n", "advance remote")
    _run(advancer, "push", "origin", "main")

    raced = check_publication_base(runner, branch="main")
    assert raced["unchanged"] is False
    assert raced["local_base"] != raced["remote_base"]

    project = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(project / "scripts" / "check_publication_base.py"),
        "--root",
        str(runner),
        "--branch",
        "main",
    ]
    completed = subprocess.run(
        command,
        env=dict(os.environ, GITHUB_REF_NAME="main"),
        capture_output=True,
        text=True,
    )
    assert completed.returncode == STALE_PUBLICATION_EXIT
    assert "stale publication refused" in completed.stdout
    assert not (runner / STATE).exists()

    retry_time = datetime.fromisoformat("2026-10-04T18:37:00+02:00")
    assert should_run(runner, "schedule", retry_time) is True


def test_publication_base_guard_cli_accepts_unchanged_remote(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"

    subprocess.run(
        ["git", "init", "--bare", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    repo.mkdir()
    _run(repo, "init")
    _configure(repo)
    _commit(repo, "seed.txt", "seed\n", "seed")
    _run(repo, "branch", "-M", "main")
    _run(repo, "remote", "add", "origin", str(remote))
    _run(repo, "push", "-u", "origin", "main")

    project = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(project / "scripts" / "check_publication_base.py"),
            "--root",
            str(repo),
            "--branch",
            "main",
        ],
        env=dict(os.environ, GITHUB_REF_NAME="main"),
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert '"unchanged": true' in completed.stdout.lower()
