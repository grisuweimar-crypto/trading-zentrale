"""Regression tests for the strictly read-only artifact inventory."""
import subprocess

from scripts.audit_repository_artifacts import classify, inventory


def _git(root, *args):
    return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def test_classification_is_conservative():
    assert classify("artifacts/research/history_analysis.csv") == "retain_source_or_audit"
    assert classify("artifacts/research/watch_runtime/shard_00.json") == "retain_current_publication"
    assert classify("artifacts/research/monthly/history_monthly_2026-09.csv") == "derived_review_required"
    assert classify("artifacts/watchlist/watchlist (1).csv") == "possible_copy_review_required"
    assert classify("artifacts/research/new_unknown.json") == "unclassified_review_required"


def test_audit_never_mutates_files_and_identical_blobs_are_not_assumed_disk_savings(tmp_path):
    _git(tmp_path, "init", "-q")
    d = tmp_path / "artifacts" / "watchlist"
    d.mkdir(parents=True)
    source = d / "watchlist.csv"
    second = d / "watchlist_full_raw.csv"
    source.write_text("ticker\nABC\n", encoding="utf-8")
    second.write_bytes(source.read_bytes())
    _git(tmp_path, "add", "artifacts/")
    before = source.read_bytes(), second.read_bytes()
    first = inventory(tmp_path)
    second_run = inventory(tmp_path)
    assert first == second_run
    assert (source.read_bytes(), second.read_bytes()) == before
    assert first["tracked_artifact_count"] == 2
    assert len(first["identical_blob_groups"]) == 1
    assert first["identical_blob_groups"][0]["logical_extra_bytes"] == len(before[0])
    assert not first["possible_copy_paths"]
