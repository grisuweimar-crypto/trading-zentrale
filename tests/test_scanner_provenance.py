from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.reports.scanner_provenance import (
    ScannerProvenanceError,
    capture_pre_run_provenance,
    finalize_scanner_input_provenance,
    validate_bound_provenance,
    write_runtime_provenance,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _root(tmp_path: Path) -> Path:
    _write(tmp_path / "run.py", "")
    (tmp_path / "src").mkdir(exist_ok=True)
    _write(
        tmp_path / "artifacts/watchlist/watchlist.csv",
        "Ticker,Score\nAAA,1\n",
    )
    _write(
        tmp_path / "data/inputs/universe_master.csv",
        "symbol,name,active\nAAA,Alpha,true\n",
    )
    _write(
        tmp_path / "artifacts/mapping/yahoo_taxonomy.csv",
        "yahoo_symbol,sector\nAAA,Technology\n",
    )
    _write(
        tmp_path / "artifacts/mapping/pillars.csv",
        "ticker,pillar_primary\nAAA,Gehirn\n",
    )
    return tmp_path


def _metadata() -> dict:
    return {
        "snapshot_id": "snapshot-001",
        "as_of": "2026-10-04",
        "source": {
            "file": "artifacts/watchlist/watchlist_full.csv",
            "sha256": "1" * 64,
        },
        "latest_scanner": {
            "path": "artifacts/research/latest_scanner.csv",
            "sha256": "2" * 64,
        },
    }


def test_prospective_provenance_binds_pre_run_runtime_and_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("SCANNER_FETCH_YAHOO", "1")

    pre = capture_pre_run_provenance(root)
    assert pre["run_id"] == "github-123-2"
    assert pre["historical_backfill"] is False
    assert pre["inputs"][0]["role"] == "persistent_watchlist_state"
    assert pre["inputs"][0]["sha256"]

    _write(
        root / "artifacts/watchlist/watchlist.csv",
        "Ticker,Score,Trend200\nAAA,1,0.2\n",
    )
    _write(
        root / "artifacts/watchlist/watchlist_full_raw.csv",
        "Ticker,Score,Trend200\nAAA,1,0.2\n",
    )
    runtime = write_runtime_provenance(
        root,
        selected_source=root / "artifacts/watchlist/watchlist.csv",
        scoring_rows_path=root / "artifacts/watchlist/watchlist_full_raw.csv",
        yahoo_report={
            "enabled": True,
            "tickers_total": 1,
            "tickers_fetched": 1,
            "tickers_failed": 0,
            "provider_frame_sha256": "9" * 64,
            "provider_frame_rows": 250,
        },
    )
    assert runtime["run_id"] == "github-123-2"
    assert runtime["scoring_rows_input"]["sha256"]
    assert runtime["selected_scoring_universe"]["sha256"]

    reference = finalize_scanner_input_provenance(
        root,
        receipt={
            "run_id": "github-123-2",
            "scanner_pre_run_provenance": pre,
        },
        research_metadata=_metadata(),
    )
    assert reference["complete"] is True
    assert reference["historical_backfill"] is False
    assert reference["snapshot_id"] == "snapshot-001"

    final = validate_bound_provenance(
        root,
        reference,
        expected_snapshot_id="snapshot-001",
    )
    assert final["coverage"]["pre_run_state_bound"] is True
    assert final["coverage"]["direct_scoring_input_bound"] is True
    assert final["coverage"]["scoring_universe_bound"] is True
    assert final["coverage"]["scanner_output_bound"] is True
    assert final["coverage"]["provider_frame_digest_bound"] is True
    assert final["coverage"]["historical_backfill"] is False
    assert final["boundaries"]["historical_provenance_inferred"] is False


def test_runtime_provenance_rejects_run_identity_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "10")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    pre = capture_pre_run_provenance(root)

    _write(root / "artifacts/watchlist/watchlist_full_raw.csv", "Ticker\nAAA\n")
    write_runtime_provenance(
        root,
        selected_source=root / "artifacts/watchlist/watchlist.csv",
        scoring_rows_path=root / "artifacts/watchlist/watchlist_full_raw.csv",
        yahoo_report=None,
    )

    changed = copy.deepcopy(pre)
    changed["run_id"] = "github-other-1"
    with pytest.raises(
        ScannerProvenanceError, match="pre_run_receipt_identity_mismatch"
    ):
        finalize_scanner_input_provenance(
            root,
            receipt={
                "run_id": "github-10-1",
                "scanner_pre_run_provenance": changed,
            },
            research_metadata=_metadata(),
        )


def test_bound_provenance_rejects_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "20")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SHA", "c" * 40)
    pre = capture_pre_run_provenance(root)
    _write(root / "artifacts/watchlist/watchlist_full_raw.csv", "Ticker\nAAA\n")
    write_runtime_provenance(
        root,
        selected_source=root / "artifacts/watchlist/watchlist.csv",
        scoring_rows_path=root / "artifacts/watchlist/watchlist_full_raw.csv",
        yahoo_report=None,
    )
    reference = finalize_scanner_input_provenance(
        root,
        receipt={
            "run_id": "github-20-1",
            "scanner_pre_run_provenance": pre,
        },
        research_metadata=_metadata(),
    )

    path = root / reference["path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["coverage"]["direct_scoring_input_bound"] = False
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        ScannerProvenanceError, match="scanner_provenance_file_hash_mismatch"
    ):
        validate_bound_provenance(
            root,
            reference,
            expected_snapshot_id="snapshot-001",
        )


def test_pre_run_provenance_records_optional_absence_instead_of_guessing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path
    _write(root / "artifacts/watchlist/watchlist.csv", "Ticker\nAAA\n")
    monkeypatch.setenv("GITHUB_SHA", "d" * 40)

    result = capture_pre_run_provenance(root)
    optional = {row["role"]: row for row in result["inputs"][1:]}
    assert optional["active_universe_master"]["exists"] is False
    assert optional["taxonomy_mapping"]["exists"] is False
    assert optional["pillar_mapping"]["exists"] is False
    assert result["historical_backfill"] is False


def test_active_yahoo_enrichment_requires_provider_frame_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "30")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SHA", "e" * 40)
    pre = capture_pre_run_provenance(root)
    _write(root / "artifacts/watchlist/watchlist_full_raw.csv", "Ticker\nAAA\n")
    write_runtime_provenance(
        root,
        selected_source=root / "artifacts/watchlist/watchlist.csv",
        scoring_rows_path=root / "artifacts/watchlist/watchlist_full_raw.csv",
        yahoo_report={"enabled": True, "provider_frame_rows": 10},
    )
    with pytest.raises(
        ScannerProvenanceError, match="yahoo_provider_frame_hash_required"
    ):
        finalize_scanner_input_provenance(
            root,
            receipt={
                "run_id": "github-30-1",
                "scanner_pre_run_provenance": pre,
            },
            research_metadata=_metadata(),
        )


def test_empty_yahoo_frame_is_a_valid_bound_fallback_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "31")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_SHA", "f" * 40)
    pre = capture_pre_run_provenance(root)
    _write(root / "artifacts/watchlist/watchlist_full_raw.csv", "Ticker\nAAA\n")
    write_runtime_provenance(
        root,
        selected_source=root / "artifacts/watchlist/watchlist.csv",
        scoring_rows_path=root / "artifacts/watchlist/watchlist_full_raw.csv",
        yahoo_report={
            "enabled": True,
            "provider_frame_sha256": "8" * 64,
            "provider_frame_rows": 0,
            "tickers_total": 1,
            "tickers_fetched": 0,
            "tickers_failed": 1,
        },
    )
    reference = finalize_scanner_input_provenance(
        root,
        receipt={
            "run_id": "github-31-1",
            "scanner_pre_run_provenance": pre,
        },
        research_metadata=_metadata(),
    )
    final = validate_bound_provenance(
        root,
        reference,
        expected_snapshot_id="snapshot-001",
    )
    assert final["coverage"]["provider_frame_digest_bound"] is True
    assert final["runtime"]["yahoo_enrichment"]["provider_frame_rows"] == 0
