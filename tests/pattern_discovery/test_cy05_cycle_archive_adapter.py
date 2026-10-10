"""CY-05: independent read-only inspection tests for the exact CY-03 v1 format."""
from datetime import date, timedelta
from hashlib import sha256
import json
from pathlib import Path

import pytest

from scanner.reports.cycle_history import (
    COVERAGE, FIELDS, MASK, ROOT, VERSION, coverage_rows, csv_bytes, lag_mask,
)
from scanner.research.pattern_discovery.cycle_cy05_adapter import (
    CycleCY05ArchiveError, inspect_cycle_archive, load_cycle_cy05_rows,
)
from scanner.research.pattern_discovery.feature_library import FeatureLibrary


def _digest(value):
    return sha256(value).hexdigest()


def _fixture(tmp_path):
    folder = tmp_path / ROOT
    (folder / "bars").mkdir(parents=True)
    ledger = []
    for i in range(2):
        day = date(2026, 10, 10) + timedelta(days=i)
        asof = day.isoformat()
        sid = f"00000000-0000-4000-8000-{i + 1:012d}"
        bars = f"synthetic-bar-{i}".encode()
        barhash = _digest(bars)
        (folder / "bars" / f"{sid}.csv.gz").write_bytes(bars)
        row = {column: "" for column in FIELDS}
        row.update(
            schema_version=VERSION, snapshot_id=sid, run_id=f"synthetic-{i}",
            asset_id="AAA", listing_symbol="AAA", price_symbol="AAA",
            currency="USD", market_timezone="UNVERIFIED", as_of=asof,
            generated_at=f"{asof}T12:00:00+00:00",
            cycle_as_of=f"{asof}T11:00:00+00:00",
            computed_at=f"{asof}T11:10:00+00:00",
            last_bar=(day - timedelta(days=1)).isoformat(),
            cycle=str(20 + i * 4), quality="VALID", reason="SYNTHETIC_ONLY",
            source="YAHOO_PIT_CYCLE_V1", formula="cycle_detrended_sma20_range40_v1",
            price_source="yfinance.download", price_basis="1d_auto_adjust_true_close",
            price_sha256=_digest(f"price-{i}".encode()),
            watchlist_sha256=_digest(b"synthetic-watchlist"), bars_sha256=barhash,
            currency_lineage="WATCHLIST_DECLARED_ONLY",
            session_time_quality="SESSION_DATE_CUTOFF_ONLY",
            availability="PROVISIONAL_REPLAYED",
        )
        ledger.append(row)
    mask = lag_mask(ledger)
    outputs = {
        "observations.csv": csv_bytes(FIELDS, ledger),
        "eligibility.csv": csv_bytes(MASK, mask),
        "coverage.csv": csv_bytes(COVERAGE, coverage_rows(mask)),
    }
    for name, content in outputs.items():
        (folder / name).write_bytes(content)
    manifest = {
        "schema_version": VERSION,
        "latest_snapshot_id": ledger[-1]["snapshot_id"],
        "run_id": ledger[-1]["run_id"],
        "as_of": ledger[-1]["as_of"],
        "observations": len(ledger), "snapshots": 2,
        "research_eligible": 0,
        "research_gate": "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269",
        "file_sha256": {key: _digest(value) for key, value in outputs.items()},
    }
    (folder / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return folder


def _edit_manifest(folder, **changes):
    path = folder / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.update(changes)
    path.write_text(json.dumps(manifest), encoding="utf-8")


def test_verifiable_synthetic_cycle_archive_remains_blocked_and_read_only(tmp_path):
    folder = _fixture(tmp_path)
    before = {path.relative_to(folder): path.read_bytes()
              for path in folder.rglob("*") if path.is_file()}
    report = inspect_cycle_archive(tmp_path)
    assert report["verified_internal_archive_integrity"] is True
    assert report["independent_external_pit_verified"] is False
    assert report["research_released"] is False
    assert report["snapshots"] == 2
    assert report["observations"] == 2
    assert report["lag_status_counts"]["1"]["PROVISIONAL_CHAIN"] == 1
    for lag in (5, 10):
        assert report["lag_status_counts"][str(lag)]["NO_PRIOR"] == 2
    assert report["research_eligible"] == 0

    with pytest.raises(CycleCY05ArchiveError, match="research_gate_not_released"):
        load_cycle_cy05_rows(tmp_path)

    projections = load_cycle_cy05_rows(tmp_path, diagnostic_only=True)
    assert len(projections) == 2
    assert projections[-1]["cycle_lag_1obs"] == "PROVISIONAL_CHAIN"
    assert projections[-1]["cycle_research_status"] == "BLOCKED_EXTERNAL_VERIFICATION_269"
    lib = FeatureLibrary("configs/pattern_discovery/feature_library_cycle_v2.json")
    available = lib.pit_availability({
        "feature_id": "scanner.cycle", "feature_version": "v2",
        "transformation_id": "level_band", "transformation_version": "v1",
        "parameters": {},
    }, projections[-1], data_cutoff="2026-10-11")
    assert available["available"] is False
    assert available["status"] == "CYCLE_RESEARCH_NOT_RELEASED"
    after = {path.relative_to(folder): path.read_bytes()
             for path in folder.rglob("*") if path.is_file()}
    assert before == after


def test_changed_archived_observations_rejected(tmp_path):
    folder = _fixture(tmp_path)
    with (folder / "observations.csv").open("ab") as f:
        f.write(b"extra\n")
    with pytest.raises(CycleCY05ArchiveError, match="integrity_failed"):
        inspect_cycle_archive(tmp_path)


def test_changed_mask_even_with_forged_manifest_digest_rejected(tmp_path):
    folder = _fixture(tmp_path)
    path = folder / "eligibility.csv"
    path.write_bytes(path.read_bytes().replace(b"PROVISIONAL_CHAIN", b"NO_PRIOR"))
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    manifest["file_sha256"]["eligibility.csv"] = _digest(path.read_bytes())
    _edit_manifest(folder, **manifest)
    with pytest.raises(CycleCY05ArchiveError, match="eligibility_not_reproducible"):
        inspect_cycle_archive(tmp_path)


def test_research_release_cannot_be_claimed_by_manifest_edit(tmp_path):
    folder = _fixture(tmp_path)
    _edit_manifest(
        folder, research_gate="EXTERNAL_VERIFIED",
        research_eligible=2,
    )
    with pytest.raises(CycleCY05ArchiveError, match="unrecognized_research_release_contract"):
        inspect_cycle_archive(tmp_path)


def test_manifest_latest_snapshot_mismatch_is_rejected(tmp_path):
    folder = _fixture(tmp_path)
    _edit_manifest(folder, latest_snapshot_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    with pytest.raises(CycleCY05ArchiveError, match="latest_identity_mismatch"):
        inspect_cycle_archive(tmp_path)


def test_snapshot_bars_hash_drift_rejected(tmp_path):
    folder = _fixture(tmp_path)
    bar = next((folder / "bars").glob("*.csv.gz"))
    bar.write_bytes(b"changed")
    with pytest.raises(CycleCY05ArchiveError, match="integrity_failed"):
        inspect_cycle_archive(tmp_path)


def test_real_repo_archive_has_no_unapproved_cycle_research_release():
    """Real CI fixture is the published snapshot, NOT future market outcomes."""
    root = Path(".")
    if not (root / ROOT / "manifest.json").exists():
        pytest.skip("Repo fixture not packaged")
    report = inspect_cycle_archive(root)
    assert report["observations"] >= 1
    assert report["snapshots"] >= 1
    assert report["research_eligible"] == 0
    assert report["research_released"] is False
    with pytest.raises(CycleCY05ArchiveError, match="research_gate_not_released"):
        load_cycle_cy05_rows(root)
