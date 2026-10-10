"""CY-02/CY-03 historical research eligibility audit: synthetic controls."""
import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.audit_cycle_existing_history import audit, audit_cy03, audit_file


def write_csv(root: Path, path: str, columns: list[str], data: list[dict]):
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
    return p


def test_old_zeros_and_fifties_are_counted_but_not_research_certified(tmp_path):
    p = write_csv(tmp_path, "artifacts/research/history_analysis.csv",
                  ["date", "symbol", "cycle", "cycle_quality", "cycle_source"],
                  [
                    {"date": "2026-10-08", "symbol": "A", "cycle": "0",
                     "cycle_quality": "", "cycle_source": ""},
                    {"date": "2026-10-09", "symbol": "A", "cycle": "50",
                     "cycle_quality": "VALID", "cycle_source": "LEGACY"},
                    {"date": "2026-10-10", "symbol": "B", "cycle": "",
                     "cycle_quality": "MISSING_SOURCE", "cycle_source": ""},
                  ])
    result = audit_file(tmp_path, p.relative_to(tmp_path).as_posix())
    assert result["rows"] == 3
    assert result["cycle_zero"] == 1
    assert result["cycle_fifty"] == 1
    assert result["numeric_without_complete_verified_claim"] == 2
    assert result["formula_and_row_sha_candidates_NOT_RELEASED"] == 0


def test_syntactically_complete_row_cannot_claim_independent_pit(tmp_path):
    row = {"date": "2026-10-10", "as_of": "2026-10-10",
           "symbol": "AAA", "snapshot_id": "fixture-id",
           "cycle": "0", "cycle_quality": "VALID",
           "cycle_source": "YAHOO_PIT_CYCLE_V1",
           "cycle_formula_version": "cycle_detrended_sma20_range40_v1",
           "cycle_price_sha256": "a" * 64}
    p = write_csv(tmp_path, "artifacts/research/history_analysis.csv",
                  list(row), [row])
    result = audit_file(tmp_path, p.relative_to(tmp_path).as_posix())
    assert result["cycle_zero"] == 1
    assert result["formula_and_row_sha_candidates_NOT_RELEASED"] == 1
    # A syntactically complete stored hash is still NOT a provider publishing timestamp.


def test_real_cy03_manifest_integrity_and_release_authority_fail_closed(tmp_path):
    root = "artifacts/cycle_history"
    data = {
        "observations.csv": (
            "snapshot_id,asset_id,cycle\ncurrent,AAA,50\n"
        ),
        "eligibility.csv": (
            "snapshot_id,asset_id,research_status,lag_1obs,lag_5obs,lag_10obs\n"
            "current,AAA,BLOCKED_EXTERNAL_VERIFICATION_269,NO_PRIOR,NO_PRIOR,NO_PRIOR\n"
        ),
        "coverage.csv": "asset_id,month,observations\nAAA,2026-10,1\n",
    }
    for name, text in data.items():
        p = tmp_path / root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    meta = {"schema_version": "cycle_observations_cy03_v1",
            "file_sha256": {n: hashlib.sha256(v.encode()).hexdigest() for n, v in data.items()},
            "research_gate": "BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269",
            "research_eligible": 0, "snapshots": 1, "observations": 1,
            "latest_snapshot_id": "current", "as_of": "2026-10-10",
            "new_current_valid": 1, "new_current_excluded": 0}
    manifest = tmp_path / root / "manifest.json"
    manifest.write_text(json.dumps(meta), encoding="utf-8")
    assert audit_cy03(tmp_path)["research_eligible"] == 0
    meta["research_eligible"] = 1
    manifest.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(ValueError, match="unrecognized_cy03_release_authority"):
        audit_cy03(tmp_path)
    meta["research_eligible"] = 0
    manifest.write_text(json.dumps(meta), encoding="utf-8")
    (tmp_path / root / "observations.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="cycle_manifest_sha_mismatch"):
        audit_cy03(tmp_path)
