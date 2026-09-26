from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "hydrate_external_evidence_8c_candidate_audit.py"

spec = importlib.util.spec_from_file_location("hydrate_candidate_audit", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def _anchor(family: str, symbol: str, accession: str, excerpt_hash: str, excerpt: str) -> dict:
    return {
        "family": family,
        "symbol": symbol,
        "accession_number": accession,
        "excerpt_sha256": excerpt_hash,
        "excerpt": excerpt,
        "filename": "x.htm",
        "document_type": "EX-99.1",
    }


def test_irrelevant_duplicate_anchor_is_ignored() -> None:
    relevant = _anchor("BUYBACK", "AAA", "0001", "a" * 64, "relevant")
    irrelevant = _anchor("CAPITAL_RAISE", "BBB", "0002", "b" * 64, "same")
    rows = [relevant, irrelevant, dict(irrelevant)]
    index, duplicate_count = module._build_relevant_anchor_index(rows, {module.anchor_id(relevant)})
    assert set(index) == {module.anchor_id(relevant)}
    assert duplicate_count == 0


def test_identical_relevant_duplicate_is_deduplicated() -> None:
    relevant = _anchor("DIVIDEND", "AAA", "0001", "a" * 64, "same excerpt")
    rows = [relevant, dict(relevant)]
    index, duplicate_count = module._build_relevant_anchor_index(rows, {module.anchor_id(relevant)})
    assert index[module.anchor_id(relevant)]["excerpt"] == "same excerpt"
    assert duplicate_count == 1


def test_conflicting_relevant_duplicate_fails_closed() -> None:
    relevant = _anchor("BUYBACK", "AAA", "0001", "a" * 64, "first")
    conflicting = dict(relevant)
    conflicting["excerpt"] = "different"
    with pytest.raises(ValueError, match="Conflicting duplicate relevant 8C-G anchor_id"):
        module._build_relevant_anchor_index(
            [relevant, conflicting], {module.anchor_id(relevant)}
        )
