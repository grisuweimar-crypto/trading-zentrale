from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.exposure_review_queue_8f import (
    ExposureReviewQueue8FError,
    build_exposure_review_queue,
)


ROOT = Path(__file__).resolve().parents[1]


def _load_domain() -> dict:
    return json.loads(
        (ROOT / "configs/external_evidence_8f_research_domain_v1.json").read_text(encoding="utf-8")
    )


def _load_universe() -> str:
    return (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")


def test_review_queue_is_pinned_to_pre8f_universe_and_has_207_subjects():
    result = build_exposure_review_queue(
        domain_config=_load_domain(),
        universe_csv=_load_universe(),
    )
    assert result["status"] == "OUTCOME_BLIND_DOCUMENTARY_REVIEW_QUEUE"
    assert result["subject_count"] == 207
    assert result["active_stock_source_rows"] == 216
    assert result["deduplicated_duplicate_rows"] == 9
    assert result["guards"]["market_outcomes_read"] is False
    assert result["guards"]["factor_mapping_inferred_from_sector"] is False
    assert all(item["review_status"] == "PENDING_DOCUMENTARY_REVIEW" for item in result["subjects"])
    assert all(item["factor_mapping_proposed"] is False for item in result["subjects"])
    assert all("factor_id" not in item for item in result["subjects"])


def test_review_queue_fails_if_pinned_universe_changes():
    with pytest.raises(ExposureReviewQueue8FError, match="universe source blob changed"):
        build_exposure_review_queue(
            domain_config=_load_domain(),
            universe_csv=_load_universe() + "\n",
        )
