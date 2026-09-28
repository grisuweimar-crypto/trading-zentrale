from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.evaluation_8g import new_holdout_consumption_ledger


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = json.loads((ROOT / "configs" / "external_evidence_8g_evaluation_protocol_v1.json").read_text())
LEDGER = json.loads((ROOT / "artifacts" / "research" / "external_evidence_8g_holdout_consumption_v1.json").read_text())


def test_persisted_holdout_seed_matches_code_generated_sealed_ledger() -> None:
    expected = new_holdout_consumption_ledger(PROTOCOL)
    assert LEDGER["schema_version"] == expected["schema_version"]
    assert LEDGER["phase"] == "8G-F"
    assert expected["phase"] == "8G-F"
    assert LEDGER["append_only"] is True
    assert LEDGER["outcomes_read_while_creating_ledger"] is False
    assert set(LEDGER["streams"]) == set(expected["streams"])
    assert all(item["state"] == "SEALED" for item in LEDGER["streams"].values())
    assert all(item["evaluation_sha256"] is None for item in LEDGER["streams"].values())
