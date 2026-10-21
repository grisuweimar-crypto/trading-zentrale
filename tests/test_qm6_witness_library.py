import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _path(value, dotted):
    for part in dotted.split("."):
        value = value[part]
    return value


def test_qm6_productive_symbol_witnesses():
    contract = json.loads((ROOT / "configs/qm6_witness_library_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    rows = {row["symbol"]: row for row in runtime["rows"]}
    for witness in contract["witnesses"]:
        assert witness["symbol"] in rows, witness["id"]
        decision = rows[witness["symbol"]]["decision"]
        for key in witness["required_decision_fields"]:
            assert key in decision, f'{witness["id"]}:{key}'
        assert witness["historical_reference_expect"]
    assert contract["snapshot_dependent_outcomes_must_not_be_frozen"] is True


def test_qm6_global_fail_closed_witnesses():
    contract = json.loads((ROOT / "configs/qm6_witness_library_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    for witness in contract["global_witnesses"]:
        if witness.get("scope") == "historical_qm10_proof":
            historic = json.loads((ROOT / witness["artifact"]).read_text())
            assert historic["as_of"] == "2026-10-06"
            assert _path(historic, witness["path"]) == witness["expected"], witness["id"]
        else:
            assert _path(runtime, witness["path"]) == witness["expected"], witness["id"]

    # The live status is not frozen to the historical QM10 snapshot.
    w10 = json.loads((ROOT / "artifacts/research/decision_snapshot_w10.json").read_text())
    phase = w10["stages"]["phase6_elliott"]
    assert phase["status"] == runtime["diagnostics"]["elliott_6h_source_status"]
    assert phase["status"] in {"available", "not_supplied"}
    assert runtime["diagnostics"]["elliott_direction_used_as_vote"] is False
    assert runtime["diagnostics"]["elliott_changed_universal_stance"] is False
    if phase["status"] == "available":
        assert phase["snapshot_identity_state"] == "verified"
        assert phase["decision_effect"] == "review_only_downstream_of_7d_7e"
        assert phase["elliott_direction_used_as_vote"] is False
        assert phase["multi_degree_reducer_used"] is False
    else:
        assert phase["decision_effect"] == "none"


def test_qm6_witness_library_covers_required_named_cases():
    contract = json.loads((ROOT / "configs/qm6_witness_library_v1.json").read_text())
    ids = {item["id"] for item in contract["witnesses"] + contract["global_witnesses"]}
    assert {"WPM_PROFIT_PROTECTION","AURORA_OVEREXTENSION_INSUFFICIENT","FERRARI_DIRECTIONAL_CONFLICT","PERNOD_INSUFFICIENT","DRONESHIELD_INSUFFICIENT","MAGMA_POSITIVE_EXISTING_LONG","ELLIOTT_NOT_SUPPLIED"} <= ids
