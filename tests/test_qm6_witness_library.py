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
        for key, expected in witness["expect"].items():
            assert decision[key] == expected, f'{witness["id"]}:{key}'


def test_qm6_global_fail_closed_witnesses():
    contract = json.loads((ROOT / "configs/qm6_witness_library_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    for witness in contract["global_witnesses"]:
        assert _path(runtime, witness["path"]) == witness["expected"], witness["id"]


def test_qm6_witness_library_covers_required_named_cases():
    contract = json.loads((ROOT / "configs/qm6_witness_library_v1.json").read_text())
    ids = {item["id"] for item in contract["witnesses"] + contract["global_witnesses"]}
    assert {"WPM_PROFIT_PROTECTION","AURORA_OVEREXTENSION_INSUFFICIENT","FERRARI_DIRECTIONAL_CONFLICT","PERNOD_INSUFFICIENT","DRONESHIELD_INSUFFICIENT","MAGMA_POSITIVE_EXISTING_LONG","ELLIOTT_NOT_SUPPLIED"} <= ids
