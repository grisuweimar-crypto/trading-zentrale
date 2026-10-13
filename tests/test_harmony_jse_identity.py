"""Identity contract for Harmony Gold native JSE shares versus US ADR."""
import csv
from pathlib import Path


def test_harmony_native_jse_listing_distinct_from_adr() -> None:
    path = Path(__file__).resolve().parents[1] / "data/inputs/universe_master.csv"
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    instruments = {item["symbol"]: item for item in rows}
    native = instruments["HAR.JO"]
    adr = instruments["HMY"]
    assert native["active"] == "1"
    assert native["isin"] == "ZAE000015228"
    assert native["currency"] == "ZAc"
    assert native["asset_type"] == "stock"
    assert native["country"] == "South Africa"
    assert adr["isin"] == "US4132163001"
    assert adr["currency"] == "USD"
    assert native["isin"] != adr["isin"]
    assert len([r for r in rows if r["isin"] == native["isin"]]) == 1
