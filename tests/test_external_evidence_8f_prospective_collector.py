from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.prospective_collector_8f import (
    ECB_USD_EUR_URL,
    FED_H15_RELEASE_URL,
    RawSnapshot,
    collect_phase8f_prospective,
    eia_series_url,
)


ROOT = Path(__file__).resolve().parents[1]

FED_HTML = """
<html><body>
<table>
<tr><th>Instruments</th><th>2026 Sep 23</th><th>2026 Sep 24</th></tr>
<tr><td>Federal funds (effective) 1 2 3</td><td>3.88</td><td>3.88</td></tr>
<tr><td>Treasury constant maturities</td><td></td><td></td></tr>
<tr><td>Nominal</td><td></td><td></td></tr>
<tr><td>2-year</td><td>4.85</td><td>4.87</td></tr>
<tr><td>10-year</td><td>5.11</td><td>5.18</td></tr>
<tr><td>Inflation indexed</td><td></td><td></td></tr>
</table>
</body></html>
"""

ECB_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,TIME_PERIOD,OBS_VALUE
EXR.D.USD.EUR.SP00.A,D,USD,EUR,2026-09-23,1.1800
EXR.D.USD.EUR.SP00.A,D,USD,EUR,2026-09-24,1.1825
"""


def _snapshot(url: str, text: str, content_type: str) -> RawSnapshot:
    import hashlib

    return RawSnapshot(
        source_id="",
        url=url,
        content_type=content_type,
        text=text,
        sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


def test_eia_url_requests_recent_rows_and_keeps_key_out_of_series_path():
    url = eia_series_url("PET.RWTC.D", "secret-key", length=10)
    assert "/v2/seriesid/PET.RWTC.D?" in url
    assert "length=10" in url
    assert "sort%5B0%5D%5Bcolumn%5D=period" in url
    assert "secret-key" in url


def test_collector_persists_raw_fed_ecb_and_builds_append_only_ledger_without_eia_key(tmp_path: Path):
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )

    def fake_fetch(url: str) -> RawSnapshot:
        if url == FED_H15_RELEASE_URL:
            return _snapshot(url, FED_HTML, "text/html")
        if url == ECB_USD_EUR_URL:
            return _snapshot(url, ECB_CSV, "text/csv")
        raise AssertionError(f"unexpected network URL: {url}")

    ledger = tmp_path / "ledger.json"
    raw_dir = tmp_path / "raw"
    result = collect_phase8f_prospective(
        macro_config=macro,
        existing_ledger_path=ledger,
        output_ledger_path=ledger,
        raw_snapshot_dir=raw_dir,
        ingested_at=datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc),
        eia_api_key="",
        fetcher=fake_fetch,
    )

    assert result["status"] == "COLLECTED_OUTCOME_BLIND_PIT_SNAPSHOT"
    assert result["eia_status"] == "SKIPPED_NO_API_KEY"
    assert result["new_observation_count"] == 8
    assert result["ledger_knowable_row_count"] == 8
    assert result["guards"]["historical_current_values_retrojected"] is False
    assert len(result["raw_snapshots"]) == 2
    assert all(Path(item["path"]).exists() for item in result["raw_snapshots"])

    payload = json.loads(ledger.read_text(encoding="utf-8"))
    assert payload["row_count"] == 8
    assert payload["coverage"]["series_count"] == 4
    assert set(payload["coverage"]["factor_counts"]) == {"fx", "rates_policy", "yield_curve"}
    assert all(
        row["availability_proof_type"] == "ACTUAL_PROSPECTIVE_INGESTION"
        for row in payload["observations"]
    )


def test_repeating_same_collection_time_and_payload_is_idempotent(tmp_path: Path):
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )

    def fake_fetch(url: str) -> RawSnapshot:
        if url == FED_H15_RELEASE_URL:
            return _snapshot(url, FED_HTML, "text/html")
        if url == ECB_USD_EUR_URL:
            return _snapshot(url, ECB_CSV, "text/csv")
        raise AssertionError(url)

    ledger = tmp_path / "ledger.json"
    raw_dir = tmp_path / "raw"
    ts = datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc)
    first = collect_phase8f_prospective(
        macro_config=macro,
        existing_ledger_path=ledger,
        output_ledger_path=ledger,
        raw_snapshot_dir=raw_dir,
        ingested_at=ts,
        eia_api_key="",
        fetcher=fake_fetch,
    )
    second = collect_phase8f_prospective(
        macro_config=macro,
        existing_ledger_path=ledger,
        output_ledger_path=ledger,
        raw_snapshot_dir=raw_dir,
        ingested_at=ts,
        eia_api_key="",
        fetcher=fake_fetch,
    )
    assert first["ledger_row_count"] == second["ledger_row_count"] == 8
