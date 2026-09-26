from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from scanner.research.external_evidence.ecb_fx_8f import (
    build_ecb_fx_prospective_macro_observations,
)
from scanner.research.external_evidence.eia_energy_8f import (
    SERIES_SPECS as EIA_SERIES_SPECS,
    build_eia_prospective_macro_observations,
)
from scanner.research.external_evidence.fed_h15_release_8f import (
    build_fed_h15_release_prospective_observations,
)
from scanner.research.external_evidence.macro_ledger_8f import build_macro_ledger


FED_H15_RELEASE_URL = "https://www.federalreserve.gov/releases/h15/current/"
ECB_USD_EUR_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
    "?format=csvdata&lastNObservations=10"
)
EIA_SERIES_BASE_URL = "https://api.eia.gov/v2/seriesid/{series_id}"


class ProspectiveCollector8FError(RuntimeError):
    pass


@dataclass(frozen=True)
class RawSnapshot:
    source_id: str
    url: str
    content_type: str
    text: str
    sha256: str


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fetch_text(url: str, *, timeout_seconds: int = 30) -> RawSnapshot:
    request = Request(
        url,
        headers={
            "User-Agent": "Scanner-vNext-Phase8F/1.0 (+research collector)",
            "Accept": "text/html,text/csv,application/json;q=0.9,*/*;q=0.5",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - fixed official-source URLs
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            text = raw.decode(charset, errors="strict")
            content_type = response.headers.get_content_type() or "application/octet-stream"
    except Exception as exc:  # network errors remain explicit; no silent fallback data
        raise ProspectiveCollector8FError(f"failed to fetch {url}: {exc}") from exc
    return RawSnapshot(
        source_id="",
        url=url,
        content_type=content_type,
        text=text,
        sha256=_sha256(text),
    )


def eia_series_url(series_id: str, api_key: str, *, length: int = 10) -> str:
    if series_id not in EIA_SERIES_SPECS:
        raise ProspectiveCollector8FError(f"unregistered EIA series: {series_id}")
    if not api_key.strip():
        raise ProspectiveCollector8FError("EIA api_key cannot be empty")
    query = urlencode(
        {
            "api_key": api_key,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": str(length),
        }
    )
    return EIA_SERIES_BASE_URL.format(series_id=quote(series_id, safe=".-")) + "?" + query


def _load_existing_observations(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(row) for row in payload]
    if isinstance(payload, Mapping):
        observations = payload.get("observations")
        if isinstance(observations, list):
            return [dict(row) for row in observations]
    raise ProspectiveCollector8FError("existing ledger must be a ledger object or observation list")


def _write_raw_snapshot(root: Path, *, source_id: str, snapshot: RawSnapshot, timestamp: str) -> str:
    source_dir = root / source_id
    source_dir.mkdir(parents=True, exist_ok=True)
    extension = {
        "text/html": ".html",
        "text/csv": ".csv",
        "application/json": ".json",
    }.get(snapshot.content_type, ".txt")
    safe_timestamp = timestamp.replace(":", "-").replace("+", "_")
    path = source_dir / f"{safe_timestamp}_{snapshot.sha256[:12]}{extension}"
    path.write_text(snapshot.text, encoding="utf-8")
    return str(path)


def collect_phase8f_prospective(
    *,
    macro_config: Mapping[str, Any],
    existing_ledger_path: Path | None,
    output_ledger_path: Path,
    raw_snapshot_dir: Path,
    ingested_at: datetime | None = None,
    eia_api_key: str | None = None,
    fetcher: Callable[[str], RawSnapshot] = fetch_text,
) -> dict[str, Any]:
    now = ingested_at or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ProspectiveCollector8FError("ingested_at must be timezone-aware")

    allowed_series_ids = {
        str(row.get("series_id"))
        for row in macro_config.get("series_catalog") or []
        if row.get("series_id")
    }
    if not allowed_series_ids:
        raise ProspectiveCollector8FError("macro config has no allowed series_catalog")

    observations: list[dict[str, Any]] = []
    raw_manifest: list[dict[str, Any]] = []
    timestamp = now.isoformat()

    fed = fetcher(FED_H15_RELEASE_URL)
    fed = RawSnapshot("federal_reserve_board_h15", fed.url, fed.content_type, fed.text, fed.sha256)
    observations.extend(build_fed_h15_release_prospective_observations(fed.text, ingested_at=now))
    raw_manifest.append(
        {
            "source_id": fed.source_id,
            "url": fed.url,
            "sha256": fed.sha256,
            "path": _write_raw_snapshot(raw_snapshot_dir, source_id=fed.source_id, snapshot=fed, timestamp=timestamp),
        }
    )

    ecb = fetcher(ECB_USD_EUR_URL)
    ecb = RawSnapshot("ecb_data_portal", ecb.url, ecb.content_type, ecb.text, ecb.sha256)
    observations.extend(build_ecb_fx_prospective_macro_observations(ecb.text, ingested_at=now))
    raw_manifest.append(
        {
            "source_id": ecb.source_id,
            "url": ecb.url,
            "sha256": ecb.sha256,
            "path": _write_raw_snapshot(raw_snapshot_dir, source_id=ecb.source_id, snapshot=ecb, timestamp=timestamp),
        }
    )

    key = (eia_api_key if eia_api_key is not None else os.getenv("EIA_API_KEY", "")).strip()
    eia_status = "SKIPPED_NO_API_KEY"
    if key:
        eia_status = "COLLECTED"
        for series_id in sorted(EIA_SERIES_SPECS):
            url = eia_series_url(series_id, key, length=10)
            snap = fetcher(url)
            snap = RawSnapshot("eia_open_data_energy", snap.url, snap.content_type, snap.text, snap.sha256)
            try:
                payload = json.loads(snap.text)
            except json.JSONDecodeError as exc:
                raise ProspectiveCollector8FError(f"EIA response is not JSON for {series_id}") from exc
            observations.extend(
                build_eia_prospective_macro_observations(payload, series_id=series_id, ingested_at=now)
            )
            raw_manifest.append(
                {
                    "source_id": snap.source_id,
                    "series_id": series_id,
                    "url": url.replace(key, "***"),
                    "sha256": snap.sha256,
                    "path": _write_raw_snapshot(raw_snapshot_dir, source_id=snap.source_id, snapshot=snap, timestamp=timestamp),
                }
            )

    unknown_series = {row["series_id"] for row in observations} - allowed_series_ids
    if unknown_series:
        raise ProspectiveCollector8FError(
            f"collector produced series outside frozen catalog: {sorted(unknown_series)}"
        )

    ledger = build_macro_ledger(
        existing_rows=_load_existing_observations(existing_ledger_path),
        new_rows=observations,
        allowed_series_ids=allowed_series_ids,
        as_of=now,
    )
    output_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    output_ledger_path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {
        "schema_version": "external_evidence_8f_prospective_collection_v1",
        "phase": "8F_macro_exposure_context",
        "status": "COLLECTED_OUTCOME_BLIND_PIT_SNAPSHOT",
        "ingested_at": timestamp,
        "new_observation_count": len(observations),
        "ledger_row_count": ledger["row_count"],
        "ledger_knowable_row_count": ledger["knowable_row_count"],
        "eia_status": eia_status,
        "raw_snapshots": raw_manifest,
        "guards": {
            "raw_payloads_persisted": True,
            "append_only_ledger_used": True,
            "market_outcomes_read": False,
            "historical_current_values_retrojected": False,
            "phase7_integration_enabled": False,
        },
    }
