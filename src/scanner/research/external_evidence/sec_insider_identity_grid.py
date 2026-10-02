from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Mapping

IDENTITY_SCHEMA = "external_evidence_8d_insider_asof_identity_v1"


class SecInsiderIdentityGridError(ValueError):
    pass


def verified_feature_grid(identity_payload: Mapping[str, Any]) -> list[dict[str, str]]:
    if identity_payload.get("schema_version") != IDENTITY_SCHEMA:
        raise SecInsiderIdentityGridError("unsupported B5 identity schema")
    guards = identity_payload.get("guards") or {}
    if guards.get("market_outcomes_read") is not False:
        raise SecInsiderIdentityGridError("B5 identity result must remain outcome-blind")
    if guards.get("current_ticker_retrojection_enabled") is not False:
        raise SecInsiderIdentityGridError("B5 current-ticker retrojection must remain disabled")

    selected: dict[tuple[str, str], dict[str, str]] = {}
    for row in identity_payload.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        if row.get("identity_status") != "VERIFIED_FAMILY_PIT_IDENTITY":
            continue
        cik = str(row.get("issuer_cik") or "").strip()
        as_of = str(row.get("as_of") or "").strip()
        if not cik or not as_of:
            raise SecInsiderIdentityGridError("verified identity row missing issuer_cik/as_of")
        key = (cik, as_of)
        selected[key] = {"issuer_cik": cik, "as_of": as_of}
    return [selected[key] for key in sorted(selected, key=lambda value: (value[1], value[0]))]


def write_feature_grid_csv(rows: list[Mapping[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["issuer_cik", "as_of"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "issuer_cik": str(row.get("issuer_cik") or ""),
                    "as_of": str(row.get("as_of") or ""),
                }
            )
