from __future__ import annotations

from typing import Any, Mapping

from scanner.research.external_evidence.bls_cpi_8f import SERIES_MOM_SA, SERIES_YOY_NSA
from scanner.research.external_evidence.ecb_fx_8f import SERIES_ID as ECB_FX_SERIES_ID
from scanner.research.external_evidence.eia_energy_8f import SERIES_SPECS as EIA_SERIES_SPECS
from scanner.research.external_evidence.fed_h15_8f import SERIES_SPECS as FED_H15_SERIES_SPECS


class SeriesCatalog8FError(ValueError):
    pass


def validate_series_catalog(
    macro_config: Mapping[str, Any],
    source_candidates: Mapping[str, Any],
) -> dict[str, Any]:
    catalog = macro_config.get("series_catalog") or []
    if not catalog:
        raise SeriesCatalog8FError("Phase 8F series_catalog cannot be empty once adapters are implemented")

    source_by_id = {
        str(item.get("source_id")): item
        for item in source_candidates.get("sources") or []
        if item.get("source_id")
    }
    blocked_ids = {
        str(item.get("source_id"))
        for item in source_candidates.get("blocked_sources") or []
        if item.get("source_id")
    }
    factor_ids = {
        str(item.get("factor_id"))
        for item in macro_config.get("factor_catalog") or []
        if item.get("factor_id")
    }

    seen: set[str] = set()
    implemented_sources: set[str] = set()
    for item in catalog:
        series_id = str(item.get("series_id") or "").strip()
        factor_id = str(item.get("factor_id") or "").strip()
        source_id = str(item.get("source_id") or "").strip()
        availability_mode = str(item.get("availability_mode") or "").strip()
        status = str(item.get("status") or "").strip()

        if not series_id or not factor_id or not source_id or not availability_mode or not status:
            raise SeriesCatalog8FError("every series_catalog row requires series_id/factor_id/source_id/availability_mode/status")
        if series_id in seen:
            raise SeriesCatalog8FError(f"duplicate Phase 8F series_id: {series_id}")
        seen.add(series_id)
        if factor_id not in factor_ids:
            raise SeriesCatalog8FError(f"series {series_id} references unknown factor {factor_id}")
        if source_id in blocked_ids:
            raise SeriesCatalog8FError(f"series {series_id} uses blocked source {source_id}")
        source = source_by_id.get(source_id)
        if source is None:
            raise SeriesCatalog8FError(f"series {series_id} uses unknown source {source_id}")
        if factor_id not in set(source.get("factors") or []):
            raise SeriesCatalog8FError(f"series {series_id} factor {factor_id} is not declared by source {source_id}")
        if status == "ADAPTER_IMPLEMENTED":
            implemented_sources.add(source_id)

    required_groups = {
        "BLS": {SERIES_MOM_SA, SERIES_YOY_NSA},
        "EIA": set(EIA_SERIES_SPECS),
        "FED_H15": set(FED_H15_SERIES_SPECS),
        "ECB_FX": {ECB_FX_SERIES_ID},
    }
    for label, required in required_groups.items():
        missing = required - seen
        if missing:
            raise SeriesCatalog8FError(
                f"implemented {label} adapter series missing from catalog: {sorted(missing)}"
            )

    declared_adapter_sources = set(macro_config.get("source_contract", {}).get("adapter_source_ids") or [])
    if declared_adapter_sources != implemented_sources:
        raise SeriesCatalog8FError(
            f"adapter source mismatch declared={sorted(declared_adapter_sources)} implemented={sorted(implemented_sources)}"
        )

    return {
        "schema_version": "external_evidence_8f_series_catalog_gate_v1",
        "status": "PASS_SERIES_CATALOG_CONTRACT",
        "series_count": len(seen),
        "implemented_adapter_sources": sorted(implemented_sources),
        "blocked_series_count": 0,
    }
