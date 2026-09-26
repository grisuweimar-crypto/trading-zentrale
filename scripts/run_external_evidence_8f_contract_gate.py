from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import load_effective_exposure_map
from scanner.research.external_evidence.macro_exposure_8f import validate_phase8f_contract
from scanner.research.external_evidence.market_proxies_8f import validate_market_proxies
from scanner.research.external_evidence.series_catalog_8f import validate_series_catalog
from scanner.research.external_evidence.source_candidates_8f import validate_source_candidates


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MACRO_CONFIG = ROOT / "configs/external_evidence_8f_macro_exposure_v1.json"
DEFAULT_EXPOSURE_MAP = ROOT / "configs/external_evidence_8f_exposure_map_v1.json"
DEFAULT_OVERLAY_REGISTRY = ROOT / "configs/external_evidence_8f_exposure_overlays_v1.json"
DEFAULT_SOURCE_CANDIDATES = ROOT / "configs/external_evidence_8f_source_candidates_v1.json"
DEFAULT_MARKET_PROXIES = ROOT / "configs/external_evidence_8f_market_proxies_v1.json"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_exposure(path: Path, overlay_registry: Path) -> dict:
    if path.resolve() == DEFAULT_EXPOSURE_MAP.resolve():
        return load_effective_exposure_map(root=ROOT, registry_path=overlay_registry)
    return _load_json(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the outcome-blind Phase 8F macro-vintage, source-routing, "
            "series-catalog, market-proxy and effective exposure-map foundation."
        )
    )
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO_CONFIG)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE_MAP)
    parser.add_argument("--overlay-registry", type=Path, default=DEFAULT_OVERLAY_REGISTRY)
    parser.add_argument("--source-candidates", type=Path, default=DEFAULT_SOURCE_CANDIDATES)
    parser.add_argument("--market-proxies", type=Path, default=DEFAULT_MARKET_PROXIES)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    macro_config = _load_json(args.macro_config)
    exposure_map = _load_exposure(args.exposure_map, args.overlay_registry)
    source_candidates = _load_json(args.source_candidates)
    market_proxies = _load_json(args.market_proxies)

    foundation = validate_phase8f_contract(macro_config, exposure_map)
    allowed_factor_ids = {
        str(item.get("factor_id"))
        for item in macro_config.get("factor_catalog") or []
        if item.get("factor_id")
    }
    routing = validate_source_candidates(source_candidates, allowed_factor_ids=allowed_factor_ids)
    series_catalog = validate_series_catalog(macro_config, source_candidates)
    proxies = validate_market_proxies(market_proxies, allowed_factor_ids=allowed_factor_ids)

    result = {
        "schema_version": "external_evidence_8f_foundation_gate_v3",
        "phase": "8F_macro_exposure_context",
        "status": "PASS_FOUNDATION_SOURCE_SERIES_PROXY_AND_EFFECTIVE_EXPOSURE_CONTRACTS",
        "foundation": foundation,
        "source_routing": routing,
        "series_catalog": series_catalog,
        "market_proxies": proxies,
    }
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
