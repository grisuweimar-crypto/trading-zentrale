from __future__ import annotations

from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8f_market_proxies_v1"


class MarketProxy8FError(ValueError):
    pass


def validate_market_proxies(
    config: Mapping[str, Any],
    *,
    allowed_factor_ids: set[str],
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise MarketProxy8FError("unexpected Phase 8F market-proxy schema_version")

    rules = config.get("rules") or {}
    required_rules = {
        "proxy_may_be_labeled_as_underlying_spot": False,
        "different_proxy_series_may_be_spliced_into_one_history": False,
        "proxy_direction_may_be_assigned": False,
        "proxy_weight_may_be_selected": False,
        "market_outcomes_may_be_read": False,
        "price_source_rights_must_be_cleared_before_ingestion": True,
        "historical_price_basis_must_be_explicit": True,
        "adjusted_history_may_be_retrojected_without_corporate_action_provenance": False,
    }
    wrong = [key for key, expected in required_rules.items() if rules.get(key) is not expected]
    if wrong:
        raise MarketProxy8FError("invalid Phase 8F market-proxy rules: " + ", ".join(sorted(wrong)))

    proxies = config.get("proxies") or []
    proxy_by_id: dict[str, Mapping[str, Any]] = {}
    for proxy in proxies:
        proxy_id = str(proxy.get("proxy_id") or "").strip()
        factor_id = str(proxy.get("factor_id") or "").strip()
        if not proxy_id:
            raise MarketProxy8FError("every market proxy requires proxy_id")
        if proxy_id in proxy_by_id:
            raise MarketProxy8FError(f"duplicate proxy_id: {proxy_id}")
        if factor_id not in allowed_factor_ids:
            raise MarketProxy8FError(f"proxy {proxy_id} references unknown factor {factor_id}")
        for field in (
            "instrument",
            "proxy_class",
            "relationship_to_factor",
            "preferred_measure",
            "pit_status",
            "license_status",
            "build_status",
        ):
            if not str(proxy.get(field) or "").strip():
                raise MarketProxy8FError(f"proxy {proxy_id} missing {field}")
        if not proxy.get("source_urls"):
            raise MarketProxy8FError(f"proxy {proxy_id} requires source_urls")
        if not proxy.get("limitations"):
            raise MarketProxy8FError(f"proxy {proxy_id} requires explicit limitations")
        proxy_by_id[proxy_id] = proxy

    routing = config.get("preferred_routing") or {}
    if set(routing) != {"uranium", "lithium"}:
        raise MarketProxy8FError("market-proxy routing must cover uranium and lithium only")

    for factor_id, proxy_ids in routing.items():
        if not proxy_ids:
            raise MarketProxy8FError(f"market-proxy routing for {factor_id} cannot be empty")
        if len(proxy_ids) != len(set(proxy_ids)):
            raise MarketProxy8FError(f"duplicate proxy routing for factor {factor_id}")
        for proxy_id in proxy_ids:
            proxy = proxy_by_id.get(str(proxy_id))
            if proxy is None:
                raise MarketProxy8FError(f"unknown proxy {proxy_id} routed to {factor_id}")
            if proxy.get("factor_id") != factor_id:
                raise MarketProxy8FError(f"proxy {proxy_id} does not belong to routed factor {factor_id}")

    if "uranium_physical_sput_nav" not in routing["uranium"]:
        raise MarketProxy8FError("SPUT physical NAV challenger must remain in uranium routing")
    if "lithium_cme_futures" not in routing["lithium"]:
        raise MarketProxy8FError("CME lithium futures challenger must remain in lithium routing")

    return {
        "schema_version": "external_evidence_8f_market_proxy_gate_v1",
        "status": "PASS_MARKET_PROXY_CONTRACT",
        "proxy_count": len(proxy_by_id),
        "uranium_proxy_count": len(routing["uranium"]),
        "lithium_proxy_count": len(routing["lithium"]),
        "spot_substitution_enabled": False,
        "proxy_splicing_enabled": False,
        "direction_assigned": False,
        "outcomes_read": False,
    }
