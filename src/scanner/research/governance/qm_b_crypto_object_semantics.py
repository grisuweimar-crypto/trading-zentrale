"""QM-B crypto object semantics.

Research-only. This layer types preserved crypto identifiers without collapsing a
base-asset reference, a quote pair and a provider/venue instrument into one object.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_b_crypto_identifier import (
    build_crypto_identifier_timeline,
    parse_crypto_identifier,
)

SCHEMA_VERSION = "qm_b_crypto_object_semantics_v1"
RESULT_SCHEMA_VERSION = "qm_b_crypto_object_semantics_result_v1"
DEFAULT_CONTRACT = Path(__file__).resolve().parents[4] / "configs" / "qm_b_crypto_object_semantics_v1.json"


class CryptoObjectSemanticsError(ValueError):
    pass


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _sha256(path: Path) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise CryptoObjectSemanticsError(f"input_unreadable:{path}") from exc


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CryptoObjectSemanticsError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise CryptoObjectSemanticsError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise CryptoObjectSemanticsError("contract_scope_invalid")
    rules = payload.get("rules") or {}
    forbidden_true = (
        "shared_base_token_proves_identity",
        "internal_base_equals_quote_pair",
        "different_quote_pairs_are_equivalent",
        "quote_currency_may_be_dropped",
        "provider_namespace_may_be_inferred",
        "venue_may_be_inferred",
        "current_crypto_symbol_may_rewrite_historical_identifier",
        "historical_pair_may_promote_current_membership",
        "pair_presence_proves_asset_listing",
        "pair_presence_proves_asset_tradability",
        "pair_presence_proves_project_investability",
    )
    if any(rules.get(key) is not False for key in forbidden_true):
        raise CryptoObjectSemanticsError("unsafe_crypto_equivalence_rule")
    promotion = payload.get("promotion") or {}
    if any(value is not False for value in promotion.values()):
        raise CryptoObjectSemanticsError("crypto_promotion_must_remain_disabled")
    return payload


def classify_identifier(identifier: Any) -> dict[str, Any]:
    symbol = _clean(identifier).upper()
    parsed = parse_crypto_identifier(symbol)
    if parsed is None:
        return {
            "identifier": symbol,
            "parse_status": "UNSUPPORTED",
            "object_class": "UNKNOWN",
            "base": None,
            "quote": None,
            "semantic_object_id": None,
            "base_component_id": None,
            "stable_asset_identity_verified": False,
            "provider_instrument_identity_verified": False,
        }

    base = parsed["base"]
    base_component_id = f"candidate:crypto-base-component:{base}"
    if parsed["style"] == "INTERNAL_BASE":
        return {
            "identifier": symbol,
            "parse_status": "PARSED",
            "object_class": "INTERNAL_BASE_REFERENCE",
            "base": base,
            "quote": None,
            "semantic_object_id": f"candidate:project-crypto-reference:{base}",
            "base_component_id": base_component_id,
            "relationship_to_base_component": "REFERENCES_BASE_COMPONENT",
            "stable_asset_identity_verified": False,
            "provider_instrument_identity_verified": False,
        }

    quote = parsed["quote"]
    return {
        "identifier": symbol,
        "parse_status": "PARSED",
        "object_class": "QUOTE_PAIR_REFERENCE",
        "base": base,
        "quote": quote,
        "semantic_object_id": f"candidate:crypto-quote-pair:{base}:{quote}:provider-unknown",
        "base_component_id": base_component_id,
        "relationship_to_base_component": "HAS_BASE_COMPONENT",
        "stable_asset_identity_verified": False,
        "provider_instrument_identity_verified": False,
    }


def _validate_frozen_inputs(root: Path, contract: Mapping[str, Any]) -> dict[str, str]:
    actual: dict[str, str] = {}
    for key in ("history_analysis", "current_universe"):
        spec = (contract.get("inputs") or {}).get(key) or {}
        rel = _clean(spec.get("path"))
        expected = _clean(spec.get("sha256"))
        if not rel or not expected:
            raise CryptoObjectSemanticsError(f"frozen_input_spec_invalid:{key}")
        digest = _sha256(root / rel)
        actual[key] = digest
        if digest != expected:
            raise CryptoObjectSemanticsError(f"frozen_input_hash_mismatch:{key}:{digest}")
    return actual


def audit(root: str | Path, *, contract_path: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root).resolve()
    contract = load_contract(contract_path)
    hashes = _validate_frozen_inputs(root_path, contract)
    inputs = contract["inputs"]
    timeline = build_crypto_identifier_timeline(
        root_path / inputs["history_analysis"]["path"],
        current_universe_path=root_path / inputs["current_universe"]["path"],
        source_path_label=inputs["history_analysis"]["path"],
    )

    identifiers: dict[str, dict[str, Any]] = {}
    row_class_counts: Counter[str] = Counter()
    quote_sets: dict[str, set[str]] = defaultdict(set)
    bases_with_internal: set[str] = set()
    bases_with_pairs: set[str] = set()

    for base_row in timeline["bases"]:
        for detail in base_row["identifier_details"]:
            semantic = classify_identifier(detail["identifier"])
            if semantic["parse_status"] != "PARSED":
                raise CryptoObjectSemanticsError(f"timeline_identifier_unparseable:{detail['identifier']}")
            identifiers[semantic["identifier"]] = {
                **semantic,
                "first_seen": detail["first_seen"],
                "last_seen": detail["last_seen"],
                "row_count": detail["row_count"],
                "observed_currencies": detail["currencies"],
            }
            row_class_counts[semantic["object_class"]] += int(detail["row_count"])
            if semantic["object_class"] == "INTERNAL_BASE_REFERENCE":
                bases_with_internal.add(str(semantic["base"]))
            elif semantic["object_class"] == "QUOTE_PAIR_REFERENCE":
                bases_with_pairs.add(str(semantic["base"]))
                quote_sets[str(semantic["base"])].add(str(semantic["quote"]))

    current_symbols = sorted({symbol for row in timeline["bases"] for symbol in row["current_crypto_symbols"]})
    cross_class_bases = sorted(bases_with_internal & bases_with_pairs)
    multiple_quote_bases = sorted(base for base, quotes in quote_sets.items() if len(quotes) > 1)

    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "audit_gate_status": "PASS",
        "remaining_identity_status": "PARTIAL_BY_DESIGN",
        "research_only": True,
        "productive_integration_enabled": False,
        "frozen_input_commit": contract["frozen_input_commit"],
        "input_sha256": hashes,
        "scanner_crypto_row_count": int(timeline["scanner_crypto_row_count"]),
        "base_count": int(timeline["base_count"]),
        "identifier_form_count": len(identifiers),
        "current_crypto_symbol_count": len(current_symbols),
        "current_crypto_symbols": current_symbols,
        "row_object_class_counts": dict(sorted(row_class_counts.items())),
        "cross_class_base_count": len(cross_class_bases),
        "cross_class_bases": cross_class_bases,
        "multiple_quote_base_count": len(multiple_quote_bases),
        "multiple_quote_bases": multiple_quote_bases,
        "identifiers": [identifiers[key] for key in sorted(identifiers)],
        "stable_asset_identity_promoted": False,
        "provider_instrument_identity_promoted": False,
        "quote_pair_equivalence_promoted": False,
        "membership_promoted": False,
        "listing_promoted": False,
        "tradability_promoted": False,
        "investability_promoted": False,
        "semantic_invariants": {
            "internal_base_reference_is_quote_pair": False,
            "shared_base_component_is_identity": False,
            "quote_currency_is_semantically_material": True,
            "provider_instrument_requires_separate_evidence": True,
            "historical_identifier_is_preserved_verbatim": True,
        },
    }
