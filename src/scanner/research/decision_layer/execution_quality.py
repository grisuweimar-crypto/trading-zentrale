"""Private ARGUS execution-quality evidence. No spread inferred from settlement alone."""
from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import json

SCHEMA = "argus_execution_quality_v1"

def money(v):
    return Decimal(str(v))

def build_evidence(*, isin, document_kind, document_time, bid=None, ask=None,
                   executed_price=None, units=None, cash_total=None,
                   quoted_spread_pct=None, estimated_entry_cost=None,
                   estimated_roundtrip_cost=None, standard_order_eur=None,
                   exchange=None, source_sha256=None):
    if document_kind not in {"ex_ante", "execution_statement"}:
        raise ValueError("unsupported_document_kind")
    timestamp = datetime.fromisoformat(document_time.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("timezone_required")
    if not isin or len(isin) != 12:
        raise ValueError("isin_required")
    if not source_sha256 or len(source_sha256) != 64:
        raise ValueError("source_hash_required")
    if (bid is None) != (ask is None):
        raise ValueError("bid_ask_must_be_paired")
    spread = None
    if bid is not None:
        b, a = money(bid), money(ask)
        if b <= 0 or a < b:
            raise ValueError("invalid_bid_ask")
        spread = str((a-b) / ((a+b)/2)*100)
    if document_kind == "execution_statement":
        if executed_price is None or units is None or cash_total is None:
            raise ValueError("execution_requires_fills")
        if money(executed_price) <= 0 or money(units) <= 0:
            raise ValueError("invalid_fill")
    if document_kind == "ex_ante" and executed_price is not None:
        raise ValueError("ex_ante_cannot_claim_execution")
    return {
        "schema_version": SCHEMA,
        "isin": isin, "document_kind": document_kind,
        "document_time": timestamp.isoformat(), "exchange": exchange,
        "source_sha256": source_sha256,
        "bid": str(money(bid)) if bid is not None else None,
        "ask": str(money(ask)) if ask is not None else None,
        "quoted_spread_pct_from_mid": spread,
        "broker_estimated_spread_pct": str(money(quoted_spread_pct)) if quoted_spread_pct is not None else None,
        "estimated_entry_cost_eur": str(money(estimated_entry_cost)) if estimated_entry_cost is not None else None,
        "estimated_roundtrip_cost_eur": str(money(estimated_roundtrip_cost)) if estimated_roundtrip_cost is not None else None,
        "standard_order_eur": str(money(standard_order_eur)) if standard_order_eur is not None else None,
        "executed_price_eur": str(money(executed_price)) if executed_price is not None else None,
        "units": str(money(units)) if units is not None else None,
        "settled_cash_eur": str(money(cash_total)) if cash_total is not None else None,
        "execution_spread_verified": False,
        "price_reference_quality": "quote_indicative" if document_kind == "ex_ante" else "fill_only",
        "is_trading_signal": False,
    }
