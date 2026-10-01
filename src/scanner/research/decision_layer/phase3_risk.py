"""Typed same-snapshot bridge from existing Risk-vNext fields into Phase 7A.

This module does not calculate a new risk score, class, volatility threshold,
ATR metric, gate, lock, position size or stop.  It only transports fields that
already exist on the authoritative scanner row.  Missing source fields remain
missing.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Mapping

import pandas as pd

from scanner.reports.risk_vnext import RISK_FEATURE_ALIASES


PHASE3_RISK_SOURCE_VERSION = "phase3_risk_vnext_current"

# W3 names these as desirable existing outputs.  They are pass-through only:
# if the authoritative snapshot does not contain them, the adapter does not
# synthesize them.  The current 2026-09-30 snapshot does not contain these
# columns, so they remain absent until an upstream producer actually publishes
# them.
OPTIONAL_EXISTING_RISK_FIELDS = (
    "atr_pct",
    "persistent_strong_volatility",
    "risk_class",
    "volatility_lock",
    "volatility_gate",
    "risk_quality_status",
)


class Phase3RiskAdapterError(ValueError):
    """Raised when a current Risk-vNext row cannot be admitted without inference."""


def _clean(value: object) -> object | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (AttributeError, ValueError):
            pass
    return deepcopy(value)


def _first_existing_value(row: Mapping[str, object], aliases: tuple[str, ...]) -> tuple[str | None, object | None]:
    for name in aliases:
        if name not in row:
            continue
        value = _clean(row.get(name))
        if value is not None:
            return name, value
    return None, None


def build_phase3_risk_row(
    *,
    row: Mapping[str, object],
    expected_snapshot_id: str,
) -> dict[str, object] | None:
    """Return one non-directional Risk family row from existing snapshot data.

    No missing metric is reconstructed.  Existing gate/lock fields, if an
    upstream producer publishes them, are copied byte-for-value/semantic-value
    and are never relaxed or reclassified here.
    """
    symbol = str(row.get("symbol") or "").strip()
    snapshot_id = str(row.get("snapshot_id") or "").strip()
    generated_at = str(row.get("generated_at") or "").strip()
    if not symbol:
        raise Phase3RiskAdapterError("risk_symbol_required")
    if not expected_snapshot_id or snapshot_id != expected_snapshot_id:
        raise Phase3RiskAdapterError("risk_snapshot_mismatch")
    if not generated_at:
        raise Phase3RiskAdapterError("risk_generated_at_required")

    payload: dict[str, object] = {}
    source_fields: dict[str, str] = {}

    # Reuse the frozen Phase-3 alias definitions rather than defining a second
    # risk taxonomy in the Decision Layer.
    for canonical, aliases in RISK_FEATURE_ALIASES.items():
        source, value = _first_existing_value(row, aliases)
        if source is not None:
            payload[canonical] = value
            source_fields[canonical] = source

    # Pass through only exact upstream W3 fields.  No fallback formula or
    # classification is allowed here.
    for field in OPTIONAL_EXISTING_RISK_FIELDS:
        if field in row:
            value = _clean(row.get(field))
            if value is not None:
                payload[field] = value
                source_fields[field] = field

    if not payload:
        return None

    payload["source_fields"] = source_fields
    payload["missing_values_backfilled"] = False
    payload["risk_is_directional_vote"] = False

    # Presence of the existing aggregate scanner risk makes the current risk
    # context available.  Factor-only rows are retained as limited coverage;
    # nothing is imputed to upgrade them.
    coverage_state = "available" if "aggregate_risk" in payload else "limited"
    source_version = (
        f"{PHASE3_RISK_SOURCE_VERSION}:"
        f"{row.get('scoring_version')}:{row.get('schema_version')}"
    )
    return {
        "family": "risk",
        "claim_id": f"risk:{symbol}:phase3:{snapshot_id}",
        "as_of": generated_at,
        "available_from": generated_at,
        "source_version": source_version,
        "coverage_state": coverage_state,
        "maturity_state": "not_yet_mature",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": payload,
    }
