"""Research-only, PIT-safe external context attached to a private Depot Watch.

This is a transport/visibility adapter, NOT Phase-8 promotion or trading evidence.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping


class ExternalWatchContextError(ValueError):
    pass


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ExternalWatchContextError(f"{label}_required")
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExternalWatchContextError(f"{label}_invalid") from exc
    if date.tzinfo is None or date.utcoffset() is None:
        raise ExternalWatchContextError(f"{label}_must_have_timezone")
    return date.astimezone(timezone.utc)


def attach_external_context(
    watch: Mapping[str, object],
    external: Mapping[str, object],
    *,
    decision_as_of: str,
) -> dict[str, object]:
    """Attach vetted external observations without modifying any Decision fields.

    Unknown/missing evidence cannot be interpreted as neutral, and observations
    discovered after the decision timestamp are rejected, not backdated.
    """
    if external.get("schema_version") != "argus_watch_external_context_v1":
        raise ExternalWatchContextError("external_schema_version_invalid")
    if external.get("research_only") is not True:
        raise ExternalWatchContextError("external_must_be_research_only")
    cutoff = _timestamp(decision_as_of, "decision_as_of")
    records = external.get("observations")
    if not isinstance(records, list):
        raise ExternalWatchContextError("external_observations_must_be_list")
    rows = watch.get("rows")
    if not isinstance(rows, list):
        raise ExternalWatchContextError("watch_rows_must_be_list")
    symbols = {
        str(row.get("symbol"))
        for row in rows
        if isinstance(row, Mapping) and isinstance(row.get("symbol"), str)
    }
    by_symbol: dict[str, list[dict[str, object]]] = {symbol: [] for symbol in symbols}
    global_observations: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in records:
        if not isinstance(item, Mapping):
            raise ExternalWatchContextError("external_observation_invalid")
        required = ("id", "symbol", "source", "published_at", "ingested_at", "status", "summary")
        if any(not isinstance(item.get(k), str) or not item[k].strip() for k in required):
            raise ExternalWatchContextError("external_observation_required_fields_missing")
        evidence_id = str(item["id"])
        if evidence_id in seen:
            raise ExternalWatchContextError("external_duplicate_id")
        seen.add(evidence_id)
        published = _timestamp(item["published_at"], "published_at")
        ingested = _timestamp(item["ingested_at"], "ingested_at")
        if published > ingested:
            raise ExternalWatchContextError("external_ingested_before_published")
        if ingested > cutoff or published > cutoff:
            raise ExternalWatchContextError("external_observation_future_of_decision")
        if item["status"] not in {
            "KNOWN", "UNKNOWN", "NOT_APPLICABLE", "STALE",
            "LICENSED_OUT", "LOW_COVERAGE", "CONFLICTING_SOURCES",
        }:
            raise ExternalWatchContextError("external_status_invalid")
        if item["status"] == "KNOWN" and not str(item.get("source_url") or "").startswith(("https://", "http://")):
            raise ExternalWatchContextError("external_known_requires_source_url")
        sym = str(item["symbol"])
        if sym == "__GLOBAL__" or sym in by_symbol:
            target = global_observations if sym == "__GLOBAL__" else by_symbol[sym]
            target.append({
                "id": evidence_id,
                "source": str(item["source"]),
                "source_url": item.get("source_url"),
                "published_at": item["published_at"],
                "ingested_at": item["ingested_at"],
                "status": item["status"],
                "summary": item["summary"],
                "research_only": True,
                "decision_effect": "none",
            })
    result = deepcopy(dict(watch))
    for row in result["rows"]:
        if not isinstance(row, dict):
            continue
        evidence = by_symbol.get(str(row.get("symbol")), [])
        row["external_research_context"] = {
            "status": "AVAILABLE_RESEARCH_ONLY" if evidence else "NOT_SUPPLIED",
            "observations": evidence,
            "decision_effect": "none",
        }
    result["global_external_research_context"] = {
        "status": "AVAILABLE_RESEARCH_ONLY" if global_observations else "NOT_SUPPLIED",
        "observations": global_observations,
        "decision_effect": "none",
    }
    result["external_research_context_diagnostics"] = {
        "schema_version": "argus_watch_external_context_diagnostics_v1",
        "research_only": True,
        "decision_effect": "none",
        "observation_count": len(records),
        "matched_symbol_count": sum(bool(v) for v in by_symbol.values()),
        "global_observation_count": len(global_observations),
        "unmatched_observation_count": sum(1 for o in records if o["symbol"] not in symbols and o["symbol"] != "__GLOBAL__"),
        "publication_cutoff": decision_as_of,
    }
    return result
