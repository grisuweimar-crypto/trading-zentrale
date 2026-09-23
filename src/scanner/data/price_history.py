"""Validation, immutable merge and coverage for observed daily market prices."""
from collections import Counter, defaultdict
from datetime import date
import math
from statistics import median


# `adj_close` is persisted separately from raw OHLCV.  Existing repositories may
# still contain the legacy schemas without this field; callers migrate them by
# re-encoding the validated rows with the canonical columns below.
MARKET_COLUMNS = "date symbol currency open high low close adj_close volume retrieved_at".split()
LEGACY_MARKET_COLUMNS = "date symbol currency open high low close volume retrieved_at".split()
PRICE_COLUMNS = "date symbol currency open high low close adj_close volume source retrieved_at observation_type".split()
LEGACY_PRICE_COLUMNS = "date symbol currency open high low close volume source retrieved_at observation_type".split()
STATUSES = ("price_data_ok", "price_data_partial", "price_data_unavailable", "ticker_mapping_missing", "provider_error")


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def invalid_reason(row):
    if not str(row.get("symbol") or "").strip():
        return "empty_symbol"
    try:
        date.fromisoformat(str(row.get("date", "")))
    except ValueError:
        return "invalid_date"
    close = number(row.get("close"))
    if close is None or close <= 0:
        return "invalid_close"
    adjusted = row.get("adj_close")
    if adjusted not in (None, ""):
        adjusted_number = number(adjusted)
        if adjusted_number is None or adjusted_number <= 0:
            return "invalid_adj_close"
    values = {"close": close}
    for field in ("open", "high", "low", "volume"):
        value = row.get(field)
        if value in (None, ""):
            continue
        parsed = number(value)
        if parsed is None or (parsed < 0 if field == "volume" else parsed <= 0):
            return "invalid_" + field
        values[field] = parsed
    high, low = values.get("high"), values.get("low")
    if high is not None and any(v > high * (1 + 1e-10) for k, v in values.items() if k not in ("high", "volume")):
        return "inconsistent_high"
    if low is not None and any(v < low * (1 - 1e-10) for k, v in values.items() if k not in ("low", "volume")):
        return "inconsistent_low"
    return None


def signature(row):
    """Raw observed-session signature; adjusted close is enrichment, not OHLCV."""
    # Retrieval timestamps/provenance and adjusted close do not make the raw
    # observed session different.  This lets a later provider fetch enrich a
    # legacy row with adj_close without rewriting its raw OHLCV observation.
    return (row.get("currency", ""), *(number(row.get(k)) for k in ("open", "high", "low", "close", "volume")))


def _adj_close(row):
    value = number(row.get("adj_close"))
    return value if value is not None and value > 0 else None


def _fill_adjusted(existing, incoming):
    """Return an enriched copy when only a previously-missing adj_close is added."""
    if _adj_close(existing) is not None or _adj_close(incoming) is None:
        return existing, False
    updated = dict(existing)
    updated["adj_close"] = incoming.get("adj_close", "")
    return updated, True


def validated_rows(rows):
    """Reject malformed rows and conflicting duplicate keys; retain original fields.

    Duplicate copies of the same raw session may enrich a missing adjusted close.
    Two different non-empty adjusted closes for the same otherwise-identical raw
    session are treated as an ambiguity rather than silently choosing one.
    """
    kept, conflicts, issues = {}, set(), defaultdict(Counter)
    for original in rows:
        row = {key: "" if value is None else str(value) for key, value in original.items()}
        row["symbol"] = row.get("symbol", "").strip()
        symbol = row["symbol"]
        reason = invalid_reason(row)
        if reason:
            issues[symbol][reason] += 1
            continue
        key = (symbol, row["date"])
        if key in kept:
            previous = kept[key]
            if signature(previous) != signature(row):
                conflicts.add(key)
                continue
            previous_adj, row_adj = _adj_close(previous), _adj_close(row)
            if previous_adj is not None and row_adj is not None and not math.isclose(previous_adj, row_adj, rel_tol=1e-12, abs_tol=1e-12):
                conflicts.add(key)
                continue
            enriched, changed = _fill_adjusted(previous, row)
            if changed:
                kept[key] = enriched
        else:
            kept[key] = row
    for key in conflicts:
        kept.pop(key, None)
        issues[key[0]]["conflicting_duplicate_session"] += 1
    return list(kept.values()), {symbol: dict(counts) for symbol, counts in issues.items()}


def merge_prices(existing, incoming):
    """Keep raw observed OHLCV immutable while allowing adj_close enrichment.

    Conflicting duplicates inside either input have no established winner and
    are excluded. A later download cannot rewrite an existing raw observation,
    but it may fill a previously missing adjusted close for the same symbol/date.
    """
    old, old_issues = validated_rows(existing)
    new, new_issues = validated_rows(incoming)
    issues = defaultdict(Counter)
    for source in (old_issues, new_issues):
        for symbol, counts in source.items():
            issues[symbol].update(counts)
    result = {(r["symbol"], r["date"]): r for r in old}
    old_valid_keys = set(result)
    ambiguous = {(str(r.get("symbol", "")).strip(), r.get("date")) for r in existing if not invalid_reason(r)} - old_valid_keys
    for row in new:
        key = (row["symbol"], row["date"])
        if key in ambiguous:
            continue
        if key in result:
            current = result[key]
            if signature(current) != signature(row):
                issues[row["symbol"]]["provider_revision_kept_existing"] += 1
            enriched, changed = _fill_adjusted(current, row)
            if changed:
                result[key] = enriched
        else:
            result[key] = row
    return sorted(result.values(), key=lambda row: (row["symbol"], row["date"])), {
        symbol: dict(counts) for symbol, counts in issues.items() if counts
    }


def coverage(required_symbols, rows, *, minimum_sessions=300, as_of, fetch_state=None):
    valid, invalid = validated_rows(rows)
    grouped = defaultdict(list)
    adjusted = defaultdict(int)
    for row in valid:
        if row["date"] <= as_of:
            grouped[row["symbol"]].append(row["date"])
            if _adj_close(row) is not None:
                adjusted[row["symbol"]] += 1
    details = {}
    fetch_state = fetch_state or {}
    for symbol in sorted(set(required_symbols)):
        days = sorted(grouped[symbol])
        fetch = fetch_state.get(symbol, {})
        status = "price_data_ok" if len(days) >= minimum_sessions else "price_data_partial" if days else "price_data_unavailable"
        if fetch.get("status") in ("ticker_mapping_missing", "provider_error"):
            status = fetch["status"]
        details[symbol] = {
            "status": status, "sessions": len(days), "adjusted_sessions": adjusted[symbol],
            "first_session": days[0] if days else None, "last_session": days[-1] if days else None,
            "provider_symbol": fetch.get("provider_symbol"), "update_mode": fetch.get("update_mode"),
            "last_attempt_at": fetch.get("last_attempt_at"), "reason": fetch.get("reason"),
            "validation_issues": dict(Counter(fetch.get("validation_issues", {})) + Counter(invalid.get(symbol, {}))),
        }
        if status == "price_data_partial" and not details[symbol]["reason"]:
            details[symbol]["reason"] = "fewer than target observed sessions; no synthetic prices"
    counts = [item["sessions"] for item in details.values()]
    return {
        "required_symbol_count": len(details), "covered_symbol_count": sum(n > 0 for n in counts),
        "complete_symbol_count": sum(n >= minimum_sessions for n in counts),
        "partial_symbol_count": sum(0 < n < minimum_sessions for n in counts),
        "unavailable_symbol_count": sum(n == 0 for n in counts),
        "minimum_sessions_target": minimum_sessions,
        "symbols_with_40_or_more_sessions": sum(n >= 40 for n in counts),
        "symbols_with_300_or_more_sessions": sum(n >= 300 for n in counts),
        "median_sessions": median(counts) if counts else 0,
        "min_sessions": min(counts, default=0), "max_sessions": max(counts, default=0),
        "as_of": as_of,
        "status_counts": {s: sum(d["status"] == s for d in details.values()) for s in STATUSES},
        "symbols": details,
    }
