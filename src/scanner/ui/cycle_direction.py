"""CY-04 read-only descriptive direction; never a research or trading signal.

The public loader fails closed until a separately reviewed CY-03 release adapter
certifies source/PIT eligibility. The pure projection can be tested with
synthetic, explicitly eligible historical observations without promoting the
quarantined production ledger.
"""
from __future__ import annotations

from datetime import date
import math
from pathlib import Path
from typing import Any, Mapping


LAGS = (1, 5, 10)
RELEASED_STATUS = "ELIGIBLE"
AVAILABLE_CHAIN = "PROVISIONAL_CHAIN"


def _number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 0 <= number <= 100 else None


def _identity(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(row.get(k, "")) for k in (
        "asset_id", "listing_symbol", "price_symbol", "currency", "source",
        "formula", "price_source", "price_basis", "currency_lineage",
        "session_time_quality",
    ))


def project_eligible_directions(
    observations: list[dict[str, str]],
    eligibility: list[dict[str, str]],
    *,
    latest_snapshot_id: str,
) -> dict[str, dict[str, Any]]:
    """Project only explicit ELIGIBLE, immutable-identity 1/5/10obs chains.

    Caller MUST supply eligibility from an independently released,
    integrity-verified history adapter. This pure function is not a release
    gate. Missingness, stale rows and blocked research remain unavailable.
    Deltas are cycle *points*, not price returns or exchange sessions.
    """
    observations_by_key: dict[tuple[str, str], dict[str, str]] = {}
    snapshots_by_day: dict[str, tuple[str, str]] = {}
    for item in observations:
        key = (item["snapshot_id"], item["asset_id"])
        if key in observations_by_key:
            return {}
        observations_by_key[key] = item
        stamp = (item["generated_at"], item["snapshot_id"])
        d = item["as_of"]
        if d not in snapshots_by_day or stamp > snapshots_by_day[d]:
            snapshots_by_day[d] = stamp
    if latest_snapshot_id not in {sid for _, sid in snapshots_by_day.values()}:
        return {}
    ordered_dates = sorted(snapshots_by_day)
    latest_day = next(
        (d for d in ordered_dates if snapshots_by_day[d][1] == latest_snapshot_id),
        None,
    )
    if latest_day is None or latest_day != ordered_dates[-1]:
        return {}
    masks = {(m["snapshot_id"], m["asset_id"]): m for m in eligibility}
    result: dict[str, dict[str, Any]] = {}
    for (snapshot, asset), current in observations_by_key.items():
        if snapshot != latest_snapshot_id:
            continue
        status = masks.get((snapshot, asset))
        value = _number(current.get("cycle"))
        if (status is None or status.get("research_status") != RELEASED_STATUS
            or current.get("availability") != "PROVISIONAL_REPLAYED"
            or current.get("quality") != "VALID" or value is None
            or status.get("as_of") != latest_day):
            continue
        current_identity = _identity(current)
        record: dict[str, Any] = {
            "snapshot_id": snapshot, "run_id": current["run_id"],
            "as_of": latest_day, "asset_id": asset,
            "cycle": value, "quality": "VALID",
            "formula": current["formula"],
            "currency": current["currency"],
            "listing_symbol": current["listing_symbol"],
            "price_symbol": current["price_symbol"],
            "price_sha256": current["price_sha256"],
            "last_bar": current["last_bar"],
        }
        any_lag = False
        index = ordered_dates.index(latest_day)
        for lag in LAGS:
            delta: float | None = None
            if index >= lag and status.get(f"lag_{lag}obs") == AVAILABLE_CHAIN:
                prior_day = ordered_dates[index - lag]
                previous = observations_by_key.get((snapshots_by_day[prior_day][1], asset))
                if (previous is not None and _identity(previous) == current_identity
                    and previous.get("quality") == "VALID"
                    and previous.get("availability") == "PROVISIONAL_REPLAYED"
                    and date.fromisoformat(previous["as_of"]) < date.fromisoformat(latest_day)):
                    before = _number(previous.get("cycle"))
                    if before is not None:
                        delta = round(value - before, 8)
                        any_lag = True
            record[f"delta_{lag}obs"] = delta
            record[f"direction_{lag}obs"] = (
                "UP" if delta is not None and delta > 0 else
                "DOWN" if delta is not None and delta < 0 else
                "UNCHANGED" if delta == 0 else "UNAVAILABLE"
            )
        if any_lag:
            result[asset] = record
    return result


def load_direction_for_ui(repo_root: Path) -> dict[str, Any]:
    """Safely load verified *released* observations, never legacy history.

    CY-03 v1 adapter is quarantined: its own inspection explicitly reports
    research_released=False. Never interpret a hand-edited manifest or masks
    as permission to turn research-blocked rows into dashboard arrows.
    """
    unavailable = {
        "schema_version": "cycle_direction_ui_cy04_v1",
        "state": "UNAVAILABLE",
        "reason": "HISTORY_NOT_RELEASED_OR_INSUFFICIENT",
        "by_asset": {},
    }
    try:
        from scanner.research.pattern_discovery.cycle_cy05_adapter import (
            inspect_cycle_archive,
        )
        from scanner.reports.cycle_history import ROOT, read_ledger, lag_mask

        inspection = inspect_cycle_archive(repo_root)
        if inspection.get("research_released") is not True:
            return unavailable
        # Fail closed until CY-03 introduces a reviewed, released schema.
        # The current v1 adapter deliberately never returns True.
        if inspection.get("archive_version") == "cycle_observations_cy03_v1":
            return unavailable
        folder = Path(repo_root) / ROOT
        rows = read_ledger(folder / "observations.csv")
        masks = lag_mask(rows)
        by_asset = project_eligible_directions(
            rows, masks, latest_snapshot_id=inspection["latest_snapshot_id"]
        )
        return {
            "schema_version": "cycle_direction_ui_cy04_v1",
            "state": "RELEASED" if by_asset else "UNAVAILABLE",
            "reason": "" if by_asset else "NO_ELIGIBLE_CHAINS",
            "latest_snapshot_id": inspection["latest_snapshot_id"],
            "by_asset": by_asset,
        }
    except (OSError, ValueError, KeyError, TypeError, ImportError):
        return unavailable



def attach_verified_directions(
    records: list[dict[str, Any]], projection: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Attach only source-identical released lag descriptions to UI rows.

    Asset IDs (not Yahoo aliases), original currency/price symbols, formula,
    bar hash, quality and the current observed cycle must all match. Nulls
    remain null; no legacy, ticker guess, synthetic zero or cross-listing join.
    """
    by_asset = projection.get("by_asset", {}) if projection.get("state") == "RELEASED" else {}
    for row in records:
        for n in LAGS:
            row[f"cy04_delta_{n}obs"] = None
            row[f"cy04_direction_{n}obs"] = "UNAVAILABLE"
        row["cy04_as_of"] = None
        row["cy04_last_bar"] = None
        row["cy04_quality"] = "UNAVAILABLE"
        if row.get("cycle_quality") != "VALID":
            continue
        asset_id = row.get("asset_id")
        rec = by_asset.get(asset_id) if isinstance(by_asset, dict) and isinstance(asset_id, str) else None
        if not isinstance(rec, dict) or rec.get("snapshot_id") != projection.get("latest_snapshot_id"):
            continue
        if (str(row.get("cycle_formula_version", "")) != rec.get("formula")
            or str(row.get("cycle_currency", "")) != rec.get("currency")
            or str(row.get("cycle_price_symbol", "")) != rec.get("price_symbol")
            or str(row.get("cycle_price_sha256", "")) != rec.get("price_sha256")
            or _number(row.get("cycle")) != _number(rec.get("cycle"))):
            continue
        if rec.get("quality") != "VALID":
            continue
        row["cy04_as_of"] = rec["as_of"]
        row["cy04_last_bar"] = rec["last_bar"]
        row["cy04_quality"] = "ELIGIBLE_DESCRIPTIVE_ONLY"
        for n in LAGS:
            delta = rec.get(f"delta_{n}obs")
            if isinstance(delta, (int, float)) and not isinstance(delta, bool) and math.isfinite(delta):
                row[f"cy04_delta_{n}obs"] = delta
                row[f"cy04_direction_{n}obs"] = (
                    "UP" if delta > 0 else "DOWN" if delta < 0 else "UNCHANGED"
                )
    return records
