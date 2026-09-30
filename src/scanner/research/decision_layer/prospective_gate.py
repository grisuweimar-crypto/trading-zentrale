"""Prospective deployment boundary for Depot-Watch Decision evidence.

The orchestration was merged to ``main`` at 2026-09-30T08:47:58Z. Evidence
captured from an older scanner snapshot after that deployment would be a
retroactive reconstruction, not prospective evidence. This module therefore
fails closed before any Phase-7A packet is built or archived.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


PROSPECTIVE_NOT_BEFORE = "2026-09-30T08:47:58+00:00"
DEPLOYMENT_COMMIT = "dbb0ed42213b5e7a958ca76fd996a919795998a5"


class ProspectiveBoundaryError(ValueError):
    """Raised when a scanner snapshot predates the orchestration deployment."""


def _aware(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ProspectiveBoundaryError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProspectiveBoundaryError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProspectiveBoundaryError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def assert_orchestration_snapshot_eligible(daily: Mapping[str, object]) -> dict[str, object]:
    """Require a genuinely post-deployment authoritative scanner snapshot.

    The equality case is accepted because the boundary is the deployment instant;
    in practice a scanner publication will be later than it. No date-only or
    guessed timestamp is accepted.
    """
    generated_at = str(daily.get("generated_at") or "").strip()
    observed = _aware(generated_at, "daily_generated_at")
    boundary = _aware(PROSPECTIVE_NOT_BEFORE, "prospective_not_before")
    if observed < boundary:
        raise ProspectiveBoundaryError(
            "snapshot_before_depot_watch_prospective_boundary:"
            f"{observed.isoformat()}<{boundary.isoformat()}"
        )
    return {
        "prospective_boundary_passed": True,
        "snapshot_generated_at": observed.isoformat(),
        "prospective_not_before": boundary.isoformat(),
        "deployment_commit": DEPLOYMENT_COMMIT,
        "retroactive_backfill_allowed": False,
    }
