"""Phase L14 continuous Pattern Discovery laboratory operations.

L14 is an operations control plane over L1-L13. It schedules and audits the
existing scientific stages; it does not redefine their statistics, governance,
ratings, promotion rules, or productive Decision semantics.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence

from scanner.research.governance.ba_qm12_continuous_qm import (
    evaluate_continuous_qm,
)
from scanner.research.governance.qm_c_negative_results import (
    NegativeResultRegistry,
)
from .boundary import PatternDiscoveryBoundary
from .candidate_registry import verify_freeze_snapshot
from .confirmation_engine import (
    ConfirmationLookRegistry,
    confirmation_registry_repo_path,
)
from .dependency_graph import verify_dependency_graph
from .outcome_maturation import (
    MaturedOutcomeRegistry,
    maturation_registry_repo_path,
)
from .prospective_capture import (
    ProspectiveClaimRegistry,
    prospective_claim_registry_repo_path,
)
from .rating_engine import (
    RatingHistoryRegistry,
    rating_registry_repo_path,
)
from .run_contract import verify_run_manifest


SCHEMA_VERSION = "pattern_discovery_l14_continuous_operations_v1"
CYCLE_SCHEMA_VERSION = "pattern_discovery_l14_operations_cycle_v1"
EVENT_SCHEMA_VERSION = "pattern_discovery_l14_operations_event_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l14_continuous_operations_v1.json"
)


class ContinuousOperationsError(ValueError):
    """Raised when an L14 operations invariant is violated."""


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ContinuousOperationsError(f"value_required:{field}")
    return result


def _sha(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", result):
        raise ContinuousOperationsError(f"sha256_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ContinuousOperationsError(
            f"invalid_timestamp:{field}"
        ) from exc
    if parsed.tzinfo is None:
        raise ContinuousOperationsError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _day(value: Any, field: str) -> date:
    text = _text(value, field)
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ContinuousOperationsError(f"invalid_date:{field}") from exc


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContinuousOperationsError(
            f"required_json_missing:{path.as_posix()}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ContinuousOperationsError(
            f"invalid_json:{path.as_posix()}"
        ) from exc
    if not isinstance(value, dict):
        raise ContinuousOperationsError(
            f"json_object_required:{path.as_posix()}"
        )
    return value


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContinuousOperationsError(
                f"invalid_jsonl:{path.as_posix()}:{line_number}"
            ) from exc
        if not isinstance(value, dict):
            raise ContinuousOperationsError(
                f"jsonl_object_required:{path.as_posix()}:{line_number}"
            )
        rows.append(value)
    return rows


def load_operations_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContinuousOperationsError(
            f"operations_contract_unreadable:{target}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ContinuousOperationsError("operations_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise ContinuousOperationsError("operations_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise ContinuousOperationsError(
            "operations_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise ContinuousOperationsError("operations_execution_forbidden")
    principles = payload.get("principles")
    if not isinstance(principles, Mapping):
        raise ContinuousOperationsError("operations_principles_missing")
    required_true = (
        "l1_through_l13_are_reused_not_reimplemented",
        "discovery_is_data_triggered_not_calendar_triggered",
        "daily_operations_may_check_without_forcing_discovery",
        "formal_confirmation_only_at_qm_c4_permitted_looks",
        "qm_c5_is_the_negative_result_authority",
        "manual_statistical_override_forbidden",
        "all_cycles_are_hash_bound",
        "errors_fail_closed",
        "automatic_promotion_forbidden",
        "productive_semantics_must_not_change",
    )
    for field in required_true:
        if principles.get(field) is not True:
            raise ContinuousOperationsError(
                f"operations_principle_missing:{field}"
            )
    return payload


def operations_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    return _hash(
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )


def _current_scanner_state(root: Path) -> dict[str, Any]:
    metadata = _json(root / "artifacts/research/history_metadata.json")
    if metadata.get("schema_version") != "research_views_v1":
        raise ContinuousOperationsError("history_metadata_schema_invalid")
    if metadata.get("latest_run_complete") is not True:
        raise ContinuousOperationsError("scanner_snapshot_incomplete")
    validation = metadata.get("validation")
    if not isinstance(validation, Mapping) or validation.get("status") != "ok":
        raise ContinuousOperationsError("scanner_snapshot_validation_not_ok")
    snapshot_id = _text(metadata.get("snapshot_id"), "snapshot_id")
    as_of = _text(metadata.get("as_of"), "snapshot_as_of")
    generated_at = _timestamp(
        metadata.get("generated_at"), "snapshot_generated_at"
    )
    required = int(validation.get("required_symbol_count") or 0)
    observed = int(validation.get("symbol_count") or 0)
    if required <= 0 or observed < required:
        raise ContinuousOperationsError("scanner_snapshot_coverage_incomplete")
    return {
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "generated_at": generated_at,
        "required_symbol_count": required,
        "observed_symbol_count": observed,
    }


def _history_information(
    root: Path,
    *,
    cutoff: str | None = None,
) -> dict[str, Any]:
    path = root / "artifacts/research/history_analysis.csv"
    if not path.exists() or path.stat().st_size == 0:
        return {
            "available": False,
            "reason": "HISTORY_ANALYSIS_UNAVAILABLE",
            "observation_count": None,
            "trading_dates": [],
            "baseline_observation_count": None,
            "new_observation_count": None,
            "new_trading_dates": [],
        }

    seen: set[tuple[str, str]] = set()
    dates: set[str] = set()
    baseline_seen: set[tuple[str, str]] = set()
    new_seen: set[tuple[str, str]] = set()
    new_dates: set[str] = set()
    cutoff_day = _day(cutoff, "discovery_cutoff") if cutoff else None

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return {
                "available": False,
                "reason": "HISTORY_ANALYSIS_HEADER_MISSING",
                "observation_count": None,
                "trading_dates": [],
                "baseline_observation_count": None,
                "new_observation_count": None,
                "new_trading_dates": [],
            }
        for raw in reader:
            if str(raw.get("observation_type") or "observed_scanner") != "observed_scanner":
                continue
            symbol = str(raw.get("symbol") or "").strip()
            observed = str(raw.get("as_of") or raw.get("date") or "").strip()
            if not symbol or not observed:
                continue
            day_text = observed[:10]
            try:
                observed_day = date.fromisoformat(day_text)
            except ValueError:
                continue
            key = (day_text, symbol)
            seen.add(key)
            dates.add(day_text)
            if cutoff_day is not None:
                if observed_day <= cutoff_day:
                    baseline_seen.add(key)
                else:
                    new_seen.add(key)
                    new_dates.add(day_text)

    if not seen:
        return {
            "available": False,
            "reason": "NO_USABLE_PIT_OBSERVATIONS",
            "observation_count": 0,
            "trading_dates": [],
            "baseline_observation_count": 0,
            "new_observation_count": 0,
            "new_trading_dates": [],
        }
    return {
        "available": True,
        "reason": None,
        "observation_count": len(seen),
        "trading_dates": sorted(dates),
        "baseline_observation_count": (
            len(baseline_seen) if cutoff_day is not None else None
        ),
        "new_observation_count": (
            len(new_seen) if cutoff_day is not None else None
        ),
        "new_trading_dates": (
            sorted(new_dates) if cutoff_day is not None else []
        ),
    }


def _manifest_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(
        root.glob(
            "artifacts/research/pattern_discovery/discovery_runs/*/manifest.json"
        )
    ):
        value = _json(path)
        verify_run_manifest(value)
        rows.append(
            {
                "path": path.relative_to(root).as_posix(),
                "manifest": value,
            }
        )
    rows.sort(
        key=lambda item: str(
            item["manifest"]["preregistration"]["declared_start_at"]
        )
    )
    return rows


def _freeze_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(
        root.glob(
            "artifacts/research/pattern_discovery/discovery_runs/*/l5_freeze_snapshot.json"
        )
    ):
        value = _json(path)
        verify_freeze_snapshot(value)
        rows.append(
            {
                "path": path.relative_to(root).as_posix(),
                "snapshot": value,
            }
        )
    return rows


def _pattern_identity(snapshot: Mapping[str, Any]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for pattern in snapshot.get("frozen_patterns") or []:
        if not isinstance(pattern, Mapping):
            continue
        result.append(
            {
                "pattern_id": _text(pattern.get("pattern_id"), "pattern_id"),
                "pattern_version": _text(
                    pattern.get("pattern_version"), "pattern_version"
                ),
                "pattern_spec_hash": _sha(
                    pattern.get("pattern_spec_hash"), "pattern_spec_hash"
                ),
            }
        )
    return result


def _pattern_key(identity: Mapping[str, Any]) -> str:
    return "::".join(
        (
            _text(identity.get("pattern_id"), "pattern_id"),
            _text(identity.get("pattern_version"), "pattern_version"),
            _sha(identity.get("pattern_spec_hash"), "pattern_spec_hash"),
        )
    )


def _rating_state(
    root: Path,
) -> tuple[dict[str, str], list[dict[str, Any]], dict[str, Any]]:
    path = root / rating_registry_repo_path()
    registry = RatingHistoryRegistry(path)
    integrity = registry.verify_integrity()
    events = _jsonl(path)
    current: dict[str, str] = {}
    for event in events:
        transition = event.get("transition")
        if not isinstance(transition, Mapping):
            continue
        key = "::".join(
            (
                str(transition.get("pattern_id")),
                str(transition.get("pattern_version")),
                str(transition.get("pattern_spec_hash")),
            )
        )
        current[key] = str(transition.get("new_rating"))
    return current, events, integrity


def _confirmation_state(
    root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = root / confirmation_registry_repo_path()
    registry = ConfirmationLookRegistry(path)
    integrity = registry.verify_integrity()
    return _jsonl(path), integrity


def _claim_state(root: Path) -> dict[str, Any]:
    path = root / prospective_claim_registry_repo_path()
    return ProspectiveClaimRegistry(path).verify_integrity()


def _maturation_state(root: Path) -> dict[str, Any]:
    path = root / maturation_registry_repo_path()
    return MaturedOutcomeRegistry(path).verify_integrity()


def _dependency_state(
    root: Path,
    frozen_identities: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    expected = sorted(_pattern_key(item) for item in frozen_identities)
    expected_hash = _hash(expected)
    graphs: list[dict[str, Any]] = []
    for path in sorted(
        root.glob(
            "artifacts/research/pattern_discovery/dependency_graphs/*.json"
        )
    ):
        graph = _json(path)
        verify_dependency_graph(graph)
        identities = sorted(
            "::".join(
                (
                    str(node.get("pattern_id")),
                    str(node.get("pattern_version")),
                    str(node.get("pattern_spec_hash")),
                )
            )
            for node in graph.get("nodes") or []
            if isinstance(node, Mapping)
        )
        graphs.append(
            {
                "path": path.relative_to(root).as_posix(),
                "graph_id": graph.get("graph_id"),
                "graph_hash": graph.get("graph_hash"),
                "pattern_set_hash": _hash(identities),
                "matches_current_pattern_set": identities == expected,
            }
        )
    matching = [row for row in graphs if row["matches_current_pattern_set"]]
    return {
        "current_pattern_set_hash": expected_hash,
        "graph_count": len(graphs),
        "current_graph_available": bool(matching) or not expected,
        "current_graph_hash": (
            str(matching[-1]["graph_hash"]) if matching else None
        ),
        "graphs": graphs,
    }


def _captured_snapshot_ids(root: Path) -> set[str]:
    result: set[str] = set()
    for path in root.glob(
        "artifacts/research/pattern_discovery/prospective_captures/*/*.json"
    ):
        try:
            value = _json(path)
        except ContinuousOperationsError:
            raise
        binding = value.get("snapshot_binding")
        if isinstance(binding, Mapping) and binding.get("snapshot_id"):
            result.add(str(binding["snapshot_id"]))
    return result


def _qm_state(
    root: Path,
    *,
    evaluator: Callable[[str | Path], Mapping[str, Any]] | None = None,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    boundary = PatternDiscoveryBoundary()
    bindings = boundary.validate_qm_c_bindings()
    qm_path = root / str(contract["qm"]["negative_result_registry_path"])
    negative = NegativeResultRegistry(qm_path).verify_integrity()
    evaluate = evaluator or evaluate_continuous_qm
    try:
        receipt = dict(evaluate(root))
        qm_status = "PASS"
        error = None
        receipt_hash = _hash(receipt)
    except Exception as exc:  # fail-closed receipt, never silent
        qm_status = "BLOCKED"
        error = f"{type(exc).__name__}:{exc}"
        receipt_hash = None
    return {
        "status": qm_status,
        "error": error,
        "continuous_qm_receipt_hash": receipt_hash,
        "qm_c_bindings": dict(sorted(bindings.items())),
        "negative_result_registry": negative,
        "negative_result_authority": "QM-C5",
        "parallel_negative_registry_created": False,
    }


def validate_extraordinary_request(
    request: Mapping[str, Any] | None,
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any] | None:
    if request is None:
        return None
    if not isinstance(request, Mapping):
        raise ContinuousOperationsError(
            "extraordinary_request_must_be_object"
        )
    reason = _text(request.get("reason_code"), "extraordinary.reason_code")
    allowed = set(
        contract["extraordinary_discovery"]["allowed_reason_codes"]
    )
    if reason not in allowed:
        raise ContinuousOperationsError(
            f"extraordinary_reason_not_allowed:{reason}"
        )
    evidence_hash = _sha(
        request.get("evidence_hash"), "extraordinary.evidence_hash"
    )
    requested_at = _timestamp(
        request.get("requested_at"), "extraordinary.requested_at"
    )
    return {
        "reason_code": reason,
        "evidence_hash": evidence_hash,
        "requested_at": requested_at,
        "new_l1_preregistration_required": True,
    }


def evaluate_discovery_trigger(
    *,
    history_information: Mapping[str, Any],
    last_regular_manifest: Mapping[str, Any] | None,
    run_processed: bool,
    unprocessed_candidate_count: int,
    candidate_budget_total: int | None,
    extraordinary_request: Mapping[str, Any] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )
    extraordinary = validate_extraordinary_request(
        extraordinary_request, contract=spec
    )
    if extraordinary is not None:
        return {
            "status": "ELIGIBLE_EXTRAORDINARY",
            "eligible": True,
            "mode": "EXTRAORDINARY",
            "reason_codes": [extraordinary["reason_code"]],
            "extraordinary_request": extraordinary,
            "new_l1_preregistration_required": True,
            "automatic_search_start_allowed": False,
        }

    if last_regular_manifest is None:
        return {
            "status": "INITIAL_DISCOVERY_READY_FOR_PREREGISTRATION",
            "eligible": True,
            "mode": "INITIAL",
            "reason_codes": ["NO_PRIOR_REGULAR_DISCOVERY"],
            "metrics": None,
            "new_l1_preregistration_required": True,
            "automatic_search_start_allowed": False,
        }

    if history_information.get("available") is not True:
        return {
            "status": "BLOCKED_HISTORY_INFORMATION_UNAVAILABLE",
            "eligible": False,
            "mode": "REGULAR",
            "reason_codes": [
                str(
                    history_information.get("reason")
                    or "HISTORY_INFORMATION_UNAVAILABLE"
                )
            ],
            "metrics": None,
            "new_l1_preregistration_required": True,
            "automatic_search_start_allowed": False,
        }

    cutoff = str(
        last_regular_manifest["preregistration"]["data_cutoff"]
    )
    new_dates = list(history_information.get("new_trading_dates") or [])
    baseline_n = int(
        history_information.get("baseline_observation_count") or 0
    )
    new_n = int(history_information.get("new_observation_count") or 0)
    growth = (new_n / baseline_n) if baseline_n > 0 else None
    cfg = spec["regular_discovery_trigger"]
    block = int(cfg["support_region_block_sessions"])
    support_regions = len(new_dates) // block
    budget_fraction = (
        (unprocessed_candidate_count / candidate_budget_total)
        if candidate_budget_total and candidate_budget_total > 0
        else (0.0 if unprocessed_candidate_count == 0 else None)
    )
    candidate_gate = run_processed or (
        budget_fraction is not None
        and budget_fraction
        <= float(cfg["open_unprocessed_candidate_fraction_max"])
    )
    checks = {
        "minimum_new_trading_days": (
            len(new_dates) >= int(cfg["minimum_new_trading_days"])
        ),
        "minimum_usable_observation_growth_fraction": (
            growth is not None
            and growth
            >= float(cfg["minimum_usable_observation_growth_fraction"])
        ),
        "minimum_new_support_regions": (
            support_regions >= int(cfg["minimum_new_support_regions"])
        ),
        "previous_run_processed_or_open_budget_below_limit": candidate_gate,
    }
    eligible = all(checks.values())
    return {
        "status": "ELIGIBLE_REGULAR" if eligible else "BLOCKED_REGULAR_TRIGGER",
        "eligible": eligible,
        "mode": "REGULAR",
        "reason_codes": [
            key.upper()
            for key, passed in checks.items()
            if not passed
        ],
        "checks": checks,
        "metrics": {
            "last_regular_data_cutoff": cutoff,
            "new_trading_day_count": len(new_dates),
            "baseline_usable_observation_count": baseline_n,
            "new_usable_observation_count": new_n,
            "usable_observation_growth_fraction": growth,
            "new_support_region_count": support_regions,
            "support_region_block_sessions": block,
            "unprocessed_candidate_count": unprocessed_candidate_count,
            "candidate_budget_total": candidate_budget_total,
            "open_unprocessed_candidate_fraction": budget_fraction,
            "previous_run_processed": run_processed,
        },
        "new_l1_preregistration_required": True,
        "automatic_search_start_allowed": False,
    }


def _decay_alerts(
    *,
    rating_events: Sequence[Mapping[str, Any]],
    confirmation_events: Sequence[Mapping[str, Any]],
    current_ratings: Mapping[str, str],
    since_at: str | None,
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    since = (
        datetime.fromisoformat(since_at.replace("Z", "+00:00"))
        if since_at
        else None
    )
    downgrade = set(contract["decay"]["downgrade_transitions"])
    negative = set(contract["decay"]["negative_l9_result_classes"])
    alerts: list[dict[str, Any]] = []

    for event in rating_events:
        transition = event.get("transition")
        if not isinstance(transition, Mapping):
            continue
        observed = datetime.fromisoformat(
            str(transition.get("observed_at")).replace("Z", "+00:00")
        )
        if since is not None and observed <= since:
            continue
        previous = transition.get("previous_rating")
        new = transition.get("new_rating")
        code = f"{previous}->{new}" if previous else None
        if code not in downgrade:
            continue
        level = "CRITICAL" if new == "F" else "WARNING"
        alerts.append(
            {
                "alert_type": "RATING_DOWNGRADE",
                "level": level,
                "pattern_id": transition.get("pattern_id"),
                "pattern_version": transition.get("pattern_version"),
                "observed_at": transition.get("observed_at"),
                "transition": code,
                "source_hash": transition.get("transition_hash"),
                "rating_change_performed_by_alert": False,
            }
        )

    for event in confirmation_events:
        report = event.get("report")
        if not isinstance(report, Mapping):
            continue
        observed_text = str(report.get("evaluated_at") or event.get("recorded_at") or "")
        if not observed_text:
            continue
        observed = datetime.fromisoformat(
            observed_text.replace("Z", "+00:00")
        )
        if since is not None and observed <= since:
            continue
        for result in report.get("pattern_results") or []:
            if not isinstance(result, Mapping):
                continue
            key_prefix = (
                f"{result.get('pattern_id')}::{result.get('pattern_version')}::"
            )
            current = next(
                (
                    rating
                    for key, rating in current_ratings.items()
                    if key.startswith(key_prefix)
                ),
                None,
            )
            cls = str(result.get("result_class") or "")
            if cls in negative and current in {"A", "B", "C"}:
                alerts.append(
                    {
                        "alert_type": "NEGATIVE_CONFIRMATION_PENDING_RATING_REFRESH",
                        "level": "CRITICAL",
                        "pattern_id": result.get("pattern_id"),
                        "pattern_version": result.get("pattern_version"),
                        "observed_at": observed_text,
                        "result_class": cls,
                        "look_hash": report.get("look_hash"),
                        "rating_change_performed_by_alert": False,
                    }
                )
            elif (
                cls == "INCONCLUSIVE"
                and current in {"A", "B", "C"}
                and contract["decay"].get(
                    "inconclusive_after_established_rating_is_warning"
                )
                is True
            ):
                alerts.append(
                    {
                        "alert_type": "INCONCLUSIVE_AFTER_ESTABLISHED_RATING",
                        "level": "WARNING",
                        "pattern_id": result.get("pattern_id"),
                        "pattern_version": result.get("pattern_version"),
                        "observed_at": observed_text,
                        "result_class": cls,
                        "look_hash": report.get("look_hash"),
                        "rating_change_performed_by_alert": False,
                    }
                )

    unique: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        key = _hash(alert)
        unique[key] = alert
    return [unique[key] for key in sorted(unique)]


class OperationsRegistry:
    """Append-only L14 cycle audit and Discovery-run classification registry."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = (
            dict(contract)
            if contract is not None
            else load_operations_contract()
        )

    def _read(self) -> list[dict[str, Any]]:
        return _jsonl(self.path)

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        previous: str | None = None
        cycles: list[dict[str, Any]] = []
        classifications: dict[str, dict[str, Any]] = {}
        for sequence, raw in enumerate(events, 1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise ContinuousOperationsError(
                    f"operations_registry_schema_invalid:{sequence}"
                )
            if event.get("sequence") != sequence:
                raise ContinuousOperationsError(
                    f"operations_registry_sequence_invalid:{sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise ContinuousOperationsError(
                    f"operations_registry_previous_hash_invalid:{sequence}"
                )
            stored = _sha(
                event.get("entry_hash"),
                f"operations_registry.entry_hash.{sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise ContinuousOperationsError(
                    f"operations_registry_hash_invalid:{sequence}"
                )
            event_type = event.get("event_type")
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise ContinuousOperationsError(
                    "operations_registry_payload_missing"
                )
            if event_type == "OPERATIONS_CYCLE_RECORDED":
                cycle = payload.get("cycle")
                if not isinstance(cycle, Mapping):
                    raise ContinuousOperationsError(
                        "operations_registry_cycle_missing"
                    )
                verify_operations_cycle(cycle, contract=self.contract)
                cycles.append(dict(cycle))
            elif event_type == "DISCOVERY_RUN_CLASSIFIED":
                run_id = _text(payload.get("run_id"), "run_id")
                mode = _text(payload.get("mode"), "mode").upper()
                if mode not in {"INITIAL", "REGULAR", "EXTRAORDINARY"}:
                    raise ContinuousOperationsError(
                        f"discovery_run_mode_invalid:{mode}"
                    )
                if run_id in classifications:
                    if classifications[run_id] != dict(payload):
                        raise ContinuousOperationsError(
                            "discovery_run_classification_collision"
                        )
                else:
                    classifications[run_id] = dict(payload)
            else:
                raise ContinuousOperationsError(
                    f"operations_registry_event_type_unknown:{event_type}"
                )
            previous = stored
        return {
            "cycles": cycles,
            "classifications": classifications,
            "head_hash": previous,
        }

    def _append(
        self,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        recorded_at: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError as exc:
                raise ContinuousOperationsError(
                    "operations_registry_lock_already_held"
                ) from exc
            events = self._read()
            state = self._replay(events)
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_type": event_type,
                "recorded_at": _timestamp(recorded_at, "recorded_at"),
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "payload": dict(payload),
                "previous_event_hash": state["head_hash"],
            }
            event["event_id"] = "POE-" + _hash(event)[:24].upper()
            event["entry_hash"] = _hash(event)
            self._replay([*events, event])
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, (_canonical(event) + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            return event
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def state(self) -> dict[str, Any]:
        return self._replay(self._read())

    def latest_cycle(self) -> dict[str, Any] | None:
        cycles = self.state()["cycles"]
        return dict(cycles[-1]) if cycles else None

    def classify_discovery_run(
        self,
        *,
        run_id: str,
        manifest_hash: str,
        mode: str,
        trigger_cycle_hash: str | None,
        recorded_at: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        run_id = _text(run_id, "run_id")
        manifest_hash = _sha(manifest_hash, "manifest_hash")
        mode = _text(mode, "mode").upper()
        if mode not in {"INITIAL", "REGULAR", "EXTRAORDINARY"}:
            raise ContinuousOperationsError(
                f"discovery_run_mode_invalid:{mode}"
            )
        trigger_hash = (
            _sha(trigger_cycle_hash, "trigger_cycle_hash")
            if trigger_cycle_hash is not None
            else None
        )
        existing = self.state()["classifications"].get(run_id)
        payload = {
            "run_id": run_id,
            "manifest_hash": manifest_hash,
            "mode": mode,
            "trigger_cycle_hash": trigger_hash,
        }
        if existing is not None:
            if existing != payload:
                raise ContinuousOperationsError(
                    "discovery_run_classification_collision"
                )
            return {
                "valid": True,
                "idempotent": True,
                "run_id": run_id,
            }
        event = self._append(
            "DISCOVERY_RUN_CLASSIFIED",
            payload,
            recorded_at=recorded_at,
            actor_id=actor_id,
            actor_role=actor_role,
        )
        return {
            "valid": True,
            "idempotent": False,
            "run_id": run_id,
            "entry_hash": event["entry_hash"],
        }

    def record_cycle(
        self,
        cycle: Mapping[str, Any],
        *,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        verify_operations_cycle(cycle, contract=self.contract)
        for existing in self.state()["cycles"]:
            if existing.get("cycle_hash") == cycle.get("cycle_hash"):
                if existing != dict(cycle):
                    raise ContinuousOperationsError(
                        "operations_cycle_identity_collision"
                    )
                return {
                    "valid": True,
                    "idempotent": True,
                    "cycle_hash": cycle["cycle_hash"],
                }
        event = self._append(
            "OPERATIONS_CYCLE_RECORDED",
            {"cycle": dict(cycle)},
            recorded_at=str(cycle["evaluated_at"]),
            actor_id=actor_id,
            actor_role=actor_role,
        )
        return {
            "valid": True,
            "idempotent": False,
            "cycle_hash": cycle["cycle_hash"],
            "entry_hash": event["entry_hash"],
        }

    def verify_integrity(self) -> dict[str, Any]:
        events = self._read()
        state = self._replay(events)
        return {
            "valid": True,
            "event_count": len(events),
            "cycle_count": len(state["cycles"]),
            "classified_run_count": len(state["classifications"]),
            "head_hash": state["head_hash"],
        }


def _last_regular_manifest(
    manifest_rows: Sequence[Mapping[str, Any]],
    classifications: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, list[str]]:
    regular: list[Mapping[str, Any]] = []
    unclassified: list[str] = []
    for row in manifest_rows:
        manifest = row["manifest"]
        run_id = str(manifest["run_id"])
        classification = classifications.get(run_id)
        if classification is None:
            unclassified.append(run_id)
            continue
        if classification.get("manifest_hash") != manifest.get("manifest_hash"):
            raise ContinuousOperationsError(
                f"classified_manifest_hash_mismatch:{run_id}"
            )
        if classification.get("mode") in {"INITIAL", "REGULAR"}:
            regular.append(row)
    if not regular:
        return None, sorted(unclassified)
    return dict(regular[-1]["manifest"]), sorted(unclassified)


def _candidate_state_for_run(
    *,
    run_id: str | None,
    manifest: Mapping[str, Any] | None,
    freeze_rows: Sequence[Mapping[str, Any]],
    current_ratings: Mapping[str, str],
) -> dict[str, Any]:
    if run_id is None or manifest is None:
        return {
            "freeze_snapshot_present": False,
            "frozen_candidate_count": 0,
            "unprocessed_candidate_count": 0,
            "candidate_budget_total": None,
            "run_processed": False,
        }
    matching = [
        row["snapshot"]
        for row in freeze_rows
        if row["snapshot"].get("run_id") == run_id
        or row["snapshot"].get("discovery_run_id") == run_id
    ]
    budget = int(
        manifest["preregistration"]["candidate_budget"][
            "max_frozen_candidates_total"
        ]
    )
    if not matching:
        return {
            "freeze_snapshot_present": False,
            "frozen_candidate_count": 0,
            "unprocessed_candidate_count": budget,
            "candidate_budget_total": budget,
            "run_processed": False,
        }
    identities: list[dict[str, str]] = []
    for snapshot in matching:
        identities.extend(_pattern_identity(snapshot))
    keys = {_pattern_key(item) for item in identities}
    unprocessed = sum(1 for key in keys if key not in current_ratings)
    return {
        "freeze_snapshot_present": True,
        "frozen_candidate_count": len(keys),
        "unprocessed_candidate_count": unprocessed,
        "candidate_budget_total": budget,
        "run_processed": unprocessed == 0,
    }


def build_operations_cycle(
    repo_root: str | Path,
    *,
    evaluated_at: str,
    extraordinary_request: Mapping[str, Any] | None = None,
    qm_evaluator: Callable[[str | Path], Mapping[str, Any]] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )
    root = Path(repo_root).resolve()
    evaluated = _timestamp(evaluated_at, "evaluated_at")
    registry_path = root / str(spec["storage"]["registry_path"])
    registry = OperationsRegistry(registry_path, contract=spec)
    registry_state = registry.state()
    previous = (
        dict(registry_state["cycles"][-1])
        if registry_state["cycles"]
        else None
    )

    blockers: list[str] = []
    try:
        scanner = _current_scanner_state(root)
    except Exception as exc:
        scanner = {
            "status": "BLOCKED",
            "error": f"{type(exc).__name__}:{exc}",
        }
        blockers.append("CURRENT_SCANNER_STATE_INVALID")

    manifests = _manifest_rows(root)
    freeze_rows = _freeze_rows(root)
    frozen_identities: list[dict[str, str]] = []
    for row in freeze_rows:
        frozen_identities.extend(_pattern_identity(row["snapshot"]))
    unique_frozen = {
        _pattern_key(item): item for item in frozen_identities
    }

    try:
        current_ratings, rating_events, rating_integrity = _rating_state(root)
        confirmation_events, confirmation_integrity = _confirmation_state(root)
        claim_integrity = _claim_state(root)
        maturation_integrity = _maturation_state(root)
        dependency = _dependency_state(
            root, list(unique_frozen.values())
        )
    except Exception as exc:
        blockers.append("LAB_REGISTRY_INTEGRITY_FAILURE")
        current_ratings = {}
        rating_events = []
        confirmation_events = []
        rating_integrity = {"valid": False, "error": str(exc)}
        confirmation_integrity = {"valid": False, "error": str(exc)}
        claim_integrity = {"registry_claim_count": 0, "registry_head_hash": None}
        maturation_integrity = {"registry_outcome_count": 0, "registry_head_hash": None}
        dependency = {
            "current_pattern_set_hash": _hash([]),
            "graph_count": 0,
            "current_graph_available": not unique_frozen,
            "current_graph_hash": None,
            "graphs": [],
        }

    last_regular, unclassified = _last_regular_manifest(
        manifests, registry_state["classifications"]
    )
    if unclassified:
        blockers.append("UNCLASSIFIED_DISCOVERY_RUN_PRESENT")

    candidate = _candidate_state_for_run(
        run_id=(str(last_regular["run_id"]) if last_regular else None),
        manifest=last_regular,
        freeze_rows=freeze_rows,
        current_ratings=current_ratings,
    )
    cutoff = (
        str(last_regular["preregistration"]["data_cutoff"])
        if last_regular
        else None
    )
    history = _history_information(root, cutoff=cutoff)
    trigger = evaluate_discovery_trigger(
        history_information=history,
        last_regular_manifest=last_regular,
        run_processed=bool(candidate["run_processed"]),
        unprocessed_candidate_count=int(
            candidate["unprocessed_candidate_count"]
        ),
        candidate_budget_total=candidate["candidate_budget_total"],
        extraordinary_request=extraordinary_request,
        contract=spec,
    )

    captured = _captured_snapshot_ids(root)
    snapshot_id = scanner.get("snapshot_id") if isinstance(scanner, Mapping) else None
    capture_due = (
        not blockers
        and bool(unique_frozen)
        and isinstance(snapshot_id, str)
        and snapshot_id not in captured
    )
    claim_count = int(claim_integrity.get("registry_claim_count") or 0)
    outcome_count = int(maturation_integrity.get("registry_outcome_count") or 0)
    maturation_due = not blockers and claim_count > outcome_count

    previous_heads = (
        previous.get("source_heads")
        if isinstance(previous, Mapping)
        and isinstance(previous.get("source_heads"), Mapping)
        else {}
    )
    l8_head = maturation_integrity.get("registry_head_hash")
    l9_head = confirmation_integrity.get("head_hash")
    l10_head = rating_integrity.get("head_hash")
    confirmation_check_due = (
        not blockers
        and l8_head is not None
        and l8_head != previous_heads.get("l8_maturation_registry")
    )
    rating_due = (
        not blockers
        and l9_head is not None
        and l9_head != previous_heads.get("l9_confirmation_registry")
    )
    dependency_due = (
        not blockers
        and bool(unique_frozen)
        and dependency.get("current_graph_available") is not True
    )

    qm = _qm_state(root, evaluator=qm_evaluator, contract=spec)
    if qm["status"] != "PASS":
        blockers.append("CONTINUOUS_QM_BLOCKED")

    since_at = (
        str(previous.get("evaluated_at"))
        if isinstance(previous, Mapping)
        else None
    )
    alerts = _decay_alerts(
        rating_events=rating_events,
        confirmation_events=confirmation_events,
        current_ratings=current_ratings,
        since_at=since_at,
        contract=spec,
    )

    work_items = [
        {
            "stage": "DISCOVERY_TRIGGER",
            "status": (
                "DUE_PREREGISTRATION"
                if trigger.get("eligible") is True and not blockers
                else (
                    "BLOCKED"
                    if blockers
                    else "NOT_DUE"
                )
            ),
            "automatic_statistical_override_allowed": False,
            "details": trigger,
        },
        {
            "stage": "PROSPECTIVE_CAPTURE",
            "status": "DUE" if capture_due else ("BLOCKED" if blockers else "NOT_DUE"),
            "existing_stage": "L7",
            "reason": (
                "NEW_VALID_SNAPSHOT_WITH_FROZEN_PATTERNS"
                if capture_due
                else None
            ),
        },
        {
            "stage": "OUTCOME_MATURATION",
            "status": "DUE" if maturation_due else ("BLOCKED" if blockers else "NOT_DUE"),
            "existing_stage": "L8",
            "unmatured_claim_upper_bound": max(0, claim_count - outcome_count),
        },
        {
            "stage": "SEQUENTIAL_CONFIRMATION",
            "status": (
                "DUE_QM_C4_READINESS_CHECK"
                if confirmation_check_due
                else ("BLOCKED" if blockers else "NOT_DUE")
            ),
            "existing_stage": "L9",
            "qm_c4_remains_authority": True,
            "l14_may_invent_look": False,
        },
        {
            "stage": "RATING_UPDATE",
            "status": "DUE" if rating_due else ("BLOCKED" if blockers else "NOT_DUE"),
            "existing_stage": "L10",
            "l14_may_recompute_l9_statistics": False,
        },
        {
            "stage": "DEPENDENCY_REFRESH",
            "status": "DUE" if dependency_due else ("BLOCKED" if blockers else "NOT_DUE"),
            "existing_stage": "L6",
            "reason": (
                "FROZEN_PATTERN_SET_CHANGED"
                if dependency_due
                else None
            ),
        },
        {
            "stage": "NEGATIVE_RESULT_AUDIT",
            "status": "PASS" if qm["negative_result_registry"].get("valid") is True else "BLOCKED",
            "authority": "QM-C5",
            "parallel_registry_created": False,
        },
        {
            "stage": "PATTERN_DECAY_AUDIT",
            "status": "ALERTS_PRESENT" if alerts else "PASS",
            "alert_count": len(alerts),
            "rating_change_performed": False,
        },
        {
            "stage": "QM_REGRESSION",
            "status": qm["status"],
            "authority": "BA-QM12",
        },
    ]

    source_heads = {
        "scanner_snapshot_id": snapshot_id,
        "l5_pattern_set_hash": dependency["current_pattern_set_hash"],
        "l6_dependency_graph": dependency.get("current_graph_hash"),
        "l7_claim_registry": claim_integrity.get("registry_head_hash"),
        "l8_maturation_registry": l8_head,
        "l9_confirmation_registry": l9_head,
        "l10_rating_registry": l10_head,
        "qm_c5_negative_registry": qm["negative_result_registry"].get(
            "head_hash"
        ),
    }
    due_count = sum(
        1
        for item in work_items
        if str(item["status"]).startswith("DUE")
    )
    if blockers:
        cycle_status = "BLOCKED_FAIL_CLOSED"
    elif due_count or alerts:
        cycle_status = "READY_WITH_WORK"
    else:
        cycle_status = "QUIET"

    cycle: dict[str, Any] = {
        "schema_version": CYCLE_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L14",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "evaluated_at": evaluated,
        "cycle_status": cycle_status,
        "blocking_reasons": sorted(set(blockers)),
        "current_scanner": scanner,
        "history_information": {
            "available": history.get("available"),
            "reason": history.get("reason"),
            "observation_count": history.get("observation_count"),
            "trading_date_count": len(history.get("trading_dates") or []),
        },
        "discovery": {
            "manifest_count": len(manifests),
            "classified_run_count": len(registry_state["classifications"]),
            "unclassified_run_ids": unclassified,
            "last_regular_run_id": (
                last_regular.get("run_id") if last_regular else None
            ),
            "candidate_state": candidate,
            "trigger": trigger,
        },
        "laboratory_state": {
            "frozen_pattern_count": len(unique_frozen),
            "claim_count": claim_count,
            "matured_outcome_count": outcome_count,
            "confirmation_look_count": int(
                confirmation_integrity.get("look_count") or 0
            ),
            "rating_event_count": int(
                rating_integrity.get("registry_event_count") or 0
            ),
            "dependency_graph_current": dependency.get(
                "current_graph_available"
            ),
        },
        "work_items": work_items,
        "decay_alerts": alerts,
        "qm_audit": qm,
        "source_heads": source_heads,
        "l14_contract_hash": operations_contract_hash(spec),
        "boundaries": dict(spec["boundaries"]),
    }
    PatternDiscoveryBoundary().assert_research_payload(cycle)
    cycle["cycle_hash"] = _hash(cycle)
    return cycle


def verify_operations_cycle(
    cycle: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )
    if (
        not isinstance(cycle, Mapping)
        or cycle.get("schema_version") != CYCLE_SCHEMA_VERSION
    ):
        raise ContinuousOperationsError("operations_cycle_schema_invalid")
    if cycle.get("research_only") is not True:
        raise ContinuousOperationsError(
            "operations_cycle_research_only_missing"
        )
    if cycle.get("productive_integration_enabled") is not False:
        raise ContinuousOperationsError(
            "operations_cycle_productive_integration_forbidden"
        )
    if cycle.get("execution_allowed") is not False:
        raise ContinuousOperationsError(
            "operations_cycle_execution_forbidden"
        )
    if cycle.get("l14_contract_hash") != operations_contract_hash(spec):
        raise ContinuousOperationsError(
            "operations_cycle_contract_hash_mismatch"
        )
    for field, expected in spec["boundaries"].items():
        if cycle.get("boundaries", {}).get(field) is not expected:
            raise ContinuousOperationsError(
                f"operations_cycle_boundary_invalid:{field}"
            )
    work = cycle.get("work_items")
    if not isinstance(work, list) or len(work) != len(
        spec["scheduler"]["stage_order"]
    ):
        raise ContinuousOperationsError(
            "operations_cycle_work_items_invalid"
        )
    if [item.get("stage") for item in work] != spec["scheduler"]["stage_order"]:
        raise ContinuousOperationsError(
            "operations_cycle_stage_order_invalid"
        )
    negative = [
        item for item in work if item.get("stage") == "NEGATIVE_RESULT_AUDIT"
    ]
    if (
        len(negative) != 1
        or negative[0].get("authority") != "QM-C5"
        or negative[0].get("parallel_registry_created") is not False
    ):
        raise ContinuousOperationsError(
            "operations_cycle_qm_c5_authority_invalid"
        )
    stored = _sha(cycle.get("cycle_hash"), "cycle_hash")
    body = dict(cycle)
    body.pop("cycle_hash", None)
    if _hash(body) != stored:
        raise ContinuousOperationsError("operations_cycle_hash_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(cycle)
    return {
        "valid": True,
        "cycle_hash": stored,
        "cycle_status": cycle.get("cycle_status"),
    }


def cycle_repo_path(
    cycle: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )
    return str(spec["storage"]["cycle_path_template"]).format(
        cycle_hash=_sha(cycle.get("cycle_hash"), "cycle_hash")
    )


def persist_operations_cycle(
    repo_root: str | Path,
    cycle: Mapping[str, Any],
    *,
    actor_id: str,
    actor_role: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_operations_contract()
    )
    verify_operations_cycle(cycle, contract=spec)
    root = Path(repo_root).resolve()
    guard = PatternDiscoveryBoundary()
    cycle_repo = cycle_repo_path(cycle, contract=spec)
    registry_repo = str(spec["storage"]["registry_path"])
    latest_repo = str(spec["storage"]["latest_path"])
    for repo_path in (cycle_repo, registry_repo, latest_repo):
        guard.assert_write_path_allowed(repo_path)

    path = (root / cycle_repo).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(
        dict(cycle),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise ContinuousOperationsError(
                "operations_cycle_identity_collision"
            )
    else:
        path.write_text(text, encoding="utf-8", newline="\n")

    registry = OperationsRegistry(
        root / registry_repo,
        contract=spec,
    )
    registry_result = registry.record_cycle(
        cycle,
        actor_id=actor_id,
        actor_role=actor_role,
    )
    latest_path = root / latest_repo
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_payload = {
        "schema_version": "pattern_discovery_l14_latest_pointer_v1",
        "cycle_hash": cycle["cycle_hash"],
        "cycle_path": cycle_repo,
        "evaluated_at": cycle["evaluated_at"],
        "cycle_status": cycle["cycle_status"],
    }
    latest_path.write_text(
        json.dumps(
            latest_payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return {
        "valid": True,
        "cycle_path": cycle_repo,
        "registry_path": registry_repo,
        "latest_path": latest_repo,
        "registry": registry_result,
    }
