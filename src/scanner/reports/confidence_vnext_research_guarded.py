from __future__ import annotations

"""Phase 4B-D application guards for the empirical Confidence-vNext registry.

The underlying registry keeps the historical evidence inventory intact. This
module controls whether that evidence is applicable to the *current* scanner
state. Three facts require fail-closed handling as of 2026-09-23:

1. Phase 1-3 validation is stock-only. Crypto rows must not inherit stock
   Selection/Timing/Risk evidence.
2. Stored ``volatility`` changes scale abruptly on 2026-09-17. Historical
   pre-break cutoffs therefore cannot classify post-break current values.
3. Phase 2/3 evidence is applicable only when current scanner ``as_of`` and
   each report ``source.as_of`` are present, parseable and chronologically
   compatible. Unknown chronology is not PIT-valid evidence.

No conversion factor is guessed. Volatility remains visible as historical
research evidence but is excluded from current Model Agreement until the new
metric epoch has independently mature validation evidence.
"""

from collections import Counter
from dataclasses import asdict
from pathlib import Path
import json

import pandas as pd

from scanner.reports.confidence_vnext_research import (
    Phase4ResearchConfig,
    _agreement_state,
    _provenance_quality,
    analyze as _base_analyze,
)
from scanner.reports.selection_timing import _scanner_rows


VOLATILITY_SCALE_BREAK_DATE = "2026-09-17"
VOLATILITY_GUARD_REASON = (
    "stored volatility scale changes abruptly on 2026-09-17; pre-break Phase-3 "
    "cutoffs are not comparable with post-break current values"
)


def _risk_state_without_incompatible_volatility(risk: dict) -> dict:
    observed = [
        dict(item)
        for item in (risk.get("features") or [])
        if str(item.get("feature")) != "volatility"
    ]
    levels = {str(item.get("level")) for item in observed}
    if not observed:
        state = "unknown"
    elif "high" in levels and "low" in levels:
        state = "mixed"
    elif "high" in levels:
        state = "elevated"
    elif levels == {"low"}:
        state = "low"
    else:
        state = "middle"
    return {
        "state": state,
        "features": observed,
        "excluded_features": [
            {
                "feature": "volatility",
                "status": "scale_incompatible",
                "scale_break_date": VOLATILITY_SCALE_BREAK_DATE,
                "reason": VOLATILITY_GUARD_REASON,
                "conversion_applied": False,
            }
        ],
        "role": "downside_protection_only_not_return_vote",
    }


def _guard_risk_data_quality(data_quality: dict, risk: dict) -> dict:
    original = dict((data_quality or {}).get("risk") or {})
    required = [str(x) for x in original.get("required_fields", []) if str(x) != "volatility"]
    present = [str(x) for x in original.get("present_fields", []) if str(x) != "volatility"]
    missing = [str(x) for x in original.get("missing_fields", []) if str(x) != "volatility"]
    provenance = dict(original.get("provenance") or {})

    if not required:
        state = "proxy_insufficient"
        fraction = None
    elif not provenance.get("complete") or not present:
        state = "proxy_insufficient"
        fraction = float(len(present) / len(required))
    elif missing:
        state = "proxy_partial"
        fraction = float(len(present) / len(required))
    else:
        state = "proxy_complete"
        fraction = 1.0

    guarded = {
        **original,
        "state": state,
        "required_fields": required,
        "present_fields": present,
        "missing_fields": missing,
        "present_fraction": fraction,
        "excluded_incompatible_fields": [
            {
                "field": "volatility",
                "status": "scale_incompatible",
                "scale_break_date": VOLATILITY_SCALE_BREAK_DATE,
                "reason": VOLATILITY_GUARD_REASON,
            }
        ],
        "full_data_quality_claimed": False,
        "note": (
            "Claim-specific presence/provenance proxy. Volatility is excluded from "
            "current applicability because its metric scale is incompatible across "
            "the 2026-09-17 break."
        ),
    }
    out = dict(data_quality or {})
    out["risk"] = guarded
    return out


def _guard_timing_data_quality(data_quality: dict, provenance: dict) -> dict:
    out = dict(data_quality or {})
    original = dict(out.get("timing") or {})
    if original.get("state") in {"proxy_complete_no_claim", "proxy_partial_no_claim"}:
        original["provenance"] = dict(provenance or {})
        if not provenance.get("complete"):
            original["state"] = "proxy_insufficient"
            original["note"] = (
                "No robust timing claim matched, but required provenance is incomplete; "
                "the no-claim state therefore fails closed as insufficient evidence."
            )
    out["timing"] = original
    return out


def _mark_registry_application_guard(statistical_registry: dict) -> None:
    for horizon in (statistical_registry.get("horizons") or {}).values():
        risk = horizon.get("risk") or {}
        volatility = risk.get("volatility")
        if isinstance(volatility, dict):
            volatility["current_application_status"] = "scale_incompatible"
            volatility["current_application_reason"] = VOLATILITY_GUARD_REASON
            volatility["current_application_conversion_applied"] = False


def _crypto_symbols_from_latest(latest: pd.DataFrame) -> list[str]:
    scanner = _scanner_rows(latest)
    return sorted(scanner.loc[scanner["is_crypto"], "symbol"].astype(str).unique().tolist())


def _assert_fail_closed_pit_sources(latest: pd.DataFrame, phase2: dict, risk_report: dict) -> dict:
    scanner = _scanner_rows(latest)
    if scanner.empty:
        raise ValueError("latest scanner is empty")
    if "as_of" not in scanner.columns:
        raise ValueError("PIT unverifiable: current scanner as_of is missing")

    scanner_as_of = pd.to_datetime(scanner["as_of"], errors="coerce", utc=True)
    if scanner_as_of.isna().any():
        raise ValueError("PIT unverifiable: current scanner as_of is missing or unparseable")
    current_as_of = scanner_as_of.max()

    checks: dict[str, object] = {"current_as_of": current_as_of.isoformat()}
    for label, report in (("phase2", phase2), ("phase3", risk_report)):
        raw = (report.get("source") or {}).get("as_of")
        parsed = pd.to_datetime(raw, errors="coerce", utc=True) if raw is not None else pd.NaT
        if pd.isna(parsed):
            raise ValueError(f"PIT unverifiable: {label} source.as_of is missing or unparseable")
        if parsed > current_as_of:
            raise ValueError(
                f"PIT violation: {label} evidence as_of {parsed.isoformat()} "
                f"is after current scanner {current_as_of.isoformat()}"
            )
        checks[label] = {
            "source_as_of": parsed.isoformat(),
            "not_after_current_scan": True,
        }
    return checks


def _legacy_pit_compatible_inputs(
    latest: pd.DataFrame,
    phase2: dict,
    risk_report: dict,
) -> tuple[pd.DataFrame, dict, dict]:
    """Normalize chronology before the base registry repeats its legacy PIT check.

    The strict guard above already established that all timestamps are present,
    parseable and not future-derived. The base research registry still performs
    its older PIT comparison without ``utc=True``. Normalize all chronology to
    timezone-naive UTC solely for that redundant internal comparison so valid
    mixtures of offset-aware and offset-less ISO inputs cannot raise a
    tz-aware/tz-naive TypeError.
    """
    normalized_latest = latest.copy()
    parsed_current = pd.to_datetime(normalized_latest["as_of"], errors="raise", utc=True)
    normalized_latest["as_of"] = parsed_current.dt.tz_convert(None)

    def normalized_report(report: dict) -> dict:
        out = dict(report)
        source = dict(report.get("source") or {})
        parsed = pd.to_datetime(source.get("as_of"), errors="raise", utc=True)
        source["as_of"] = pd.Timestamp(parsed).tz_convert(None).isoformat()
        out["source"] = source
        return out

    return normalized_latest, normalized_report(phase2), normalized_report(risk_report)


def _current_provenance_by_symbol(latest: pd.DataFrame, config: Phase4ResearchConfig) -> dict[str, dict]:
    scanner = _scanner_rows(latest)
    if scanner.empty:
        return {}
    latest_date = pd.to_datetime(scanner["date"], errors="coerce").max()
    current = scanner.loc[
        pd.to_datetime(scanner["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())
    ]
    return {
        str(row.get("symbol")): _provenance_quality(pd.Series(row), config.require_provenance_fields)
        for row in current.to_dict("records")
    }


def _assert_scale_audit(risk_scale: dict | None) -> dict:
    if not isinstance(risk_scale, dict):
        return {
            "status": "guarded_without_runtime_audit",
            "scale_break_date": VOLATILITY_SCALE_BREAK_DATE,
            "conversion_applied": False,
        }
    volatility = ((risk_scale.get("features") or {}).get("volatility") or {})
    return {
        "status": "scale_incompatible",
        "scale_break_date": VOLATILITY_SCALE_BREAK_DATE,
        "conversion_applied": False,
        "pre_recent_median": ((volatility.get("pre_recent") or {}).get("median")),
        "recent_median": ((volatility.get("recent") or {}).get("median")),
        "current_median": ((volatility.get("current") or {}).get("median")),
        "recent_to_pre_median_ratio": volatility.get("recent_to_pre_median_ratio"),
        "current_to_pre_median_ratio": volatility.get("current_to_pre_median_ratio"),
    }


def analyze(
    history: pd.DataFrame,
    latest: pd.DataFrame,
    phase2: dict,
    risk_report: dict,
    risk_scale: dict | None = None,
    config: Phase4ResearchConfig = Phase4ResearchConfig(),
) -> dict:
    strict_pit = _assert_fail_closed_pit_sources(latest, phase2, risk_report)
    base_latest, base_phase2, base_risk_report = _legacy_pit_compatible_inputs(latest, phase2, risk_report)
    result = _base_analyze(history, base_latest, base_phase2, base_risk_report, config)

    crypto_symbols = set(_crypto_symbols_from_latest(latest))
    provenance_by_symbol = _current_provenance_by_symbol(latest, config)
    original_rows = list(result["current"]["rows"])
    stock_rows = [row for row in original_rows if str(row.get("symbol")) not in crypto_symbols]

    for row in stock_rows:
        corrected_risk = _risk_state_without_incompatible_volatility(dict(row.get("risk") or {}))
        row["risk"] = corrected_risk
        data_quality = _guard_risk_data_quality(dict(row.get("data_quality") or {}), corrected_risk)
        provenance = provenance_by_symbol.get(
            str(row.get("symbol")),
            {
                "required": list(config.require_provenance_fields),
                "present": [],
                "missing": list(config.require_provenance_fields),
                "complete": False,
            },
        )
        row["data_quality"] = _guard_timing_data_quality(data_quality, provenance)
        row["model_agreement"] = _agreement_state(
            dict(row.get("selection") or {}),
            dict(row.get("timing") or {}),
            corrected_risk,
        )

    _mark_registry_application_guard(result["current"]["statistical_registry"])
    result["current"]["pit_source_checks"] = strict_pit
    result["current"]["rows"] = stock_rows
    result["current"]["scanner_rows_total"] = int(len(_scanner_rows(latest)))
    result["current"]["scanner_rows"] = int(len({row.get("symbol") for row in stock_rows}))
    result["current"]["evidence_rows"] = int(len(stock_rows))
    result["current"]["asset_scope"] = "stocks_only"
    result["current"]["excluded_unsupported_crypto_rows"] = int(len(crypto_symbols))
    result["current"]["excluded_unsupported_crypto_symbols"] = sorted(crypto_symbols)
    result["current"]["risk_metric_applicability"] = {
        "volatility": _assert_scale_audit(risk_scale),
        "drawdown": {
            "status": "no_comparable_scale_break_detected_by_phase4_diagnostic",
            "conversion_applied": False,
        },
    }

    counts = Counter(str(row["model_agreement"]["state"]) for row in stock_rows)
    result["agreement_counts"] = dict(sorted(counts.items()))
    result["semantics"].update(
        {
            "asset_scope": "stocks_only_until_non_stock_evidence_is_validated",
            "stock_evidence_applied_to_crypto": False,
            "volatility_current_application": "scale_incompatible_fail_closed",
            "volatility_conversion_applied": False,
            "pit_unknown_chronology": "fail_closed_unverifiable",
        }
    )
    result["config"] = asdict(config)
    return result


def run(
    history_path: str | Path,
    latest_path: str | Path,
    phase2_path: str | Path,
    risk_path: str | Path,
    output_path: str | Path,
    config: Phase4ResearchConfig = Phase4ResearchConfig(),
    risk_scale_path: str | Path | None = "artifacts/research/confidence_vnext_risk_scale_4.json",
) -> dict:
    history = pd.read_csv(history_path, low_memory=False)
    latest = pd.read_csv(latest_path, low_memory=False)
    phase2 = json.loads(Path(phase2_path).read_text(encoding="utf-8"))
    risk_report = json.loads(Path(risk_path).read_text(encoding="utf-8"))
    risk_scale = None
    if risk_scale_path is not None and Path(risk_scale_path).exists():
        risk_scale = json.loads(Path(risk_scale_path).read_text(encoding="utf-8"))

    result = analyze(history, latest, phase2, risk_report, risk_scale, config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result
