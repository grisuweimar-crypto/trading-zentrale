"""Append-only, public, matched A/B uranium observation at W10 seal time.

This is an exploratory shadow diary, NOT a Pattern Discovery L7 claim,
not a portfolio action, and not a retrospective prediction.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import re
from pathlib import Path

STUDY_ID = "URAN-REBOUND-U1-v1"
SCHEMA = "uran_ab_observation_v1"
SYMBOLS = ("UEC", "UUUU")
REGISTERED_AT = "2026-10-14T13:22:52Z"
MAX_CAPTURE_HOURS = 36


def _utc(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timezone_required")
    return result.astimezone(timezone.utc)


def _sha(path):
    return sha256(path.read_bytes()).hexdigest()


def build_observation(daily, w10, current_packets, captured_at, source_hashes):
    now = _utc(captured_at)
    generated = _utc(daily["generated_at"])
    sealed = _utc(w10["sealed_at"])
    snapshot_id = str(daily.get("snapshot_id") or "")
    if (not snapshot_id or daily.get("source_snapshot_id") != snapshot_id
            or w10.get("snapshot_id") != snapshot_id
            or w10.get("status") != "sealed"):
        raise ValueError("snapshot_not_identical_and_sealed")
    if generated > sealed or sealed > now:
        raise ValueError("time_order_invalid")
    if generated <= _utc(REGISTERED_AT):
        return None, "SKIP_BEFORE_REGISTRATION"
    if now - sealed > timedelta(hours=MAX_CAPTURE_HOURS):
        return None, "SKIP_STALE_W10_NO_RETROSPECTIVE_CAPTURE"
    rows = daily.get("symbols")
    if not isinstance(rows, dict):
        raise ValueError("daily_symbols_missing")
    if set(current_packets) != set(SYMBOLS):
        raise ValueError("paired_current_packets_missing")
    pair = []
    for symbol in SYMBOLS:
        row = rows.get(symbol)
        if not isinstance(row, dict):
            raise ValueError("scanner_symbol_missing:" + symbol)
        current = row.get("current")
        if not isinstance(current, dict):
            raise ValueError("current_features_missing:" + symbol)
        baseline = current_packets[symbol]
        if baseline.get("source_snapshot_id") != snapshot_id:
            raise ValueError("baseline_snapshot_mismatch:" + symbol)
        stance = baseline.get("universal_stance")
        if not isinstance(stance, dict) or not isinstance(stance.get("state"), str):
            raise ValueError("baseline_stance_missing:" + symbol)
        features = {key: current.get(key) for key in (
            "score", "r_code", "rs3m", "trend200", "cycle",
            "confidence", "confidence_label", "close", "currency"
        )}
        rs3m, trend = features["rs3m"], features["trend200"]
        features_ok = (
            features["r_code"] is not None
            and isinstance(rs3m, (int, float)) and not isinstance(rs3m, bool)
            and isinstance(trend, (int, float)) and not isinstance(trend, bool)
        )
        eligible = (
            features["r_code"] == "R1" and trend < -0.20 and rs3m < 0.0
        ) if features_ok else None
        pair.append({
            "symbol": symbol,
            "current_observed_features": features,
            "A_phase7d_universal_stance": stance["state"],
            "A_is_portfolio_independent": True,
            "A_private_7h_action": "NOT_SUPPLIED",
            "B_hypothesis_condition": eligible,
            "B_validation": "EXPERIMENTAL_UNVALIDATED",
            "B_trade_signal": "NONE",
            "outcome": "UNMATURED",
        })
    result = {
        "schema_version": SCHEMA,
        "study_id": STUDY_ID,
        "research_only": True,
        "private_position_data_included": False,
        "productive_decision_change": False,
        "snapshot_id": snapshot_id,
        "scanner_as_of": daily["as_of"],
        "scanner_generated_at": daily["generated_at"],
        "w10_sealed_at": w10["sealed_at"],
        "captured_at": now.isoformat().replace("+00:00", "Z"),
        "source_sha256": source_hashes,
        "hypothesis_condition": "r_code == R1 AND trend200 < -0.20 AND rs3m < 0",
        "paired_symbols": pair,
        "future_outcomes_used": False,
        "L7_claim_created": False,
    }
    return result, "CAPTURED"


def run(root, captured_at):
    daily_path = root / "artifacts/research/daily_research.json"
    w10_path = root / "artifacts/research/decision_snapshot_w10.json"
    runtime_path = root / "artifacts/research/watch_runtime/manifest.json"
    daily = json.loads(daily_path.read_text(encoding="utf-8"))
    w10 = json.loads(w10_path.read_text(encoding="utf-8"))
    now = _utc(captured_at)
    generated = _utc(daily["generated_at"])
    sealed = _utc(w10["sealed_at"])
    if daily.get("snapshot_id") != w10.get("snapshot_id") or w10.get("status") != "sealed":
        raise ValueError("scanner_w10_not_aligned")
    if generated <= _utc(REGISTERED_AT):
        return {"status": "SKIP_BEFORE_REGISTRATION"}
    if now - sealed > timedelta(hours=MAX_CAPTURE_HOURS):
        return {"status": "SKIP_STALE_W10"}
    from scanner.research.decision_layer.watch_runtime import load_runtime_packets_for_symbols
    from scanner.research.decision_layer.universal_stance import compute_universal_stance
    packets, meta = load_runtime_packets_for_symbols(
        root / "artifacts/research/watch_runtime", SYMBOLS,
        w10_manifest=w10, expected_snapshot_id=daily["snapshot_id"],
    )
    current = {}
    for packet in packets:
        symbol = packet["symbol"]
        if packet["source_snapshot_id"] == daily["snapshot_id"]:
            if symbol in current:
                raise ValueError("duplicate_current_baseline:" + symbol)
            current[symbol] = compute_universal_stance(packet)
    record, status = build_observation(
        daily, w10, current, captured_at,
        {"daily_research": _sha(daily_path), "w10": _sha(w10_path),
         "runtime_manifest": _sha(runtime_path)},
    )
    if record is None:
        return {"status": status}
    return persist_observation(root, record)


def persist_observation(root, record):
    """Idempotent append-only capture; existing snapshots are never overwritten."""
    snap = str(record["snapshot_id"])
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", snap):
        raise ValueError("invalid_snapshot_path")
    target = (root / "artifacts/research/uran_shadow/captures"
              / str(record["scanner_as_of"]) / (snap + ".json"))
    if target.exists():
        old = json.loads(target.read_text(encoding="utf-8"))
        if old.get("snapshot_id") != snap or old.get("source_sha256") != record["source_sha256"]:
            raise ValueError("immutable_capture_collision")
        return {"status": "ALREADY_CAPTURED", "path": str(target.relative_to(root))}
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(record, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {"status": "CAPTURED", "path": str(target.relative_to(root)),
            "snapshot_id": snap, "symbols": list(SYMBOLS)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--captured-at")
    args = parser.parse_args()
    moment = args.captured_at or datetime.now(timezone.utc).isoformat()
    print(json.dumps(run(args.root.resolve(), moment), sort_keys=True))


if __name__ == "__main__":
    main()
