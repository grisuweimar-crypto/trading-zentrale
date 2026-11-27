"""CY-03 v2: verify a separately signed external review against immutable v1 rows.

This is a research release candidate auditor, not a CY-05 adapter or promotion.
A reviewer attests source semantics; hashes alone cannot establish them.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
from zoneinfo import ZoneInfo

from scanner.reports.cycle_history import ROOT, lag_mask, read_ledger
from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive

POLICY = Path("configs/cycle_direction/cy03_v2_review_authorities.json")
SCHEMA = "cycle_cy03_v2_external_review_v1"
HEX = re.compile(r"[0-9a-f]{64}")
SAFE = re.compile(r"[A-Za-z0-9_-]{1,80}")
REQUIRED = ("listing", "quote_unit", "exchange_calendar", "publisher_pit")


def digest(raw):
    return sha256(raw).hexdigest()


def row_digest(row):
    return digest(json.dumps(row, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=True, allow_nan=False).encode())


def utc(value):
    t = datetime.fromisoformat(value)
    if t.tzinfo is None or t.utcoffset() != timezone.utc.utcoffset(t):
        raise ValueError("cy03v2:utc_required")
    return t


def read_bytes(path):
    if not path.is_file() or path.is_symlink():
        raise ValueError("cy03v2:missing_or_symlink:" + str(path))
    return path.read_bytes()


def _verify_signature(root, bundle, raw, doc):
    policy = json.loads(read_bytes(root / POLICY))
    if policy.get("schema_version") != "cycle_cy03_v2_review_authorities_v1":
        raise ValueError("cy03v2:unknown_authority_policy")
    reviewer = doc["reviewer_id"]
    if not isinstance(reviewer, str) or not SAFE.fullmatch(reviewer):
        raise ValueError("cy03v2:invalid_reviewer")
    pin = policy["reviewers"].get(reviewer)
    if not isinstance(pin, dict) or pin.get("active") is not True:
        raise ValueError("cy03v2:reviewer_not_trusted")
    key = root / POLICY.parent / "reviewer_keys" / (reviewer + ".pem")
    if digest(read_bytes(key)) != pin.get("public_key_sha256"):
        raise ValueError("cy03v2:reviewer_key_mismatch")
    signature = bundle / "review.sig"
    read_bytes(signature)
    # Verify the exact signed JSON bytes rather than reserializing the document.
    import tempfile
    with tempfile.TemporaryDirectory() as temp:
        p = Path(temp) / "review.json"
        p.write_bytes(raw)
        result = subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(key.resolve()),
             "-signature", str(signature.resolve()), str(p)],
            capture_output=True, timeout=15, check=False)
    if result.returncode != 0:
        raise ValueError("cy03v2:invalid_review_signature")


def verify_review(root, bundle, *, now=None):
    root, bundle = Path(root), Path(bundle)
    inspection = inspect_cycle_archive(root)
    ledger = read_ledger(root / ROOT / "observations.csv")
    return _verify_review(root, bundle, ledger, inspection, now=now)


def _verify_review(root, bundle, ledger, inspection, *, now=None):
    raw = read_bytes(bundle / "review.json")
    doc = json.loads(raw)
    if doc.get("schema_version") != SCHEMA or doc.get("scope") != "RESEARCH_ONLY":
        raise ValueError("cy03v2:wrong_review_contract")
    _verify_signature(root, bundle, raw, doc)
    reviewed = utc(doc["reviewed_at"])
    if reviewed > (now or datetime.now(timezone.utc)):
        raise ValueError("cy03v2:future_review")
    # Bind the decision to the exact historical row and both price hashes.
    # A growing archive is allowed; the v1 verifier recomputes its current masks.
    index = {(r["snapshot_id"], r["asset_id"]): r for r in ledger}
    if len(index) != len(ledger):
        raise ValueError("cy03v2:duplicate_ledger_key")
    masks = {(r["snapshot_id"], r["asset_id"]): r for r in lag_mask(ledger)}
    accepted = set()
    proof_dir = bundle / "proofs"
    if proof_dir.is_symlink():
        raise ValueError("cy03v2:proof_directory_symlink")
    for claim in doc["observations"]:
        k = (claim["snapshot_id"], claim["asset_id"])
        row = index.get(k)
        if k in accepted or row is None or row["availability"] != "PROVISIONAL_REPLAYED":
            raise ValueError("cy03v2:duplicate_unknown_or_invalid_observation")
        if masks[k]["lag_1obs"] == "SUPERSEDED_SAME_DAY":
            raise ValueError("cy03v2:noncanonical_observation")
        if claim["ledger_row_sha256"] != row_digest(row):
            raise ValueError("cy03v2:ledger_row_mismatch")
        for field in ("listing_symbol", "price_symbol", "currency", "formula",
                      "price_basis", "price_sha256", "bars_sha256", "last_bar"):
            if claim[field] != row[field]:
                raise ValueError("cy03v2:identity_mismatch:" + field)
        if not claim["venue"] or claim["quote_unit"] != row["currency"]:
            raise ValueError("cy03v2:quote_unit_or_venue_mismatch")
        ZoneInfo(claim["market_timezone"])
        close, available = utc(claim["bar_closed_at"]), utc(claim["bar_available_at"])
        if not close <= available <= utc(row["cycle_as_of"]) <= utc(row["generated_at"]) <= reviewed:
            raise ValueError("cy03v2:invalid_pit_order")
        if close.astimezone(ZoneInfo(claim["market_timezone"])).date().isoformat() != row["last_bar"]:
            raise ValueError("cy03v2:wrong_session_date")
        proofs = claim["proofs"]
        if set(proofs) != set(REQUIRED):
            raise ValueError("cy03v2:incomplete_external_proofs")
        for kind in REQUIRED:
            proof = proofs[kind]
            h = proof["sha256"]
            if not isinstance(h, str) or not HEX.fullmatch(h):
                raise ValueError("cy03v2:invalid_proof_hash")
            if not proof["source_url"].startswith("https://"):
                raise ValueError("cy03v2:missing_external_source")
            if proof["evidence_type"] != "INDEPENDENT_EXTERNAL_SOURCE":
                raise ValueError("cy03v2:client_receipt_not_external_proof")
            if digest(read_bytes(proof_dir / (h + ".bin"))) != h:
                raise ValueError("cy03v2:proof_bytes_changed")
        accepted.add(k)
    # Same canonical snapshot per date as v1. Every intervening observation
    # must be independently approved, not merely the lag endpoints.
    canonical = {}
    for row in ledger:
        day = row["as_of"]
        key = (utc(row["generated_at"]).isoformat(), row["snapshot_id"])
        if day not in canonical or key > canonical[day]:
            canonical[day] = key
    days = sorted(canonical)
    position = {d: i for i, d in enumerate(days)}
    output = []
    for k in sorted(accepted):
        row = index[k]
        i = position[row["as_of"]]
        eligible = []
        for n in (1, 5, 10):
            if masks[k][f"lag_{n}obs"] != "PROVISIONAL_CHAIN" or i < n:
                continue
            if all((canonical[d][1], k[1]) in accepted for d in days[i-n:i+1]):
                eligible.append(n)
        output.append({"snapshot_id": k[0], "asset_id": k[1],
                       "as_of": row["as_of"], "ledger_row_sha256": row_digest(row),
                       "verified_lags": eligible})
    return {"schema_version": "cycle_cy03_v2_review_audit_v1",
            "review_sha256": digest(raw), "reviewer_id": doc["reviewer_id"],
            "source_archive_manifest_hashes": inspection["manifest_file_hashes"],
            "verified_observations": len(output),
            "verified_lag_counts": {str(n): sum(n in r["verified_lags"] for r in output)
                                    for n in (1, 5, 10)},
            "rows": output, "research_released": False,
            "cy05_adapter_enabled": False, "production_effect": False}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", type=Path, default=Path("."))
    p.add_argument("--bundle", type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(verify_review(args.repo_root, args.bundle), indent=2, sort_keys=True))
