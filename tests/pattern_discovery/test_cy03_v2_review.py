"""Controlled, synthetic review-contract tests. No real review is fabricated."""
from datetime import date, timedelta, datetime, timezone
import json
from pathlib import Path
import subprocess

import pytest

from scanner.research.pattern_discovery.cycle_cy03_v2_review import (
    POLICY, REQUIRED, SCHEMA, _verify_review, digest, row_digest,
)


@pytest.fixture
def fixture(tmp_path):
    root = tmp_path / "repo"
    policy = root / POLICY
    key = policy.parent / "reviewer_keys" / "test.pem"
    key.parent.mkdir(parents=True)
    private = tmp_path / "test-private.pem"
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt",
                    "rsa_keygen_bits:2048", "-out", str(private)],
                   capture_output=True, check=True)
    subprocess.run(["openssl", "pkey", "-in", str(private), "-pubout",
                    "-out", str(key)], capture_output=True, check=True)
    policy.write_text(json.dumps({
        "schema_version": "cycle_cy03_v2_review_authorities_v1",
        "reviewers": {"test": {"active": True,
                               "public_key_sha256": digest(key.read_bytes())}}
    }))
    bundle = tmp_path / "review"
    proofs = bundle / "proofs"
    proofs.mkdir(parents=True)
    refs = {}
    for kind in REQUIRED:
        raw = ("SYNTHETIC ONLY " + kind).encode()
        h = digest(raw)
        (proofs / (h + ".bin")).write_bytes(raw)
        refs[kind] = {"sha256": h, "source_url": "https://example.invalid/" + kind,
                      "evidence_type": "INDEPENDENT_EXTERNAL_SOURCE"}
    ledger, claims = [], []
    for i in range(11):
        day = date(2026, 10, 10) + timedelta(days=i)
        asof, last = day.isoformat(), (day - timedelta(days=1)).isoformat()
        row = dict(snapshot_id=f"synthetic-{i}", asset_id="AAA",
                   as_of=asof, generated_at=asof+"T12:00:00+00:00",
                   cycle_as_of=asof+"T11:00:00+00:00",
                   last_bar=last, availability="PROVISIONAL_REPLAYED",
                   listing_symbol="AAA", price_symbol="AAA", currency="USD",
                   source="YAHOO_PIT_CYCLE_V1", formula="f",
                   price_source="archive", price_basis="adjusted",
                   currency_lineage="WATCHLIST_DECLARED_ONLY",
                   session_time_quality="SESSION_DATE_CUTOFF_ONLY", reason="",
                   price_sha256=digest(f"price-{i}".encode()),
                   bars_sha256=digest(f"bars-{i}".encode()), run_id=f"run-{i}")
        ledger.append(row)
        claim = {k: row[k] for k in ("snapshot_id", "asset_id", "listing_symbol",
                 "price_symbol", "currency", "formula", "price_basis",
                 "price_sha256", "bars_sha256", "last_bar")}
        claim.update(ledger_row_sha256=row_digest(row), venue="TEST",
                     quote_unit="USD", market_timezone="UTC",
                     bar_closed_at=last+"T16:00:00+00:00",
                     bar_available_at=last+"T16:01:00+00:00",
                     proofs={k: dict(v) for k,v in refs.items()})
        claims.append(claim)
    doc = dict(schema_version=SCHEMA, scope="RESEARCH_ONLY", reviewer_id="test",
               reviewed_at="2026-10-21T12:00:00+00:00", observations=claims)
    def sign():
        path = bundle / "review.json"
        path.write_text(json.dumps(doc))
        subprocess.run(["openssl", "dgst", "-sha256", "-sign", str(private),
                        "-out", str(bundle / "review.sig"), str(path)],
                       capture_output=True, check=True)
    def run():
        return _verify_review(root, bundle, ledger, {"manifest_file_hashes": {}},
                              now=datetime(2026, 10, 22, tzinfo=timezone.utc))
    sign()
    return root, bundle, doc, ledger, sign, run


def test_complete_review_lags_and_no_promotion(fixture):
    *_, run = fixture
    r = run()
    assert r["verified_observations"] == 11
    assert r["verified_lag_counts"] == {"1": 10, "5": 6, "10": 1}
    assert not r["research_released"]
    assert not r["cy05_adapter_enabled"]


def test_missing_interior_invalidates_long_chain(fixture):
    _, _, doc, _, sign, run = fixture
    del doc["observations"][5]
    sign()
    r = run()
    assert r["verified_observations"] == 10
    assert r["verified_lag_counts"]["10"] == 0
    assert r["verified_lag_counts"]["5"] == 0


def test_unsigned_mutation_is_rejected(fixture):
    _, bundle, _, _, _, run = fixture
    p = bundle / "review.json"
    p.write_bytes(p.read_bytes() + b" ")
    with pytest.raises(ValueError, match="invalid_review_signature"):
        run()


def test_unknown_reviewer_even_with_signature(fixture):
    root, _, _, _, _, run = fixture
    (root / POLICY).write_text(json.dumps({
        "schema_version": "cycle_cy03_v2_review_authorities_v1", "reviewers": {}}))
    with pytest.raises(ValueError, match="reviewer_not_trusted"):
        run()


@pytest.mark.parametrize("field,value,error", [
    ("quote_unit", "EUR", "quote_unit_or_venue_mismatch"),
    ("price_symbol", "OTHER", "identity_mismatch"),
    ("bar_available_at", "2026-10-21T10:00:00+00:00", "invalid_pit_order"),
    ("bar_closed_at", "2026-10-01T16:00:00+00:00", "wrong_session_date"),
])
def test_signed_wrong_claim_still_rejected(fixture, field, value, error):
    _, _, doc, _, sign, run = fixture
    doc["observations"][0][field] = value
    sign()
    with pytest.raises(ValueError, match=error):
        run()


def test_proof_tampering_rejected(fixture):
    _, bundle, _, _, _, run = fixture
    next((bundle / "proofs").glob("*.bin")).write_bytes(b"changed")
    with pytest.raises(ValueError, match="proof_bytes_changed"):
        run()


def test_client_receipt_is_not_external_proof(fixture):
    _, _, doc, _, sign, run = fixture
    doc["observations"][0]["proofs"]["publisher_pit"]["evidence_type"] = "CLIENT_ONLY"
    sign()
    with pytest.raises(ValueError, match="client_receipt_not_external_proof"):
        run()


def test_archived_row_mutation_rejected(fixture):
    _, _, _, ledger, _, run = fixture
    ledger[0]["price_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="ledger_row_mismatch"):
        run()
