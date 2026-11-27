from datetime import datetime, timezone
import pytest
from scanner.research.pattern_discovery.cycle_cy03_v2_review import row_digest
from scanner.research.pattern_discovery.cycle_cy05_v2_release import _projection, SCHEMA


def inputs():
    row=dict(snapshot_id="test",asset_id="AAA",generated_at="2026-11-01T10:00:00+00:00",
             as_of="2026-11-01",quality="VALID",cycle="23.4",currency="USD",
             listing_symbol="AAA",price_symbol="AAA",formula="f")
    item=dict(snapshot_id="test",asset_id="AAA",ledger_row_sha256=row_digest(row),verified_lags=[])
    review=dict(review_sha256="testhash",reviewer_id="test",source_archive_manifest_hashes={"x":"y"},rows=[item])
    release=dict(schema_version=SCHEMA,scope="CY05_RESEARCH_ONLY",review_sha256="testhash",
                 reviewer_id="test",source_archive_manifest_hashes={"x":"y"},
                 data_cutoff_utc="2026-11-01T11:00:00+00:00",
                 reviewed_at="2026-11-02T11:00:00+00:00",production_enabled=False,
                 independent_review_approved=True,rows=[dict(item)])
    return [row],review,release


def test_exact_reviewed_projection():
    ledger,review,release=inputs()
    rows=_projection(ledger,review,release,now=datetime(2026,11,3,tzinfo=timezone.utc))
    assert rows[0]["cycle_research_status"]=="ELIGIBLE"
    assert rows[0]["cycle_lag_5obs"]=="NOT_RELEASED"


@pytest.mark.parametrize("mutation,error",[
    ({"review_sha256":"wrong"},"review_hash_mismatch"),
    ({"production_enabled":True},"production_forbidden"),
    ({"independent_review_approved":False},"independent_review_missing"),
    ({"rows":[]},"empty_release"),
    ({"data_cutoff_utc":"2026-10-31T00:00:00+00:00"},"cutoff_or_quality"),
])
def test_wrong_release_rejected(mutation,error):
    ledger,review,release=inputs()
    release.update(mutation)
    with pytest.raises(ValueError,match=error):
        _projection(ledger,review,release,now=datetime(2026,11,3,tzinfo=timezone.utc))


def test_no_forged_lag():
    ledger,review,release=inputs()
    release["rows"][0]["verified_lags"]=[5]
    with pytest.raises(ValueError,match="lag_claim_mismatch"):
        _projection(ledger,review,release,now=datetime(2026,11,3,tzinfo=timezone.utc))
