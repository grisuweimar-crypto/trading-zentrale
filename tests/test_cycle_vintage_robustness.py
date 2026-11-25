from datetime import date, timedelta
import pytest
from scripts.audit_cycle_vintage_robustness import calc, compare, band, direction


def sample():
    # Weekday bars, strictly prior to cutoff.
    days=[]
    d=date(2026,7,1)
    while len(days)<60:
        if d.weekday()<5:
            days.append(d.isoformat())
        d+=timedelta(days=1)
    prices=[100+i*.2+(i%9)*2 for i in range(60)]
    row={"price_symbol":"AAA","currency":"USD","cycle_as_of":d.isoformat()+"T12:00:00+00:00"}
    row["cycle"]=calc(row,days,prices)
    assert row["cycle"] is not None
    return row,days,prices


def test_constant_scale_invariant():
    r,d,p=sample()
    c=compare(r,d,p,dict(zip(d,[v*.5 for v in p])),"SINGLE_RESPONSE")
    assert c["changed_bars"]==60
    assert c["abs_cycle_delta"]==pytest.approx(0,abs=.0001)
    assert c["uniform_scale_1bp_on_captured_subset"]
    assert not c["band_changed"]


def test_partial_never_mislabeled_complete():
    r,d,p=sample()
    observed={d[-1]:p[-1]*1.2}
    assert compare(r,d,p,observed,"SINGLE_RESPONSE") is None
    assert compare(r,d,p,observed,"MULTI_RESPONSE") is None
    c=compare(r,d,p,observed,"PARTIAL_HYBRID")
    assert c["captured"]==1
    assert c["abs_cycle_delta"]>0
    assert compare(r,d,p,dict(zip(d,p)),"PARTIAL_HYBRID") is None


def test_thresholds_and_directions():
    assert [band(x) for x in (0,24.9999,25,50,75,100)]==[0,0,1,2,3,3]
    assert direction(1,0)=="UP"
    assert direction(0,1)=="DOWN"
    assert direction(0,0)=="FLAT"
