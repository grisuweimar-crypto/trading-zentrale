"""Point-in-time guardrails for scanner-vs-external comparison archive."""
import csv
import importlib.util
import io
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location("ckpt",Path(__file__).resolve().parents[1]/"scripts/argus_external_scanner_checkpoint.py")
ckpt=importlib.util.module_from_spec(spec)
spec.loader.exec_module(ckpt)


def input_data():
    meta={"latest_run_complete":True,"validation":{"status":"ok"},
          "snapshot_id":"abc123","as_of":"2026-10-10",
          "generated_at":"2026-10-10T19:12:00+00:00",
          "latest_scanner":{"row_count":1}}
    row={"symbol":"IFX.DE","snapshot_id":"abc123",
         "observation_type":"observed_scanner","data_source":"scanner_run",
         "score":"60.7","opportunity":"58.0","risk":"25.5",
         "confidence":"68.2","currency":"EUR"}
    stream=io.StringIO()
    writer=csv.DictWriter(stream,fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    return meta,stream.getvalue().encode()


def test_immutable_validated_snapshot_capture():
    meta,content=input_data()
    result=ckpt.capture(meta,content)
    assert result["snapshot_id"]=="abc123"
    assert result["published_at"]=="2026-10-10T19:12:00+00:00"
    assert result["symbols"]["IFX.DE"]["currency"]=="EUR"
    assert result["csv_sha256"]
    assert result["research_only"] is True


def test_reject_invalid_or_mixed_snapshot():
    meta,content=input_data()
    meta["latest_run_complete"]=False
    with pytest.raises(ValueError,match="not_validated"):
        ckpt.capture(meta,content)
    meta["latest_run_complete"]=True
    with pytest.raises(ValueError,match="row_mismatch"):
        ckpt.capture(meta,content.replace(b"abc123",b"changed"))


def test_strict_pre_publication_snapshot_join():
    meta,content=input_data()
    snap=ckpt.capture(meta,content)
    report={"schema_version":"argus_external_thesis_followup_v1",
            "research_only":True,
            "claims":[
              {"symbol":"IFX.DE","publication_date":"2026-10-09","currency":"EUR"},
              {"symbol":"IFX.DE","publication_date":"2026-10-10","currency":"EUR"},
              {"symbol":"IFX.DE","publication_date":"2026-10-11","currency":"EUR"},
              {"symbol":"IFX.DE","publication_date":"2026-10-11","currency":"USD"}
            ]}
    amended=ckpt.attach(report,[snap])
    assert amended["claims"][0]["scanner_comparison"]=="NO_VERIFIED_ASOF_SNAPSHOT"
    assert amended["claims"][1]["scanner_comparison"]=="NO_VERIFIED_ASOF_SNAPSHOT"
    found=amended["claims"][2]["scanner_comparison"]
    assert found["state"]=="PRIOR_SNAPSHOT_OBSERVED"
    assert found["score"]=="60.7"
    assert found["published_at"]<"2026-10-11T00:00:00+00:00"
    assert amended["claims"][3]["scanner_comparison"]=="NOT_IN_VERIFIED_PRIOR_SCANNER"


def test_fail_on_unknown_report_schema():
    with pytest.raises(ValueError,match="invalid_external"):
        ckpt.attach({"schema_version":"unknown","research_only":True,"claims":[]},[])
