from pathlib import Path
import pytest
from scanner.research.decision_layer.watch_runtime_v2_capacity import audit, convert, load_symbol


def test_actual_published_runtime_roundtrip_and_headroom():
    root=Path(__file__).resolve().parents[1]
    if not (root/"artifacts/research/watch_runtime/manifest.json").exists():
        pytest.skip("no real archive fixture")
    result=audit(root)
    assert result["packets_verified_lossless"]>0
    assert result["new_headroom_ratio"]>=.20
    assert result["capacity_preview"]=="PASS"
    assert not result["production_publication"]


def test_reader_rejects_v1():
    with pytest.raises(ValueError,match="invalid_manifest"):
        load_symbol({"schema_version":"decision_watch_runtime_manifest_v1"},"AAA",lambda n:b"")
