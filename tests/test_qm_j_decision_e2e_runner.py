from __future__ import annotations

import importlib.util
from pathlib import Path

from scanner.research.decision_layer.evidence_archive import load_evidence_archive


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_qm_j_decision_e2e_falsification.py"


def _load_runner_module():
    spec = importlib.util.spec_from_file_location("qm_j_decision_e2e_runner_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_qm_j_runner_uses_canonical_compressed_decision_archive() -> None:
    module = _load_runner_module()
    path = Path(module.DEFAULT_ARCHIVE_PATH)
    assert path == ROOT / "artifacts/research/decision_evidence_7a.jsonl.gz"
    assert path.exists()
    packets, metadata = load_evidence_archive(path, missing_ok=False)
    assert packets
    assert int(metadata["packet_count"]) == len(packets)
