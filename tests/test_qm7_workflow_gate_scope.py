import json
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]

def test_required_gate_paths_are_declared():
    contract=json.loads((ROOT/"configs/qm7_workflow_gate_scope_v1.json").read_text())
    for binding in contract["required_gate_bindings"]:
        workflow=yaml.safe_load((ROOT/binding["workflow"]).read_text())
        paths=workflow[True]["pull_request"]["paths"]
        for required in binding["required_paths"]:
            assert required in paths, f'{binding["workflow"]}:{required}'
    assert contract["gate_weakening_allowed"] is False
