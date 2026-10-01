import pytest

from scanner.research.governance.qm_i_lineage import LineageError, LineageRegistry, content_hash


def _node(node_id):
    return {
        "node_id": node_id,
        "version_id": "v1",
        "node_type": "RAW_SOURCE",
        "content_hash": content_hash({"id": node_id}),
        "lineage_complete": True,
        "as_of": "2026-09-30T17:00:00Z",
        "metadata": {},
    }


def test_independence_claim_successor_cannot_change_endpoint_pair(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    for node_id in ("A", "B", "C"):
        registry.register_node(record=_node(node_id), actor_id="tester", actor_role="researcher")

    registry.register_independence_claim(
        record={
            "independence_claim_id": "IC-1",
            "version_id": "v1",
            "left_node_id": "A",
            "left_version_id": "v1",
            "right_node_id": "B",
            "right_version_id": "v1",
            "status": "INDEPENDENT_SUPPORTED",
            "review_reference": "review-1",
            "rationale": "initial reviewed pair",
        },
        actor_id="tester",
        actor_role="reviewer",
    )

    with pytest.raises(LineageError, match="independence_claim_successor_must_preserve_endpoints"):
        registry.register_independence_claim(
            record={
                "independence_claim_id": "IC-1",
                "version_id": "v2",
                "left_node_id": "A",
                "left_version_id": "v1",
                "right_node_id": "C",
                "right_version_id": "v1",
                "status": "INDEPENDENT_SUPPORTED",
                "review_reference": "review-2",
                "rationale": "endpoint mutation must fail",
            },
            actor_id="tester",
            actor_role="reviewer",
            supersedes_version_id="v1",
        )
