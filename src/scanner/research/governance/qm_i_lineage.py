"""QM-I / BA-QM3 evidence lineage and double-counting review governance.

The lineage registry is research-only and append-only. It records stable,
versioned evidence identities and their material provenance without changing
scanner weights, Decision-Layer semantics, portfolio actions or execution.
Common ancestry is a review trigger, not an automatic defect. Missing lineage
never counts as evidence of independence.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import uuid4

from scanner.research.decision_layer.depot_watch import validate_depot_watch
from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.portfolio_action import validate_portfolio_action
from scanner.research.decision_layer.universal_stance import validate_universal_stance
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_multiplicity import MultiplicityMonitoringRegistry
from scanner.research.governance.qm_c_results import ResultRegistry


EVENT_SCHEMA_VERSION = "qm_i_lineage_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_i_evidence_lineage_v1.json"


class LineageError(ValueError):
    """Raised when a QM-I provenance invariant is violated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise LineageError(f"value_required:{field}")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_qm_i_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LineageError(f"qm_i_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_i_evidence_lineage_v1":
        raise LineageError("qm_i_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise LineageError("qm_i_contract_scope_invalid")
    return payload


def content_hash(value: Any) -> str:
    """Canonical SHA-256 for immutable artifact identity."""
    return _hash(value)


class LineageRegistry:
    """Append-only typed provenance graph plus immutable independence claims."""

    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_i_contract(contract_path)
        self.event_types = set(self.contract["registry"]["event_types"])
        node_spec = self.contract["node"]
        edge_spec = self.contract["edge"]
        independence_spec = self.contract["independence_claim"]
        self.node_types = set(node_spec["node_types"])
        self.root_node_types = set(node_spec["root_node_types"])
        self.node_required_fields = tuple(node_spec["required_fields"])
        self.edge_relations = set(edge_spec["relations"])
        self.ancestry_relations = set(edge_spec["ancestry_relations"])
        self.edge_required_fields = tuple(edge_spec["required_fields"])
        self.independence_statuses = set(independence_spec["statuses"])
        self.independence_required_fields = tuple(independence_spec["required_fields"])

    @staticmethod
    def _node_key(node_id: str, version_id: str) -> str:
        return f"{node_id}::{version_id}"

    @staticmethod
    def _pair_key(left_key: str, right_key: str) -> tuple[str, str]:
        return tuple(sorted((left_key, right_key)))  # type: ignore[return-value]

    def _normalize_node(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("node_record_must_be_object")
        missing = [field for field in self.node_required_fields if field not in record]
        if missing:
            raise LineageError("node_fields_missing:" + ",".join(missing))
        node_type = _nonblank(record.get("node_type"), field="node_type").upper()
        if node_type not in self.node_types:
            raise LineageError(f"node_type_invalid:{node_type}")
        lineage_complete = record.get("lineage_complete")
        if not isinstance(lineage_complete, bool):
            raise LineageError("lineage_complete_must_be_boolean")
        metadata = record.get("metadata")
        if not isinstance(metadata, Mapping):
            raise LineageError("node_metadata_must_be_object")
        content = _nonblank(record.get("content_hash"), field="content_hash")
        if len(content) != 64 or any(char not in "0123456789abcdefABCDEF" for char in content):
            raise LineageError("content_hash_must_be_sha256_hex")
        return {
            "node_id": _nonblank(record.get("node_id"), field="node_id"),
            "version_id": _nonblank(record.get("version_id"), field="version_id"),
            "node_type": node_type,
            "content_hash": content.lower(),
            "lineage_complete": lineage_complete,
            "as_of": _optional_text(record.get("as_of")),
            "metadata": json.loads(_canonical_json(dict(metadata))),
        }

    def _normalize_edge(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("edge_record_must_be_object")
        missing = [field for field in self.edge_required_fields if field not in record]
        if missing:
            raise LineageError("edge_fields_missing:" + ",".join(missing))
        relation = _nonblank(record.get("relation"), field="edge.relation").upper()
        if relation not in self.edge_relations:
            raise LineageError(f"edge_relation_invalid:{relation}")
        material = record.get("material_for_ancestry")
        if not isinstance(material, bool):
            raise LineageError("edge_material_for_ancestry_must_be_boolean")
        if material and relation not in self.ancestry_relations:
            raise LineageError(f"non_ancestry_relation_cannot_be_material:{relation}")
        return {
            "edge_id": _nonblank(record.get("edge_id"), field="edge_id"),
            "from_node_id": _nonblank(record.get("from_node_id"), field="edge.from_node_id"),
            "from_version_id": _nonblank(record.get("from_version_id"), field="edge.from_version_id"),
            "to_node_id": _nonblank(record.get("to_node_id"), field="edge.to_node_id"),
            "to_version_id": _nonblank(record.get("to_version_id"), field="edge.to_version_id"),
            "relation": relation,
            "material_for_ancestry": material,
        }

    def _normalize_independence_claim(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("independence_claim_must_be_object")
        missing = [field for field in self.independence_required_fields if field not in record]
        if missing:
            raise LineageError("independence_claim_fields_missing:" + ",".join(missing))
        status = _nonblank(record.get("status"), field="independence_claim.status").upper()
        if status not in self.independence_statuses:
            raise LineageError(f"independence_status_invalid:{status}")
        return {
            "independence_claim_id": _nonblank(record.get("independence_claim_id"), field="independence_claim_id"),
            "version_id": _nonblank(record.get("version_id"), field="independence_claim.version_id"),
            "left_node_id": _nonblank(record.get("left_node_id"), field="left_node_id"),
            "left_version_id": _nonblank(record.get("left_version_id"), field="left_version_id"),
            "right_node_id": _nonblank(record.get("right_node_id"), field="right_node_id"),
            "right_version_id": _nonblank(record.get("right_version_id"), field="right_version_id"),
            "status": status,
            "review_reference": _optional_text(record.get("review_reference")),
            "rationale": _nonblank(record.get("rationale"), field="independence_claim.rationale"),
        }

    def _read_raw_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LineageError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(value, dict):
                raise LineageError(f"lineage_registry_line_not_object:{line_number}")
            events.append(value)
        return events

    def _verify_hash_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous_hash: str | None = None
        for sequence, raw in enumerate(events, start=1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise LineageError(f"lineage_event_schema_invalid:{sequence}")
            if event.get("sequence") != sequence:
                raise LineageError(f"lineage_sequence_invalid:{sequence}")
            if event.get("previous_event_hash") != previous_hash:
                raise LineageError(f"lineage_previous_hash_invalid:{sequence}")
            stored_hash = str(event.get("entry_hash") or "")
            if not stored_hash:
                raise LineageError(f"lineage_entry_hash_missing:{sequence}")
            body = dict(event)
            body.pop("entry_hash", None)
            if stored_hash != _hash(body):
                raise LineageError(f"lineage_entry_hash_invalid:{sequence}")
            previous_hash = stored_hash

    @staticmethod
    def _forward_graph(edges: Mapping[str, Mapping[str, Any]]) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {}
        for edge in edges.values():
            if edge["material_for_ancestry"] is not True:
                continue
            source = f"{edge['from_node_id']}::{edge['from_version_id']}"
            target = f"{edge['to_node_id']}::{edge['to_version_id']}"
            graph.setdefault(source, set()).add(target)
        return graph

    @staticmethod
    def _reachable(graph: Mapping[str, set[str]], start: str, target: str) -> bool:
        queue = deque([start])
        seen: set[str] = set()
        while queue:
            current = queue.popleft()
            if current == target:
                return True
            if current in seen:
                continue
            seen.add(current)
            queue.extend(graph.get(current, set()) - seen)
        return False

    def _replay(self, events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        nodes: dict[str, dict[str, Any]] = {}
        edges: dict[str, dict[str, Any]] = {}
        claims: dict[str, dict[str, Any]] = {}
        latest_node_version: dict[str, str] = {}
        latest_claim_version: dict[str, str] = {}

        for raw_event in events:
            event = dict(raw_event)
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise LineageError(f"lineage_event_type_unknown:{event_type}")
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise LineageError("lineage_event_payload_must_be_object")

            if event_type == "NODE_REGISTERED":
                record = self._normalize_node(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                key = self._node_key(record["node_id"], record["version_id"])
                if key in nodes:
                    raise LineageError(f"lineage_node_version_already_registered:{key}")
                supersedes = payload.get("supersedes_version_id")
                if supersedes is not None:
                    supersedes = _nonblank(supersedes, field="node.supersedes_version_id")
                    predecessor = self._node_key(record["node_id"], supersedes)
                    if predecessor not in nodes:
                        raise LineageError(f"lineage_superseded_node_version_missing:{predecessor}")
                    if latest_node_version.get(record["node_id"]) != supersedes:
                        raise LineageError("lineage_node_successor_must_supersede_latest_version")
                    if supersedes == record["version_id"]:
                        raise LineageError("lineage_node_version_cannot_supersede_itself")
                elif record["node_id"] in latest_node_version:
                    raise LineageError("lineage_node_successor_requires_supersedes_reference")
                nodes[key] = {
                    **record,
                    "supersedes_version_id": supersedes,
                    "registered_at": event["recorded_at"],
                    "registered_by": event["actor_id"],
                }
                latest_node_version[record["node_id"]] = record["version_id"]
                continue

            if event_type == "EDGE_REGISTERED":
                record = self._normalize_edge(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["edge_id"] in edges:
                    raise LineageError(f"lineage_edge_already_registered:{record['edge_id']}")
                source = self._node_key(record["from_node_id"], record["from_version_id"])
                target = self._node_key(record["to_node_id"], record["to_version_id"])
                if source not in nodes or target not in nodes:
                    raise LineageError(f"lineage_edge_endpoint_missing:{source}->{target}")
                if source == target:
                    raise LineageError("lineage_self_edge_forbidden")
                if any(
                    edge["from_node_id"] == record["from_node_id"]
                    and edge["from_version_id"] == record["from_version_id"]
                    and edge["to_node_id"] == record["to_node_id"]
                    and edge["to_version_id"] == record["to_version_id"]
                    and edge["relation"] == record["relation"]
                    for edge in edges.values()
                ):
                    raise LineageError("lineage_duplicate_semantic_edge")
                if record["material_for_ancestry"]:
                    forward = self._forward_graph(edges)
                    if self._reachable(forward, target, source):
                        raise LineageError(f"lineage_cycle_forbidden:{source}->{target}")
                edges[record["edge_id"]] = record
                continue

            record = self._normalize_independence_claim(
                payload.get("record") if isinstance(payload.get("record"), Mapping) else {}
            )
            claim_key = self._node_key(record["independence_claim_id"], record["version_id"])
            if claim_key in claims:
                raise LineageError(f"independence_claim_version_already_registered:{claim_key}")
            left = self._node_key(record["left_node_id"], record["left_version_id"])
            right = self._node_key(record["right_node_id"], record["right_version_id"])
            if left not in nodes or right not in nodes:
                raise LineageError("independence_claim_endpoint_missing")
            supersedes = payload.get("supersedes_version_id")
            if supersedes is not None:
                supersedes = _nonblank(supersedes, field="independence_claim.supersedes_version_id")
                predecessor = self._node_key(record["independence_claim_id"], supersedes)
                if predecessor not in claims:
                    raise LineageError(f"superseded_independence_claim_missing:{predecessor}")
                if latest_claim_version.get(record["independence_claim_id"]) != supersedes:
                    raise LineageError("independence_claim_successor_must_supersede_latest_version")
            elif record["independence_claim_id"] in latest_claim_version:
                raise LineageError("independence_claim_successor_requires_supersedes_reference")
            claims[claim_key] = {
                **record,
                "supersedes_version_id": supersedes,
                "registered_at": event["recorded_at"],
                "registered_by": event["actor_id"],
            }
            latest_claim_version[record["independence_claim_id"]] = record["version_id"]

        return {"nodes": nodes, "edges": edges, "independence_claims": claims}

    def _load_and_validate(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        events = self._read_raw_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def _acquire_lock(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise LineageError(f"lineage_registry_lock_exists:{self.lock_path}") from exc

    def _release_lock(self, fd: int) -> None:
        try:
            os.close(fd)
        finally:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def _append_event(self, *, event_type: str, actor_id: str, actor_role: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if event_type not in self.event_types:
            raise LineageError(f"unsupported_lineage_event_type:{event_type}")
        actor_id = _nonblank(actor_id, field="actor_id")
        actor_role = _nonblank(actor_role, field="actor_role")
        lock_fd = self._acquire_lock()
        try:
            events, _ = self._load_and_validate()
            event: dict[str, Any] = {
                "schema_version": EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_id": str(uuid4()),
                "event_type": event_type,
                "recorded_at": _utc_now(),
                "actor_id": actor_id,
                "actor_role": actor_role,
                "payload": dict(payload),
                "previous_event_hash": events[-1]["entry_hash"] if events else None,
            }
            event["entry_hash"] = _hash(event)
            candidate = [*events, event]
            self._verify_hash_chain(candidate)
            self._replay(candidate)
            line = (_canonical_json(event) + "\n").encode("utf-8")
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            try:
                os.write(fd, line)
                os.fsync(fd)
            finally:
                os.close(fd)
            self._load_and_validate()
            return event
        finally:
            self._release_lock(lock_fd)

    def register_node(
        self,
        *,
        record: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        supersedes_version_id: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_node(record)
        _, state = self._load_and_validate()
        key = self._node_key(normalized["node_id"], normalized["version_id"])
        if key in state["nodes"]:
            raise LineageError(f"lineage_node_version_already_registered:{key}")
        return self._append_event(
            event_type="NODE_REGISTERED",
            actor_id=actor_id,
            actor_role=actor_role,
            payload={"record": normalized, "supersedes_version_id": supersedes_version_id},
        )

    def register_edge(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        normalized = self._normalize_edge(record)
        return self._append_event(
            event_type="EDGE_REGISTERED",
            actor_id=actor_id,
            actor_role=actor_role,
            payload={"record": normalized},
        )

    def get_node(self, node_id: str, version_id: str) -> dict[str, Any]:
        _, state = self._load_and_validate()
        key = self._node_key(node_id, version_id)
        if key not in state["nodes"]:
            raise LineageError(f"lineage_node_not_registered:{key}")
        return dict(state["nodes"][key])

    def _material_graphs(self, state: Mapping[str, Any]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        forward: dict[str, set[str]] = {}
        parents: dict[str, set[str]] = {}
        for edge in state["edges"].values():
            if edge["material_for_ancestry"] is not True:
                continue
            source = self._node_key(edge["from_node_id"], edge["from_version_id"])
            target = self._node_key(edge["to_node_id"], edge["to_version_id"])
            forward.setdefault(source, set()).add(target)
            parents.setdefault(target, set()).add(source)
        return forward, parents

    @staticmethod
    def _ancestors(parents: Mapping[str, set[str]], node_key: str) -> set[str]:
        found: set[str] = set()
        queue = deque(parents.get(node_key, set()))
        while queue:
            current = queue.popleft()
            if current in found:
                continue
            found.add(current)
            queue.extend(parents.get(current, set()) - found)
        return found

    @staticmethod
    def _path_from_ancestor(parents: Mapping[str, set[str]], ancestor: str, target: str) -> list[str] | None:
        queue: deque[tuple[str, list[str]]] = deque([(target, [target])])
        seen: set[str] = set()
        while queue:
            current, backward_path = queue.popleft()
            if current == ancestor:
                return list(reversed(backward_path))
            if current in seen:
                continue
            seen.add(current)
            for parent in sorted(parents.get(current, set())):
                queue.append((parent, [*backward_path, parent]))
        return None

    def _lineage_is_complete(self, state: Mapping[str, Any], node_key: str) -> tuple[bool, list[str]]:
        nodes = state["nodes"]
        _, parents = self._material_graphs(state)
        queue = deque([node_key])
        seen: set[str] = set()
        reasons: list[str] = []
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            node = nodes[current]
            if node["lineage_complete"] is not True:
                reasons.append(f"lineage_incomplete:{current}")
            incoming = parents.get(current, set())
            if node["node_type"] not in self.root_node_types and not incoming:
                reasons.append(f"complete_nonroot_without_material_parent:{current}")
            queue.extend(incoming - seen)
        return not reasons, sorted(set(reasons))

    def analyze_ancestry(
        self,
        *,
        left_node_id: str,
        left_version_id: str,
        right_node_id: str,
        right_version_id: str,
    ) -> dict[str, Any]:
        _, state = self._load_and_validate()
        left = self._node_key(left_node_id, left_version_id)
        right = self._node_key(right_node_id, right_version_id)
        if left not in state["nodes"] or right not in state["nodes"]:
            raise LineageError("ancestry_endpoint_missing")
        forward, parents = self._material_graphs(state)
        if left == right:
            return {
                "classification": "SAME_EVIDENCE",
                "left": left,
                "right": right,
                "common_ancestors": [left],
                "paths": [{"ancestor": left, "left_path": [left], "right_path": [right]}],
                "lineage_complete": False,
                "completeness_reasons": ["same_evidence_cannot_be_independent"],
            }
        if self._reachable(forward, left, right) or self._reachable(forward, right, left):
            source, target = (left, right) if self._reachable(forward, left, right) else (right, left)
            path = self._path_from_ancestor(parents, source, target)
            left_complete, left_reasons = self._lineage_is_complete(state, left)
            right_complete, right_reasons = self._lineage_is_complete(state, right)
            return {
                "classification": "DIRECT_DEPENDENCY",
                "left": left,
                "right": right,
                "direct_source": source,
                "direct_target": target,
                "direct_path": path,
                "common_ancestors": [],
                "paths": [],
                "lineage_complete": left_complete and right_complete,
                "completeness_reasons": sorted(set(left_reasons + right_reasons)),
            }

        left_ancestors = self._ancestors(parents, left)
        right_ancestors = self._ancestors(parents, right)
        common = sorted(left_ancestors & right_ancestors)
        left_complete, left_reasons = self._lineage_is_complete(state, left)
        right_complete, right_reasons = self._lineage_is_complete(state, right)
        if common:
            paths = [
                {
                    "ancestor": ancestor,
                    "left_path": self._path_from_ancestor(parents, ancestor, left),
                    "right_path": self._path_from_ancestor(parents, ancestor, right),
                }
                for ancestor in common
            ]
            return {
                "classification": "COMMON_ANCESTRY",
                "left": left,
                "right": right,
                "common_ancestors": common,
                "paths": paths,
                "lineage_complete": left_complete and right_complete,
                "completeness_reasons": sorted(set(left_reasons + right_reasons)),
            }
        if not left_complete or not right_complete:
            return {
                "classification": "UNKNOWN_INCOMPLETE_LINEAGE",
                "left": left,
                "right": right,
                "common_ancestors": [],
                "paths": [],
                "lineage_complete": False,
                "completeness_reasons": sorted(set(left_reasons + right_reasons)),
            }
        return {
            "classification": "NO_COMMON_ANCESTRY_DETECTED",
            "left": left,
            "right": right,
            "common_ancestors": [],
            "paths": [],
            "lineage_complete": True,
            "completeness_reasons": [],
        }

    def register_independence_claim(
        self,
        *,
        record: Mapping[str, Any],
        actor_id: str,
        actor_role: str,
        supersedes_version_id: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_independence_claim(record)
        analysis = self.analyze_ancestry(
            left_node_id=normalized["left_node_id"],
            left_version_id=normalized["left_version_id"],
            right_node_id=normalized["right_node_id"],
            right_version_id=normalized["right_version_id"],
        )
        status = normalized["status"]
        classification = analysis["classification"]
        if status == "INDEPENDENT_SUPPORTED":
            if not normalized["review_reference"]:
                raise LineageError("independent_supported_requires_review_reference")
            if classification != "NO_COMMON_ANCESTRY_DETECTED" or analysis["lineage_complete"] is not True:
                raise LineageError(f"independent_supported_contradicted_by_ancestry:{classification}")
        elif status == "DEPENDENT_COMMON_ANCESTRY" and classification != "COMMON_ANCESTRY":
            raise LineageError(f"dependent_common_ancestry_status_mismatch:{classification}")
        elif status == "DEPENDENT_DIRECT_REFERENCE" and classification not in {"DIRECT_DEPENDENCY", "SAME_EVIDENCE"}:
            raise LineageError(f"dependent_direct_status_mismatch:{classification}")
        elif status == "UNKNOWN_REVIEW_REQUIRED" and classification not in {"UNKNOWN_INCOMPLETE_LINEAGE", "COMMON_ANCESTRY", "DIRECT_DEPENDENCY", "SAME_EVIDENCE"}:
            raise LineageError("unknown_status_not_justified")
        return self._append_event(
            event_type="INDEPENDENCE_CLAIM_REGISTERED",
            actor_id=actor_id,
            actor_role=actor_role,
            payload={
                "record": normalized,
                "ancestry_at_registration": analysis,
                "supersedes_version_id": supersedes_version_id,
            },
        )

    def _latest_independence_claim_for_pair(self, state: Mapping[str, Any], left: str, right: str) -> dict[str, Any] | None:
        pair = self._pair_key(left, right)
        candidates: list[dict[str, Any]] = []
        for claim in state["independence_claims"].values():
            claim_left = self._node_key(claim["left_node_id"], claim["left_version_id"])
            claim_right = self._node_key(claim["right_node_id"], claim["right_version_id"])
            if self._pair_key(claim_left, claim_right) == pair:
                candidates.append(claim)
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: item["registered_at"])[-1]

    def double_counting_review(
        self,
        *,
        combination_id: str,
        evidence_nodes: Sequence[Mapping[str, str]],
        purports_independent: bool,
    ) -> dict[str, Any]:
        combination_id = _nonblank(combination_id, field="combination_id")
        if len(evidence_nodes) < 2:
            raise LineageError("double_counting_review_requires_at_least_two_nodes")
        refs: list[tuple[str, str, str]] = []
        _, state = self._load_and_validate()
        seen: set[str] = set()
        for index, ref in enumerate(evidence_nodes):
            if not isinstance(ref, Mapping):
                raise LineageError(f"evidence_node_ref_must_be_object:{index}")
            node_id = _nonblank(ref.get("node_id"), field=f"evidence_nodes[{index}].node_id")
            version_id = _nonblank(ref.get("version_id"), field=f"evidence_nodes[{index}].version_id")
            key = self._node_key(node_id, version_id)
            if key not in state["nodes"]:
                raise LineageError(f"double_counting_node_missing:{key}")
            if key in seen:
                raise LineageError(f"double_counting_duplicate_node:{key}")
            seen.add(key)
            refs.append((node_id, version_id, key))

        pair_reviews: list[dict[str, Any]] = []
        triggers: list[dict[str, Any]] = []
        for left_index in range(len(refs)):
            for right_index in range(left_index + 1, len(refs)):
                left_id, left_version, left_key = refs[left_index]
                right_id, right_version, right_key = refs[right_index]
                analysis = self.analyze_ancestry(
                    left_node_id=left_id,
                    left_version_id=left_version,
                    right_node_id=right_id,
                    right_version_id=right_version,
                )
                claim = self._latest_independence_claim_for_pair(state, left_key, right_key)
                classification = analysis["classification"]
                trigger: str | None = None
                if claim is not None and claim["status"] == "INDEPENDENT_SUPPORTED" and classification != "NO_COMMON_ANCESTRY_DETECTED":
                    trigger = "INDEPENDENCE_CLAIM_CONTRADICTION_REVIEW_REQUIRED"
                elif classification in {"SAME_EVIDENCE", "DIRECT_DEPENDENCY"}:
                    trigger = "DIRECT_DEPENDENCY_REVIEW_REQUIRED"
                elif classification == "COMMON_ANCESTRY":
                    trigger = "COMMON_ANCESTRY_REVIEW_REQUIRED"
                elif classification == "UNKNOWN_INCOMPLETE_LINEAGE":
                    trigger = "INCOMPLETE_LINEAGE_REVIEW_REQUIRED"
                elif purports_independent and (claim is None or claim["status"] != "INDEPENDENT_SUPPORTED"):
                    trigger = "INDEPENDENCE_CLAIM_MISSING_REVIEW_REQUIRED"

                pair_review = {
                    "left": left_key,
                    "right": right_key,
                    "ancestry": analysis,
                    "latest_independence_claim": None if claim is None else {
                        "independence_claim_id": claim["independence_claim_id"],
                        "version_id": claim["version_id"],
                        "status": claim["status"],
                        "review_reference": claim["review_reference"],
                    },
                    "trigger": trigger,
                }
                pair_reviews.append(pair_review)
                if trigger:
                    triggers.append({
                        "trigger": trigger,
                        "left": left_key,
                        "right": right_key,
                        "classification": classification,
                        "common_ancestors": analysis.get("common_ancestors", []),
                    })
        return {
            "schema_version": "qm_i_double_counting_review_v1",
            "combination_id": combination_id,
            "purports_independent": bool(purports_independent),
            "status": "REVIEW_REQUIRED" if triggers else "CLEAR",
            "pair_reviews": pair_reviews,
            "triggers": triggers,
            "common_ancestry_is_automatic_error": False,
            "automatic_weight_change_performed": False,
            "scanner_or_decision_semantics_changed": False,
        }

    def verify_integrity(self) -> dict[str, Any]:
        events, state = self._load_and_validate()
        stale_independence_claims: list[dict[str, Any]] = []
        for claim in state["independence_claims"].values():
            if claim["status"] != "INDEPENDENT_SUPPORTED":
                continue
            current = self.analyze_ancestry(
                left_node_id=claim["left_node_id"],
                left_version_id=claim["left_version_id"],
                right_node_id=claim["right_node_id"],
                right_version_id=claim["right_version_id"],
            )
            if current["classification"] != "NO_COMMON_ANCESTRY_DETECTED" or current["lineage_complete"] is not True:
                stale_independence_claims.append({
                    "independence_claim_id": claim["independence_claim_id"],
                    "version_id": claim["version_id"],
                    "current_classification": current["classification"],
                })
        return {
            "schema_version": "qm_i_lineage_verification_v1",
            "valid": True,
            "event_count": len(events),
            "node_version_count": len(state["nodes"]),
            "edge_count": len(state["edges"]),
            "independence_claim_version_count": len(state["independence_claims"]),
            "stale_independence_claims": stale_independence_claims,
            "head_hash": events[-1]["entry_hash"] if events else None,
        }

    def _ensure_node(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        normalized = self._normalize_node(record)
        _, state = self._load_and_validate()
        key = self._node_key(normalized["node_id"], normalized["version_id"])
        existing = state["nodes"].get(key)
        if existing is None:
            self.register_node(record=normalized, actor_id=actor_id, actor_role=actor_role)
            return self.get_node(normalized["node_id"], normalized["version_id"])
        comparable = {field: existing[field] for field in self.node_required_fields}
        if comparable != normalized:
            raise LineageError(f"existing_lineage_node_conflicts:{key}")
        return dict(existing)

    def _ensure_edge(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        normalized = self._normalize_edge(record)
        _, state = self._load_and_validate()
        existing = state["edges"].get(normalized["edge_id"])
        if existing is not None:
            if existing != normalized:
                raise LineageError(f"existing_lineage_edge_conflicts:{normalized['edge_id']}")
            return dict(existing)
        self.register_edge(record=normalized, actor_id=actor_id, actor_role=actor_role)
        _, state = self._load_and_validate()
        return dict(state["edges"][normalized["edge_id"]])

    def register_phase7_packet(self, packet: Mapping[str, Any], *, actor_id: str, actor_role: str) -> dict[str, Any]:
        """Register only provenance explicitly present in a validated Phase-7A packet.

        The packet snapshot is recorded as a known common source, but claim nodes
        remain ``lineage_complete=False`` because Phase-7A does not itself prove
        the full upstream feature/raw-data chain.
        """
        validated = validate_input_packet(packet)
        snapshot_id = str(validated["source_snapshot_id"])
        as_of = str(validated["as_of"])
        snapshot_version = as_of
        self._ensure_node(
            record={
                "node_id": snapshot_id,
                "version_id": snapshot_version,
                "node_type": "RAW_SOURCE",
                "content_hash": content_hash({"source_snapshot_id": snapshot_id, "as_of": as_of}),
                "lineage_complete": True,
                "as_of": as_of,
                "metadata": {"source": "phase7a_packet_snapshot", "schema_version": validated["schema_version"]},
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
        claims: list[dict[str, str]] = []
        rows = validated.get("evidence", [])
        assert isinstance(rows, list)
        for row in rows:
            assert isinstance(row, Mapping)
            claim_id = str(row["claim_id"])
            version_id = str(row["source_version"])
            self._ensure_node(
                record={
                    "node_id": claim_id,
                    "version_id": version_id,
                    "node_type": "CLAIM",
                    "content_hash": content_hash(dict(row)),
                    "lineage_complete": False,
                    "as_of": str(row["as_of"]),
                    "metadata": {
                        "family": row["family"],
                        "integration_mode": row["integration_mode"],
                        "coverage_state": row["coverage_state"],
                        "maturity_state": row["maturity_state"],
                        "pit_state": row["pit_state"],
                    },
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
            self._ensure_edge(
                record={
                    "edge_id": "qm-i:" + content_hash([snapshot_id, snapshot_version, claim_id, version_id, "PRODUCES"])[:24],
                    "from_node_id": snapshot_id,
                    "from_version_id": snapshot_version,
                    "to_node_id": claim_id,
                    "to_version_id": version_id,
                    "relation": "PRODUCES",
                    "material_for_ancestry": True,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
            claims.append({"node_id": claim_id, "version_id": version_id})

        claims_by_id = {str(row["claim_id"]): row for row in rows if isinstance(row, Mapping)}
        for row in rows:
            assert isinstance(row, Mapping)
            claim_ref = str(row.get("claim_ref") or "").strip()
            if not claim_ref:
                continue
            referenced = claims_by_id.get(claim_ref)
            if referenced is None:
                raise LineageError(f"phase7_claim_ref_missing_after_validation:{claim_ref}")
            source_version = str(referenced["source_version"])
            target_id = str(row["claim_id"])
            target_version = str(row["source_version"])
            self._ensure_edge(
                record={
                    "edge_id": "qm-i:" + content_hash([claim_ref, source_version, target_id, target_version, "REFERENCES"])[:24],
                    "from_node_id": claim_ref,
                    "from_version_id": source_version,
                    "to_node_id": target_id,
                    "to_version_id": target_version,
                    "relation": "REFERENCES",
                    "material_for_ancestry": True,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
        return {
            "source_snapshot": {"node_id": snapshot_id, "version_id": snapshot_version},
            "claims": claims,
        }

    def register_phase7_stance(
        self,
        stance: Mapping[str, Any],
        *,
        decision_id: str,
        version_id: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        """Register a validated 7D stance using an explicitly supplied stable Decision ID.

        Phase 7D has no native stable decision ID, so this function refuses to
        invent one from historical content. The caller must provide it.
        """
        validated = validate_universal_stance(stance)
        decision_id = _nonblank(decision_id, field="decision_id")
        version_id = _nonblank(version_id, field="decision.version_id")
        self._ensure_node(
            record={
                "node_id": decision_id,
                "version_id": version_id,
                "node_type": "DECISION",
                "content_hash": content_hash(validated),
                "lineage_complete": False,
                "as_of": str(validated["as_of"]),
                "metadata": {
                    "phase": "7D",
                    "schema_version": validated["schema_version"],
                    "source_snapshot_id": validated["source_snapshot_id"],
                    "research_only": True,
                },
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
        structure = validated.get("evidence_structure")
        claim_ids = structure.get("known_directional_claim_ids", []) if isinstance(structure, Mapping) else []
        for claim_id in claim_ids:
            claim_id = str(claim_id)
            _, state = self._load_and_validate()
            versions = [node for node in state["nodes"].values() if node["node_id"] == claim_id and node["node_type"] == "CLAIM"]
            if len(versions) != 1:
                raise LineageError(f"decision_claim_version_not_unambiguous:{claim_id}")
            claim = versions[0]
            self._ensure_edge(
                record={
                    "edge_id": "qm-i:" + content_hash([claim_id, claim["version_id"], decision_id, version_id, "INFORMS"])[:24],
                    "from_node_id": claim_id,
                    "from_version_id": claim["version_id"],
                    "to_node_id": decision_id,
                    "to_version_id": version_id,
                    "relation": "INFORMS",
                    "material_for_ancestry": True,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
        return self.get_node(decision_id, version_id)

    def register_phase7_portfolio_action(
        self,
        action: Mapping[str, Any],
        *,
        action_id: str,
        version_id: str,
        decision_id: str,
        decision_version_id: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        validated = validate_portfolio_action(action)
        self.get_node(decision_id, decision_version_id)
        self._ensure_node(
            record={
                "node_id": _nonblank(action_id, field="portfolio_action_id"),
                "version_id": _nonblank(version_id, field="portfolio_action.version_id"),
                "node_type": "PORTFOLIO_ACTION",
                "content_hash": content_hash(validated),
                "lineage_complete": False,
                "as_of": str(validated["as_of"]),
                "metadata": {"phase": "7F", "schema_version": validated["schema_version"], "research_only": True},
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
        self._ensure_edge(
            record={
                "edge_id": "qm-i:" + content_hash([decision_id, decision_version_id, action_id, version_id, "INFORMS"])[:24],
                "from_node_id": decision_id,
                "from_version_id": decision_version_id,
                "to_node_id": action_id,
                "to_version_id": version_id,
                "relation": "INFORMS",
                "material_for_ancestry": True,
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
        return self.get_node(action_id, version_id)

    def register_phase7_watch(
        self,
        watch: Mapping[str, Any],
        *,
        parent_nodes: Sequence[Mapping[str, str]],
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        validated = validate_depot_watch(watch)
        watch_id = _nonblank(validated.get("watch_id"), field="watch_id")
        version_id = str(validated["schema_version"])
        self._ensure_node(
            record={
                "node_id": watch_id,
                "version_id": version_id,
                "node_type": "WATCH",
                "content_hash": content_hash(validated),
                "lineage_complete": False,
                "as_of": str(validated["as_of"]),
                "metadata": {
                    "phase": "7H",
                    "schema_version": validated["schema_version"],
                    "source_snapshot_id": validated["source_snapshot_id"],
                    "research_only": True,
                },
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
        for index, parent in enumerate(parent_nodes):
            if not isinstance(parent, Mapping):
                raise LineageError(f"watch_parent_must_be_object:{index}")
            parent_id = _nonblank(parent.get("node_id"), field=f"watch_parent[{index}].node_id")
            parent_version = _nonblank(parent.get("version_id"), field=f"watch_parent[{index}].version_id")
            self.get_node(parent_id, parent_version)
            self._ensure_edge(
                record={
                    "edge_id": "qm-i:" + content_hash([parent_id, parent_version, watch_id, version_id, "PRESENTS"])[:24],
                    "from_node_id": parent_id,
                    "from_version_id": parent_version,
                    "to_node_id": watch_id,
                    "to_version_id": version_id,
                    "relation": "PRESENTS",
                    "material_for_ancestry": True,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
        return self.get_node(watch_id, version_id)

    def register_qm_c_result_chain(
        self,
        *,
        result_id: str,
        result_version: str,
        hypothesis_registry: HypothesisRegistry,
        analysis_plan_registry: AnalysisPlanRegistry,
        control_registry: MultiplicityMonitoringRegistry,
        result_registry: ResultRegistry,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        """Import the exact stable QM-C identity chain without re-keying it."""
        result = result_registry.get_result(result_id, result_version)
        hypothesis = hypothesis_registry.get_hypothesis(result["hypothesis_id"], result["hypothesis_version"])
        if hypothesis["hypothesis_version_hash"] != result["hypothesis_version_hash"]:
            raise LineageError("qm_c_hypothesis_hash_mismatch")
        chain: list[tuple[str, str, str, str, dict[str, Any]]] = [
            (
                hypothesis["hypothesis_id"], hypothesis["hypothesis_version"], "HYPOTHESIS",
                hypothesis["hypothesis_version_hash"],
                {"hypothesis_family_id": hypothesis["hypothesis_family_id"], "research_mode": hypothesis["research_mode"]},
            )
        ]
        if result["analysis_plan_id"] is not None:
            plan = analysis_plan_registry.get_plan(result["analysis_plan_id"], result["analysis_plan_version"])
            if plan["analysis_plan_hash"] != result["analysis_plan_hash"]:
                raise LineageError("qm_c_analysis_plan_hash_mismatch")
            chain.append((plan["analysis_plan_id"], plan["analysis_plan_version"], "ANALYSIS_PLAN", plan["analysis_plan_hash"], {}))
        if result["control_plan_id"] is not None:
            control = control_registry.get_control_plan(result["control_plan_id"], result["control_plan_version"])
            if control["control_plan_hash"] != result["control_plan_hash"]:
                raise LineageError("qm_c_control_plan_hash_mismatch")
            chain.append((control["control_plan_id"], control["control_plan_version"], "CONTROL_PLAN", control["control_plan_hash"], {}))
        chain.append((result["result_id"], result["result_version"], "RESULT", result["result_hash"], {"outcome_classification": result["outcome_classification"], "evidence_scope": result["evidence_scope"]}))

        refs: list[dict[str, str]] = []
        for node_id, version, node_type, identity_hash, metadata in chain:
            self._ensure_node(
                record={
                    "node_id": node_id,
                    "version_id": version,
                    "node_type": node_type,
                    "content_hash": identity_hash,
                    "lineage_complete": False,
                    "as_of": result.get("registered_at"),
                    "metadata": metadata,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
            refs.append({"node_id": node_id, "version_id": version})
        for parent, child in zip(refs, refs[1:]):
            self._ensure_edge(
                record={
                    "edge_id": "qm-i:" + content_hash([parent, child, "REFERENCES"])[:24],
                    "from_node_id": parent["node_id"],
                    "from_version_id": parent["version_id"],
                    "to_node_id": child["node_id"],
                    "to_version_id": child["version_id"],
                    "relation": "REFERENCES",
                    "material_for_ancestry": True,
                },
                actor_id=actor_id,
                actor_role=actor_role,
            )
        return {"nodes": refs, "result": refs[-1]}
