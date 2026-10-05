"""QM-I / BA-QM3 evidence lineage and double-counting governance.

Research-only, append-only provenance control. QM-I records stable upstream
identities and known ancestry. It never changes scanner weights, Decision-Layer
semantics, portfolio actions, execution, or QM-A evidence-consumption state.
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
from scanner.research.governance.qm_c_families_multiplicity import FamilyMultiplicityRegistry
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry
from scanner.research.governance.qm_c_sequential_monitoring import SequentialMonitoringRegistry

EVENT_SCHEMA_VERSION = "qm_i_lineage_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_i_evidence_lineage_v1.json"


class LineageError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise LineageError(f"value_required:{field}")
    return result


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def content_hash(value: Any) -> str:
    return _hash(value)


def load_qm_i_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LineageError(f"qm_i_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_i_evidence_lineage_v1":
        raise LineageError("qm_i_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise LineageError("qm_i_contract_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise LineageError("qm_i_execution_scope_invalid")
    return payload


class LineageRegistry:
    def __init__(self, path: str | Path, *, contract_path: str | Path | None = None) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = load_qm_i_contract(contract_path)
        self.event_types = set(self.contract["registry"]["event_types"])
        node = self.contract["node"]
        edge = self.contract["edge"]
        claim = self.contract["independence_claim"]
        self.node_types = set(node["node_types"])
        self.root_types = set(node["root_node_types"])
        self.node_fields = tuple(node["required_fields"])
        self.edge_relations = set(edge["relations"])
        self.ancestry_relations = set(edge["ancestry_relations"])
        self.edge_fields = tuple(edge["required_fields"])
        self.claim_statuses = set(claim["statuses"])
        self.claim_fields = tuple(claim["required_fields"])

    @staticmethod
    def _key(node_id: str, version_id: str) -> str:
        return f"{node_id}::{version_id}"

    @staticmethod
    def _pair(left: str, right: str) -> tuple[str, str]:
        values = sorted((left, right))
        return values[0], values[1]

    def _node(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("node_record_must_be_object")
        missing = [f for f in self.node_fields if f not in record]
        if missing:
            raise LineageError("node_fields_missing:" + ",".join(missing))
        node_type = _text(record.get("node_type"), "node_type").upper()
        if node_type not in self.node_types:
            raise LineageError(f"node_type_invalid:{node_type}")
        complete = record.get("lineage_complete")
        if not isinstance(complete, bool):
            raise LineageError("lineage_complete_must_be_boolean")
        metadata = record.get("metadata")
        if not isinstance(metadata, Mapping):
            raise LineageError("node_metadata_must_be_object")
        digest = _text(record.get("content_hash"), "content_hash").lower()
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise LineageError("content_hash_must_be_sha256_hex")
        return {
            "node_id": _text(record.get("node_id"), "node_id"),
            "version_id": _text(record.get("version_id"), "version_id"),
            "node_type": node_type,
            "content_hash": digest,
            "lineage_complete": complete,
            "as_of": _optional(record.get("as_of")),
            "metadata": json.loads(_json(dict(metadata))),
        }

    def _edge(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("edge_record_must_be_object")
        missing = [f for f in self.edge_fields if f not in record]
        if missing:
            raise LineageError("edge_fields_missing:" + ",".join(missing))
        relation = _text(record.get("relation"), "edge.relation").upper()
        if relation not in self.edge_relations:
            raise LineageError(f"edge_relation_invalid:{relation}")
        material = record.get("material_for_ancestry")
        if not isinstance(material, bool):
            raise LineageError("edge_material_for_ancestry_must_be_boolean")
        if material and relation not in self.ancestry_relations:
            raise LineageError(f"non_ancestry_relation_cannot_be_material:{relation}")
        return {
            "edge_id": _text(record.get("edge_id"), "edge_id"),
            "from_node_id": _text(record.get("from_node_id"), "edge.from_node_id"),
            "from_version_id": _text(record.get("from_version_id"), "edge.from_version_id"),
            "to_node_id": _text(record.get("to_node_id"), "edge.to_node_id"),
            "to_version_id": _text(record.get("to_version_id"), "edge.to_version_id"),
            "relation": relation,
            "material_for_ancestry": material,
        }

    def _claim(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise LineageError("independence_claim_must_be_object")
        missing = [f for f in self.claim_fields if f not in record]
        if missing:
            raise LineageError("independence_claim_fields_missing:" + ",".join(missing))
        status = _text(record.get("status"), "independence_claim.status").upper()
        if status not in self.claim_statuses:
            raise LineageError(f"independence_status_invalid:{status}")
        return {
            "independence_claim_id": _text(record.get("independence_claim_id"), "independence_claim_id"),
            "version_id": _text(record.get("version_id"), "independence_claim.version_id"),
            "left_node_id": _text(record.get("left_node_id"), "left_node_id"),
            "left_version_id": _text(record.get("left_version_id"), "left_version_id"),
            "right_node_id": _text(record.get("right_node_id"), "right_node_id"),
            "right_version_id": _text(record.get("right_version_id"), "right_version_id"),
            "status": status,
            "review_reference": _optional(record.get("review_reference")),
            "rationale": _text(record.get("rationale"), "independence_claim.rationale"),
        }

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows = []
        for number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LineageError(f"invalid_jsonl_line:{number}") from exc
            if not isinstance(value, dict):
                raise LineageError(f"lineage_registry_line_not_object:{number}")
            rows.append(value)
        return rows

    def _verify_chain(self, events: Sequence[Mapping[str, Any]]) -> None:
        previous = None
        for number, raw in enumerate(events, 1):
            event = dict(raw)
            if event.get("schema_version") != EVENT_SCHEMA_VERSION or event.get("sequence") != number:
                raise LineageError(f"lineage_event_schema_or_sequence_invalid:{number}")
            if event.get("previous_event_hash") != previous:
                raise LineageError(f"lineage_previous_hash_invalid:{number}")
            stored = str(event.get("entry_hash") or "")
            body = dict(event)
            body.pop("entry_hash", None)
            if not stored or stored != _hash(body):
                raise LineageError(f"lineage_entry_hash_invalid:{number}")
            previous = stored

    @staticmethod
    def _forward(edges: Mapping[str, Mapping[str, Any]]) -> dict[str, set[str]]:
        result: dict[str, set[str]] = {}
        for edge in edges.values():
            if edge["material_for_ancestry"]:
                source = f"{edge['from_node_id']}::{edge['from_version_id']}"
                target = f"{edge['to_node_id']}::{edge['to_version_id']}"
                result.setdefault(source, set()).add(target)
        return result

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
        latest_nodes: dict[str, str] = {}
        latest_claims: dict[str, str] = {}
        for raw in events:
            event = dict(raw)
            event_type = str(event.get("event_type") or "")
            if event_type not in self.event_types:
                raise LineageError(f"lineage_event_type_unknown:{event_type}")
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise LineageError("lineage_event_payload_must_be_object")
            if event_type == "NODE_REGISTERED":
                record = self._node(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                key = self._key(record["node_id"], record["version_id"])
                if key in nodes:
                    raise LineageError(f"lineage_node_version_already_registered:{key}")
                supersedes = payload.get("supersedes_version_id")
                if supersedes is not None:
                    supersedes = _text(supersedes, "node.supersedes_version_id")
                    if self._key(record["node_id"], supersedes) not in nodes or latest_nodes.get(record["node_id"]) != supersedes:
                        raise LineageError("lineage_node_successor_must_supersede_latest_version")
                elif record["node_id"] in latest_nodes:
                    raise LineageError("lineage_node_successor_requires_supersedes_reference")
                nodes[key] = {**record, "supersedes_version_id": supersedes, "registered_at": event["recorded_at"], "registered_by": event["actor_id"]}
                latest_nodes[record["node_id"]] = record["version_id"]
                continue
            if event_type == "EDGE_REGISTERED":
                record = self._edge(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
                if record["edge_id"] in edges:
                    raise LineageError(f"lineage_edge_already_registered:{record['edge_id']}")
                source = self._key(record["from_node_id"], record["from_version_id"])
                target = self._key(record["to_node_id"], record["to_version_id"])
                if source not in nodes or target not in nodes:
                    raise LineageError(f"lineage_edge_endpoint_missing:{source}->{target}")
                if source == target:
                    raise LineageError("lineage_self_edge_forbidden")
                if any(e["from_node_id"] == record["from_node_id"] and e["from_version_id"] == record["from_version_id"] and e["to_node_id"] == record["to_node_id"] and e["to_version_id"] == record["to_version_id"] and e["relation"] == record["relation"] for e in edges.values()):
                    raise LineageError("lineage_duplicate_semantic_edge")
                if record["material_for_ancestry"] and self._reachable(self._forward(edges), target, source):
                    raise LineageError(f"lineage_cycle_forbidden:{source}->{target}")
                edges[record["edge_id"]] = record
                continue
            record = self._claim(payload.get("record") if isinstance(payload.get("record"), Mapping) else {})
            key = self._key(record["independence_claim_id"], record["version_id"])
            if key in claims:
                raise LineageError(f"independence_claim_version_already_registered:{key}")
            left = self._key(record["left_node_id"], record["left_version_id"])
            right = self._key(record["right_node_id"], record["right_version_id"])
            if left not in nodes or right not in nodes:
                raise LineageError("independence_claim_endpoint_missing")
            supersedes = payload.get("supersedes_version_id")
            if supersedes is not None:
                supersedes = _text(supersedes, "independence_claim.supersedes_version_id")
                predecessor_key = self._key(record["independence_claim_id"], supersedes)
                predecessor = claims.get(predecessor_key)
                if predecessor is None or latest_claims.get(record["independence_claim_id"]) != supersedes:
                    raise LineageError("independence_claim_successor_must_supersede_latest_version")
                predecessor_left = self._key(predecessor["left_node_id"], predecessor["left_version_id"])
                predecessor_right = self._key(predecessor["right_node_id"], predecessor["right_version_id"])
                if self._pair(predecessor_left, predecessor_right) != self._pair(left, right):
                    raise LineageError("independence_claim_successor_must_preserve_endpoints")
            elif record["independence_claim_id"] in latest_claims:
                raise LineageError("independence_claim_successor_requires_supersedes_reference")
            claims[key] = {**record, "supersedes_version_id": supersedes, "registered_at": event["recorded_at"], "registered_by": event["actor_id"]}
            latest_claims[record["independence_claim_id"]] = record["version_id"]
        return {"nodes": nodes, "edges": edges, "independence_claims": claims}

    def _load(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        events = self._read()
        self._verify_chain(events)
        return events, self._replay(events)

    def _append(self, event_type: str, actor_id: str, actor_role: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            lock = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise LineageError(f"lineage_registry_lock_exists:{self.lock_path}") from exc
        try:
            events, _ = self._load()
            event = {"schema_version": EVENT_SCHEMA_VERSION, "sequence": len(events) + 1, "event_id": str(uuid4()), "event_type": event_type, "recorded_at": _now(), "actor_id": _text(actor_id, "actor_id"), "actor_role": _text(actor_role, "actor_role"), "payload": dict(payload), "previous_event_hash": events[-1]["entry_hash"] if events else None}
            event["entry_hash"] = _hash(event)
            self._verify_chain([*events, event])
            self._replay([*events, event])
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            try:
                os.write(fd, (_json(event) + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            return event
        finally:
            os.close(lock)
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def register_node(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str, supersedes_version_id: str | None = None) -> dict[str, Any]:
        return self._append("NODE_REGISTERED", actor_id, actor_role, {"record": self._node(record), "supersedes_version_id": supersedes_version_id})

    def register_edge(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        return self._append("EDGE_REGISTERED", actor_id, actor_role, {"record": self._edge(record)})

    def get_node(self, node_id: str, version_id: str) -> dict[str, Any]:
        _, state = self._load()
        key = self._key(node_id, version_id)
        if key not in state["nodes"]:
            raise LineageError(f"lineage_node_not_registered:{key}")
        return dict(state["nodes"][key])

    def _graphs(self, state: Mapping[str, Any]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        forward: dict[str, set[str]] = {}
        parents: dict[str, set[str]] = {}
        for edge in state["edges"].values():
            if not edge["material_for_ancestry"]:
                continue
            source = self._key(edge["from_node_id"], edge["from_version_id"])
            target = self._key(edge["to_node_id"], edge["to_version_id"])
            forward.setdefault(source, set()).add(target)
            parents.setdefault(target, set()).add(source)
        return forward, parents

    @staticmethod
    def _ancestors(parents: Mapping[str, set[str]], node: str) -> set[str]:
        found: set[str] = set()
        queue = deque(parents.get(node, set()))
        while queue:
            current = queue.popleft()
            if current in found:
                continue
            found.add(current)
            queue.extend(parents.get(current, set()) - found)
        return found

    @staticmethod
    def _path(parents: Mapping[str, set[str]], ancestor: str, target: str) -> list[str] | None:
        queue: deque[tuple[str, list[str]]] = deque([(target, [target])])
        seen: set[str] = set()
        while queue:
            current, backward = queue.popleft()
            if current == ancestor:
                return list(reversed(backward))
            if current in seen:
                continue
            seen.add(current)
            for parent in sorted(parents.get(current, set())):
                queue.append((parent, [*backward, parent]))
        return None

    def _complete(self, state: Mapping[str, Any], node: str) -> tuple[bool, list[str]]:
        _, parents = self._graphs(state)
        queue = deque([node])
        seen: set[str] = set()
        reasons: list[str] = []
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            record = state["nodes"][current]
            if not record["lineage_complete"]:
                reasons.append(f"lineage_incomplete:{current}")
            incoming = parents.get(current, set())
            if record["node_type"] not in self.root_types and not incoming:
                reasons.append(f"complete_nonroot_without_material_parent:{current}")
            queue.extend(incoming - seen)
        return not reasons, sorted(set(reasons))

    def analyze_ancestry(self, *, left_node_id: str, left_version_id: str, right_node_id: str, right_version_id: str) -> dict[str, Any]:
        _, state = self._load()
        left = self._key(left_node_id, left_version_id)
        right = self._key(right_node_id, right_version_id)
        if left not in state["nodes"] or right not in state["nodes"]:
            raise LineageError("ancestry_endpoint_missing")
        forward, parents = self._graphs(state)
        if left == right:
            return {"classification": "SAME_EVIDENCE", "left": left, "right": right, "common_ancestors": [left], "paths": [{"ancestor": left, "left_path": [left], "right_path": [right]}], "lineage_complete": False, "completeness_reasons": ["same_evidence_cannot_be_independent"]}
        lr = self._reachable(forward, left, right)
        rl = self._reachable(forward, right, left)
        lc, lreason = self._complete(state, left)
        rc, rreason = self._complete(state, right)
        if lr or rl:
            source, target = (left, right) if lr else (right, left)
            return {"classification": "DIRECT_DEPENDENCY", "left": left, "right": right, "direct_source": source, "direct_target": target, "direct_path": self._path(parents, source, target), "common_ancestors": [], "paths": [], "lineage_complete": lc and rc, "completeness_reasons": sorted(set(lreason + rreason))}
        common = sorted(self._ancestors(parents, left) & self._ancestors(parents, right))
        if common:
            return {"classification": "COMMON_ANCESTRY", "left": left, "right": right, "common_ancestors": common, "paths": [{"ancestor": ancestor, "left_path": self._path(parents, ancestor, left), "right_path": self._path(parents, ancestor, right)} for ancestor in common], "lineage_complete": lc and rc, "completeness_reasons": sorted(set(lreason + rreason))}
        if not lc or not rc:
            return {"classification": "UNKNOWN_INCOMPLETE_LINEAGE", "left": left, "right": right, "common_ancestors": [], "paths": [], "lineage_complete": False, "completeness_reasons": sorted(set(lreason + rreason))}
        return {"classification": "NO_COMMON_ANCESTRY_DETECTED", "left": left, "right": right, "common_ancestors": [], "paths": [], "lineage_complete": True, "completeness_reasons": []}

    def register_independence_claim(self, *, record: Mapping[str, Any], actor_id: str, actor_role: str, supersedes_version_id: str | None = None) -> dict[str, Any]:
        claim = self._claim(record)
        analysis = self.analyze_ancestry(left_node_id=claim["left_node_id"], left_version_id=claim["left_version_id"], right_node_id=claim["right_node_id"], right_version_id=claim["right_version_id"])
        classification = analysis["classification"]
        if claim["status"] == "INDEPENDENT_SUPPORTED":
            if not claim["review_reference"]:
                raise LineageError("independent_supported_requires_review_reference")
            if classification != "NO_COMMON_ANCESTRY_DETECTED" or not analysis["lineage_complete"]:
                raise LineageError(f"independent_supported_contradicted_by_ancestry:{classification}")
        elif claim["status"] == "DEPENDENT_COMMON_ANCESTRY" and classification != "COMMON_ANCESTRY":
            raise LineageError(f"dependent_common_ancestry_status_mismatch:{classification}")
        elif claim["status"] == "DEPENDENT_DIRECT_REFERENCE" and classification not in {"DIRECT_DEPENDENCY", "SAME_EVIDENCE"}:
            raise LineageError(f"dependent_direct_status_mismatch:{classification}")
        return self._append("INDEPENDENCE_CLAIM_REGISTERED", actor_id, actor_role, {"record": claim, "ancestry_at_registration": analysis, "supersedes_version_id": supersedes_version_id})

    def _latest_claim(self, state: Mapping[str, Any], left: str, right: str) -> dict[str, Any] | None:
        pair = self._pair(left, right)
        candidates = []
        for claim in state["independence_claims"].values():
            cl = self._key(claim["left_node_id"], claim["left_version_id"])
            cr = self._key(claim["right_node_id"], claim["right_version_id"])
            if self._pair(cl, cr) == pair:
                candidates.append(claim)
        return None if not candidates else sorted(candidates, key=lambda row: row["registered_at"])[-1]

    def double_counting_review(self, *, combination_id: str, evidence_nodes: Sequence[Mapping[str, str]], purports_independent: bool) -> dict[str, Any]:
        combination_id = _text(combination_id, "combination_id")
        if len(evidence_nodes) < 2:
            raise LineageError("double_counting_review_requires_at_least_two_nodes")
        _, state = self._load()
        refs = []
        seen = set()
        for index, ref in enumerate(evidence_nodes):
            node_id = _text(ref.get("node_id"), f"evidence_nodes[{index}].node_id")
            version_id = _text(ref.get("version_id"), f"evidence_nodes[{index}].version_id")
            key = self._key(node_id, version_id)
            if key not in state["nodes"]:
                raise LineageError(f"double_counting_node_missing:{key}")
            if key in seen:
                raise LineageError(f"double_counting_duplicate_node:{key}")
            seen.add(key)
            refs.append((node_id, version_id, key))
        pair_reviews = []
        triggers = []
        for i in range(len(refs)):
            for j in range(i + 1, len(refs)):
                left_id, left_ver, left_key = refs[i]
                right_id, right_ver, right_key = refs[j]
                analysis = self.analyze_ancestry(left_node_id=left_id, left_version_id=left_ver, right_node_id=right_id, right_version_id=right_ver)
                claim = self._latest_claim(state, left_key, right_key)
                classification = analysis["classification"]
                trigger = None
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
                pair_reviews.append({"left": left_key, "right": right_key, "ancestry": analysis, "latest_independence_claim": None if claim is None else {"independence_claim_id": claim["independence_claim_id"], "version_id": claim["version_id"], "status": claim["status"], "review_reference": claim["review_reference"]}, "trigger": trigger})
                if trigger:
                    triggers.append({"trigger": trigger, "left": left_key, "right": right_key, "classification": classification, "common_ancestors": analysis.get("common_ancestors", [])})
        return {"schema_version": "qm_i_double_counting_review_v1", "combination_id": combination_id, "purports_independent": bool(purports_independent), "status": "REVIEW_REQUIRED" if triggers else "CLEAR", "pair_reviews": pair_reviews, "triggers": triggers, "common_ancestry_is_automatic_error": False, "automatic_weight_change_performed": False, "scanner_or_decision_semantics_changed": False}

    def verify_integrity(self) -> dict[str, Any]:
        events, state = self._load()
        stale = []
        for claim in state["independence_claims"].values():
            if claim["status"] != "INDEPENDENT_SUPPORTED":
                continue
            current = self.analyze_ancestry(left_node_id=claim["left_node_id"], left_version_id=claim["left_version_id"], right_node_id=claim["right_node_id"], right_version_id=claim["right_version_id"])
            if current["classification"] != "NO_COMMON_ANCESTRY_DETECTED" or not current["lineage_complete"]:
                stale.append({"independence_claim_id": claim["independence_claim_id"], "version_id": claim["version_id"], "current_classification": current["classification"]})
        return {"schema_version": "qm_i_lineage_verification_v1", "valid": True, "event_count": len(events), "node_version_count": len(state["nodes"]), "edge_count": len(state["edges"]), "independence_claim_version_count": len(state["independence_claims"]), "stale_independence_claims": stale, "head_hash": events[-1]["entry_hash"] if events else None}

    def _ensure_node(self, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        normalized = self._node(record)
        _, state = self._load()
        key = self._key(normalized["node_id"], normalized["version_id"])
        existing = state["nodes"].get(key)
        if existing is None:
            self.register_node(record=normalized, actor_id=actor_id, actor_role=actor_role)
            return self.get_node(normalized["node_id"], normalized["version_id"])
        if {field: existing[field] for field in self.node_fields} != normalized:
            raise LineageError(f"existing_lineage_node_conflicts:{key}")
        return dict(existing)

    def _ensure_edge(self, record: Mapping[str, Any], actor_id: str, actor_role: str) -> dict[str, Any]:
        normalized = self._edge(record)
        _, state = self._load()
        existing = state["edges"].get(normalized["edge_id"])
        if existing is not None:
            if existing != normalized:
                raise LineageError(f"existing_lineage_edge_conflicts:{normalized['edge_id']}")
            return dict(existing)
        self.register_edge(record=normalized, actor_id=actor_id, actor_role=actor_role)
        _, state = self._load()
        return dict(state["edges"][normalized["edge_id"]])

    def register_phase7_packet(self, packet: Mapping[str, Any], *, actor_id: str, actor_role: str) -> dict[str, Any]:
        validated = validate_input_packet(packet)
        snapshot_id = str(validated["source_snapshot_id"])
        as_of = str(validated["as_of"])
        snapshot_version = as_of
        self._ensure_node({"node_id": snapshot_id, "version_id": snapshot_version, "node_type": "RAW_SOURCE", "content_hash": content_hash({"source_snapshot_id": snapshot_id, "as_of": as_of}), "lineage_complete": True, "as_of": as_of, "metadata": {"source": "phase7a_packet_snapshot", "schema_version": validated["schema_version"]}}, actor_id, actor_role)
        rows = validated.get("evidence", [])
        claims = []
        for row in rows:
            claim_id = str(row["claim_id"])
            version_id = str(row["source_version"])
            self._ensure_node({"node_id": claim_id, "version_id": version_id, "node_type": "CLAIM", "content_hash": content_hash(dict(row)), "lineage_complete": False, "as_of": str(row["as_of"]), "metadata": {"family": row["family"], "integration_mode": row["integration_mode"], "coverage_state": row["coverage_state"], "maturity_state": row["maturity_state"], "pit_state": row["pit_state"]}}, actor_id, actor_role)
            self._ensure_edge({"edge_id": "qm-i:" + content_hash([snapshot_id, snapshot_version, claim_id, version_id, "PRODUCES"])[:24], "from_node_id": snapshot_id, "from_version_id": snapshot_version, "to_node_id": claim_id, "to_version_id": version_id, "relation": "PRODUCES", "material_for_ancestry": True}, actor_id, actor_role)
            claims.append({"node_id": claim_id, "version_id": version_id})
        by_id = {str(row["claim_id"]): row for row in rows}
        for row in rows:
            claim_ref = str(row.get("claim_ref") or "").strip()
            if not claim_ref:
                continue
            referenced = by_id.get(claim_ref)
            if referenced is None:
                raise LineageError(f"phase7_claim_ref_missing_after_validation:{claim_ref}")
            self._ensure_edge({"edge_id": "qm-i:" + content_hash([claim_ref, str(referenced["source_version"]), str(row["claim_id"]), str(row["source_version"]), "REFERENCES"])[:24], "from_node_id": claim_ref, "from_version_id": str(referenced["source_version"]), "to_node_id": str(row["claim_id"]), "to_version_id": str(row["source_version"]), "relation": "REFERENCES", "material_for_ancestry": True}, actor_id, actor_role)
        return {"source_snapshot": {"node_id": snapshot_id, "version_id": snapshot_version}, "claims": claims}

    def register_phase7_stance(self, stance: Mapping[str, Any], *, decision_id: str, version_id: str, actor_id: str, actor_role: str) -> dict[str, Any]:
        validated = validate_universal_stance(stance)
        decision_id = _text(decision_id, "decision_id")
        version_id = _text(version_id, "decision.version_id")
        self._ensure_node({"node_id": decision_id, "version_id": version_id, "node_type": "DECISION", "content_hash": content_hash(validated), "lineage_complete": False, "as_of": str(validated["as_of"]), "metadata": {"phase": "7D", "schema_version": validated["schema_version"], "source_snapshot_id": validated["source_snapshot_id"], "research_only": True}}, actor_id, actor_role)
        structure = validated.get("evidence_structure")
        for claim_id in structure.get("known_directional_claim_ids", []) if isinstance(structure, Mapping) else []:
            _, state = self._load()
            versions = [node for node in state["nodes"].values() if node["node_id"] == str(claim_id) and node["node_type"] == "CLAIM"]
            if len(versions) != 1:
                raise LineageError(f"decision_claim_version_not_unambiguous:{claim_id}")
            claim = versions[0]
            self._ensure_edge({"edge_id": "qm-i:" + content_hash([str(claim_id), claim["version_id"], decision_id, version_id, "INFORMS"])[:24], "from_node_id": str(claim_id), "from_version_id": claim["version_id"], "to_node_id": decision_id, "to_version_id": version_id, "relation": "INFORMS", "material_for_ancestry": True}, actor_id, actor_role)
        return self.get_node(decision_id, version_id)

    def register_phase7_portfolio_action(self, action: Mapping[str, Any], *, action_id: str, version_id: str, decision_id: str, decision_version_id: str, actor_id: str, actor_role: str) -> dict[str, Any]:
        validated = validate_portfolio_action(action)
        self.get_node(decision_id, decision_version_id)
        action_id = _text(action_id, "portfolio_action_id")
        version_id = _text(version_id, "portfolio_action.version_id")
        position = validated.get("position_context")
        cost = validated.get("cost_context")
        if not isinstance(position, Mapping):
            raise LineageError("phase7_position_context_missing")
        position_id = _text(position.get("source_snapshot_id"), "position_source_snapshot_id")
        position_version = _text(position.get("as_of"), "position_snapshot.version_id")
        position_material = {
            "symbol": validated.get("symbol"),
            "position_context": dict(position),
            "transaction_cost_bps": cost.get("transaction_cost_bps") if isinstance(cost, Mapping) else None,
        }
        self._ensure_node({"node_id": position_id, "version_id": position_version, "node_type": "POSITION_SNAPSHOT", "content_hash": content_hash(position_material), "lineage_complete": True, "as_of": position_version, "metadata": {"phase": "7F", "source": "position_context", "research_only": True}}, actor_id, actor_role)
        state_history = validated.get("state_history_context")
        depot_policy = validated.get("depot_action_policy")
        w8_evaluated = isinstance(depot_policy, Mapping)
        self._ensure_node({"node_id": action_id, "version_id": version_id, "node_type": "PORTFOLIO_ACTION", "content_hash": content_hash(validated), "lineage_complete": False, "as_of": str(validated["as_of"]), "metadata": {"phase": "7F/W8" if w8_evaluated else "7F", "schema_version": validated["schema_version"], "research_only": True, "w8_action_policy_evaluated": w8_evaluated}}, actor_id, actor_role)
        self._ensure_edge({"edge_id": "qm-i:" + content_hash([decision_id, decision_version_id, action_id, version_id, "INFORMS"])[:24], "from_node_id": decision_id, "from_version_id": decision_version_id, "to_node_id": action_id, "to_version_id": version_id, "relation": "INFORMS", "material_for_ancestry": True}, actor_id, actor_role)
        self._ensure_edge({"edge_id": "qm-i:" + content_hash([position_id, position_version, action_id, version_id, "USES_POSITION"])[:24], "from_node_id": position_id, "from_version_id": position_version, "to_node_id": action_id, "to_version_id": version_id, "relation": "USES_POSITION", "material_for_ancestry": True}, actor_id, actor_role)

        # BA-QM11/W6 closure: Elliott review context is a material immediate
        # parent whenever 7F consumed it.  New prospective 6H captures carry
        # exact source hashes and output identities, allowing QM-I to bind the
        # review context to raw market/daily inputs.  Older/manual W6 sources
        # without those fields remain explicitly lineage-incomplete; no
        # historical provenance is guessed.
        swing = validated.get("swing_management")
        if isinstance(swing, Mapping) and swing.get("source") is not None:
            context_id = _text(
                swing.get("source_output_id"),
                "swing_management.source_output_id",
            )
            source_name = _text(
                swing.get("source"),
                "swing_management.source",
            )
            w6_meta = swing.get("w6")
            source_provenance = (
                w6_meta.get("source_provenance")
                if isinstance(w6_meta, Mapping)
                else None
            )
            source_hashes = (
                source_provenance.get("source_hashes")
                if isinstance(source_provenance, Mapping)
                else None
            )
            market_hash = (
                str(source_hashes.get("market_ohlcv_sha256") or "").lower()
                if isinstance(source_hashes, Mapping)
                else ""
            )
            daily_hash = (
                str(source_hashes.get("daily_research_sha256") or "").lower()
                if isinstance(source_hashes, Mapping)
                else ""
            )
            source_capture_id = (
                str(source_provenance.get("source_capture_id") or "").strip()
                if isinstance(source_provenance, Mapping)
                else ""
            )
            snapshot_id = (
                str(source_provenance.get("snapshot_id") or "").strip()
                if isinstance(source_provenance, Mapping)
                else ""
            )
            output_ids = (
                [str(value).strip() for value in w6_meta.get("source_output_ids") or []]
                if isinstance(w6_meta, Mapping)
                else []
            )
            sha256_hex = lambda value: (
                len(value) == 64
                and all(char in "0123456789abcdef" for char in value)
            )
            provenance_complete = bool(
                source_capture_id
                and snapshot_id
                and sha256_hex(market_hash)
                and sha256_hex(daily_hash)
                and output_ids
                and all(sha256_hex(value.lower()) for value in output_ids)
            )
            context_version = (
                f"{source_name}:capture:{source_capture_id}"
                if provenance_complete
                else source_name
            )
            context_material = {
                "symbol": validated.get("symbol"),
                "source": source_name,
                "source_output_id": context_id,
                "review_contexts": list(swing.get("review_contexts") or []),
                "context_conflict": swing.get("context_conflict"),
            }

            if provenance_complete:
                source_as_of = str(validated["as_of"])
                market_node_id = f"elliott:market_ohlcv:{market_hash}"
                daily_node_id = f"elliott:daily_research:{snapshot_id}"
                self._ensure_node({
                    "node_id": market_node_id,
                    "version_id": market_hash,
                    "node_type": "RAW_SOURCE",
                    "content_hash": market_hash,
                    "lineage_complete": True,
                    "as_of": source_as_of,
                    "metadata": {
                        "phase": "6A-6H",
                        "source": "artifacts/market_data/yahoo_ohlcv.csv",
                        "source_capture_id": source_capture_id,
                        "research_only": True,
                    },
                }, actor_id, actor_role)
                self._ensure_node({
                    "node_id": daily_node_id,
                    "version_id": daily_hash,
                    "node_type": "RAW_SOURCE",
                    "content_hash": daily_hash,
                    "lineage_complete": True,
                    "as_of": source_as_of,
                    "metadata": {
                        "phase": "6H prospective capture",
                        "source": "daily_research",
                        "snapshot_id": snapshot_id,
                        "source_capture_id": source_capture_id,
                        "research_only": True,
                    },
                }, actor_id, actor_role)

                timeframe_rows = {
                    str(row.get("output_id") or ""): dict(row)
                    for row in (w6_meta.get("timeframe_degrees") or [])
                    if isinstance(row, Mapping)
                } if isinstance(w6_meta, Mapping) else {}
                for output_id in output_ids:
                    output_id = output_id.lower()
                    row = timeframe_rows.get(output_id, {})
                    self._ensure_node({
                        "node_id": output_id,
                        "version_id": "elliott_vnext_output_v2",
                        "node_type": "INDICATOR",
                        "content_hash": output_id,
                        "lineage_complete": True,
                        "as_of": source_as_of,
                        "metadata": {
                            "phase": "6H",
                            "module": "elliott_vnext",
                            "timeframe": row.get("timeframe"),
                            "degree": row.get("degree"),
                            "source_capture_id": source_capture_id,
                            "validation_source": (
                                dict(source_provenance.get("validation_source"))
                                if isinstance(source_provenance.get("validation_source"), Mapping)
                                else None
                            ),
                            "research_only": True,
                        },
                    }, actor_id, actor_role)
                    for raw_node_id, raw_version, marker in (
                        (market_node_id, market_hash, "MARKET_OHLCV"),
                        (daily_node_id, daily_hash, "DAILY_RESEARCH"),
                    ):
                        self._ensure_edge({
                            "edge_id": "qm-i:" + content_hash([
                                raw_node_id,
                                raw_version,
                                output_id,
                                "elliott_vnext_output_v2",
                                marker,
                                "PRODUCES",
                            ])[:24],
                            "from_node_id": raw_node_id,
                            "from_version_id": raw_version,
                            "to_node_id": output_id,
                            "to_version_id": "elliott_vnext_output_v2",
                            "relation": "PRODUCES",
                            "material_for_ancestry": True,
                        }, actor_id, actor_role)

            self._ensure_node({
                "node_id": context_id,
                "version_id": context_version,
                "node_type": "DECISION_CONTEXT",
                "content_hash": content_hash(context_material),
                "lineage_complete": provenance_complete,
                "as_of": str(validated["as_of"]),
                "metadata": {
                    "phase": "W6",
                    "source": source_name,
                    "context_type": "elliott_review_context",
                    "research_only": True,
                    "upstream_elliott_binding_complete": provenance_complete,
                    "source_capture_id": source_capture_id or None,
                    "snapshot_id": snapshot_id or None,
                },
            }, actor_id, actor_role)

            if provenance_complete:
                for output_id in output_ids:
                    output_id = output_id.lower()
                    self._ensure_edge({
                        "edge_id": "qm-i:" + content_hash([
                            output_id,
                            "elliott_vnext_output_v2",
                            context_id,
                            context_version,
                            "INFORMS_W6_CONTEXT",
                        ])[:24],
                        "from_node_id": output_id,
                        "from_version_id": "elliott_vnext_output_v2",
                        "to_node_id": context_id,
                        "to_version_id": context_version,
                        "relation": "INFORMS",
                        "material_for_ancestry": True,
                    }, actor_id, actor_role)

            self._ensure_edge({
                "edge_id": "qm-i:" + content_hash([
                    context_id,
                    context_version,
                    action_id,
                    version_id,
                    "INFORMS_W6_ACTION",
                ])[:24],
                "from_node_id": context_id,
                "from_version_id": context_version,
                "to_node_id": action_id,
                "to_version_id": version_id,
                "relation": "INFORMS",
                "material_for_ancestry": True,
            }, actor_id, actor_role)

        # BA-QM11 CAPA: W7/W8 path-state is a material parent whenever it is
        # attached to the final portfolio-action artifact.  Before this guard,
        # QM-I could register a W8-routed action using only 7D + position
        # parents, hiding the scanner-path claim that actually changed the
        # review state.
        if isinstance(state_history, Mapping):
            source_claim_id = _text(
                state_history.get("source_claim_id"),
                "state_history.source_claim_id",
            )
            _, state = self._load()
            claim_versions = [
                node
                for node in state["nodes"].values()
                if node["node_id"] == source_claim_id and node["node_type"] == "CLAIM"
            ]
            if len(claim_versions) != 1:
                raise LineageError(
                    f"w8_state_history_claim_version_not_unambiguous:{source_claim_id}"
                )
            source_claim = claim_versions[0]
            self._ensure_edge({
                "edge_id": "qm-i:" + content_hash([
                    source_claim_id,
                    source_claim["version_id"],
                    action_id,
                    version_id,
                    "INFORMS_W8_ACTION",
                ])[:24],
                "from_node_id": source_claim_id,
                "from_version_id": source_claim["version_id"],
                "to_node_id": action_id,
                "to_version_id": version_id,
                "relation": "INFORMS",
                "material_for_ancestry": True,
            }, actor_id, actor_role)
        elif (
            isinstance(depot_policy, Mapping)
            and depot_policy.get("action_changed") is True
        ):
            raise LineageError("w8_changed_action_requires_registered_state_history")

        return self.get_node(action_id, version_id)

    def register_phase7_watch(self, watch: Mapping[str, Any], *, parent_nodes: Sequence[Mapping[str, str]], actor_id: str, actor_role: str) -> dict[str, Any]:
        validated = validate_depot_watch(watch)
        watch_id = _text(validated.get("watch_id"), "watch_id")
        version_id = str(validated["schema_version"])
        self._ensure_node({"node_id": watch_id, "version_id": version_id, "node_type": "WATCH", "content_hash": content_hash(validated), "lineage_complete": False, "as_of": str(validated["as_of"]), "metadata": {"phase": "7H", "schema_version": validated["schema_version"], "source_snapshot_id": validated["source_snapshot_id"], "research_only": True}}, actor_id, actor_role)
        for index, parent in enumerate(parent_nodes):
            parent_id = _text(parent.get("node_id"), f"watch_parent[{index}].node_id")
            parent_version = _text(parent.get("version_id"), f"watch_parent[{index}].version_id")
            self.get_node(parent_id, parent_version)
            self._ensure_edge({"edge_id": "qm-i:" + content_hash([parent_id, parent_version, watch_id, version_id, "PRESENTS"])[:24], "from_node_id": parent_id, "from_version_id": parent_version, "to_node_id": watch_id, "to_version_id": version_id, "relation": "PRESENTS", "material_for_ancestry": True}, actor_id, actor_role)
        return self.get_node(watch_id, version_id)

    def register_qm_c_result_chain(self, *, result_id: str, result_version: str, hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, control_registry: FamilyMultiplicityRegistry, result_registry: NegativeResultRegistry, actor_id: str, actor_role: str, monitoring_registry: SequentialMonitoringRegistry | None = None) -> dict[str, Any]:
        result = result_registry.get_result(result_id, result_version)
        hypothesis = hypothesis_registry.get_hypothesis(result["hypothesis_id"], result["hypothesis_version"])
        if hypothesis["hypothesis_version_hash"] != result["hypothesis_version_hash"]:
            raise LineageError("qm_c_hypothesis_hash_mismatch")
        chain: list[tuple[str, str, str, str, dict[str, Any]]] = [(hypothesis["hypothesis_id"], hypothesis["hypothesis_version"], "HYPOTHESIS", hypothesis["hypothesis_version_hash"], {"hypothesis_family_id": hypothesis["hypothesis_family_id"], "research_mode": hypothesis["research_mode"]})]
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
        if result["monitoring_plan_id"] is not None:
            if monitoring_registry is None:
                raise LineageError("qm_c_monitoring_registry_required")
            monitoring = monitoring_registry.get_monitoring_plan(result["monitoring_plan_id"], result["monitoring_plan_version"])
            if monitoring["monitoring_plan_hash"] != result["monitoring_plan_hash"]:
                raise LineageError("qm_c_monitoring_plan_hash_mismatch")
            chain.append((monitoring["monitoring_plan_id"], monitoring["monitoring_plan_version"], "MONITORING_PLAN", monitoring["monitoring_plan_hash"], {}))
        chain.append((result["result_id"], result["result_version"], "RESULT", result["result_hash"], {"outcome_classification": result["outcome_classification"], "evidence_scope": result["evidence_scope"]}))
        refs = []
        for node_id, version, node_type, digest, metadata in chain:
            self._ensure_node({"node_id": node_id, "version_id": version, "node_type": node_type, "content_hash": digest, "lineage_complete": False, "as_of": result.get("registered_at"), "metadata": metadata}, actor_id, actor_role)
            refs.append({"node_id": node_id, "version_id": version})
        for parent, child in zip(refs, refs[1:]):
            self._ensure_edge({"edge_id": "qm-i:" + content_hash([parent, child, "REFERENCES"])[:24], "from_node_id": parent["node_id"], "from_version_id": parent["version_id"], "to_node_id": child["node_id"], "to_version_id": child["version_id"], "relation": "REFERENCES", "material_for_ancestry": True}, actor_id, actor_role)
        return {"nodes": refs, "result": refs[-1]}
