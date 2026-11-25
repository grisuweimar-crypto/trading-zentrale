"""Lossless v2 capacity transport, isolated until all consumers are migrated.

Split at packet boundaries rather than delete history or raise the 2 MB cap.
A v2 manifest has a distinct schema; old readers must fail closed.
"""
from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path
import re

from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.watch_runtime import (
    MANIFEST_SCHEMA_VERSION, SHARD_SCHEMA_VERSION, _canonical_sha256,
    validate_runtime_manifest,
)

SCHEMA="decision_watch_runtime_manifest_v2"
PART_SCHEMA="decision_watch_runtime_packet_part_v2"
TARGET=1_400_000
HARD_LIMIT=2_000_000
FILENAME=re.compile(r"part_[0-9]{6}\.json")


def encode(doc):
    return (json.dumps(doc,sort_keys=True,separators=(",",":"),ensure_ascii=False,
                       allow_nan=False)+"\n").encode("utf-8")


def convert(manifest, shards, *, target=TARGET):
    source=validate_runtime_manifest(manifest)
    if not 1000<=target<=1_600_000:
        raise ValueError("v2:invalid_target")
    if len(shards)!=source["shard_count"]:
        raise ValueError("v2:source_shard_count")
    if _canonical_sha256({k:_canonical_sha256(v) for k,v in shards.items()})!=source["runtime_projection_sha256"]:
        raise ValueError("v2:source_projection_hash")
    histories=defaultdict(list)
    for shard_id,shard in sorted(shards.items()):
        if (shard.get("schema_version")!=SHARD_SCHEMA_VERSION or
            shard.get("snapshot_id")!=source["snapshot_id"] or
            shard.get("shard_id")!=shard_id or shard.get("private_position_data_included") is not False):
            raise ValueError("v2:source_shard_identity")
        if shard.get("packet_count")!=len(shard["packets"]):
            raise ValueError("v2:source_packet_count")
        if set(shard["symbols"])!={p["symbol"] for p in shard["packets"]}:
            raise ValueError("v2:source_symbols")
        for p in shard["packets"]:
            validate_input_packet(p)
            if source["symbol_shards"].get(p["symbol"])!=f"shard_{shard_id}.json":
                raise ValueError("v2:source_routing")
            histories[p["symbol"]].append(p)
    if set(histories)!=set(source["symbol_shards"]) or sum(map(len,histories.values()))!=source["packet_count"]:
        raise ValueError("v2:incomplete_source")
    parts={}
    routes={}
    def part_document(name,symbol,packets):
        return {"schema_version":PART_SCHEMA,"part":name,"symbol":symbol,
                "snapshot_id":source["snapshot_id"],"packets":packets,
                "packet_count":len(packets),"private_position_data_included":False}
    for symbol, packets in sorted(histories.items()):
        if len({p["source_snapshot_id"] for p in packets})!=len(packets):
            raise ValueError("v2:duplicate_snapshot")
        if packets[-1]["source_snapshot_id"]!=source["snapshot_id"]:
            raise ValueError("v2:current_packet_not_latest")
        routes[symbol]=[]
        chunk=[]
        for packet in packets:
            name=f"part_{len(parts):06d}.json"
            if len(encode(part_document(name,symbol,chunk+[packet])))>target:
                if not chunk:
                    raise ValueError("v2:individual_packet_exceeds_target")
                parts[name]=part_document(name,symbol,chunk)
                routes[symbol].append(name)
                chunk=[]
            chunk.append(packet)
        if chunk:
            name=f"part_{len(parts):06d}.json"
            doc=part_document(name,symbol,chunk)
            if len(encode(doc))>target:
                raise ValueError("v2:individual_packet_exceeds_target")
            parts[name]=doc
            routes[symbol].append(name)
    v2={k:v for k,v in source.items() if k not in ("symbol_shards","shard_count","partition_strategy","runtime_projection_sha256")}
    v2.update(schema_version=SCHEMA,partition_strategy="lossless_packet_parts_v2",
              source_v1_projection_sha256=source["runtime_projection_sha256"],
              symbol_parts=routes,part_count=len(parts),
              part_sha256={k:sha256(encode(v)).hexdigest() for k,v in parts.items()},
              packet_history_sha256={s:sha256(encode(v)).hexdigest() for s,v in histories.items()},
              target_part_bytes=target)
    v2["runtime_projection_sha256"]=_canonical_sha256({k:v2["part_sha256"][k] for k in sorted(parts)})
    return v2,parts


def load_symbol(manifest, symbol, read):
    if manifest.get("schema_version")!=SCHEMA or manifest.get("decision_logic_changed") is not False or manifest.get("private_position_data_included") is not False:
        raise ValueError("v2:invalid_manifest")
    route=manifest["symbol_parts"][symbol]
    if not route or len(route)!=len(set(route)):
        raise ValueError("v2:invalid_route")
    result=[]
    for name in route:
        if not FILENAME.fullmatch(name):
            raise ValueError("v2:invalid_part_path")
        raw=read(name)
        if len(raw)>manifest["target_part_bytes"] or sha256(raw).hexdigest()!=manifest["part_sha256"][name]:
            raise ValueError("v2:part_hash_or_size")
        doc=json.loads(raw)
        if (doc.get("schema_version")!=PART_SCHEMA or doc.get("symbol")!=symbol or
            doc.get("part")!=name or doc.get("snapshot_id")!=manifest["snapshot_id"] or
            doc.get("private_position_data_included") is not False or
            doc.get("packet_count")!=len(doc["packets"])):
            raise ValueError("v2:part_identity")
        for p in doc["packets"]:
            validate_input_packet(p)
            if p["symbol"]!=symbol:
                raise ValueError("v2:packet_identity")
            result.append(p)
    if (not result or result[-1]["source_snapshot_id"]!=manifest["snapshot_id"] or
        len({p["source_snapshot_id"] for p in result})!=len(result) or
        sha256(encode(result)).hexdigest()!=manifest["packet_history_sha256"][symbol]):
        raise ValueError("v2:history_integrity")
    return result


def audit(root):
    directory=root/"artifacts/research/watch_runtime"
    manifest=json.loads((directory/"manifest.json").read_text())
    shards={}
    for p in sorted(directory.glob("shard_*.json")):
        if p.is_symlink():
            raise ValueError("v2:symlink")
        shards[p.stem.removeprefix("shard_")]=json.loads(p.read_text())
    v2,parts=convert(manifest,shards)
    count=0
    for symbol in v2["symbol_parts"]:
        restored=load_symbol(v2,symbol,lambda n:encode(parts[n]))
        old=[p for p in shards[manifest["symbol_shards"][symbol].removeprefix("shard_").removesuffix(".json")]["packets"] if p["symbol"]==symbol]
        if restored!=old:
            raise ValueError("v2:lossy_transport")
        count+=len(restored)
    if count!=manifest["packet_count"]:
        raise ValueError("v2:packet_count_mismatch")
    before=max(len(encode(x)) for x in shards.values())
    after=max(len(encode(x)) for x in parts.values())
    return {"schema_version":"watch_runtime_v2_capacity_preview_v1",
            "source_snapshot_id":manifest["snapshot_id"],"symbols":len(v2["symbol_parts"]),
            "packets_verified_lossless":count,"old_max_bytes":before,"new_max_bytes":after,
            "part_count":len(parts),"multi_part_symbols":sum(len(x)>1 for x in v2["symbol_parts"].values()),
            "new_headroom_ratio":(HARD_LIMIT-after)/HARD_LIMIT,
            "capacity_preview":"PASS" if after<=1_600_000 else "REVIEW_REQUIRED",
            "production_publication":False,"decision_logic_changed":False}


if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root",type=Path,default=Path("."))
    args=p.parse_args()
    print(json.dumps(audit(args.repo_root),indent=2))
