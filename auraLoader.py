#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import hashlib
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    print("Missing dependency: requests. Install with `pip install requests`.", file=sys.stderr)
    sys.exit(1)

try:
    from neo4j import GraphDatabase
except ImportError:
    print("Missing dependency: neo4j. Install with `pip install neo4j`.", file=sys.stderr)
    sys.exit(1)


@dataclass
class SourceFile:
    name: str
    path: str
    checksum_sha256: Optional[str] = None
    rows: Optional[int] = None

def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def is_url(p: str) -> bool:
    return p.startswith("http://") or p.startswith("https://")

def to_url(base_url: str, path: str) -> str:
    if is_url(path):
        return path
    return urljoin(base_url.rstrip('/') + '/', os.path.basename(path))

def sha256_of_url(url: str, chunk_size: int = 1024 * 1024) -> str:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        h = hashlib.sha256()
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                h.update(chunk)
        return h.hexdigest()

def count_rows_of_url(url: str) -> int:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        lines = 0
        for _ in r.iter_lines(decode_unicode=True):
            lines += 1
    return max(0, lines - 1)


class Neo:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_lifetime=3600)

    def close(self):
        self.driver.close()

    def run(self, cypher: str, params: Dict[str, Any] = None):
        params = params or {}
        with self.driver.session() as session:
            res = session.run(cypher, params)
            return [r.data() for r in res]

    def run_void(self, cypher: str, params: Dict[str, Any] = None) -> None:
        _ = self.run(cypher, params)


def build_constraint_cypher(runtime_indexes):
    stmts = []
    for idx in runtime_indexes:
        kind = idx.get("kind")
        if kind == "constraint":
            label = idx["label"]
            props = idx["properties"]
            if idx.get("unique", False):
                stmts.append(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{label}`) REQUIRE n.`{props[0]}` IS UNIQUE;")
            else:
                stmts.append(f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{props[0]}`);")
        elif kind == "rel_index":
            rtype = idx["type"]
            props = idx["properties"]
            stmts.append(f"CREATE INDEX IF NOT EXISTS FOR ()-[r:`{rtype}`]-() ON (r.`{props[0]}`);")
    return stmts

def type_cast(expr: str, spec):
    t = spec.get("type", "string")
    transforms = spec.get("transform", [])
    e = expr
    if "trim" in transforms:
        e = f"trim({e})"
    if "lower" in transforms:
        e = f"toLower({e})"
    if t == "int":
        e = f"toInteger({e})"
    elif t == "float":
        e = f"toFloat({e})"
    return e

def nullable_cast(expr: str, spec):
    base = type_cast(expr, spec)
    if spec.get("nullable", False):
        return f"CASE WHEN {expr} IS NULL OR {expr} = '' THEN NULL ELSE {base} END"
    return base

def build_node_load_cypher(node_spec, url_param: str, batch_id: str) -> str:
    label = node_spec["label"]
    mappings = node_spec["mappings"]
    key_fields = node_spec["key"]

    merge_on = ", ".join(
        f"{k}: {type_cast('row.' + mappings[k]['column'], mappings[k])}"
        for k in key_fields
    )

    setters = []
    for prop, spec in mappings.items():
        if prop in key_fields:
            setters.append(f"n.`{prop}` = {type_cast('row.' + spec['column'], spec)}")
        else:
            setters.append(f"n.`{prop}` = {nullable_cast('row.' + spec['column'], spec)}")
    setters.append("n.source_system = $source_system")
    setters.append("n.ingest_batch  = $ingest_batch")
    setters.append("n.ingested_at   = datetime()")

    cypher = (
        f"LOAD CSV WITH HEADERS FROM ${url_param} AS row\n"
        f"MERGE (n:`{label}` {{ {merge_on} }})\n"
        "SET " + ", ".join(setters)
    )
    return cypher


def build_rel_load_cypher(rel_spec, url_param: str, batch_id: str) -> str:
    rtype = rel_spec["type"]
    direction = rel_spec.get("direction", "OUT").upper()
    from_spec = rel_spec["from"]
    to_spec   = rel_spec["to"]
    props     = rel_spec.get("properties", {})

    def match_expr(role: str, spec):
        label = spec["label"]
        parts = []
        for k in spec["match_on"]:
            if ":" in k:
                src_col, target_prop = k.split(":")
            else:
                src_col, target_prop = k, k
            parts.append(f"`{target_prop}`: toInteger(row.`{src_col}`)")
        return f"(x_{role}:`{label}` {{ {', '.join(parts)} }})"

    pattern = f"(x_from)-[r:`{rtype}`]->(x_to)" if direction == "OUT" else f"(x_to)-[r:`{rtype}`]->(x_from)"

    setters = []
    for prop, spec in props.items():
        setters.append(f"r.`{prop}` = {nullable_cast('row.' + spec['column'], spec)}")
    setters.append("r.source_system = $source_system")
    setters.append("r.ingest_batch  = $ingest_batch")
    setters.append("r.ingested_at   = datetime()")

    cypher = (
        f"LOAD CSV WITH HEADERS FROM ${url_param} AS row\n"
        f"MATCH {match_expr('from', from_spec)}, {match_expr('to', to_spec)}\n"
        f"MERGE {pattern}\n"
        "SET " + ", ".join(setters)
    )
    return cypher

def promote_named_edges(neo, mapping):
    derived = mapping.get("derived_relationships", {}).get("promote_named_edges")
    if not derived:
        return
    for m in derived["map"]:
        lid = int(m["linkid"])
        rtype = m["type"]
        cypher = (
            "MATCH (a:synset)-[r:SYNSET {linkid:$lid}]->(b:synset)\n"
            f"MERGE (a)-[x:`{rtype}`]->(b)\n"
            "ON CREATE SET "
            "  x.source_system = coalesce(r.source_system, 'wordnet'), "
            "  x.ingest_batch  = coalesce(r.ingest_batch,  'derived'), "
            "  x.ingested_at   = datetime()"
        )
        neo.run_void(cypher, {"lid": lid})


def main():
    ap = argparse.ArgumentParser(description="Aura Loader for WordNet mapping spec")
    ap.add_argument("--aura-uri", required=True, help="bolt+s://<host>:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"))
    ap.add_argument("--mapping", required=True, help="Path to mapping JSON")
    ap.add_argument("--manifest", required=True, help="Path to manifest.json (with sha256 & rows)")
    ap.add_argument("--base-url", required=True, help="Base HTTPS URL where CSVs are hosted")
    ap.add_argument("--batch-id", default=None, help="Ingest batch id; default = mapping.version + timestamp")
    ap.add_argument("--verify-checksums", action="store_true")
    ap.add_argument("--verify-rowcounts", action="store_true")
    args = ap.parse_args()

    if not args.password:
        print("ERROR: Provide --password or set NEO4J_PASSWORD env var.", file=sys.stderr)
        sys.exit(2)

    mapping = read_json(args.mapping)
    manifest = read_json(args.manifest)

    mani_index = { f["name"]: f for f in manifest.get("files", []) }

    for s in mapping.get("sources", []):
        base = os.path.basename(s["path"])
        if base in mani_index:
            s["checksum_sha256"] = mani_index[base].get("sha256")
            s["rows"] = mani_index[base].get("rows")

    if not args.batch_id:
        version = mapping.get("version", "v")
        args.batch_id = f"{version}-{int(time.time())}"

    neo = Neo(args.aura_uri, args.user, args.password)

    # constraints/indexes
    for stmt in build_constraint_cypher(mapping.get("runtime", {}).get("indexes", [])):
        neo.run_void(stmt)

    # preflight
    if args.verify_checksums or args.verify_rowcounts:
        print("Preflight: verifying sources")
        for src in mapping.get("sources", []):
            url = to_url(args.base_url, src["path"])
            print(f"  {src['name']}: {url}")
            if args.verify_checksums and src.get("checksum_sha256"):
                h = sha256_of_url(url)
                if h != src["checksum_sha256"]:
                    print(f"    ERROR checksum mismatch (got {h}, expected {src['checksum_sha256']})", file=sys.stderr)
                    sys.exit(3)
                print("    checksum OK")
            if args.verify_rowcounts and src.get("rows") is not None:
                n = count_rows_of_url(url)
                if n != int(src["rows"]):
                    print(f"    ERROR rowcount mismatch (got {n}, expected {src['rows']})", file=sys.stderr)
                    sys.exit(4)
                print("    rowcount OK")

    params_common = {
        "source_system": mapping.get("ingest_batch", {}).get("attach_properties", {}).get("source_system", "wordnet"),
        "ingest_batch": args.batch_id,
    }

    load_order = mapping.get("load_order", [])
    nodes = mapping.get("nodes", {})
    rels  = mapping.get("relationships", {})

    def url_for_source_name(name: str) -> str:
        src = next((s for s in mapping.get("sources", []) if s["name"] == name), None)
        if not src:
            raise RuntimeError(f"Source '{name}' not found in mapping.sources")
        return to_url(args.base_url, src["path"])

    for item in load_order:
        if item.startswith("nodes."):
            key = item.split(".", 1)[1]
            spec = nodes[key]
            url = url_for_source_name(spec["source"])
            cypher = build_node_load_cypher(spec, "url", args.batch_id)
            print(f"Loading nodes {key} from {url}")
            neo.run_void(cypher, {**params_common, "url": url})
        elif item.startswith("relationships."):
            key = item.split(".", 1)[1]
            spec = rels[key]
            url = url_for_source_name(spec["source"])
            cypher = build_rel_load_cypher(spec, "url", args.batch_id)
            print(f"Loading rels {key} from {url}")
            neo.run_void(cypher, {**params_common, "url": url})
        elif item.startswith("derived_relationships."):
            print("Promoting named edges...")
            promote_named_edges(neo, mapping)
        else:
            print(f"Skipping unknown load_order item: {item}")

    # validations
    for assertion in mapping.get("validation", {}).get("graph_assertions", []):
        cypher = assertion["cypher"]
        rows = neo.run(cypher)
        if rows and "ok" in rows[0]:
            ok = rows[0]["ok"]
            if ok is True or ok == 1:
                print(f"Validation OK: {cypher}")
            else:
                print(f"Validation FAILED: {cypher} -> {rows}", file=sys.stderr)
                sys.exit(5)
        else:
            print(f"Validation result: {rows}")

    neo.close()
    print("Ingest complete. Batch:", args.batch_id)


if __name__ == "__main__":
    main()
