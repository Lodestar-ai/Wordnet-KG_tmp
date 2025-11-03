#!/usr/bin/env python3
import os, json, hashlib, argparse, csv
from datetime import datetime, timezone

# --------------------------
# Config / Defaults
# --------------------------
SCHEMA_VERSION   = "1.0"
DATASET          = "wordnet"
VERSION          = "3.0"
ENCODING         = "utf-8"
MAPPING_FILENAME = "wordnet-3.0.json"

REQUIRED_SOURCES = ["synset.csv", "word.csv", "sense.csv", "semlinkref.csv", "linkdef.csv"]

# --------------------------
# Helpers
# --------------------------
def sha256(path, chunk_size=1024*1024):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

def read_header(csv_path):
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            return [h.strip().lstrip("\ufeff") for h in header]
    except FileNotFoundError:
        return []

def ensure_dirs(base):
    os.makedirs(os.path.join(base, "manifest"), exist_ok=True)
    os.makedirs(os.path.join(base, "mappings"), exist_ok=True)
    os.makedirs(os.path.join(base, "src"), exist_ok=True)

# --------------------------
# Manifest (preserve exact behavior)
# --------------------------
def build_manifest(base):
    src_dir  = os.path.join(base, "src")
    mani_dir = os.path.join(base, "manifest")
    ensure_dirs(base)

    files = sorted(f for f in os.listdir(src_dir) if f.endswith(".csv"))
    entries = []
    for f in files:
        path = os.path.join(src_dir, f)
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            total = sum(1 for _ in fh)
        rows = max(0, total - 1)  # exclude header
        entries.append({"name": f, "sha256": sha256(path), "rows": rows, "format": "csv"})

    manifest = {
        "dataset": DATASET,
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": entries
    }

    mjson = json.dumps(manifest, indent=2, sort_keys=True).encode()
    with open(os.path.join(mani_dir, "manifest.json"), "wb") as f:
        f.write(mjson)
    with open(os.path.join(mani_dir, "manifest.sha256"), "w", encoding="utf-8") as f:
        f.write(hashlib.sha256(mjson).hexdigest() + " manifest.json\n")

    print(f"✓ Wrote {os.path.join(mani_dir, 'manifest.json')} and manifest.sha256")

# --------------------------
# Mapping (required shape)
# --------------------------
def build_required_mapping(base):
    src_dir = os.path.join(base, "src")

    # Detect semlinkref column names (either synsetid1/synsetid2 OR synset1id/synset2id)
    sem_cols = read_header(os.path.join(src_dir, "semlinkref.csv"))
    syn1 = "synsetid1" if "synsetid1" in sem_cols else ("synset1id" if "synset1id" in sem_cols else "synsetid1")
    syn2 = "synsetid2" if "synsetid2" in sem_cols else ("synset2id" if "synset2id" in sem_cols else "synsetid2")

    mapping = {
      "schema_version": SCHEMA_VERSION,
      "dataset": DATASET,
      "version": VERSION,
      "encoding": ENCODING,
      "csv": {
        "delimiter": ",",
        "quotechar": "\"",
        "newline": "lf",
        "has_header": True
      },
      "sources": [
        {"name": "synset",     "path": "synset.csv"},
        {"name": "word",       "path": "word.csv"},
        {"name": "sense",      "path": "sense.csv"},
        {"name": "semlinkref", "path": "semlinkref.csv"},
        {"name": "linkdef",    "path": "linkdef.csv"}
      ],
      "ingest_batch": {
        "id_prefix": "wn-3.0-",
        "attach_properties": {
          "source_system": "wordnet",
          "ingested_at": "@now_utc_iso8601"
        }
      },
      "nodes": {
        "synset": {
          "source": "synset",
          "label": "synset",
          "key": ["synsetid"],
          "mappings": {
            "synsetid":  {"column": "synsetid",  "type": "int"},
            "pos":       {"column": "pos",       "type": "string", "transform": ["trim","lower"]},
            "definition":{"column": "definition","type": "string", "transform": ["trim"]}
          }
        },
        "word": {
          "source": "word",
          "label": "word",
          "key": ["wordid"],
          "mappings": {
            "wordid": {"column": "wordid", "type": "int"},
            "lemma":  {"column": "lemma",  "type": "string", "transform": ["trim","lower"]}
          }
        }
      },
      "relationships": {
        "membership_WORD": {
          "source": "sense",
          "type": "WORD",
          "direction": "OUT",
          "from": {"label": "synset", "match_on": ["synsetid"]},
          "to":   {"label": "word",   "match_on": ["wordid"]},
          "key": ["synsetid","wordid"],
          "properties": {
            "rank":    {"column": "rank",    "type": "int", "nullable": True},
            "lexid":   {"column": "lexid",   "type": "int", "nullable": True},
            "tagcount":{"column": "tagcount","type": "int", "nullable": True}
          }
        },
        "semantic_SYNSET": {
          "source": "semlinkref",
          "type": "SYNSET",
          "direction": "OUT",
          "from": {"label": "synset", "match_on": [f"{syn1}:synsetid"]},
          "to":   {"label": "synset", "match_on": [f"{syn2}:synsetid"]},
          "key": [syn1, syn2, "linkid"],
          "properties": {
            "linkid": {"column": "linkid", "type": "int"}
          }
        }
      },
      "derived_relationships": {
        "promote_named_edges": {
          "from": "semantic_SYNSET",
          "strategy": "by_linkid",
          "map": [
            {"linkid": 1,  "type": "HYPERNYM"},
            {"linkid": 2,  "type": "HYPONYM"},
            {"linkid": 3,  "type": "INSTANCE_HYPERNYM"},
            {"linkid": 4,  "type": "INSTANCE_HYPONYM"},
            {"linkid": 12, "type": "PART_MERONYM"},
            {"linkid": 14, "type": "MEMBER_MERONYM"},
            {"linkid": 16, "type": "SUBSTANCE_MERONYM"}
          ],
          "deduplicate": True,
          "retain_source_edge": True
        }
      },
      "validation": {
        "graph_assertions": [
          {"cypher": "MATCH (s:synset) RETURN count(s) > 0 AS ok"},
          {"cypher": "MATCH ()-[r:SYNSET]->() RETURN count(r) > 0 AS ok"},
          {"cypher": "MATCH ()-[r:SYNSET]->() WHERE r.linkid IS NULL RETURN count(r)=0 AS ok"},
          {"cypher": "MATCH (s:synset)-[:WORD]->(w:word) RETURN count(*) > 0 AS ok"}
        ]
      },
      "load_order": [
        "nodes.synset",
        "nodes.word",
        "relationships.membership_WORD",
        "relationships.semantic_SYNSET",
        "derived_relationships.promote_named_edges"
      ],
      "runtime": {
        "neo4j_mode": "aura",
        "batching": {
          "load_csv": {"periodic_commit": 10000},
          "merge":    {"use_unwind_batches_of": 10000}
        },
        "indexes": [
          {"kind": "constraint", "label": "synset", "properties": ["synsetid"], "unique": True},
          {"kind": "constraint", "label": "word",   "properties": ["wordid"],   "unique": True},
          {"kind": "rel_index",  "type": "SYNSET",  "properties": ["linkid"]}
        ],
        "provenance": {
          "attach_to_nodes": ["synset","word"],
          "attach_to_rels":  ["membership_WORD","semantic_SYNSET"],
          "properties": ["source_system","ingested_at","ingest_batch"]
        }
      }
    }
    return mapping

def write_mapping(base, mapping_obj, filename=MAPPING_FILENAME):
    path = os.path.join(base, "mappings", filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(mapping_obj, f, indent=2)
    print(f"✓ Wrote mapping: {path}")
    return path

# --------------------------
# CLI
# --------------------------
def main():
    ap = argparse.ArgumentParser(description="Build manifest and mapping metadata for staged dataset.")
    ap.add_argument("--base", default="../stage/wordnet-3.0",
                    help="Base directory containing src/, manifest/, mappings/")
    ap.add_argument("--force-mapping", action="store_true",
                    help="Rebuild mapping even if it exists")
    ap.add_argument("--mapping-name", default=MAPPING_FILENAME,
                    help="Output filename for mapping JSON")
    args = ap.parse_args()

    ensure_dirs(args.base)

    # 1) Manifest (exact behavior as legacy script)
    build_manifest(args.base)

    # 2) Mapping (create or refresh)
    mpath = os.path.join(args.base, "mappings", args.mapping_name)
    if os.path.exists(mpath) and not args.force_mapping:
        print(f"• Mapping already exists: {mpath} (use --force-mapping to regenerate)")
    else:
        mapping = build_required_mapping(args.base)
        write_mapping(args.base, mapping, filename=args.mapping_name)

if __name__ == "__main__":
    main()