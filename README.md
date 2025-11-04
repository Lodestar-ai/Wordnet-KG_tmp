# WordNet KG Staging (CSV → Neo4j/Aura)

This repo holds **immutable CSV extracts** of WordNet and a **machine‑readable manifest** (checksums + row counts) used to load into a Neo4j knowledge graph. Phase 1 stages files via **GitHub Releases**; we’ll migrate to **AWS S3** next without changing pipeline logic.

## Layout

```
stage/
  wordnet-3.0/
    src/                 # raw CSVs (immutable once released)
      synset.csv
      word.csv
      sense.csv
      semlinkref.csv
      linkdef.csv
      ...
    manifest/
      manifest.json      # per-file sha256 + row counts
      manifest.sha256    # sha256 of manifest.json
    mappings/
      wordnet-3.0.json   # mapping spec: nodes, rels, derived rels, validation
bin/
  auraLoader.py          # idempotent loader for Neo4j/Aura (LOAD CSV + batching)
  buildMetadata.py       # builds manifest.json + manifest.sha256; ensures/repairs mapping
  extractDB-CSV.py       # exports from MySQL → CSV (headers, canonical encodings)
README.md
```

## What changed vs. earlier iteration

- Scripts moved to `bin/` and **mappings** now live in `stage/wordnet-3.0/mappings/`.
- `make_manifest.py` → **`buildMetadata.py`** which:
  - Regenerates **manifest** (dataset, per‑file `sha256` + `rows`) during initial ingestion.
  - Validates/repairs **mapping** (`wordnet-3.0.json`) so required sections exist (nodes, relationships, derived, validation, indexes).

> **Lifecycle:** **Manifest is one‑off per immutable CSV drop** (created at initial ingestion). **Mappings evolve per revision** of the graph model (e.g., add a new derived edge) without changing the raw CSVs.

## Prereqs

- Python 3.8+
- `pip install neo4j requests`
- (Later) AWS CLI v2 for S3 publishing

## Ingestion — canonical order

1) Place CSVs under `stage/wordnet-3.0/src/`.
2) Build metadata:
```bash
python3 bin/buildMetadata.py --base ./stage/wordnet-3.0
```
This writes:
- `stage/wordnet-3.0/manifest/manifest.json`
- `stage/wordnet-3.0/manifest/manifest.sha256`
- Ensures `stage/wordnet-3.0/mappings/wordnet-3.0.json` is present and complete.

3) (Optional) Verify locally:
```bash
# check manifest integrity
shasum -a 256 stage/wordnet-3.0/manifest/manifest.json
cat stage/wordnet-3.0/manifest/manifest.sha256

# spot-check a file
shasum -a 256 stage/wordnet-3.0/src/semlinkref.csv
wc -l stage/wordnet-3.0/src/semlinkref.csv  # rows = (lines - 1)
```

## Phase 1: Publish via GitHub Releases (current)

1) **Tag & release**
```bash
git add -A
git commit -m "WordNet 3.0 dataset"
git tag v0.1.0-alpha
git push --tags
# create a Release for the tag and upload:
#  - all CSVs under stage/wordnet-3.0/src/
#  - stage/wordnet-3.0/manifest/manifest.json
#  - stage/wordnet-3.0/manifest/manifest.sha256
```

2) **Load into Neo4j Aura using `auraLoader.py`**
```bash
python3 ./bin/auraLoader.py   --aura-uri neo4j+s://<host>.databases.neo4j.io   --user neo4j   --password '<password>'   --mapping  stage/wordnet-3.0/mappings/wordnet-3.0.json   --manifest stage/wordnet-3.0/manifest/manifest.json   --base-url 'https://github.com/<org>/<repo>/releases/download/<tag>/'   --verify-checksums   --verify-rowcounts   --strict-missing-linkid   --auto-yes-clean-null-linkid   --batch-rows 1000
```

**Notes**
- Release assets must be **HTTPS** and reachable by Aura (no auth). If you need private access before S3, host on a public TLS static site or use S3 **presigned URLs** (see next phase).
- The loader batches with `CALL (row) { … } IN TRANSACTIONS OF N ROWS` and rejects rows with missing keys and `\N` sentinels where appropriate.

## Phase 2: Migrate Staging to AWS S3 (next)

1) Create the bucket (versioned & encrypted):
```bash
export BUCKET=kg-staging-wordnet
aws s3api create-bucket --bucket $BUCKET --region us-east-1
aws s3api put-bucket-versioning --bucket $BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration '{
  "Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

2) Upload the dataset:
```bash
aws s3 sync stage/wordnet-3.0/src        s3://$BUCKET/wordnet/3.0/src/
aws s3 cp   stage/wordnet-3.0/manifest/manifest.json   s3://$BUCKET/wordnet/3.0/manifest/manifest.json
aws s3 cp   stage/wordnet-3.0/manifest/manifest.sha256 s3://$BUCKET/wordnet/3.0/manifest/manifest.sha256
```

3) Use **presigned** links for Aura:
```bash
aws s3 presign s3://$BUCKET/wordnet/3.0/src/synset.csv --expires-in 604800
```

4) Update the loader `--base-url` to your S3 HTTPS (presigned) prefix.

## Validation & smoke tests

- The mapping includes **graph assertions** (non‑empty node set; non‑NULL `linkid` on `:SYNSET`; `:WORD` edges exist).
- Keep **golden queries** (e.g., “hypernyms of horse”) as smoke tests after each load.

## Next Steps

- **GitHub → AWS (staging):** Ship artifacts via Releases today, then automate publishing to S3 with GitHub Actions; keep the same manifest layout and loader flags.
- **Schema versions & ingestion batches:** Track `schema_version` in mapping; attach `ingest_batch` and timestamps to nodes/rels for idempotency and rollback. Maintain a changelog of mapping revisions.
- **Data‑source agnosticism:** Preserve a stable CSV contract, but formalize exporters for *any* source (SQL/NoSQL/files). Add extractors for new sources that map into the same CSVs + manifest.
- **Incremental updates API:** Provide a small service that accepts new extracts (or CDC deltas), updates manifests, stamps a new `ingest_batch`, and triggers the loader. Expose health checks and validation results per batch.

---
*Generated: 2025-11-04T19:02:16.816763+00:00*
