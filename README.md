# WordNet KG Staging (CSV → Neo4j/Aura)

This repo holds **immutable CSV extracts** of WordNet and a **machine-readable manifest** (checksums + row counts) used to load into a Neo4j knowledge graph.  
Phase 1 stages files via **GitHub Releases**. Later, we’ll migrate to **AWS S3** with the same manifest and load steps.

## Repo Purpose

- Provide a **repeatable, testable** staging area for CSVs before loading to Neo4j/Aura.
- Record **file integrity** via SHA-256 and row counts in `manifest/manifest.json`.
- Support **idempotent loads** and quick rollback using batch IDs.
- Enable a clean migration path from GitHub Releases → **AWS S3** without changing pipeline logic.

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
scripts/
  make_manifest.py
README.md
```

## Prereqs

- Python 3.8+ (for `scripts/make_manifest.py`)
- (For later) AWS CLI v2 configured (`aws configure`)

## Prepare a New Dataset Version

1) Put all CSVs under `stage/wordnet-<ver>/src/`.  
2) Build the manifest:
   ```bash
   python3 scripts/make_manifest.py --base ./stage/wordnet-3.0
   ```
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
   git tag dataset/wordnet-3.0
   git push --tags
   ```
   Create a GitHub **Release** from this tag and upload:
   - All CSVs from `stage/wordnet-3.0/src/`
   - `stage/wordnet-3.0/manifest/manifest.json`
   - `stage/wordnet-3.0/manifest/manifest.sha256`

2) **Use with Neo4j Aura (`LOAD CSV`)**  
   Copy the **HTTPS** asset URLs from the Release for each file:
   ```cypher
   // Example: load synsets (Aura)
   :PARAM synsetUrl => 'https://github.com/<org>/<repo>/releases/download/dataset%2Fwordnet-3.0/synset.csv';
   :PARAM batch => 'wn-3.0-2025-11-02';

   LOAD CSV WITH HEADERS FROM $synsetUrl AS row
   MERGE (s:synset {synsetid: toInteger(row.synsetid)})
   SET s.pos = row.pos,
       s.definition = row.definition,
       s.source_system = 'wordnet',
       s.ingest_batch = $batch,
       s.ingested_at = datetime();
   ```

> **Notes**  
> • Release assets are public; if you need private hosting before S3, use your own HTTPS static host with TLS.  
> • Aura cannot fetch from `file:///` or authenticated endpoints.

---

## Phase 2: Migrate Staging to AWS S3 (later)

### 1) Create the bucket (with versioning + encryption)
```bash
export BUCKET=kg-staging-wordnet
aws s3api create-bucket --bucket $BUCKET --region us-east-1
aws s3api put-bucket-versioning --bucket $BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration '{
  "Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

### 2) Upload the dataset
Use a structured prefix so multiple versions can live side by side.
```bash
aws s3 sync stage/wordnet-3.0/src        s3://$BUCKET/wordnet/3.0/src/
aws s3 cp   stage/wordnet-3.0/manifest/manifest.json   s3://$BUCKET/wordnet/3.0/manifest/manifest.json
aws s3 cp   stage/wordnet-3.0/manifest/manifest.sha256 s3://$BUCKET/wordnet/3.0/manifest/manifest.sha256
```

### 3) (Optional) Make read-only public or use presigned URLs
**Presigned (recommended for Aura):**
```bash
aws s3 presign s3://$BUCKET/wordnet/3.0/src/synset.csv --expires-in 604800  # 7 days
```
Repeat for each CSV and for `manifest.json`.

> If you instead make the bucket public, apply a read-only bucket policy and **block uploads** from public; presigned links are safer.

### 4) Update `LOAD CSV` URLs in Cypher
```cypher
:PARAM synsetUrl   => 'https://<presigned-url-for-synset.csv>';
:PARAM semlinkUrl  => 'https://<presigned-url-for-semlinkref.csv>';
:PARAM manifestUrl => 'https://<presigned-url-for-manifest.json>';
```

### 5) Integrity checks before load (optional but recommended)
Compare `manifest.json` values to the files you’re about to load.
```bash
# local verification of an S3 object
aws s3 cp s3://$BUCKET/wordnet/3.0/src/synset.csv - | shasum -a 256
# compare with manifest.json's sha256
```

---

## Loading Order (canonical → derived)

1) **Canonical nodes**
   - `synset.csv`, `word.csv`, (optionally `sense.csv` if modeled as nodes)

2) **Membership edges**
   - From `sense.csv` *or* via `:WORD` rel with sense props:
   ```cypher
   :PARAM senseUrl => $your_url
   LOAD CSV WITH HEADERS FROM $senseUrl AS row
   MATCH (s:synset {synsetid: toInteger(row.synsetid)}),
         (w:word   {wordid:  toInteger(row.wordid)})
   MERGE (s)-[m:WORD]->(w)
   SET m.rank = toInteger(row.rank),
       m.lexid = toInteger(row.lexid),
       m.tagcount = CASE WHEN row.tagcount <> '' THEN toInteger(row.tagcount) END;
   ```

3) **Semantic edges**
   - `semlinkref.csv` → `:SYNSET {linkid}` (source of truth)
   - Promote named rels (derived) after canonical load.

---

## Derived Edges (Hybrid Model)

Keep `:SYNSET{linkid}` as source of truth; promote only “hot” types:

```cypher
MATCH (a:synset)-[r:SYNSET {linkid:1}]->(b:synset)  MERGE (a)-[:HYPERNYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:2}]->(b:synset)  MERGE (a)-[:HYPONYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:3}]->(b:synset)  MERGE (a)-[:INSTANCE_HYPERNYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:4}]->(b:synset)  MERGE (a)-[:INSTANCE_HYPONYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:12}]->(b:synset) MERGE (a)-[:PART_MERONYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:14}]->(b:synset) MERGE (a)-[:MEMBER_MERONYM]->(b);
MATCH (a:synset)-[r:SYNSET {linkid:16}]->(b:synset) MERGE (a)-[:SUBSTANCE_MERONYM]->(b);
```

---

## Operational Tips

- Treat everything under `stage/wordnet-<ver>/src/` as **immutable** once released.
- Use a unique `ingest_batch` per load and attach it to all created/updated nodes/rels.
- Keep **golden queries** (e.g., “horse” hypernyms) as smoke tests after each load.
- For dynamic sources later, adopt CDC or timestamped incremental extracts—this static pipeline still applies, just version each batch.

---

## License & Attribution

- WordNet © Princeton University (see WordNet license).  
- This repo includes **data manifests** and **loader scripts**; actual CSV licensing follows the upstream dataset.
