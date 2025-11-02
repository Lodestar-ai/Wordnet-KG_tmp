#!/usr/bin/env python3
import os, csv, json, hashlib, argparse
from datetime import datetime, timezone
def sha256(path, chunk_size=1024*1024):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

def main(base):
    src = os.path.join(base, 'src')
    mani_dir = os.path.join(base, 'manifest')
    os.makedirs(mani_dir, exist_ok=True)

    files = sorted(f for f in os.listdir(src) if f.endswith('.csv'))
    entries = []
    for f in files:
        path = os.path.join(src, f)
        with open(path, newline='') as fh:
            total = sum(1 for _ in fh)
        rows = max(0, total - 1)  # exclude header
        entries.append({"name": f, "sha256": sha256(path), "rows": rows, "format": "csv"})

    manifest = {
		"dataset": "wordnet",
		"version": "3.0",
		"generated_at": datetime.now(timezone.utc).isoformat(),  
		"files": entries
    }
    mjson = json.dumps(manifest, indent=2, sort_keys=True).encode()
    open(os.path.join(mani_dir, 'manifest.json'), 'wb').write(mjson)
    open(os.path.join(mani_dir, 'manifest.sha256'), 'w').write(
        hashlib.sha256(mjson).hexdigest() + ' manifest.json\n'
    )
    print(f"Wrote {os.path.join(mani_dir, 'manifest.json')} and manifest.sha256")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./stage/wordnet-3.0",
                    help="Base directory containing src/ and manifest/")
    args = ap.parse_args()
    main(args.base)
