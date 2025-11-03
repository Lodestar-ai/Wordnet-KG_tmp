# USAGE: mysqlsh --py --host 0.tcp.ngrok.io --port 10168 -u root -p --file extractDB-CSV.py

import os

DB  = 'wordnet30'
OUT = '/Users/ken/mira/KGPipeline/stage/wordnet-3.0/src_h'

os.makedirs(OUT, exist_ok=True)

# `session` is provided by MySQL Shell (Python mode)
# NOTE: use ? placeholders (not %s) with run_sql()
tables = session.run_sql(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = ? AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """,
    [DB]
).fetch_all()

for (table_name,) in tables:
    cols = session.run_sql(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [DB, table_name]
    ).fetch_all()
    colnames = [c[0] for c in cols]

    final_path = f"{OUT}/{table_name}.csv"
    tmp_path   = f"{OUT}/.{table_name}.data.csv"

    # 1) write header
    with open(final_path, 'w', encoding='utf-8', newline='') as fh:
        fh.write(','.join(f'"{c}"' for c in colnames) + '\n')

    # 2) export data (no header) to temp file
    util.export_table(f"{DB}.{table_name}", tmp_path, {
        "dialect": "csv-unix",
        "fieldsEnclosedBy": '"'
        # You can also add: "fieldsTerminatedBy": ",", "linesTerminatedBy": "\n"
    })

    # 3) append data then remove temp
    with open(final_path, 'a', encoding='utf-8', newline='') as outfh, \
         open(tmp_path, 'r', encoding='utf-8', newline='') as infh:
        for line in infh:
            outfh.write(line)
    os.remove(tmp_path)

    # optional: row count report
    rowcount = session.run_sql(f"SELECT COUNT(*) FROM `{DB}`.`{table_name}`").fetch_one()[0]
    print(f"Wrote {final_path} ({rowcount} rows)")

print("All tables exported with headers.")
