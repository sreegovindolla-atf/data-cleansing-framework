# project_type_postprocess_to_new_table.py
import json
import pandas as pd
import argparse
import urllib
from pathlib import Path
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.types import NVARCHAR
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import normalize_class, _is_blank

# -----------------------
# args
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_type_extraction.jsonl"

if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Missing: {INPUT_JSONL}")

# -----------------------
# output CSV path
# -----------------------
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_project_type.csv"

# -----------------------
# SQL engine
# -----------------------
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

engine = get_sql_server_engine()

TARGET_SCHEMA = "silver"
TARGET_TABLE = "cleaned_project_type"

# -----------------------
# parse JSONL -> (project_code, project_type)
# -----------------------
rows = []
with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)

        project_code = str(doc.get("project_code") or "").strip()
        exs = doc.get("extractions") or []

        project_type = None
        for e in exs:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if cls == "project_type" and not _is_blank(val):
                project_type = str(val).strip()
                break

        if project_code and project_type:
            rows.append({"project_code": project_code, "project_type": project_type})

df = pd.DataFrame(rows).drop_duplicates(subset=["project_code"], keep="first")

if df.empty:
    print("[INFO] No project_type extracted. Nothing to write.")
    raise SystemExit(0)

print("[INFO] Extracted rows:", len(df))
print(df["project_type"].value_counts().head(20))

# -----------------------
# write CSV (incremental overwrite for this run output)
# -----------------------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print(f"[DONE] Saved CSV: {OUT_CSV}")

# -----------------------
# write to NEW SQL table (no updates to cleaned_project)
# -----------------------
dtype = {
    "project_code": NVARCHAR(length=255),
    "project_type": NVARCHAR(length=200),
}

# If you want "replace each run" behavior:
# - drop & recreate table contents
# - simplest and avoids duplicates
with engine.begin() as conn:
    conn.execute(
        sql_text(
            f"""
            IF OBJECT_ID('{TARGET_SCHEMA}.{TARGET_TABLE}', 'U') IS NOT NULL
            BEGIN
                TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE};
            END
            """
        )
    )

df.to_sql(
    TARGET_TABLE,
    engine,
    schema=TARGET_SCHEMA,
    if_exists="append",   # table exists (or will be created if not)
    index=False,
    chunksize=2000,
    dtype=dtype,
    method=None,
)

print(f"[DONE] Wrote to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
