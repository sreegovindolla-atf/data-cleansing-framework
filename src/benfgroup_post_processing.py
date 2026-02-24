import json
import pandas as pd
import argparse
import urllib
from pathlib import Path
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.types import NVARCHAR, Integer
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
INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_beneficiary_group_extraction.jsonl"

if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Missing: {INPUT_JSONL}")

# -----------------------
# output CSV path
# -----------------------
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_beneficiary_group.csv"

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
TARGET_TABLE = "cleaned_project_beneficiary_group"

# -----------------------
# parse JSONL -> (project_code, beneficiary_group, beneficiary_count)
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

        beneficiary_group = None
        beneficiary_count = 0  # default if not found

        for e in exs:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")

            if cls == "beneficiary_group" and not _is_blank(val):
                beneficiary_group = str(val).strip()

            elif cls == "beneficiary_count" and not _is_blank(val):
                try:
                    beneficiary_count = int(str(val).strip())
                except ValueError:
                    beneficiary_count = 0

        if project_code and beneficiary_group:
            rows.append({
                "project_code": project_code,
                "beneficiary_group": beneficiary_group,
                "beneficiary_count": beneficiary_count
            })

df = pd.DataFrame(rows).drop_duplicates(subset=["project_code"], keep="first")

if df.empty:
    print("[INFO] No beneficiary_group extracted. Nothing to write.")
    raise SystemExit(0)

print("[INFO] Extracted rows:", len(df))
print(df["beneficiary_group"].value_counts().head(20))

# -----------------------
# write CSV
# -----------------------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print(f"[DONE] Saved CSV: {OUT_CSV}")

# -----------------------
# write to SQL (truncate + append)
# -----------------------
dtype = {
    "project_code": NVARCHAR(length=255),
    "beneficiary_group": NVARCHAR(length=200),
    "beneficiary_count": Integer(),
}

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
    if_exists="append",
    index=False,
    chunksize=2000,
    dtype=dtype,
    method=None,
)

print(f"[DONE] Wrote to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")