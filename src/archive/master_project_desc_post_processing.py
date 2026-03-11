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
INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_master_project_description_extraction.jsonl"

if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Missing: {INPUT_JSONL}")

OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_master_project_description.csv"

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
TARGET_TABLE = "cleaned_master_project_description"

# -----------------------
# parse JSONL -> index + EN/AR descriptions
# -----------------------
rows = []
with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)

        index = str(doc.get("index") or "").strip()
        exs = doc.get("extractions") or []

        mp_desc_en = None
        mp_desc_ar = None

        for e in exs:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")

            if _is_blank(val):
                continue

            if cls == "master_project_description_en" and mp_desc_en is None:
                mp_desc_en = str(val).strip()
            elif cls == "master_project_description_ar" and mp_desc_ar is None:
                mp_desc_ar = str(val).strip()

            if mp_desc_en is not None and mp_desc_ar is not None:
                break

        # Always output one row per index (even if missing)
        if index:
            rows.append(
                {
                    "index": index,
                    "master_project_description_en": mp_desc_en or "Missing",
                    "master_project_description_ar": mp_desc_ar or "Missing",
                }
            )

df = pd.DataFrame(rows).drop_duplicates(subset=["index"], keep="first")

if df.empty:
    print("[INFO] No rows produced. Nothing to write.")
    raise SystemExit(0)

print("[INFO] Output rows:", len(df))

# -----------------------
# write CSV
# -----------------------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print(f"[DONE] Saved CSV: {OUT_CSV}")

# -----------------------
# write to NEW SQL table (truncate then insert)
# -----------------------
dtype = {
    "index": NVARCHAR(length=255),
    "master_project_description_en": NVARCHAR(length=None),
    "master_project_description_ar": NVARCHAR(length=None),
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