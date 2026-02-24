import json
import pandas as pd
import argparse
import urllib
from pathlib import Path
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.types import NVARCHAR
import sys
import re

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import normalize_class

# -----------------------
# args
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_asset_extraction.jsonl"

if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Missing: {INPUT_JSONL}")

# -----------------------
# output CSV path
# -----------------------
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_asset.csv"

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
TARGET_TABLE = "cleaned_project_asset_extracted"

# -----------------------
# helpers
# -----------------------
def to_none_if_nullish(x):
    """
    Convert common 'no value' strings to Python None so SQL becomes NULL.
    Handles: NULL, None, N/A, NA, empty, '-', etc.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    if s.lower() in {"null", "none", "n/a", "na", "nan", "nil", "-", "--"}:
        return None
    return s

def normalize_asset_text(asset: str) -> str | None:
    if asset is None:
        return None

    s = str(asset).strip()

    if s == "" or s.lower() in {"null", "none", "n/a", "na", "nan", "-"}:
        return None

    # basic whitespace normalization
    s = re.sub(r"\s+", " ", s)

    # standardize spelling variants
    # daycare -> day care (your exact issue)
    s = re.sub(r"\bdaycare\b", "day care", s, flags=re.IGNORECASE)
    s = re.sub(r"\bhealthcare\b", "health care", s, flags=re.IGNORECASE)
    s = s.title()

    return s

# -----------------------
# parse JSONL -> (project_code, asset, asset_category)
# Keep rows even when asset is NULL, so SQL stores NULLs.
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

        asset = None
        asset_category = None

        # pick first non-nullish value for each extraction_class
        for e in exs:
            cls = normalize_class(e.get("extraction_class"))
            val = to_none_if_nullish(e.get("extraction_text"))

            if cls == "asset" and asset is None:
                asset = val
            elif cls == "asset_category" and asset_category is None:
                asset_category = val

            if asset is not None and asset_category is not None:
                break

        # enforce: if asset is NULL => asset_category must be NULL
        if asset is None:
            asset_category = None

        if project_code:
            rows.append(
                {
                    "project_code": project_code,
                    "asset": asset,
                    "asset_category": asset_category,
                }
            )

df = pd.DataFrame(rows)
df["asset"] = df["asset"].apply(normalize_asset_text)

if df.empty:
    print("[INFO] No rows found in JSONL. Nothing to write.")
    raise SystemExit(0)

# Only one asset & asset category per project_code
df = df.drop_duplicates(subset=["project_code"], keep="first")

# Multiple assets & asset categories per project_code
#df = df.drop_duplicates(subset=["project_code", "asset", "asset_category"], keep="first")

print("[INFO] Total rows:", len(df))
print("[INFO] NULL asset rows:", int(df["asset"].isna().sum()))
print("[INFO] asset_category value counts (incl NULL):")
print(df["asset_category"].fillna("<<NULL>>").value_counts().head(20))

# -----------------------
# write CSV
# -----------------------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print(f"[DONE] Saved CSV: {OUT_CSV}")

# -----------------------
# write to NEW SQL table (truncate + append)
# -----------------------
dtype = {
    "project_code": NVARCHAR(length=255),
    "asset": NVARCHAR(length=400),
    "asset_category": NVARCHAR(length=100),
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
    if_exists="append",  # creates table if it doesn't exist
    index=False,
    chunksize=2000,
    dtype=dtype,
    method=None,
)

print(f"[DONE] Wrote to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")