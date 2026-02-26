import json
import pandas as pd
import argparse
import urllib
from pathlib import Path
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.types import NVARCHAR, Float, Integer
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

    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bdaycare\b", "day care", s, flags=re.IGNORECASE)
    s = re.sub(r"\bhealthcare\b", "health care", s, flags=re.IGNORECASE)
    s = s.title()
    return s

def to_int_or_none(x) -> int | None:
    """Parse integer quantities like '2', '02', '2.0'. Reject non-numeric."""
    x = to_none_if_nullish(x)
    if x is None:
        return None
    s = str(x).strip()
    s = s.replace(",", "")
    # allow "2.0" but store as 2
    if re.fullmatch(r"\d+(\.0+)?", s):
        return int(float(s))
    if re.fullmatch(r"\d+", s):
        return int(s)
    return None

def to_float_or_none(x) -> float | None:
    """Parse numeric capacities like '1000', '1,000', '1000.5'. Reject non-numeric."""
    x = to_none_if_nullish(x)
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return float(s)
    return None

def normalize_uom(uom: str) -> str | None:
    """
    Normalize unit variants to a canonical representation.
    Keep it simple + predictable for analytics.
    """
    uom = to_none_if_nullish(uom)
    if uom is None:
        return None

    s = str(uom).strip()

    # normalize common variants -> canonical
    key = s.strip().lower()

    mapping = {
        # volume
        "l": "Liter",
        "liter": "Liter",
        "litre": "Liter",
        "liters": "Liter",
        "litres": "Liter",

        "gal": "Gallon",
        "gallon": "Gallon",
        "gallons": "Gallon",

        "m3": "Cubic Meter",
        "m³": "Cubic Meter",
        "cubic meter": "Cubic Meter",
        "cubic meters": "Cubic Meter",

        # area
        "m2": "Square Meter",
        "m²": "Square Meter",
        "sqm": "Square Meter",
        "sq m": "Square Meter",
        "sq. m": "Square Meter",
        "square meter": "Square Meter",
        "square meters": "Square Meter",

        # power
        "kva": "kVA",
        "kv-a": "kVA",
        "k.v.a": "kVA",

        "kw": "kW",
        "kilowatt": "kW",
        "kilowatts": "kW",

        "w": "W",
        "watt": "W",
        "watts": "W",
    }

    canon = mapping.get(key)
    if canon:
        return canon

    # If model outputs already canonical, keep it:
    canonical_set = {"Liter", "Gallon", "Cubic Meter", "Square Meter", "kVA", "kW", "W"}
    if s in canonical_set:
        return s

    # Fallback: keep original cleaned, but title-case common words
    # (If you want STRICT enforcement, replace this with "return None")
    return s.strip()

# -----------------------
# parse JSONL -> rows
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
        asset_quantity = None
        asset_capacity = None
        asset_capacity_uom = None

        # pick first non-nullish value for each extraction_class
        for e in exs:
            cls = normalize_class(e.get("extraction_class"))
            val = to_none_if_nullish(e.get("extraction_text"))

            if cls == "asset" and asset is None:
                asset = val
            elif cls == "asset_category" and asset_category is None:
                asset_category = val
            elif cls == "asset_quantity" and asset_quantity is None:
                asset_quantity = val
            elif cls == "asset_capacity" and asset_capacity is None:
                asset_capacity = val
            elif cls == "asset_capacity_uom" and asset_capacity_uom is None:
                asset_capacity_uom = val

        # enforce: if asset is NULL => everything else must be NULL
        if asset is None:
            asset_category = None
            asset_quantity = None
            asset_capacity = None
            asset_capacity_uom = None

        if project_code:
            rows.append(
                {
                    "project_code": project_code,
                    "asset": asset,
                    "asset_category": asset_category,
                    "asset_quantity": asset_quantity,
                    "asset_capacity": asset_capacity,
                    "asset_capacity_uom": asset_capacity_uom,
                }
            )

df = pd.DataFrame(rows)

# -----------------------
# normalization / typing
# -----------------------
df["asset"] = df["asset"].apply(normalize_asset_text)
df["asset_quantity"] = df["asset_quantity"].apply(to_int_or_none)
df["asset_capacity"] = df["asset_capacity"].apply(to_float_or_none)
df["asset_capacity_uom"] = df["asset_capacity_uom"].apply(normalize_uom)

# enforce: if capacity is NULL => uom must be NULL
df.loc[df["asset_capacity"].isna(), "asset_capacity_uom"] = None

# enforce: if asset is NULL => all dependent fields NULL (again, after normalization)
null_asset_mask = df["asset"].isna()
df.loc[null_asset_mask, ["asset_category", "asset_quantity", "asset_capacity", "asset_capacity_uom"]] = None

if df.empty:
    print("[INFO] No rows found in JSONL. Nothing to write.")
    raise SystemExit(0)

# Only one asset record per project_code (keep first)
df = df.drop_duplicates(subset=["project_code"], keep="first")

print("[INFO] Total rows:", len(df))
print("[INFO] NULL asset rows:", int(df["asset"].isna().sum()))
print("[INFO] asset_category value counts (incl NULL):")
print(df["asset_category"].fillna("<<NULL>>").value_counts().head(20))
print("[INFO] asset_capacity_uom value counts (incl NULL):")
print(df["asset_capacity_uom"].fillna("<<NULL>>").value_counts().head(20))

# -----------------------
# write CSV
# -----------------------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print(f"[DONE] Saved CSV: {OUT_CSV}")

# -----------------------
# write to SQL Server (truncate + append)
# -----------------------
dtype = {
    "project_code": NVARCHAR(length=255),
    "asset": NVARCHAR(length=400),
    "asset_category": NVARCHAR(length=100),
    "asset_quantity": Integer(),
    "asset_capacity": Float(),
    "asset_capacity_uom": NVARCHAR(length=60),
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