import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib
import sys
from sqlalchemy import text as sql_text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import (
    smart_title_case
    , normalize_class
    , to_int_or_none
    , to_float_or_none
    , is_missing_or_bad
    , fmt_mp
    , fmt_prj
    , parse_langextract_grouped_pairs
)

from utils.post_processing_sql_queries import QUERIES

# -----------------------
# args + paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID

INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_CSV     = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction.csv"

# -----------------------
# SQL Server (Windows Auth)
# -----------------------
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

TARGET_SCHEMA = "dbo"
TARGET_TABLE  = "MasterTable_extracted_stg"

# -----------------------
# main parsing
# -----------------------
if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

rows = []

with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for doc_index, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        doc = json.loads(line)
        text = doc.get("text")
        index = doc.get("index")

        # raw master amount comes from JSONL top-level field
        master_project_amount_actual = to_float_or_none(doc.get("master_project_amount_actual"))
        master_project_oda_amount = to_float_or_none(doc.get("master_project_oda_amount"))
        master_project_ge_amount = to_float_or_none(doc.get("master_project_ge_amount"))
        master_project_off_amount = to_float_or_none(doc.get("master_project_off_amount"))

        master_project_code = fmt_mp(doc_index)

        reconstructed = parse_langextract_grouped_pairs(doc)

        exs = []
        for e in reconstructed:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if val is None or str(val).strip() == "":
                continue
            exs.append({"cls": cls, "val": val, "idx": e.get("extraction_index", 10**9)})
        exs.sort(key=lambda x: x["idx"])

        # master title
        master_project_title = None
        for x in exs:
            if x["cls"] == "master_project_title":
                master_project_title = smart_title_case(x["val"])
                break

        global_ben_count = None
        global_ben_group = None
        project_ben = {}

        # project-specific amount storage (from JSON: amount_extracted)
        project_amount_extracted_map = {}  # project_code -> float

        project_counter = 0
        current_project_code = None
        current_project_title = None

        current_asset = None
        pending_asset_qty = None
        pending_asset_category = None
        pending_asset_capacity = None
        pending_asset_capacity_uom = None
        #pending_uom = None

        current_item = None
        pending_item_qty = None
        pending_item_category = None
        pending_item_uom = None

        projects_with_assets = set()
        projects_with_items = set()
        seen_projects_in_doc = []

        # track row indices so we can backfill even if amount appears later
        row_indices_by_project = {}  # project_code -> list of row indices in rows[]

        def append_row(r: dict):
            rows.append(r)
            prj = r.get("project_code")
            if prj:
                row_indices_by_project.setdefault(prj, []).append(len(rows) - 1)

        def backfill_project_amount_extracted(prj_code: str, amt: float):
            for ridx in row_indices_by_project.get(prj_code, []):
                if rows[ridx].get("project_amount_extracted") is None or pd.isna(rows[ridx].get("project_amount_extracted")):
                    rows[ridx]["project_amount_extracted"] = amt

        def get_ben(prj_code):
            bc = None
            bg = None
            if prj_code in project_ben:
                bc = project_ben[prj_code].get("beneficiary_count")
                bg = project_ben[prj_code].get("beneficiary_group_name")
            if bc is None:
                bc = global_ben_count
            if bg is None:
                bg = global_ben_group
            return bc, bg

        def get_project_amt_extracted(prj_code):
            return project_amount_extracted_map.get(prj_code)

        for x in exs:
            cls, val = x["cls"], x["val"]

            # project boundary
            if cls == "project_title":
                if current_asset is not None:
                    append_row(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                if current_item is not None:
                    append_row(current_item)
                    projects_with_items.add(current_item["project_code"])
                    current_item = None

                project_counter += 1
                current_project_title = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter)
                seen_projects_in_doc.append((current_project_code, current_project_title))

                project_ben[current_project_code] = {"beneficiary_count": None, "beneficiary_group_name": None}
                project_amount_extracted_map.setdefault(current_project_code, None)

                pending_asset_qty = None
                pending_asset_category = None
                pending_asset_capacity = None
                pending_asset_capacity_uom = None
                #pending_uom = None

                pending_item_qty = None
                pending_item_category = None
                pending_item_uom = None
                
                continue

            # beneficiaries
            if cls == "beneficiary_count":
                b = to_int_or_none(val)
                if current_project_code is not None:
                    if project_ben[current_project_code]["beneficiary_count"] is None:
                        project_ben[current_project_code]["beneficiary_count"] = b
                else:
                    if global_ben_count is None:
                        global_ben_count = b
                continue

            if cls in ("beneficiary_group_name", "beneficiary_group_type"):
                g = smart_title_case(val)
                if current_project_code is not None:
                    if project_ben[current_project_code]["beneficiary_group_name"] is None:
                        project_ben[current_project_code]["beneficiary_group_name"] = g
                else:
                    if global_ben_group is None:
                        global_ben_group = g
                continue

            # capture project amount extracted per project (your JSON uses "amount_extracted")
            if cls == "project_amount_extracted":
                a = to_float_or_none(val)
                if current_project_code is not None and a is not None:
                    if project_amount_extracted_map.get(current_project_code) is None:
                        project_amount_extracted_map[current_project_code] = a
                    backfill_project_amount_extracted(current_project_code, a)

                    # if an asset row or item row is currently open, update it too
                    if (
                        current_asset is not None
                        and current_asset.get("project_code") == current_project_code
                        and is_missing_or_bad(current_asset.get("project_amount_extracted"))
                    ):
                        current_asset["project_amount_extracted"] = a
                    
                    if (
                        current_item is not None
                        and current_item.get("project_code") == current_project_code
                        and is_missing_or_bad(current_item.get("project_amount_extracted"))
                    ):
                        current_item["project_amount_extracted"] = a

                continue

            # assets
            if cls == "asset":
                if current_asset is not None:
                    append_row(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                ben_count, ben_group = get_ben(current_project_code)
                amt_extr = get_project_amt_extracted(current_project_code)

                current_asset = {
                    "index": index,
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": current_project_code,
                    "project_title": current_project_title,
                    "beneficiary_count": ben_count,
                    "beneficiary_group_name": ben_group,
                    "asset": smart_title_case(val),
                    "asset_category": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": "Unit",
                    "asset_capacity": None,
                    "asset_capacity_uom": None,
                    "item": None,
                    "item_category": None,
                    "item_quantity": None,
                    "item_quantity_uom": None,
                    "project_amount_extracted": amt_extr,
                    "input_text": text,
                }

                if pending_asset_qty is not None:
                    current_asset["asset_quantity"] = pending_asset_qty
                    pending_asset_qty = None
                if pending_asset_category is not None:
                    current_asset["asset_category"] = pending_asset_category
                    pending_asset_category = None
                if pending_asset_capacity is not None:
                    current_asset["asset_capacity"] = pending_asset_capacity
                    pending_asset_capacity = None
                if pending_asset_capacity_uom is not None:
                    current_asset["asset_capacity_uom"] = pending_asset_capacity_uom
                    pending_asset_capacity_uom = None
                #if pending_uom is not None:
                #    current_asset["asset_quantity_uom"] = pending_uom
                #    pending_uom = None
                continue

            # items
            if cls in ("item", "items"):
                if current_item is not None:
                    append_row(current_item)
                    projects_with_items.add(current_item["project_code"])
                    current_item = None

                ben_count, ben_group = get_ben(current_project_code)
                amt_extr = get_project_amt_extracted(current_project_code)

                current_item = {
                    "index": index,
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": current_project_code,
                    "project_title": current_project_title,
                    "beneficiary_count": ben_count,
                    "beneficiary_group_name": ben_group,
                    "asset": None,
                    "asset_category": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "asset_capacity": None,
                    "asset_capacity_uom": None,
                    "item": smart_title_case(val),
                    "item_category": None,
                    "item_quantity": None,
                    "item_quantity_uom": None,
                    "project_amount_extracted": amt_extr,
                    "input_text": text,
                }

                if pending_item_qty is not None:
                    current_item["item_quantity"] = pending_item_qty
                    pending_item_qty = None

                if pending_item_category is not None:
                    current_item["item_category"] = pending_item_category
                    pending_item_category = None

                if pending_item_uom is not None:
                    current_item["item_quantity_uom"] = pending_item_uom
                    pending_item_uom = None

                continue

            if cls == "asset_quantity":
                qty = to_int_or_none(val)
                if current_asset is not None:
                    current_asset["asset_quantity"] = qty
                else:
                    pending_asset_qty = qty
                continue
            
            if cls == "asset_category":
                category = smart_title_case(val)
                if current_asset is not None:
                    current_asset["asset_category"] = category
                else:
                    pending_asset_category = category
                continue
            
            if cls == "asset_capacity":
                capacity = to_float_or_none(val)
                if current_asset is not None:
                    current_asset["asset_capacity"] = capacity
                else:
                    pending_asset_capacity = capacity
                continue

            if cls == "asset_capacity_uom":
                capacity_uom = val
                if current_asset is not None:
                    current_asset["asset_capacity_uom"] = capacity_uom
                else:
                    pending_asset_capacity_uom = capacity_uom
                continue

            if cls == "item_quantity":
                qty = to_int_or_none(val)
                if current_item is not None:
                    current_item["item_quantity"] = qty
                else:
                    pending_item_qty = qty
                continue
            
            if cls == "item_category":
                category = smart_title_case(val)
                if current_item is not None:
                    current_item["item_category"] = category
                else:
                    pending_item_category = category
                continue

            if cls in ("item_quantity_uom",):
                uom = val
                if current_item is not None:
                    current_item["item_quantity_uom"] = uom
                else:
                    pending_item_uom = uom
                continue


        # flush last asset
        if current_asset is not None:
            append_row(current_asset)
            projects_with_assets.add(current_asset["project_code"])
            current_asset = None

        # flush last item
        if current_item is not None:
            append_row(current_item)
            projects_with_items.add(current_item["project_code"])
            current_item = None

        # backfill ALL rows for projects that got amount captured
        for prj_code, amt in project_amount_extracted_map.items():
            if amt is not None:
                backfill_project_amount_extracted(prj_code, amt)

        # stub project if none found
        if not seen_projects_in_doc:
            project_counter = 1
            current_project_code = fmt_prj(master_project_code, project_counter)
            seen_projects_in_doc.append((current_project_code, None))
            project_ben[current_project_code] = {"beneficiary_count": global_ben_count, "beneficiary_group_name": global_ben_group}
            project_amount_extracted_map[current_project_code] = None

        # stub rows only if project has neither assets nor items
        for prj_code, prj_title in seen_projects_in_doc:
            if (prj_code not in projects_with_assets) and (prj_code not in projects_with_items):
                bc = project_ben.get(prj_code, {}).get("beneficiary_count") or global_ben_count
                bg = project_ben.get(prj_code, {}).get("beneficiary_group_name") or global_ben_group

                append_row({
                    "index": index,
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": prj_code,
                    "project_title": prj_title,
                    "beneficiary_count": bc,
                    "beneficiary_group_name": bg,
                    "asset": None,
                    "asset_category": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "asset_capacity": None,
                    "asset_capacity_uom": None,
                    "item": None,
                    "item_category": None,
                    "item_quantity": None,
                    "item_quantity_uom": None,
                    "project_amount_extracted": project_amount_extracted_map.get(prj_code),
                    "input_text": text,
                })

# -----------------------
# write output
# -----------------------
df = pd.DataFrame(rows)

# Ensure numeric
df["master_project_amount_actual"] = pd.to_numeric(df.get("master_project_amount_actual"), errors="coerce")
df["master_project_oda_amount"] = pd.to_numeric(df.get("master_project_oda_amount"), errors="coerce")
df["master_project_ge_amount"] = pd.to_numeric(df.get("master_project_ge_amount"), errors="coerce")
df["master_project_off_amount"] = pd.to_numeric(df.get("master_project_off_amount"), errors="coerce")
df["project_amount_extracted"] = pd.to_numeric(df.get("project_amount_extracted"), errors="coerce")

# number of unique projects per master
project_counts = (
    df[["master_project_code", "project_code"]]
    .drop_duplicates()
    .groupby("master_project_code")["project_code"]
    .nunique()
)
df["_project_count"] = df["master_project_code"].map(project_counts).fillna(1).astype(int)

# -----------------------
# NEW FIELD: project_amount_actual
# project_amount_actual = master_project_amount_actual / number of projects under that master
# (same value repeated per project rows within the master)
# -----------------------
df["project_amount_actual"] = df["master_project_amount_actual"] / df["_project_count"]
df["project_oda_amount"] = df["master_project_oda_amount"] / df["_project_count"]
df["project_ge_amount"] = df["master_project_ge_amount"] / df["_project_count"]
df["project_off_amount"] = df["master_project_off_amount"] / df["_project_count"]

# Optional safety: if master is missing/bad, keep project_amount_actual as NaN
df.loc[df["master_project_amount_actual"].apply(is_missing_or_bad), "project_amount_actual"] = pd.NA
df.loc[df["master_project_oda_amount"].apply(is_missing_or_bad), "project_oda_amount"] = pd.NA
df.loc[df["master_project_ge_amount"].apply(is_missing_or_bad), "project_ge_amount"] = pd.NA
df.loc[df["master_project_off_amount"].apply(is_missing_or_bad), "project_off_amount"] = pd.NA

# Optional safety: set asset_quantity_uom to Unit when asset is not null
df.loc[df["asset"].notna(), "asset_quantity_uom"] = "Unit"

df.drop(columns=["_project_count"], inplace=True)

FINAL_COLUMNS = [
    "index",
    "master_project_code",
    "master_project_title",
    "project_code",
    "project_title",
    "beneficiary_count",
    "beneficiary_group_name",
    "asset",
    "asset_category",
    "asset_quantity",
    "asset_quantity_uom",
    "asset_capacity",
    "asset_capacity_uom",
    "item",
    "item_category",
    "item_quantity",
    "item_quantity_uom",
    "input_text",
    "master_project_amount_actual",
    "master_project_oda_amount",
    "master_project_ge_amount",
    "master_project_off_amount",
    "project_amount_actual",
    "project_amount_extracted",
    "project_oda_amount",
    "project_ge_amount",
    "project_off_amount",
]

# Backwards-compat name: your rows dict currently uses "project_amount_extracted"
# but you requested "project_amount_extracted" (same), so just ensure it's present.
if "project_amount_extracted" not in df.columns and "project_amount" in df.columns:
    df["project_amount_extracted"] = df["project_amount"]

for c in FINAL_COLUMNS:
    if c not in df.columns:
        df[c] = None

df = df[FINAL_COLUMNS]

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)

engine = get_sql_server_engine()
df.to_sql(
    TARGET_TABLE,
    engine,
    schema=TARGET_SCHEMA,
    if_exists="replace",
    index=False,
    chunksize=500,
)

print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("Rows written:", len(df))
print("Non-null counts:\n", df.notna().sum())

# Split the output into 3 different tables - master projects table, projects table, projects-assets table
#with engine.begin() as conn:
#    for q in QUERIES:
#        conn.execute(sql_text(q))
