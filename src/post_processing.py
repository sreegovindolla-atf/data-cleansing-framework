import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib
import sys
from sqlalchemy import text as sql_text
from sqlalchemy.types import NVARCHAR

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import (
    smart_title_case,
    normalize_class,
    to_int_or_none,
    to_float_or_none,
    is_missing_or_bad,
    fmt_mp,
    fmt_prj,
    parse_langextract_grouped_pairs
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

TARGET_SCHEMA = "silver"
TARGET_TABLE  = "MasterTable_extracted"

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

        master_project_amount_actual = to_float_or_none(doc.get("master_project_amount_actual"))
        master_project_oda_amount    = to_float_or_none(doc.get("master_project_oda_amount"))
        master_project_ge_amount     = to_float_or_none(doc.get("master_project_ge_amount"))
        master_project_off_amount    = to_float_or_none(doc.get("master_project_off_amount"))

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

        # -----------------------
        # master titles (EN + AR)
        # -----------------------
        master_project_title_en = None
        master_project_title_ar = None

        for x in exs:
            if x["cls"] == "master_project_title_en" and master_project_title_en is None:
                master_project_title_en = smart_title_case(x["val"])
            elif x["cls"] == "master_project_title_ar" and master_project_title_ar is None:
                master_project_title_ar = str(x["val"]).strip()

        global_ben_count = None
        global_ben_group = None
        project_ben = {}

        project_amount_extracted_map = {}  

        project_title_ar_map = {}          
        project_description_en_map = {}     
        project_description_ar_map = {}    

        project_counter = 0
        current_project_code = None
        current_project_title_en = None
        current_project_title_ar = None
        current_project_description_en = None
        current_project_description_ar = None

        current_asset = None
        pending_asset_qty = None
        pending_asset_category = None
        pending_asset_capacity = None
        pending_asset_capacity_uom = None

        current_item = None
        pending_item_qty = None
        pending_item_category = None
        pending_item_uom = None

        projects_with_assets = set()
        projects_with_items = set()
        seen_projects_in_doc = []

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

            # -----------------------------------
            # project boundary = project_title_en (EN canonical)
            # -----------------------------------
            if cls == "project_title_en":
                if current_asset is not None:
                    append_row(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                if current_item is not None:
                    append_row(current_item)
                    projects_with_items.add(current_item["project_code"])
                    current_item = None

                project_counter += 1
                current_project_title_en = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter)
                seen_projects_in_doc.append((current_project_code, current_project_title_en))

                project_ben[current_project_code] = {"beneficiary_count": None, "beneficiary_group_name": None}
                project_amount_extracted_map.setdefault(current_project_code, None)

                # reset per-project fields
                current_project_description_en = None
                current_project_description_ar = None
                current_project_title_ar = None

                project_description_en_map.setdefault(current_project_code, None)
                project_description_ar_map.setdefault(current_project_code, None)
                project_title_ar_map.setdefault(current_project_code, None)

                # reset pending
                pending_asset_qty = None
                pending_asset_category = None
                pending_asset_capacity = None
                pending_asset_capacity_uom = None

                pending_item_qty = None
                pending_item_category = None
                pending_item_uom = None

                continue

            # -----------------------------------
            # Arabic project title (attach to current project)
            # -----------------------------------
            if cls == "project_title_ar":
                if current_project_code is None:
                    continue
                ar_title = str(val).strip()
                if project_title_ar_map.get(current_project_code) in (None, "", pd.NA):
                    project_title_ar_map[current_project_code] = ar_title
                current_project_title_ar = project_title_ar_map[current_project_code]

                # update open rows if any
                if current_asset is not None and current_asset.get("project_code") == current_project_code:
                    current_asset["project_title_ar"] = current_project_title_ar
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_title_ar"] = current_project_title_ar
                continue

            # -----------------------------------
            # project description (EN canonical) attach to current project
            # -----------------------------------
            if cls == "project_description_en":
                if current_project_code is None:
                    continue
                desc = re.sub(r"\s+", " ", str(val).strip())

                if project_description_en_map.get(current_project_code) in (None, "", pd.NA):
                    project_description_en_map[current_project_code] = desc
                current_project_description_en = project_description_en_map[current_project_code]

                if current_asset is not None and current_asset.get("project_code") == current_project_code:
                    current_asset["project_description_en"] = current_project_description_en
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_description_en"] = current_project_description_en
                continue

            # -----------------------------------
            # project description AR attach to current project
            # -----------------------------------
            if cls == "project_description_ar":
                if current_project_code is None:
                    continue
                desc_ar = re.sub(r"\s+", " ", str(val).strip())

                if project_description_ar_map.get(current_project_code) in (None, "", pd.NA):
                    project_description_ar_map[current_project_code] = desc_ar
                current_project_description_ar = project_description_ar_map[current_project_code]

                if current_asset is not None and current_asset.get("project_code") == current_project_code:
                    current_asset["project_description_ar"] = current_project_description_ar
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_description_ar"] = current_project_description_ar
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

            # amount extracted
            if cls == "project_amount_extracted":
                a = to_float_or_none(val)
                if current_project_code is not None and a is not None:
                    if project_amount_extracted_map.get(current_project_code) is None:
                        project_amount_extracted_map[current_project_code] = a
                    backfill_project_amount_extracted(current_project_code, a)

                    if current_asset is not None and current_asset.get("project_code") == current_project_code and is_missing_or_bad(current_asset.get("project_amount_extracted")):
                        current_asset["project_amount_extracted"] = a
                    if current_item is not None and current_item.get("project_code") == current_project_code and is_missing_or_bad(current_item.get("project_amount_extracted")):
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
                    "master_project_title_en": master_project_title_en,
                    "master_project_title_ar": master_project_title_ar,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": current_project_code,
                    "project_title_en": current_project_title_en,
                    "project_title_ar": current_project_title_ar,
                    "project_description_en": current_project_description_en,
                    "project_description_ar": current_project_description_ar,
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
                    "master_project_title_en": master_project_title_en,
                    "master_project_title_ar": master_project_title_ar,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": current_project_code,
                    "project_title_en": current_project_title_en,
                    "project_title_ar": current_project_title_ar,
                    "project_description_en": current_project_description_en,
                    "project_description_ar": current_project_description_ar,
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

            # asset fields
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
                capacity_uom = str(val).strip()
                if current_asset is not None:
                    current_asset["asset_capacity_uom"] = capacity_uom
                else:
                    pending_asset_capacity_uom = capacity_uom
                continue

            # item fields
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

            if cls == "item_quantity_uom":
                uom = str(val).strip()
                if current_item is not None:
                    current_item["item_quantity_uom"] = uom
                else:
                    pending_item_uom = uom
                continue

        # flush last asset/item
        if current_asset is not None:
            append_row(current_asset)
            projects_with_assets.add(current_asset["project_code"])
            current_asset = None

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
            project_description_en_map[current_project_code] = None
            project_description_ar_map[current_project_code] = None
            project_title_ar_map[current_project_code] = None

        # stub rows only if project has neither assets nor items
        for prj_code, prj_title in seen_projects_in_doc:
            if (prj_code not in projects_with_assets) and (prj_code not in projects_with_items):
                bc = project_ben.get(prj_code, {}).get("beneficiary_count") or global_ben_count
                bg = project_ben.get(prj_code, {}).get("beneficiary_group_name") or global_ben_group

                append_row({
                    "index": index,
                    "master_project_code": master_project_code,
                    "master_project_title_en": master_project_title_en,
                    "master_project_title_ar": master_project_title_ar,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": prj_code,
                    "project_title_en": prj_title,
                    "project_title_ar": project_title_ar_map.get(prj_code),
                    "project_description_en": project_description_en_map.get(prj_code),
                    "project_description_ar": project_description_ar_map.get(prj_code),
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

df["project_amount_actual"] = df["master_project_amount_actual"] / df["_project_count"]
df["project_oda_amount"] = df["master_project_oda_amount"] / df["_project_count"]
df["project_ge_amount"] = df["master_project_ge_amount"] / df["_project_count"]
df["project_off_amount"] = df["master_project_off_amount"] / df["_project_count"]

df.loc[df["master_project_amount_actual"].apply(is_missing_or_bad), "project_amount_actual"] = pd.NA
df.loc[df["master_project_oda_amount"].apply(is_missing_or_bad), "project_oda_amount"] = pd.NA
df.loc[df["master_project_ge_amount"].apply(is_missing_or_bad), "project_ge_amount"] = pd.NA
df.loc[df["master_project_off_amount"].apply(is_missing_or_bad), "project_off_amount"] = pd.NA

df.loc[df["asset"].notna(), "asset_quantity_uom"] = "Unit"

df.drop(columns=["_project_count"], inplace=True)

FINAL_COLUMNS = [
    "index",
    "master_project_code",
    "master_project_title_en",
    "master_project_title_ar",
    "project_code",
    "project_title_en",
    "project_title_ar",
    "project_description_en",
    "project_description_ar",
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

if "project_amount_extracted" not in df.columns and "project_amount" in df.columns:
    df["project_amount_extracted"] = df["project_amount"]

for c in FINAL_COLUMNS:
    if c not in df.columns:
        df[c] = None

df = df[FINAL_COLUMNS]

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)

engine = get_sql_server_engine()

dtype = {
    "master_project_title_ar": NVARCHAR(length=4000),
    "project_title_ar": NVARCHAR(length=4000),
    "project_description_ar": NVARCHAR(length=None),  # NVARCHAR(MAX) if supported
}

df.to_sql(
    TARGET_TABLE,
    engine,
    schema=TARGET_SCHEMA,
    if_exists="append",
    index=False,
    chunksize=500,
    dtype=dtype
)

print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("Rows written:", len(df))
print("Non-null counts:\n", df.notna().sum())

with engine.begin() as conn:
    for q in QUERIES:
        conn.execute(sql_text(q))