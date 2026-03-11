import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib
import sys
from datetime import datetime, timezone
from sqlalchemy import text as sql_text
from sqlalchemy.types import NVARCHAR, DateTime, UnicodeText

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import (
    smart_title_case,
    normalize_class,
    to_int_or_none,
    to_float_or_none,
    is_missing_or_bad,
    fmt_mp,
    fmt_prj,
    _is_blank,
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
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction.csv"

PROCESSED_TXT = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_indexes.txt"
if not PROCESSED_TXT.exists():
    raise FileNotFoundError(f"Processed index list not found: {PROCESSED_TXT}")

processed_run_indexes = set(
    x.strip() for x in PROCESSED_TXT.read_text(encoding="utf-8").splitlines() if x.strip()
)
print(f"[INFO] Incremental mode: {len(processed_run_indexes)} indexes to post-process")

# -----------------------
# SQL Server
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

TARGET_SCHEMA = "silver"
TARGET_TABLE = "MasterTable_extracted"

def _trail_int(s: str):
    if s is None:
        return None
    m = re.search(r"(\d+)\s*$", str(s))
    return int(m.group(1)) if m else None

if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

engine = get_sql_server_engine()

index_to_master_code = {}
max_master_num = 0

try:
    existing_master = pd.read_sql(
        sql_text(
            f"""
            SELECT [index], master_project_code
            FROM {TARGET_SCHEMA}.{TARGET_TABLE}
            WHERE [index] IS NOT NULL
              AND master_project_code IS NOT NULL
            """
        ),
        engine,
    )

    existing_master["index"] = existing_master["index"].astype(str)
    existing_master = (
        existing_master.sort_values(["index", "master_project_code"])
        .drop_duplicates("index", keep="first")
    )
    index_to_master_code = dict(
        zip(existing_master["index"], existing_master["master_project_code"])
    )

    master_nums = existing_master["master_project_code"].apply(_trail_int).dropna().tolist()
    max_master_num = max(master_nums) if master_nums else 0

except Exception as e:
    print("Warning: could not load existing mappings from target table. Starting fresh.")
    print("Reason:", str(e))

next_master_num = max_master_num
TS_INSERTED = datetime.now(timezone.utc)

rows = []

with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for doc_index, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        doc = json.loads(line)
        text = doc.get("text")
        index = doc.get("index")
        index_key = str(index) if index is not None else None

        if not index_key or index_key not in processed_run_indexes:
            continue

        document_id = doc.get("document_id") or doc.get("_document_id") or doc.get("doc_id")

        master_project_amount_actual = to_float_or_none(doc.get("master_project_amount_actual"))
        master_project_oda_amount = to_float_or_none(doc.get("master_project_oda_amount"))
        master_project_ge_amount = to_float_or_none(doc.get("master_project_ge_amount"))
        master_project_off_amount = to_float_or_none(doc.get("master_project_off_amount"))

        raw_extractions = doc.get("extractions", [])

        exs = []
        for e in raw_extractions:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if val is None or str(val).strip() == "":
                continue
            exs.append(
                {
                    "cls": cls,
                    "val": val,
                    "idx": e.get("extraction_index", 10**9),
                }
            )

        exs.sort(key=lambda x: x["idx"])

        master_project_title_en = None
        master_project_title_ar = None
        master_project_description_en = None
        master_project_description_ar = None

        for x in exs:
            if x["cls"] == "master_project_title_en" and master_project_title_en is None:
                master_project_title_en = smart_title_case(x["val"])
            elif x["cls"] == "master_project_title_ar" and master_project_title_ar is None:
                master_project_title_ar = str(x["val"]).strip()
            elif x["cls"] == "master_project_description_en" and master_project_description_en is None:
                master_project_description_en = str(x["val"]).strip()
            elif x["cls"] == "master_project_description_ar" and master_project_description_ar is None:
                master_project_description_ar = str(x["val"]).strip()

        if index_key in index_to_master_code:
            master_project_code = index_to_master_code[index_key]
        else:
            next_master_num += 1
            master_project_code = fmt_mp(next_master_num)
            index_to_master_code[index_key] = master_project_code

        current_project_code = None
        current_project_title_en = None
        current_project_title_ar = None
        current_project_description_en = None
        current_project_description_ar = None

        current_item = None
        pending_item_qty = None
        pending_item_category = None
        pending_item_uom = None

        project_amount_extracted_map = {}
        project_description_en_map = {}
        project_description_ar_map = {}
        project_title_ar_map = {}

        seen_projects_in_doc = []
        projects_with_items = set()
        row_indices_by_project = {}
        index_to_next_prj = 0

        def append_row(r: dict):
            rows.append(r)
            prj = r.get("project_code")
            if prj:
                row_indices_by_project.setdefault(prj, []).append(len(rows) - 1)

        def backfill_project_amount_extracted(prj_code: str, amt: float):
            for ridx in row_indices_by_project.get(prj_code, []):
                if rows[ridx].get("project_amount_extracted") is None or pd.isna(
                    rows[ridx].get("project_amount_extracted")
                ):
                    rows[ridx]["project_amount_extracted"] = amt

        for x in exs:
            cls, val = x["cls"], x["val"]

            if cls == "project_title_en":
                if current_item is not None:
                    append_row(current_item)
                    projects_with_items.add(current_item["project_code"])
                    current_item = None

                current_project_title_en = smart_title_case(val)
                index_to_next_prj += 1
                current_project_code = fmt_prj(master_project_code, index_to_next_prj)

                seen_projects_in_doc.append((current_project_code, current_project_title_en))
                project_amount_extracted_map.setdefault(current_project_code, None)

                current_project_description_en = None
                current_project_description_ar = None
                current_project_title_ar = None

                project_description_en_map.setdefault(current_project_code, None)
                project_description_ar_map.setdefault(current_project_code, None)
                project_title_ar_map.setdefault(current_project_code, None)

                pending_item_qty = None
                pending_item_category = None
                pending_item_uom = None
                continue

            if cls == "project_title_ar":
                if current_project_code is None:
                    continue
                ar_title = str(val).strip()
                if _is_blank(project_title_ar_map.get(current_project_code)):
                    project_title_ar_map[current_project_code] = ar_title
                current_project_title_ar = project_title_ar_map[current_project_code]
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_title_ar"] = current_project_title_ar
                continue

            if cls == "project_description_en":
                if current_project_code is None:
                    continue
                desc = re.sub(r"\s+", " ", str(val).strip())
                if _is_blank(project_description_en_map.get(current_project_code)):
                    project_description_en_map[current_project_code] = desc
                current_project_description_en = project_description_en_map[current_project_code]
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_description_en"] = current_project_description_en
                continue

            if cls == "project_description_ar":
                if current_project_code is None:
                    continue
                desc_ar = re.sub(r"\s+", " ", str(val).strip())
                if _is_blank(project_description_ar_map.get(current_project_code)):
                    project_description_ar_map[current_project_code] = desc_ar
                current_project_description_ar = project_description_ar_map[current_project_code]
                if current_item is not None and current_item.get("project_code") == current_project_code:
                    current_item["project_description_ar"] = current_project_description_ar
                continue

            if cls == "project_amount_extracted":
                a = to_float_or_none(val)
                if current_project_code is not None and a is not None:
                    if project_amount_extracted_map.get(current_project_code) is None:
                        project_amount_extracted_map[current_project_code] = a
                    backfill_project_amount_extracted(current_project_code, a)
                    if (
                        current_item is not None
                        and current_item.get("project_code") == current_project_code
                        and is_missing_or_bad(current_item.get("project_amount_extracted"))
                    ):
                        current_item["project_amount_extracted"] = a
                continue

            if cls in ("item", "items"):
                if current_item is not None:
                    append_row(current_item)
                    projects_with_items.add(current_item["project_code"])
                    current_item = None

                amt_extr = project_amount_extracted_map.get(current_project_code)

                current_item = {
                    "document_id": document_id,
                    "ts_inserted": TS_INSERTED,
                    "index": index,
                    "master_project_code": master_project_code,
                    "master_project_title_en": master_project_title_en,
                    "master_project_title_ar": master_project_title_ar,
                    "master_project_description_en": master_project_description_en,
                    "master_project_description_ar": master_project_description_ar,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_oda_amount": master_project_oda_amount,
                    "master_project_ge_amount": master_project_ge_amount,
                    "master_project_off_amount": master_project_off_amount,
                    "project_code": current_project_code,
                    "project_title_en": current_project_title_en,
                    "project_title_ar": current_project_title_ar,
                    "project_description_en": current_project_description_en,
                    "project_description_ar": current_project_description_ar,
                    "beneficiary_count": None,
                    "beneficiary_group_name": None,
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

        if current_item is not None:
            append_row(current_item)
            projects_with_items.add(current_item["project_code"])
            current_item = None

        for prj_code, amt in project_amount_extracted_map.items():
            if amt is not None:
                backfill_project_amount_extracted(prj_code, amt)

        if not seen_projects_in_doc:
            index_to_next_prj += 1
            current_project_code = fmt_prj(master_project_code, index_to_next_prj)
            seen_projects_in_doc.append((current_project_code, None))
            project_amount_extracted_map[current_project_code] = None
            project_description_en_map[current_project_code] = None
            project_description_ar_map[current_project_code] = None
            project_title_ar_map[current_project_code] = None

        for prj_code, prj_title in seen_projects_in_doc:
            if prj_code not in projects_with_items:
                append_row(
                    {
                        "document_id": document_id,
                        "ts_inserted": TS_INSERTED,
                        "index": index,
                        "master_project_code": master_project_code,
                        "master_project_title_en": master_project_title_en,
                        "master_project_title_ar": master_project_title_ar,
                        "master_project_description_en": master_project_description_en,
                        "master_project_description_ar": master_project_description_ar,
                        "master_project_amount_actual": master_project_amount_actual,
                        "master_project_oda_amount": master_project_oda_amount,
                        "master_project_ge_amount": master_project_ge_amount,
                        "master_project_off_amount": master_project_off_amount,
                        "project_code": prj_code,
                        "project_title_en": prj_title,
                        "project_title_ar": project_title_ar_map.get(prj_code),
                        "project_description_en": project_description_en_map.get(prj_code),
                        "project_description_ar": project_description_ar_map.get(prj_code),
                        "beneficiary_count": None,
                        "beneficiary_group_name": None,
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
                    }
                )

df_new = pd.DataFrame(rows)
if df_new.empty:
    print("[INFO] No rows produced for this run. Nothing to upsert.")
    sys.exit(0)

df_new["master_project_amount_actual"] = pd.to_numeric(df_new.get("master_project_amount_actual"), errors="coerce")
df_new["master_project_oda_amount"] = pd.to_numeric(df_new.get("master_project_oda_amount"), errors="coerce")
df_new["master_project_ge_amount"] = pd.to_numeric(df_new.get("master_project_ge_amount"), errors="coerce")
df_new["master_project_off_amount"] = pd.to_numeric(df_new.get("master_project_off_amount"), errors="coerce")
df_new["project_amount_extracted"] = pd.to_numeric(df_new.get("project_amount_extracted"), errors="coerce")

project_counts = (
    df_new[["master_project_code", "project_code"]]
    .drop_duplicates()
    .groupby("master_project_code")["project_code"]
    .nunique()
)
df_new["_project_count"] = df_new["master_project_code"].map(project_counts).fillna(1).astype(int)

df_new["project_amount_actual"] = df_new["master_project_amount_actual"] / df_new["_project_count"]
df_new["project_oda_amount"] = df_new["master_project_oda_amount"] / df_new["_project_count"]
df_new["project_ge_amount"] = df_new["master_project_ge_amount"] / df_new["_project_count"]
df_new["project_off_amount"] = df_new["master_project_off_amount"] / df_new["_project_count"]

df_new.loc[df_new["master_project_amount_actual"].apply(is_missing_or_bad), "project_amount_actual"] = pd.NA
df_new.loc[df_new["master_project_oda_amount"].apply(is_missing_or_bad), "project_oda_amount"] = pd.NA
df_new.loc[df_new["master_project_ge_amount"].apply(is_missing_or_bad), "project_ge_amount"] = pd.NA
df_new.loc[df_new["master_project_off_amount"].apply(is_missing_or_bad), "project_off_amount"] = pd.NA

df_new.drop(columns=["_project_count"], inplace=True)

FINAL_COLUMNS = [
    "document_id",
    "ts_inserted",
    "index",
    "master_project_code",
    "master_project_title_en",
    "master_project_title_ar",
    "master_project_description_en",
    "master_project_description_ar",
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

for c in FINAL_COLUMNS:
    if c not in df_new.columns:
        df_new[c] = None
df_new = df_new[FINAL_COLUMNS]

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

df_new["index"] = df_new["index"].astype(str)

if OUT_CSV.exists():
    old_df = pd.read_csv(OUT_CSV, dtype={"index": str})
    old_df["index"] = old_df["index"].astype(str)
    old_df = old_df[~old_df["index"].isin(processed_run_indexes)]
    df_final = pd.concat([old_df, df_new], ignore_index=True)
else:
    df_final = df_new

df_final.to_csv(OUT_CSV, index=False)

PROCESSED_PROJECT_CODES_TXT = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_project_codes.txt"

processed_project_codes = sorted(
    {
        str(x).strip()
        for x in df_new["project_code"].dropna().tolist()
        if str(x).strip()
    }
)

PROCESSED_PROJECT_CODES_TXT.write_text(
    "\n".join(processed_project_codes),
    encoding="utf-8"
)

print(f"[INFO] Saved processed project_codes txt: {PROCESSED_PROJECT_CODES_TXT}")
print(f"[INFO] Project codes in this run: {len(processed_project_codes)}")

indexes_to_upsert = sorted(set(df_new["index"].dropna().astype(str)) & processed_run_indexes)

dtype = {
    "master_project_title_en": UnicodeText(),
    "master_project_description_en": UnicodeText(),
    "project_title_en": UnicodeText(),
    "project_description_en": UnicodeText(),
    "master_project_title_ar": UnicodeText(),
    "master_project_description_ar": UnicodeText(),
    "project_title_ar": UnicodeText(),
    "project_description_ar": UnicodeText(),
    "ts_inserted": DateTime(),
    "document_id": NVARCHAR(length=128),
}

if indexes_to_upsert:
    print(
        f"[INFO] Upserting {len(indexes_to_upsert)} indexes into {TARGET_SCHEMA}.{TARGET_TABLE} "
        f"(temp table delete-join)"
    )

    df_idx = pd.DataFrame({"index": indexes_to_upsert})

    with engine.begin() as conn:
        conn.execute(sql_text("IF OBJECT_ID('tempdb..#idx') IS NOT NULL DROP TABLE #idx;"))
        conn.execute(sql_text("CREATE TABLE #idx ([index] NVARCHAR(255) NOT NULL PRIMARY KEY);"))

        df_idx.to_sql("#idx", conn, if_exists="append", index=False, method=None)

        conn.execute(
            sql_text(
                f"""
                DELETE T
                FROM {TARGET_SCHEMA}.{TARGET_TABLE} AS T
                INNER JOIN #idx AS I
                    ON CAST(T.[index] AS NVARCHAR(255)) COLLATE DATABASE_DEFAULT
                     = I.[index] COLLATE DATABASE_DEFAULT;
                """
            )
        )

    df_new.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=2000,
        dtype=dtype,
        method=None,
    )

print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("Rows written (this run):", len(df_new))
print("Non-null counts (this run):\n", df_new.notna().sum())

with engine.begin() as conn:
    for q in QUERIES:
        conn.execute(sql_text(q))