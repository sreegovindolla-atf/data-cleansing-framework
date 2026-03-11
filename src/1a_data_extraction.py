from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import sys
import os
import argparse
from pathlib import Path
from sqlalchemy import create_engine
import urllib

import pandas as pd
import langextract as lx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.examples.infrastructure_projects import EXAMPLES as INFRA_EXAMPLES
from config.examples.distribution_projects import EXAMPLES as DIST_EXAMPLES
from config.examples.service_projects import EXAMPLES as SERV_EXAMPLES
from config.prompt import PROMPT

from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    jsonl_upsert_by_index,
    load_processed_indexes_from_jsonl,
    jsonl_to_json_snapshot,
    safe_str,
    build_labeled_bilingual_input,
)

# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument(
    "--force-refresh",
    action="store_true",
    help="If set, bypass cache reads and re-run extraction even if index exists in cache."
)

args = parser.parse_args()

RUN_ID = args.run_id
FORCE_REFRESH = args.force_refresh

RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.json"

processed_indexes = load_processed_indexes_from_jsonl(OUT_JSONL)
print(f"[INFO] Already in JSONL: {len(processed_indexes)} indexes")
print(f"[INFO] Force refresh mode: {FORCE_REFRESH}")

CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_lx_cache.pkl"

# =====================================
# SQL SERVER CONNECTION (WINDOWS AUTH)
# =====================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

engine = get_sql_server_engine()

# =====================================
# SOURCE QUERY
# =====================================
SOURCE_QUERY = """
SELECT *
FROM dbo.MasterTableDenormalizedCleanedFinal
where [index] = 'DAR-2012-083'
"""
df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES + SERV_EXAMPLES

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

run_cache = {}

if FORCE_REFRESH:
    disk_cache = {}
    print("[INFO] Force refresh mode: ignoring disk cache")
else:
    disk_cache = load_cache(CACHE_PKL)
    print(f"[INFO] Loaded disk cache entries: {len(disk_cache)} from {CACHE_PKL.name}")

cache_hits = 0
fresh_calls = 0
processed_this_run_indexes = set()

for i, row in enumerate(df_input.to_dict("records"), start=1):
    index = row.get("index", None) or row.get("Index", None)
    index = safe_str(index)

    if not index:
        continue

    if (not FORCE_REFRESH) and (index in processed_indexes):
        continue

    title_en = safe_str(row.get("ProjectTitleEnglish", "") or "")
    description_en = safe_str(row.get("DescriptionEnglish", "") or "")
    title_ar = safe_str(row.get("ProjectTitleArabic", "") or "")
    description_ar = safe_str(row.get("DescriptionArabic", "") or "")

    text_bilingual = build_labeled_bilingual_input(
        title_en=title_en,
        desc_en=description_en,
        title_ar=title_ar,
        desc_ar=description_ar,
    )

    if not text_bilingual or not text_bilingual.strip():
        continue

    master_project_amount_actual = row.get("Amount", None)
    master_project_oda_amount = row.get("ODA_Amount", None)
    master_project_ge_amount = row.get("GE_Amount", None)
    master_project_off_amount = row.get("OFF_Amount", None)

    h = text_hash(text_bilingual)
    input_for_llm = text_bilingual

    if h in run_cache:
        result = run_cache[h]
        cache_hits += 1
    elif (not FORCE_REFRESH) and (h in disk_cache):
        result = disk_cache[h]
        run_cache[h] = result
        cache_hits += 1
    else:
        result = lx.extract(
            text_or_documents=input_for_llm,
            prompt_description=PROMPT,
            examples=EXAMPLES,
            model_id="gpt-4.1-mini",
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=False,
        )

        run_cache[h] = result
        if not FORCE_REFRESH:
            disk_cache[h] = result

        fresh_calls += 1

    d = annotated_to_dict(result)

    out = {
        "extractions": d.get("extractions", []),
        "text": text_bilingual,
        "index": index,
        "master_project_amount_actual": master_project_amount_actual,
        "master_project_oda_amount": master_project_oda_amount,
        "master_project_ge_amount": master_project_ge_amount,
        "master_project_off_amount": master_project_off_amount,
    }

    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    jsonl_upsert_by_index(OUT_JSONL, out, index_key="index")
    processed_this_run_indexes.add(index)

    if i % CHECKPOINT_EVERY == 0:
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        if not FORCE_REFRESH:
            save_cache(disk_cache, CACHE_PKL)
        print(
            f"[checkpoint] rows_seen={i} | upserted_this_run={len(processed_this_run_indexes)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(disk_cache)}"
        )

jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)

if not FORCE_REFRESH:
    save_cache(disk_cache, CACHE_PKL)
    print(f"Saved disk cache: {CACHE_PKL} (entries={len(disk_cache)})")

print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"[DONE] upserted_this_run={len(processed_this_run_indexes)} | cache_hits={cache_hits} | fresh_calls={fresh_calls}")

PROCESSED_TXT = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_indexes.txt"
PROCESSED_TXT.write_text("\n".join(sorted(processed_this_run_indexes)), encoding="utf-8")