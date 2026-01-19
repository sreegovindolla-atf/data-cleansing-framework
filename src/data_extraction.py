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
    _jsonl_append
)

# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.json"

processed_indexes = load_processed_indexes_from_jsonl(OUT_JSONL)
print(f"[INFO] Already in JSONL: {len(processed_indexes)} indexes")

# Cache file (stores mapping: text_hash -> AnnotatedDocument)
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
FROM dbo.MasterTableDenormalizedCleanedFinal a
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.MasterTable_extracted b
    WHERE a.[index] = b.[index]
);
"""
df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES + SERV_EXAMPLES

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

processed_this_run = 0

for i, row in enumerate(df_input.itertuples(index=False), start=1):
    index = getattr(row, "Index", None)

    # if index in processed_indexes:
    #     continue

    title_en = safe_str(getattr(row, "ProjectTitleEnglish", "") or "")
    description_en = safe_str(getattr(row, "DescriptionEnglish", "") or "")
    title_ar = safe_str(getattr(row, "ProjectTitleArabic", "") or "")
    description_ar = safe_str(getattr(row, "DescriptionArabic", "") or "")

    text_bilingual = build_labeled_bilingual_input(
        title_en=title_en,
        desc_en=description_en,
        title_ar=title_ar,
        desc_ar=description_ar,
    )

    # Skip only if BOTH languages are empty
    if not text_bilingual:
        continue

    master_project_amount_actual = getattr(row, "Amount", None)
    master_project_oda_amount = getattr(row, "ODA_Amount", None)
    master_project_ge_amount = getattr(row, "GE_Amount", None)
    master_project_off_amount = getattr(row, "OFF_Amount", None)

    h = text_hash(text_bilingual)

    if h in cache:
        result = cache[h]
    else:
        result = lx.extract(
            text_or_documents=text_bilingual,
            prompt_description=PROMPT,
            examples=EXAMPLES,
            model_id="gpt-4.1-mini",
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=False,
        )
        cache[h] = result  # only update cache on fresh extraction

    # Convert result -> dict
    d = annotated_to_dict(result)

    # Build the output record in the SAME structure your post-processing expects
    out = {}
    out["extractions"] = d.get("extractions", [])
    out["text"] = text_bilingual 
    out["index"] = index
    out["master_project_amount_actual"] = master_project_amount_actual
    out["master_project_oda_amount"] = master_project_oda_amount
    out["master_project_ge_amount"] = master_project_ge_amount
    out["master_project_off_amount"] = master_project_off_amount

    # Keep document_id if available
    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    # UPSERT JSONL by index (rewrite only if same index is reprocessed)
    #jsonl_upsert_by_index(OUT_JSONL, out, index_key="index")
    # Append for historical run
    _jsonl_append(OUT_JSONL, out)
    processed_indexes.add(index)
    processed_this_run += 1

    if i % CHECKPOINT_EVERY == 0:
        # Snapshot JSON rebuilt from JSONL (valid JSON)
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        # Save cache safely
        save_cache(cache, CACHE_PKL)
        print(f"[checkpoint] processed={i} | upserted_this_run={processed_this_run} | cache={len(cache)}")

# final save
jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)

print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"Saved cache:           {CACHE_PKL}  (entries={len(cache)})")