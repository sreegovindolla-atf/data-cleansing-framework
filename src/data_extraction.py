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

# Boolean flag:
# - If provided => True
# - If omitted  => False
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
# Update the source query's WHERE condition depending on which indices are to be processed
SOURCE_QUERY = """
SELECT *
FROM dbo.MasterTableDenormalizedCleanedFinal
WHERE
[index] = 'SCA-2024-0028'
"""
df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES + SERV_EXAMPLES

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

cache_hits = 0
fresh_calls = 0
processed_this_run_indexes = set()

for i, row in enumerate(df_input.to_dict("records"), start=1):
    index = row.get("index", None) or row.get("Index", None)
    index = safe_str(index)

    if not index:
        continue

    # If NOT force refresh: skip index already written to JSONL
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

    # Skip only if BOTH languages are empty / no content
    if not text_bilingual or not text_bilingual.strip():
        continue

    master_project_amount_actual = row.get("Amount", None)
    master_project_oda_amount = row.get("ODA_Amount", None)
    master_project_ge_amount = row.get("GE_Amount", None)
    master_project_off_amount = row.get("OFF_Amount", None)

    # Optional: add nonce for debugging to ensure upstream caching can't return identical output
    # debug_text = text_bilingual + f"\n\nRUN_NONCE: {NONCE}"
    # h = text_hash(debug_text)
    # input_for_llm = debug_text

    h = text_hash(text_bilingual)
    input_for_llm = text_bilingual

    # -----------------------
    # cache policy
    # -----------------------
    if FORCE_REFRESH:
        # ALWAYS call LLM, NEVER read cache
        result = lx.extract(
            text_or_documents=input_for_llm,
            prompt_description=PROMPT,
            examples=EXAMPLES,
            model_id="gpt-4.1-mini",
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=False,
        )
        cache[h] = result  # overwrite / update cache with fresh result
        fresh_calls += 1
    else:
        if h in cache:
            result = cache[h]
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
            cache[h] = result
            fresh_calls += 1

    # Convert result -> dict
    d = annotated_to_dict(result)

    out = {
        "extractions": d.get("extractions", []),
        "text": text_bilingual,   # keep original (no nonce)
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
        save_cache(cache, CACHE_PKL)
        print(
            f"[checkpoint] rows_seen={i} | upserted_this_run={len(processed_this_run_indexes)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

# final save
jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)

save_cache(cache, CACHE_PKL)

print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"Saved cache:           {CACHE_PKL} (entries={len(cache)})")
print(f"[DONE] upserted_this_run={len(processed_this_run_indexes)} | cache_hits={cache_hits} | fresh_calls={fresh_calls}")

PROCESSED_TXT = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_indexes.txt"
PROCESSED_TXT.write_text("\n".join(sorted(processed_this_run_indexes)), encoding="utf-8")