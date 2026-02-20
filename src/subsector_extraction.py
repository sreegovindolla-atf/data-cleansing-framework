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

from config.examples.project_attributes import ATTR_EXAMPLES
from config.prompt import build_project_attr_prompt
from utils.project_attributes_list import (
    load_allowed_subsectors
    , load_allowed_mdg_targets
    , load_allowed_sdg_targets
)

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
    _jsonl_append,
    jsonl_upsert_by_project_code,
    normalize_text
)

# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs/project_attributes") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.json"

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
SELECT
    a.*
    , COALESCE(b.EmergencyTitle, b.emergencytitlear) AS emergency_title
    , b.year
FROM
(
SELECT *
FROM [silver].[cleaned_project] cp
WHERE cp.master_project_code IN (
    SELECT master_project_code
    FROM [silver].[cleaned_project]
    GROUP BY master_project_code
    HAVING COUNT(*) > 1
))a
LEFT JOIN [dbo].[MasterTableDenormalizedCleanedFinal] b
    ON a.[index] = b.[index]
"""

#SOURCE_QUERY = """
#select *
#from silver.cleaned_project_attributes
#where extracted_sector_en is null
#"""

df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = ATTR_EXAMPLES

# -----------------------
# build prompt
# -----------------------
allowed_subsectors = load_allowed_subsectors(engine)
allowed_mdg_targets = load_allowed_mdg_targets(engine)
allowed_sdg_targets = load_allowed_sdg_targets(engine)

ATTR_PROMPT = build_project_attr_prompt(allowed_subsectors)

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

processed_this_run = 0

for i, row in enumerate(df_input.itertuples(index=False), start=1):
    index = safe_str(getattr(row, "index", None) or "")
    project_code = safe_str(getattr(row, "project_code", None) or "")
    emergency_title = safe_str(getattr(row, "emergency_title", None) or "")
    year = safe_str(getattr(row, "year", None) or "")


    # if index in processed_indexes:
    #     continue

    title_en = safe_str(getattr(row, "project_title_en", "") or "")
    description_en = safe_str(getattr(row, "project_description_en", "") or "")
    title_ar = safe_str(getattr(row, "project_title_ar", "") or "")
    description_ar = safe_str(getattr(row, "project_description_ar", "") or "")

    if not (title_en.strip() or description_en.strip() or title_ar.strip() or description_ar.strip()):
        continue

    text_bilingual = f"""
                        YEAR: {year}
                        EMERGENCY_TITLE: {emergency_title}
                        {build_labeled_bilingual_input(
                            title_en=title_en,
                            desc_en=description_en,
                            title_ar=title_ar,
                            desc_ar=description_ar,
                        )}
                        """.strip()

    # Skip only if BOTH languages are empty
    if not text_bilingual:
        continue

    h = text_hash(text_bilingual)

    if h in cache:
        result = cache[h]
    else:
        result = lx.extract(
            text_or_documents=normalize_text(text_bilingual),
            prompt_description=ATTR_PROMPT,
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
    out["project_code"] = project_code
    out["emergency_title"] = emergency_title
    out["year"] = year

    # Keep document_id if available
    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    # UPSERT JSONL by index (rewrite only if same index is reprocessed)
    jsonl_upsert_by_project_code(OUT_JSONL, out, index_key="project_code")
    # Append for historical run
    #_jsonl_append(OUT_JSONL, out)
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