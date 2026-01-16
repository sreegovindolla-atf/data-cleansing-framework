from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import sys
import os
import json
import argparse
import hashlib
import pickle
import re
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
    normalize_text,
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    save_results_with_master_project_amount,
    safe_str,
    build_labeled_bilingual_input
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

all_results_with_amount = []

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

for i, row in enumerate(df_input.itertuples(index=False), start=1):
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

    index = getattr(row, "Index", None)

    master_project_amount_actual = getattr(row, "Amount", None)
    master_project_oda_amount = getattr(row, "ODA_Amount", None)
    master_project_ge_amount = getattr(row, "GE_Amount", None)
    master_project_off_amount = getattr(row, "OFF_Amount", None)

    # IMPORTANT: hash the bilingual text (not EN-only) so cache is correct
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

    cache[h] = result
    all_results_with_amount.append(
        (result, index, master_project_amount_actual, master_project_oda_amount, master_project_ge_amount, master_project_off_amount)
    )

    if i % CHECKPOINT_EVERY == 0:
        save_results_with_master_project_amount(all_results_with_amount, OUT_JSONL, OUT_JSON)
        save_cache(cache, CACHE_PKL)
        print(f"[checkpoint] saved {i} rows | cache={len(cache)}")

# final save
save_results_with_master_project_amount(all_results_with_amount, OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)

print(f"Saved extraction: {OUT_JSONL}")
print(f"Saved debug JSON: {OUT_JSON}")
print(f"Saved cache:      {CACHE_PKL}  (entries={len(cache)})")