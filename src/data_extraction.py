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
    safe_str
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
        "SERVER=SREESPOORTHY\SQLEXPRESS01;"
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
select *
from dbo.denorm_MasterTable
where ProjectTitleEnglish is not null and DescriptionEnglish is not null
"""

df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES + SERV_EXAMPLES

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

# Store tuples
all_results_with_amount = []  # [(AnnotatedDocument, raw_amount), ...]

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

for i, row in enumerate(df_input.itertuples(index=False), start=1):
    title = safe_str(getattr(row, "ProjectTitleEnglish", "") or "").strip()
    description = safe_str(getattr(row, "DescriptionEnglish", "") or "").strip()

    if title != description:
        text = f"{title} ; Description: {description}"
    else:
        text = title

    text = normalize_text(text)

    if text is None:
        text = ""

    text = str(text).strip()

    if not text:
        continue

    index = getattr(row, "Index", None)
    # raw amount from SQL row (keep as-is for now)
    master_project_amount_actual = getattr(row, "Amount", None)
    master_project_oda_amount = getattr(row, "ODA_Amount", None)
    master_project_ge_amount = getattr(row, "GE_Amount", None)
    master_project_off_amount = getattr(row, "OFF_Amount", None)
    
    h = text_hash(text)

    if h in cache:
        result = cache[h]
    else:
        result = lx.extract(
            text_or_documents=text,
            prompt_description=PROMPT,
            examples=EXAMPLES,
            model_id="gpt-4.1-mini",
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=False,
        )

    #try:
    #    result.attributes = result.attributes or {}
    #    result.attributes.update({"index": index})
    #    result.attributes.update({"master_project_amount_actual": master_project_amount_actual})
    #    result.attributes.update({"master_project_oda_amount": master_project_oda_amount})
    #    result.attributes.update({"master_project_ge_amount": master_project_ge_amount})
    #    result.attributes.update({"master_project_off_amount": master_project_off_amount})
    #except Exception:
    #    pass

    cache[h] = result
    all_results_with_amount.append((result, index, master_project_amount_actual, master_project_oda_amount, master_project_ge_amount, master_project_off_amount))

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