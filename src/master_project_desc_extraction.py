from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import os
import argparse
from pathlib import Path
from sqlalchemy import create_engine
import urllib

import pandas as pd
import langextract as lx
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    jsonl_upsert_by_index,
    load_processed_indexes_from_jsonl,
    safe_str,
    build_labeled_bilingual_input,
    jsonl_to_json_snapshot
)

# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--force-refresh", action="store_true")
args = parser.parse_args()

RUN_ID = args.run_id
FORCE_REFRESH = args.force_refresh

RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_master_project_description_extraction.jsonl"
OUT_JSON = RUN_OUTPUT_DIR / f"{RUN_ID}_master_project_description_extraction.json"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_master_project_description_cache.pkl"

processed_indexes = load_processed_indexes_from_jsonl(OUT_JSONL)
print(f"[INFO] Already in JSONL: {len(processed_indexes)} indexes")
print(f"[INFO] Force refresh mode: {FORCE_REFRESH}")

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

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
      [index]
    , ProjectTitleArabic
    , ProjectTitleEnglish
    , DescriptionArabic
    , DescriptionEnglish
FROM dbo.MasterTableDenormalizedCleanedFinal
WHERE
    ([index] IS NOT NULL)
"""

df_input = pd.read_sql(SOURCE_QUERY, engine)

# =====================================
# PROMPT (strict: one EN + one AR)
# =====================================
PROMPT = """
Extract the following fields from the input text:
- master_project_description_en
- master_project_description_ar

RULES (STRICT):
- Return EXACTLY ONE value for master_project_description_en and EXACTLY ONE value for master_project_description_ar.
- Do NOT invent facts. Do NOT add information not present in the input.
- Summarize the description using the input fields
- English description must be in master_project_description_en only
- Arabic description must be in master_project_description_ar only
- Description must be as descriptive as possible with all the relevant details for that project

OUTPUT:
Return extractions with extraction_class exactly:
- master_project_description_en
- master_project_description_ar
"""

# =====================================
# Examples in your format
# =====================================
EXAMPLES = [
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Kabul University Mosque\n"
            "EN_DESC: Construction of a mosque within Kabul University to support students and staff.\n"
            "AR_TITLE: مسجد جامعة كابول\n"
            "AR_DESC: إنشاء مسجد داخل جامعة كابول لخدمة الطلبة والموظفين."
        ),
        extractions=[
            lx.data.Extraction(
                extraction_class="master_project_description_en",
                extraction_text="Construction of a mosque within Kabul University to support students and staff.",
            ),
            lx.data.Extraction(
                extraction_class="master_project_description_ar",
                extraction_text="إنشاء مسجد داخل جامعة كابول لخدمة الطلبة والموظفين.",
            ),
        ],
    )
]

# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50
cache_hits = 0
fresh_calls = 0
processed_this_run_indexes = set()

for i, row in enumerate(df_input.to_dict("records"), start=1):
    index = safe_str(row.get("index") or row.get("Index") or "")
    if not index:
        continue

    if (not FORCE_REFRESH) and (index in processed_indexes):
        continue

    title_en = safe_str(row.get("ProjectTitleEnglish", "") or "")
    desc_en = safe_str(row.get("DescriptionEnglish", "") or "")
    title_ar = safe_str(row.get("ProjectTitleArabic", "") or "")
    desc_ar = safe_str(row.get("DescriptionArabic", "") or "")

    text_bilingual = build_labeled_bilingual_input(
        title_en=title_en,
        desc_en=desc_en,
        title_ar=title_ar,
        desc_ar=desc_ar,
    )

    if not text_bilingual or not text_bilingual.strip():
        continue

    h = text_hash(text_bilingual)

    if FORCE_REFRESH:
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
        fresh_calls += 1
    else:
        if h in cache:
            result = cache[h]
            cache_hits += 1
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
            fresh_calls += 1

    d = annotated_to_dict(result)

    out = {
        "index": index,
        "text": text_bilingual,
        "extractions": d.get("extractions", []),
    }
    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    jsonl_upsert_by_index(OUT_JSONL, out, index_key="index")
    processed_this_run_indexes.add(index)

    if i % CHECKPOINT_EVERY == 0:
        save_cache(cache, CACHE_PKL)
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        print(
            f"[checkpoint] rows_seen={i} | upserted_this_run={len(processed_this_run_indexes)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)
print(f"[DONE] upserted_this_run={len(processed_this_run_indexes)} | cache_hits={cache_hits} | fresh_calls={fresh_calls}")
print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"Saved cache:           {CACHE_PKL} (entries={len(cache)})")
