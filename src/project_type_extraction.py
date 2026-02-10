# project_type_extraction.py
from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import os
import argparse
from pathlib import Path
import urllib

import pandas as pd
import langextract as lx
from sqlalchemy import create_engine, text as sql_text
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# -----------------------
# args
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--force-refresh", action="store_true")
args = parser.parse_args()

RUN_ID = args.run_id
FORCE_REFRESH = args.force_refresh

RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_type_extraction.jsonl"
OUT_JSON = RUN_OUTPUT_DIR / f"{RUN_ID}_project_type_extraction.json"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_type_cache.pkl"

# -----------------------
# helpers (same style as yours)
# -----------------------
from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    jsonl_upsert_by_index,
    load_processed_indexes_from_jsonl,
    safe_str,
    build_labeled_bilingual_input,
    jsonl_to_json_snapshot,
    _jsonl_append
)

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
# PROMPT (project_type only)
# =====================================
ALLOWED_PROJECT_TYPES = [
    "Construction",
    "Reconstruction",
    "Rehabilitation",
    "Renovation",
    "Expansion",
    "Maintenance",
    "Repair",
    "Service Delivery",
    "Operations",
    "Program Implementation",
    "Pilot",
    "Scale-up",
    "Capacity Building",
    "Training",
    "Technical Assistance",
    "Institutional Strengthening",
    "Policy Support",
    "Emergency Response",
    "Humanitarian Assistance",
    "Early Recovery",
    "Disaster Risk Reduction",
    "Monitoring & Evaluation",
    "Research & Studies",
    "Awareness & Advocacy",
    "Data / Systems Development",
    "Other",
]

PROMPT_PROJECT_TYPE = f"""
Extract the following field from the input text:
- project_type

RULES (STRICT):
- Return EXACTLY one value for project_type.
- project_type MUST be exactly one of the allowed values below (match text exactly).
- Do NOT invent new values.
- Choose the most dominant intervention type described in the project title/description.
- If nothing is clear, return "Other".

ALLOWED VALUES:
{chr(10).join([f"- {x}" for x in ALLOWED_PROJECT_TYPES])}

OUTPUT FORMAT:
Return a single extraction with:
- extraction_class: project_type
- extraction_text: <one allowed value>
"""

import langextract as lx

EXAMPLES = [
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Kabul University Mosque - Construction\n"
            "EN_DESC: Construction of a mosque within Kabul University.\n"
        ),
        extractions=[
            lx.data.Extraction(
                extraction_class="project_type",
                extraction_text="Construction",
            ),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: School Building Rehabilitation\n"
            "EN_DESC: Rehabilitation and minor repairs to damaged classrooms.\n"
        ),
        extractions=[
            lx.data.Extraction(
                extraction_class="project_type",
                extraction_text="Rehabilitation",
            ),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Provision of primary healthcare services\n"
            "EN_DESC: Ongoing service delivery via clinics and mobile units.\n"
        ),
        extractions=[
            lx.data.Extraction(
                extraction_class="project_type",
                extraction_text="Service Delivery",
            ),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Emergency food assistance\n"
            "EN_DESC: Immediate humanitarian assistance with food baskets.\n"
        ),
        extractions=[
            lx.data.Extraction(
                extraction_class="project_type",
                extraction_text="Humanitarian Assistance",
            ),
        ],
    ),
]


# =====================================
# SOURCE QUERY (from your cleaned_project)
# =====================================
SOURCE_QUERY = """
SELECT
      cp.project_code
    , cp.project_title_en
    , cp.project_description_en
    , cp.project_title_ar
    , cp.project_description_ar
FROM silver.cleaned_project cp
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.cleaned_project_type pt
    WHERE pt.project_code = cp.project_code
);
"""

df_input = pd.read_sql(SOURCE_QUERY, engine)

processed_ids = load_processed_indexes_from_jsonl(OUT_JSONL)
print(f"[INFO] Already in JSONL: {len(processed_ids)} project_codes")
print(f"[INFO] Force refresh mode: {FORCE_REFRESH}")

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

CHECKPOINT_EVERY = 50
cache_hits = 0
fresh_calls = 0
processed_this_run = set()

for i, row in enumerate(df_input.to_dict("records"), start=1):
    project_code = safe_str(row.get("project_code", ""))

    if not project_code:
        continue

    if (not FORCE_REFRESH) and (project_code in processed_ids):
        continue

    title_en = safe_str(row.get("project_title_en", "") or "")
    desc_en  = safe_str(row.get("project_description_en", "") or "")
    title_ar = safe_str(row.get("project_title_ar", "") or "")
    desc_ar  = safe_str(row.get("project_description_ar", "") or "")

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
            prompt_description=PROMPT_PROJECT_TYPE,
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
                prompt_description=PROMPT_PROJECT_TYPE,
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
        "project_code": project_code,
        "text": text_bilingual,
        "extractions": d.get("extractions", []),
    }
    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    #jsonl_upsert_by_index(OUT_JSONL, out, index_key="project_code")
    _jsonl_append(OUT_JSONL, out)
    processed_this_run.add(project_code)

    if i % CHECKPOINT_EVERY == 0:
        save_cache(cache, CACHE_PKL)
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        print(
            f"[checkpoint] rows_seen={i} | upserted_this_run={len(processed_this_run)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

# final save
jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)
print(f"[DONE] upserted_this_run={len(processed_this_run)} | cache_hits={cache_hits} | fresh_calls={fresh_calls}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved cache:           {CACHE_PKL} (entries={len(cache)})")