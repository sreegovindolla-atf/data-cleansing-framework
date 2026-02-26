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
from sqlalchemy import create_engine
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

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_beneficiary_group_extraction.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_beneficiary_group_extraction.json"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_beneficiary_group_cache.pkl"

# -----------------------
# helpers
# -----------------------
from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
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
# PROMPT (beneficiary_group + beneficiary_count)
# =====================================
ALLOWED_BENEFICIARY_GROUPS = [
    "Affected People",
    "Households / Families",
    "Women & girls",
    "Students",
    "Persons with disabilities",
    "Orphans",
]

PROMPT_BENEFICIARY_GROUP = f"""
Extract the following fields from the input text:
- beneficiary_group
- beneficiary_count

STRICT RULES:
- Return EXACTLY ONE value for beneficiary_group and EXACTLY ONE value for beneficiary_count.
- beneficiary_group MUST match EXACTLY one of the allowed values below (case-sensitive).
- Do NOT invent new beneficiary_group values.
- Do NOT return NULL, null, None, none, or blank for beneficiary_group.
- If multiple groups are mentioned, choose the MOST DOMINANT or PRIMARY group directly targeted.
- If the beneficiary is unclear or general, return "Affected People".
- Use English only.

beneficiary_count RULES:
- beneficiary_count MUST be an integer (digits only), e.g., 159
- Do NOT include commas, plus signs, words, or units (no "people", no "families", no "~", no "about").
- If NO beneficiary count is explicitly stated anywhere in the text, return 0.
- If multiple counts are present, choose the count that corresponds to the chosen beneficiary_group.
- If multiple counts correspond to the chosen group, choose the TOTAL / overall count if it is clearly stated; otherwise choose the largest explicit count.

DECISION LOGIC for beneficiary_group:
- If explicitly mentions orphans -> "Orphans"
- If explicitly mentions persons with disabilities / special needs -> "Persons with disabilities"
- If explicitly mentions women, girls, maternal support -> "Women & girls"
- If explicitly mentions students, school children, university students -> "Students"
- If explicitly mentions families, households, vulnerable families -> "Households / Families"
- If the project benefits the wider public, community, or unspecified affected population -> "Affected People"

ALLOWED VALUES:
{chr(10).join([f"- {x}" for x in ALLOWED_BENEFICIARY_GROUPS])}

OUTPUT FORMAT:
Return extractions with:
- extraction_class: beneficiary_group
- extraction_text: <one allowed value>
AND
- extraction_class: beneficiary_count
- extraction_text: <integer digits only>
"""

# =====================================
# EXAMPLES
# =====================================
EXAMPLES = [
    lx.data.ExampleData(
        text=(
            "EN_TITLE: School supplies distribution for 200 students\n"
            "EN_DESC: Providing school bags and materials to students in rural areas.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Students"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="200"),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Food baskets for vulnerable families\n"
            "EN_DESC: Distribution of food aid to 159 low-income households.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Households / Families"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="159"),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Rehabilitation center for persons with disabilities\n"
            "EN_DESC: Supporting 75 special needs individuals with therapy services.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Persons with disabilities"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="75"),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Community health outreach campaign\n"
            "EN_DESC: Providing medical assistance to affected populations.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Affected People"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="0"),
        ],
    ),

    lx.data.ExampleData(
        text=(
            "EN_TITLE: Support for 120 orphans\n"
            "EN_DESC: Monthly assistance and educational support for orphans.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Orphans"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="120"),
        ],
    ),
]

# =====================================
# SOURCE QUERY
# =====================================
SOURCE_QUERY = """
SELECT
    [index],
    project_code,
    project_title_en,
    project_description_en,
    project_title_ar,
    project_description_ar
FROM silver.cleaned_project cp
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.cleaned_project_beneficiary_group cbg
    WHERE cbg.project_code = cp.project_code
)
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
            prompt_description=PROMPT_BENEFICIARY_GROUP,
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
                prompt_description=PROMPT_BENEFICIARY_GROUP,
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
print(f"Saved debug JSON:       {OUT_JSON}")
print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved cache:            {CACHE_PKL} (entries={len(cache)})")