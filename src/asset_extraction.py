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

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_asset_extraction.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_asset_extraction.json"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_asset_cache.pkl"

# -----------------------
# helpers
# -----------------------
from utils.post_processing_helpers import normalize_class

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

def normalize_null_extraction_text(x):
    """
    LangExtract expects extraction_text to be str/int/float.
    We normalize None / blanks / 'null' / 'none' into literal 'NULL' (string).
    """
    if x is None:
        return "NULL"
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip()
    if s == "":
        return "NULL"
    if s.lower() in {"null", "none", "nil", "n/a", "na"}:
        return "NULL"
    return s

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
# PROMPT (asset + asset_category)
# =====================================
ALLOWED_ASSET_CATEGORIES = [
    "Facility / Building",
    "Well",
    "Mosque",
    "Center",
]

PROMPT_ASSET = f"""
Extract the following fields from the input text:
- asset
- asset_category

DEFINITION (VERY IMPORTANT):
An "asset" here means ONLY a PHYSICAL CONSTRUCTION / BUILT INFRASTRUCTURE (a structure that is built/constructed/rehabilitated).
Asset must be a place/structure, not a movable item, not supplies, not cash, not a program, and not a software/system.

STRICT RULES:
- Return EXACTLY ONE value for asset and EXACTLY ONE value for asset_category.
- If the project does NOT involve building/constructing/rehabilitating a physical structure, then:
  asset = NULL
  asset_category = NULL
- Do NOT return None/none/null. Use the literal token NULL.

WHAT COUNTS AS A VALID ASSET (examples):
- Hospital Building, Clinic Building, School Building, Classrooms, Health Center, Training Center,
  Rehabilitation Center, Community Center, Mosque, Water Well, Facility, Center.

WHAT MUST NEVER BE RETURNED AS AN ASSET (even if mentioned):
- Vehicles/transport: ambulance, car, bus, truck, boat
- Non-physical aid: cash assistance, vouchers, e-vouchers, electronic voucher system, food assistance
- Consumables/NFIs: blankets, clothes, hygiene kits/supplies, food baskets/parcels, medicines, textbooks/books
- Equipment/IT: computers, devices, machines, equipment, "computerized information system", software, platforms, databases
- Programs/projects/initiatives: "Income Generation and Food Security Project", initiatives, campaigns, training activities

ASSET TEXT RULES:
- asset must be concise (2-8 words), English only.
- If multiple valid physical structures exist, choose the PRIMARY / MOST DOMINANT one.
- Use common standard spelling and avoid variants (e.g., prefer Day Care over daycare, etc).

asset_category RULES:
- asset_category MUST be exactly one of the allowed values below (match text exactly).
- Do NOT invent new values.

ALLOWED VALUES for asset_category:
{chr(10).join([f"- {x}" for x in ALLOWED_ASSET_CATEGORIES])}

MAPPING GUIDANCE:
- If the asset is explicitly a mosque -> asset_category = "Mosque"
- If the asset is explicitly a well / water well / borewell -> asset_category = "Well"
- If the asset is a "center" (health center, training center, community center, rehabilitation center, etc.) -> asset_category = "Center"
- Otherwise if it is a general building/facility (hospital, clinic, school building, classrooms, shelter, facility, building, etc.) -> asset_category = "Facility / Building"
- If asset is NULL, asset_category also MUST be NULL

OUTPUT FORMAT:
Return extractions with:
- extraction_class: asset
- extraction_text: <physical construction in English>
AND
- extraction_class: asset_category
- extraction_text: <one allowed value>

- If no physical construction exists, set:
  asset = NULL
  asset_category = NULL
"""

# =====================================
# EXAMPLES
# =====================================
EXAMPLES = [
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Construction of a 40-bed hospital in Baralyn Island\n"
            "EN_DESC: The project includes constructing a new hospital building and upgrading clinics.\n"
            "AR_TITLE: بناء مستشفى بسعة 40 سريرًا\n"
            "AR_DESC: يشمل المشروع بناء مستشفى جديد وترقية العيادات.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="asset", extraction_text="Hospital building"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Facility / Building"),
        ],
    ),
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Rehabilitation of damaged classrooms\n"
            "EN_DESC: Repair and renovation works for a school building.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="asset", extraction_text="School building"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Facility / Building"),
        ],
    ),
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Drilling of water wells in rural villages\n"
            "EN_DESC: Construction of bore wells to improve access to clean water.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="asset", extraction_text="Water well"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Well"),
        ],
    ),
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Construction of a community mosque\n"
            "EN_DESC: Building a mosque including prayer hall and ablution area.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="asset", extraction_text="Community mosque"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Mosque"),
        ],
    ),
    lx.data.ExampleData(
        text=(
            "EN_TITLE: Establishment of a rehabilitation center\n"
            "EN_DESC: Construction of a new rehabilitation center facility.\n"
        ),
        extractions=[
            lx.data.Extraction(extraction_class="asset", extraction_text="Rehabilitation center"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Center"),
        ],
    ),
]

# =====================================
# SOURCE QUERY
# =====================================
SOURCE_QUERY = """
SELECT
	cp.[index]
	, pt.project_code
	, cp.project_title_en
	, cp.project_title_ar
	, cp.project_description_en
	, cp.project_description_ar
	, pt.project_type
FROM silver.cleaned_project_type pt
LEFT JOIN silver.cleaned_project cp
    ON cp.project_code = pt.project_code
WHERE pt.project_type = 'New Construction'
    OR pt.project_type = 'Repair / Maintenance'
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
            prompt_description=PROMPT_ASSET,
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
                prompt_description=PROMPT_ASSET,
                examples=EXAMPLES,
                model_id="gpt-4.1-mini",
                api_key=os.environ.get("OPENAI_API_KEY"),
                fence_output=True,
                use_schema_constraints=False,
            )
            cache[h] = result
            fresh_calls += 1

    d = annotated_to_dict(result)

    # Normalize extraction_text to avoid ValueError
    exs = d.get("extractions", []) or []
    for e in exs:
        e["extraction_text"] = normalize_null_extraction_text(e.get("extraction_text"))

    # enforce rule: if asset is NULL => asset_category must be NULL
    asset_val = None
    asset_cat_val = None
    for e in exs:
        if normalize_class(e.get("extraction_class")) == "asset":
            asset_val = str(e.get("extraction_text")).strip()
        if normalize_class(e.get("extraction_class")) == "asset_category":
            asset_cat_val = str(e.get("extraction_text")).strip()

    if asset_val == "NULL":
        # make sure asset_category becomes NULL too
        for e in exs:
            if normalize_class(e.get("extraction_class")) == "asset_category":
                e["extraction_text"] = "NULL"

    d["extractions"] = exs

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
            f"[checkpoint] rows_seen={i} | appended_this_run={len(processed_this_run)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

# final save
jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)

print(f"[DONE] appended_this_run={len(processed_this_run)} | cache_hits={cache_hits} | fresh_calls={fresh_calls}")
print(f"Saved debug JSON:       {OUT_JSON}")
print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved cache:            {CACHE_PKL} (entries={len(cache)})")