from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import sys
import os
import json
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
    load_allowed_subsectors,
    load_allowed_mdg_targets,
    load_allowed_sdg_targets
)

from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    jsonl_to_json_snapshot,
    safe_str,
    build_labeled_bilingual_input,
    jsonl_upsert_by_project_code,
    normalize_text
)

# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument(
    "--force-refresh",
    action="store_true",
    help="If set, bypass cache and re-extract even if project_code already exists in JSONL."
)
args = parser.parse_args()

RUN_ID = args.run_id.strip()
FORCE_REFRESH = args.force_refresh

RUN_OUTPUT_DIR = Path("data/outputs/project_attributes") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.jsonl"
OUT_JSON = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.json"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_lx_cache.pkl"
PROCESSED_INDEXES_TXT = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_indexes.txt"


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
# HELPERS
# =====================================
def load_processed_project_codes_from_jsonl(path: Path) -> set[str]:
    if not path.exists():
        return set()

    processed = set()
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] Skipping invalid JSONL line {line_no}")
                continue

            project_code = safe_str(rec.get("project_code"))
            if project_code:
                processed.add(project_code)

    return processed


# =====================================
# SOURCE QUERY
# 2a must process ALL indexes from this query
# =====================================
SOURCE_QUERY = """
SELECT
    a.*
    , COALESCE(b.EmergencyTitle, b.EmergencyTitleAR) AS emergency_title
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
    )
) a
LEFT JOIN [dbo].[MasterTableDenormalizedCleanedFinal] b
    ON a.[index] = b.[index]
--where a.[index] = 'DAR-2012-083'
"""

df_input = pd.read_sql(SOURCE_QUERY, engine)
print(f"[INFO] Source rows from SQL query: {len(df_input)}")

processed_project_codes = load_processed_project_codes_from_jsonl(OUT_JSONL)
print(f"[INFO] Already in JSONL: {len(processed_project_codes)} project_code values")
print(f"[INFO] Force refresh mode: {FORCE_REFRESH}")

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

if FORCE_REFRESH:
    cache = {}
    print("[INFO] Force refresh mode: ignoring existing cache")
else:
    cache = load_cache(CACHE_PKL)
    print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

processed_this_run = 0
processed_this_run_indexes = set()
cache_hits = 0
fresh_calls = 0

for i, row in enumerate(df_input.itertuples(index=False), start=1):
    index = safe_str(getattr(row, "index", None) or "")
    project_code = safe_str(getattr(row, "project_code", None) or "")
    emergency_title = safe_str(getattr(row, "emergency_title", None) or "")
    year = safe_str(getattr(row, "year", None) or "")

    if not project_code:
        continue

    if (not FORCE_REFRESH) and project_code in processed_project_codes:
        continue

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

    if not text_bilingual:
        continue

    h = text_hash(text_bilingual)

    if h in cache:
        result = cache[h]
        cache_hits += 1
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
        cache[h] = result
        fresh_calls += 1

    d = annotated_to_dict(result)

    out = {
        "extractions": d.get("extractions", []),
        "text": text_bilingual,
        "index": index,
        "project_code": project_code,
        "emergency_title": emergency_title,
        "year": year,
    }

    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    jsonl_upsert_by_project_code(OUT_JSONL, out, index_key="project_code")
    processed_this_run += 1

    if index:
        processed_this_run_indexes.add(index)

    if i % CHECKPOINT_EVERY == 0:
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        save_cache(cache, CACHE_PKL)
        PROCESSED_INDEXES_TXT.write_text(
            "\n".join(sorted(processed_this_run_indexes)),
            encoding="utf-8"
        )
        print(
            f"[checkpoint] rows_seen={i} | upserted_this_run={processed_this_run} "
            f"| processed_indexes_this_run={len(processed_this_run_indexes)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)
PROCESSED_INDEXES_TXT.write_text(
    "\n".join(sorted(processed_this_run_indexes)),
    encoding="utf-8"
)

print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved debug JSON:      {OUT_JSON}")
print(f"Saved cache:           {CACHE_PKL} (entries={len(cache)})")
print(f"Saved processed indexes txt: {PROCESSED_INDEXES_TXT}")
print(
    f"[DONE] upserted_this_run={processed_this_run} "
    f"| processed_indexes_this_run={len(processed_this_run_indexes)} "
    f"| cache_hits={cache_hits} | fresh_calls={fresh_calls}"
)