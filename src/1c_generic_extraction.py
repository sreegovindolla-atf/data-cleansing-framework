from dotenv import load_dotenv
load_dotenv(override=True)

import logging
logging.getLogger("absl").setLevel(logging.ERROR)

import os
import json
import argparse
import urllib
from pathlib import Path

import pandas as pd
import langextract as lx
from sqlalchemy import create_engine

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.generic_extraction_config import GENERIC_CONFIG, BASE_OUTPUT_DIR

from utils.extraction_helpers import (
    text_hash,
    load_cache,
    save_cache,
    annotated_to_dict,
    safe_str,
    build_labeled_bilingual_input,
    jsonl_to_json_snapshot,
    jsonl_upsert_by_index,
)

try:
    from utils.post_processing_helpers import normalize_class
except Exception:
    def normalize_class(x):
        return str(x).strip() if x is not None else ""


# =========================================================
# ARGS
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument(
    "--entity",
    required=True,
    help="Example: asset / beneficiary_group / project_type"
)
parser.add_argument("--run-id", required=True)
parser.add_argument("--force-refresh", action="store_true")
parser.add_argument(
    "--upstream-ids-file",
    required=False,
    help="Txt file containing upstream processed indexes from 1a/1b. Read-only."
)
parser.add_argument(
    "--use-main-source-query",
    action="store_true",
    help="If set, use built-in 1c source query instead of config source_query."
)
args = parser.parse_args()

ENTITY = args.entity.strip()
RUN_ID = args.run_id.strip()
FORCE_REFRESH = args.force_refresh
UPSTREAM_IDS_FILE = Path(args.upstream_ids_file) if args.upstream_ids_file else None
USE_MAIN_SOURCE_QUERY = args.use_main_source_query

if ENTITY not in GENERIC_CONFIG:
    raise ValueError(
        f"Unknown entity='{ENTITY}'. Allowed values: {', '.join(sorted(GENERIC_CONFIG.keys()))}"
    )

CFG = GENERIC_CONFIG[ENTITY]["extraction"]


# =========================================================
# OUTPUT PATHS
# =========================================================
RUN_OUTPUT_DIR = BASE_OUTPUT_DIR / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_{CFG['output_jsonl_suffix']}"
OUT_JSON = RUN_OUTPUT_DIR / f"{RUN_ID}_{CFG['output_json_suffix']}"
CACHE_PKL = RUN_OUTPUT_DIR / f"{RUN_ID}_{CFG['cache_suffix']}"


# =========================================================
# SQL SERVER CONNECTION (WINDOWS AUTH)
# =========================================================
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


# =========================================================
# HELPERS
# =========================================================
def load_ids_txt(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def load_processed_ids_from_jsonl(path: Path, id_key: str) -> set[str]:
    if not path.exists():
        return set()

    ids = set()
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

            val = safe_str(rec.get(id_key))
            if val:
                ids.add(val)

    return ids


def build_text_from_row(row: dict, text_builder_cfg: dict) -> str:
    builder_type = text_builder_cfg.get("type", "bilingual_basic")

    if builder_type == "bilingual_basic":
        title_en = safe_str(row.get("project_title_en", "") or "")
        desc_en = safe_str(row.get("project_description_en", "") or "")
        title_ar = safe_str(row.get("project_title_ar", "") or "")
        desc_ar = safe_str(row.get("project_description_ar", "") or "")

        return build_labeled_bilingual_input(
            title_en=title_en,
            desc_en=desc_en,
            title_ar=title_ar,
            desc_ar=desc_ar,
        )

    raise ValueError(f"Unsupported text_builder type: {builder_type}")


def normalize_null_extraction_text(x):
    if x is None:
        return "NULL"
    s = str(x).strip()
    return s if s else "NULL"


def apply_post_extract_rules(extraction_dict: dict) -> dict:
    rules = CFG.get("post_extract_rules", {})
    exs = extraction_dict.get("extractions", []) or []

    if rules.get("normalize_null_extraction_text"):
        for e in exs:
            e["extraction_text"] = normalize_null_extraction_text(e.get("extraction_text"))

    if rules.get("asset_null_forces_category_null"):
        asset_val = None
        for e in exs:
            if normalize_class(e.get("extraction_class")).lower() == "asset":
                asset_val = str(e.get("extraction_text")).strip()
                break

        if asset_val == "NULL":
            for e in exs:
                if normalize_class(e.get("extraction_class")).lower() == "asset_category":
                    e["extraction_text"] = "NULL"

    extraction_dict["extractions"] = exs
    return extraction_dict


# =========================================================
# LOAD SOURCE DATA
# =========================================================
if USE_MAIN_SOURCE_QUERY:
    source_query = """
    SELECT
        cp.[index]
        , cp.project_code
        , cp.project_title_en
        , cp.project_title_ar
        , cp.project_description_en
        , cp.project_description_ar
    FROM silver.cleaned_project cp
    """
    print("[INFO] Source query mode: main.py / built-in 1c query")
else:
    source_query = CFG["source_query"]
    print("[INFO] Source query mode: config entity query")

df_input = pd.read_sql(source_query, engine)
print(f"[INFO] Source rows before upstream filtering: {len(df_input)}")

if UPSTREAM_IDS_FILE:
    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(f"Missing upstream ids file: {UPSTREAM_IDS_FILE}")

    upstream_indexes = load_ids_txt(UPSTREAM_IDS_FILE)
    if not upstream_indexes:
        print(f"[INFO] Upstream indexes file is empty: {UPSTREAM_IDS_FILE}")
        raise SystemExit(0)

    upstream_filter_column = CFG.get("upstream_filter_column", "index")
    if upstream_filter_column not in df_input.columns:
        raise ValueError(
            f"upstream_filter_column='{upstream_filter_column}' not found in source query output for entity='{ENTITY}'. "
            f"Available columns: {list(df_input.columns)}"
        )

    before_count = len(df_input)
    df_input[upstream_filter_column] = df_input[upstream_filter_column].astype(str).str.strip()
    df_input = df_input[df_input[upstream_filter_column].isin(upstream_indexes)].copy()

    print(f"[INFO] Entity: {ENTITY}")
    print(f"[INFO] Upstream indexes file: {UPSTREAM_IDS_FILE}")
    print(f"[INFO] Upstream indexes count: {len(upstream_indexes)}")
    print(f"[INFO] Rows after upstream filter on '{upstream_filter_column}': {len(df_input)} / {before_count}")
else:
    print(f"[INFO] Entity: {ENTITY}")
    print("[INFO] No upstream ids file provided. Processing full source query output.")

processed_ids = load_processed_ids_from_jsonl(OUT_JSONL, CFG["index_key"])
print(f"[INFO] Already in JSONL: {len(processed_ids)} {CFG['index_key']} values")
print(f"[INFO] Force refresh mode: {FORCE_REFRESH}")

if FORCE_REFRESH:
    cache = {}
    print("[INFO] Force refresh mode: ignoring existing cache")
else:
    cache = load_cache(CACHE_PKL)
    print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

CHECKPOINT_EVERY = CFG.get("checkpoint_every", 50)
cache_hits = 0
fresh_calls = 0
processed_this_run = set()


# =========================================================
# EXTRACTION LOOP
# =========================================================
for i, row in enumerate(df_input.to_dict("records"), start=1):
    record_id = safe_str(row.get(CFG["index_key"], ""))

    if not record_id:
        continue

    if (not FORCE_REFRESH) and (record_id in processed_ids):
        continue

    text_input = build_text_from_row(row, CFG["text_builder"])

    if not text_input or not text_input.strip():
        continue

    h = text_hash(text_input)

    if h in cache:
        result = cache[h]
        cache_hits += 1
    else:
        result = lx.extract(
            text_or_documents=text_input,
            prompt_description=CFG["prompt"],
            examples=CFG["examples"],
            model_id=CFG.get("model_id", "gpt-4.1-mini"),
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=CFG.get("use_schema_constraints", False),
        )
        cache[h] = result
        fresh_calls += 1

    d = annotated_to_dict(result)
    d = apply_post_extract_rules(d)

    out = {
        CFG["index_key"]: record_id,
        "text": text_input,
        "extractions": d.get("extractions", []),
    }

    if "index" in row and CFG["index_key"] != "index":
        out["index"] = safe_str(row.get("index", ""))

    if d.get("document_id"):
        out["document_id"] = d.get("document_id")

    jsonl_upsert_by_index(OUT_JSONL, out, index_key=CFG["index_key"])
    processed_this_run.add(record_id)

    if i % CHECKPOINT_EVERY == 0:
        save_cache(cache, CACHE_PKL)
        jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
        print(
            f"[checkpoint] rows_seen={i} | processed_this_run={len(processed_this_run)} "
            f"| cache_hits={cache_hits} | fresh_calls={fresh_calls} | cache_size={len(cache)}"
        )

# =========================================================
# FINAL SAVE
# =========================================================
jsonl_to_json_snapshot(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)

print(
    f"[DONE] entity={ENTITY} | processed_this_run={len(processed_this_run)} "
    f"| cache_hits={cache_hits} | fresh_calls={fresh_calls}"
)
print(f"Saved debug JSON:       {OUT_JSON}")
print(f"Saved extraction JSONL: {OUT_JSONL}")
print(f"Saved cache:            {CACHE_PKL} (entries={len(cache)})")