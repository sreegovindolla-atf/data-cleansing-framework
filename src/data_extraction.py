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
select top 10 *
from dbo.denorm_MasterTable
where ProjectTitleEnglish like '%AED %' and ODA_Amount <> 0
"""

df_input = pd.read_sql(SOURCE_QUERY, engine)

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES + SERV_EXAMPLES


# -----------------------
# helpers
# -----------------------
def normalize_text(t: str) -> str:
    t = t.strip()
    t = re.sub(r"\s+", " ", t)
    return t

def text_hash(t: str) -> str:
    t_norm = normalize_text(t)
    return hashlib.sha256(t_norm.encode("utf-8")).hexdigest()

def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path, "rb") as f:
            obj = pickle.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def save_cache(cache: dict, path: Path):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(cache, f)
    tmp.replace(path)

def _enum_to_value(x):
    # Converts Enum-like objects to their value, else returns x
    try:
        return x.value
    except Exception:
        return x

def extraction_to_dict(ex):
    """
    Convert langextract Extraction object -> plain dict
    """
    if isinstance(ex, dict):
        return ex

    # Most langextract Extraction objects have attributes like these:
    d = {}
    for k in [
        "extraction_class",
        "extraction_text",
        "char_interval",
        "alignment_status",
        "extraction_index",
        "group_index",
        "description",
        "attributes",
    ]:
        if hasattr(ex, k):
            v = getattr(ex, k)

            # char_interval is an object with start_pos/end_pos
            if k == "char_interval" and v is not None:
                if isinstance(v, dict):
                    d[k] = v
                else:
                    start = getattr(v, "start_pos", None)
                    end = getattr(v, "end_pos", None)
                    d[k] = {"start_pos": start, "end_pos": end}
                continue

            # alignment_status may be Enum-like
            if k == "alignment_status" and v is not None:
                d[k] = _enum_to_value(v)
                continue

            # attributes might be non-serializable sometimes; keep only dict
            if k == "attributes" and v is not None and not isinstance(v, dict):
                d[k] = None
                continue

            d[k] = v

    return d

def annotated_to_dict(res):
    """
    Robust serializer for langextract AnnotatedDocument.
    Handles:
      - AnnotatedDocument object
      - dict
      - cached JSON dict
      - cached JSON string
    """
    if isinstance(res, dict):
        return res

    # If cache stored JSON as a string
    if isinstance(res, str):
        s = res.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        # Not valid JSON -> treat as text-only
        return {"text": s, "extractions": []}

    # Try known attributes on AnnotatedDocument
    text = getattr(res, "text", None)
    exs  = getattr(res, "extractions", None)
    doc_id = getattr(res, "document_id", None)

    if text is not None or exs is not None:
        out = {
            "extractions": [extraction_to_dict(e) for e in (exs or [])],
            "text": text if text is not None else "",
        }
        if doc_id is not None:
            out["document_id"] = doc_id
        return out

    # last resort
    return {"text": str(res), "extractions": []}

def save_results_with_master_project_amount(results_with_amount, jsonl_path: Path, json_path: Path):
    """
    Writes JSONL and JSON where each doc includes:
      - extractions
      - text
      - master_project_amount_actual   (right after text)
      - document_id (if present)
    """
    docs = []

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for result, index, master_project_amount_actual, master_project_oda_amount, master_project_ge_amount, master_project_off_amount in results_with_amount:
            d = annotated_to_dict(result)
            print(index)
            # Build ordered output explicitly
            out = {}
            out["extractions"] = d.get("extractions", [])
            out["text"] = d.get("text", "")
            out["index"] = index
            out["master_project_amount_actual"] = master_project_amount_actual
            out["master_project_oda_amount"] = master_project_oda_amount
            out["master_project_ge_amount"] = master_project_ge_amount
            out["master_project_off_amount"] = master_project_off_amount

            if "document_id" in d:
                out["document_id"] = d.get("document_id")

            # If you want to keep other keys from d (optional)
            for k, v in d.items():
                if k in ("extractions", "text", "document_id"):
                    continue
                out[k] = v

            f.write(json.dumps(out, ensure_ascii=False) + "\n")
            docs.append(out)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)


# -----------------------
# extraction loop
# -----------------------
CHECKPOINT_EVERY = 50

# Store tuples so we can write master_project_amount_actual even if langextract doesn't serialize attributes
all_results_with_amount = []  # [(AnnotatedDocument, raw_amount), ...]

cache = load_cache(CACHE_PKL)
print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

for i, row in enumerate(df_input.itertuples(index=False), start=1):
    title = str(getattr(row, "ProjectTitleEnglish", "") or "").strip()
    description = str(getattr(row, "DescriptionEnglish", "") or "").strip()

    if title != description:
        text = f"{title} ; Description: {description}"
    else:
        text = title

    index = getattr(row, "Index", None)
    # raw amount from SQL row (keep as-is for now)
    master_project_amount_actual = getattr(row, "Amount", None)
    master_project_oda_amount = getattr(row, "ODA_Amount", None)
    master_project_ge_amount = getattr(row, "GE_Amount", None)
    master_project_off_amount = getattr(row, "OFF_Amount", None)
    
    h = text_hash(text + f"|{master_project_amount_actual}")

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
            use_schema_constraints=True,
        )

    # You can still keep this (harmless), but we won't rely on it for output.
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