from dotenv import load_dotenv
load_dotenv()

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

import pandas as pd
import langextract as lx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.examples.infrastructure_projects import EXAMPLES as INFRA_EXAMPLES
from config.examples.distribution_projects import EXAMPLES as DIST_EXAMPLES
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

INPUT_CSV = Path("data/input/denorm_mastertable.csv")

if not INPUT_CSV.exists():
    raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

df_input = pd.read_csv(INPUT_CSV)

if "ProjectTitleEnglish" not in df_input.columns:
    raise ValueError("Column 'ProjectTitleEnglish' not found in input CSV")

input_texts = (
    df_input["ProjectTitleEnglish"]
    .dropna()
    .astype(str)
    .str.strip()
)
input_texts = [t for t in input_texts if t]

EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES


# -----------------------
# helpers
# -----------------------
def jsonl_to_pretty_json(jsonl_path: Path, json_path: Path):
    docs = []
    if not jsonl_path.exists():
        return
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

def normalize_text(t: str) -> str:
    # light normalization: collapse whitespace, trim
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
        # if cache is corrupted or incompatible, ignore it
        return {}

def save_cache(cache: dict, path: Path):
    # atomic-ish write
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(cache, f)
    tmp.replace(path)


# -----------------------
# extraction loop (ONE prompt, ONE pass)
# + cache (avoid API calls for repeated input_text)
# + incremental checkpointing every 50 rows
# -----------------------
CHECKPOINT_EVERY = 50
all_results = []

# in-memory + persistent cache
cache = load_cache(CACHE_PKL)

print(f"[INFO] Loaded cache entries: {len(cache)} from {CACHE_PKL.name}")

for i, text in enumerate(input_texts, start=1):
    h = text_hash(text)

    if h in cache:
        # reuse cached AnnotatedDocument (no API call)
        result = cache[h]
    else:
        # new input -> API call once
        result = lx.extract(
            text_or_documents=text,
            prompt_description=PROMPT,
            examples=EXAMPLES,
            model_id="gpt-4o",
            api_key=os.environ.get("OPENAI_API_KEY"),
            fence_output=True,
            use_schema_constraints=False,
        )
        cache[h] = result

    all_results.append(result)

    # checkpoint write every N rows
    if i % CHECKPOINT_EVERY == 0:
        lx.io.save_annotated_documents(all_results, output_name=OUT_JSONL.name, output_dir=RUN_OUTPUT_DIR)
        jsonl_to_pretty_json(OUT_JSONL, OUT_JSON)
        save_cache(cache, CACHE_PKL)
        print(f"[checkpoint] saved {i} rows | cache={len(cache)}")

# final save
lx.io.save_annotated_documents(all_results, output_name=OUT_JSONL.name, output_dir=RUN_OUTPUT_DIR)
jsonl_to_pretty_json(OUT_JSONL, OUT_JSON)
save_cache(cache, CACHE_PKL)

print(f"Saved extraction: {OUT_JSONL}")
print(f"Saved debug JSON: {OUT_JSON}")
print(f"Saved cache:      {CACHE_PKL}  (entries={len(cache)})")
