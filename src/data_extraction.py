from dotenv import load_dotenv
load_dotenv()

import sys
import os
import json
import argparse
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import langextract as lx

# ---- config imports ----
# You said: "I want to do the extraction only once for all fields"
# So we use ONE prompt (PROMPT) and ONE combined examples list.
from config.examples.infrastructure_projects import EXAMPLES as INFRA_EXAMPLES
from config.examples.distribution_projects import EXAMPLES as DIST_EXAMPLES

from config.prompt import PROMPT  # combined prompt: master_project_title, project_title, beneficiary_*, asset_*


# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Single extraction output (ONE pass)
OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.json"


# -----------------------
# load input texts from CSV
# -----------------------
INPUT_CSV = Path("data/input/denorm_mastertable.csv")

if not INPUT_CSV.exists():
    raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

df_input = pd.read_csv(INPUT_CSV)

if "ProjectTitleEnglish" not in df_input.columns:
    raise ValueError("Column 'ProjectTitleEnglish' not found in input CSV")

# Clean + prepare input texts
input_texts = (
    df_input["ProjectTitleEnglish"]
    .dropna()
    .astype(str)
    .str.strip()
)

# Remove empty strings after stripping
input_texts = [t for t in input_texts if t]


# Combined examples
EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES

# -----------------------
# extraction loop (ONE prompt, ONE pass)
# -----------------------
all_results = []

for i, text in enumerate(input_texts, start=1):
    result = lx.extract(
        text_or_documents=text,
        prompt_description=PROMPT,
        examples=EXAMPLES,
        model_id="gpt-4o",
        api_key=os.environ.get("OPENAI_API_KEY"),
        fence_output=True,
        use_schema_constraints=False,
    )
    all_results.append(result)


# -----------------------
# save outputs (single file)
# -----------------------
lx.io.save_annotated_documents(all_results, output_name=OUT_JSONL.name, output_dir=RUN_OUTPUT_DIR)

# also write pretty JSON for debugging
def jsonl_to_pretty_json(jsonl_path: Path, json_path: Path):
    docs = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

jsonl_to_pretty_json(OUT_JSONL, OUT_JSON)

print(f"Saved extraction: {OUT_JSONL}")
print(f"Saved debug JSON: {OUT_JSON}")