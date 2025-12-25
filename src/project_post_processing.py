import json
import re
import pandas as pd
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Change this filename if your project_extraction.py saves with a different name
INPUT_JSONL = RUN_OUTPUT_DIR / "extraction_results.jsonl"
OUTPUT_CSV = RUN_OUTPUT_DIR / "projects.csv"

STOP_WORDS = {"of", "with", "at", "in", "on", "for", "to", "and", "or", "the", "a", "an", "by", "from"}

def smart_title_case(text):
    if text is None:
        return None
    words = str(text).strip().lower().split()
    if not words:
        return None
    out = []
    for i, w in enumerate(words):
        if i == 0 or w not in STOP_WORDS:
            out.append(w.capitalize())
        else:
            out.append(w)
    return " ".join(out)

def normalize_class(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", "_", name.strip().lower())

def build_project_rows(doc: dict) -> list[dict]:
    """
    One input_text (master) can contain multiple project_title extractions.
    Output: one row per project_title, with master_project_title repeated.
    """
    base = {
        "input_text": doc.get("text"),
        "master_project_title": None,
    }

    # Keep extraction order (helps if you later add project_segment etc.)
    exs = []
    for e in doc.get("extractions", []):
        cls = normalize_class(e.get("extraction_class"))
        val = e.get("extraction_text")
        if val is None or str(val).strip() == "":
            continue
        exs.append({
            "cls": cls,
            "val": val,
            "idx": e.get("extraction_index", 10**9),
        })
    exs.sort(key=lambda x: x["idx"])

    project_titles = []

    for x in exs:
        if x["cls"] == "master_project_title":
            base["master_project_title"] = smart_title_case(x["val"])
        elif x["cls"] == "project_title":
            project_titles.append(smart_title_case(x["val"]))

    # If no project titles extracted, still emit one row (optional)
    # You can remove this fallback if you want only extracted projects.
    if not project_titles:
        return [{
            "input_text": base["input_text"],
            "master_project_title": base["master_project_title"],
            "project_title": None,
            "project_index": 0,
        }]

    rows = []
    for idx, pt in enumerate(project_titles):
        rows.append({
            "input_text": base["input_text"],
            "master_project_title": base["master_project_title"],
            "project_title": pt,
            "project_index": idx,  # useful key for later joining to assets
        })

    return rows


# ---------- main ----------
if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

rows = []
with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        rows.extend(build_project_rows(doc))

df = pd.DataFrame(rows)

# enforce schema + order (drops unexpected keys safely)
COLUMNS = [
    "input_text",
    "master_project_title",
    "project_title",
    "project_index",
]
for c in COLUMNS:
    if c not in df.columns:
        df[c] = None
df = df[COLUMNS]

df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved: {OUTPUT_CSV}")
