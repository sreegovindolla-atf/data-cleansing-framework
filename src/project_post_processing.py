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

# Function to build master project code in format 'MP-<4digit number>'
def fmt_mp(n: int) -> str:
    return f"MP-{n:04d}"

# Function to build project code in format '# PRJ-<MP code>-<3digit number>'
def fmt_prj(mp_code: str, n: int) -> str:
    return f"PRJ-{mp_code}-{n:03d}"


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

# -------------------------------
# Add codes (post-processing)
# -------------------------------
# 1) Master project code: MP-0001, MP-0002, ... (based on unique master_project_title)
#    If master_project_title is missing, we treat it as its own group per input_text
#    so that codes are still generated deterministically.
df["_mp_group_key"] = df["master_project_title"]
missing_mp = df["_mp_group_key"].isna() | (df["_mp_group_key"].astype(str).str.strip() == "")
df.loc[missing_mp, "_mp_group_key"] = df.loc[missing_mp, "input_text"].fillna("").astype(str)

# Stable ordering for code assignment (so reruns produce same numbering for same content/order)
mp_keys_in_order = []
seen = set()
for k in df["_mp_group_key"].tolist():
    if k not in seen:
        seen.add(k)
        mp_keys_in_order.append(k)

mp_code_map = {k: fmt_mp(i + 1) for i, k in enumerate(mp_keys_in_order)}
df["master_project_code"] = df["_mp_group_key"].map(mp_code_map)

# 2) Project code: PRJ-<MP code>-001, -002, ... per master_project_code
#    Only generate for rows with a real project_title; keep None otherwise.
df["_has_project"] = df["project_title"].notna() & (df["project_title"].astype(str).str.strip() != "")
df["_prj_seq"] = (
    df[df["_has_project"]]
      .groupby("master_project_code")
      .cumcount()
      .add(1)
      .reindex(df.index)
)

df["project_code"] = None
df.loc[df["_has_project"], "project_code"] = df.loc[df["_has_project"]].apply(
    lambda r: fmt_prj(r["master_project_code"], int(r["_prj_seq"])),
    axis=1
)

# cleanup temp cols
df.drop(columns=["_mp_group_key", "_has_project", "_prj_seq"], inplace=True)

# final column order
FINAL_COLUMNS = [
    "input_text",
    "master_project_code",
    "master_project_title",
    "project_code",
    "project_title",
]
df = df[FINAL_COLUMNS]

df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved: {OUTPUT_CSV}")
