import json
import re
import pandas as pd
from datetime import datetime
from pathlib import Path

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

OUTPUT_DIR = Path("data/outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSONL_PATH = "extraction_results.jsonl"
OUTPUT_CSV = OUTPUT_DIR / f"output_{timestamp}.csv"


def normalize_class(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", "_", name.strip().lower())

def to_int_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group(0)) if m else None

def to_title_case(x):
    if x is None:
        return None
    return str(x).strip().title()



def build_row(doc: dict) -> dict:
    row = {
        "input_text": doc.get("text"),
        "project_title": None,
        "asset": None,
        "asset_quantity": 0,
        "beneficiary_count": None,
        "beneficiary_group_name": None,
    }

    extracted = {}
    for e in doc.get("extractions", []):
        cls = normalize_class(e.get("extraction_class"))
        val = e.get("extraction_text")
        if val is not None and str(val).strip() != "":
            extracted.setdefault(cls, []).append(val)

    def first(*keys):
        for k in keys:
            k = normalize_class(k)
            if k in extracted and extracted[k]:
                return extracted[k][0]
        return None

    row["project_title"] = to_title_case(first("project title", "project_title"))
    row["asset"] = to_title_case(first("asset"))
    row["asset_quantity"] = to_int_or_none(first("asset quantity", "asset_quantity")) or 0
    row["beneficiary_count"] = to_int_or_none(first("beneficiary count", "beneficiary_count"))
    row["beneficiary_group_name"] = to_title_case(first(
        "beneficiary group type",
        "beneficiary_group",
        "beneficiary_group_name"
    ))

    return row

rows = []
with open(JSONL_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        rows.append(build_row(doc))

df = pd.DataFrame(
    rows,
    columns=[
        "input_text",
        "project_title",
        "asset",
        "asset_quantity",
        "beneficiary_count",
        "beneficiary_group_name",
    ],
)

df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved: {OUTPUT_CSV}")
