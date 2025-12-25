import json
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_JSONL = RUN_OUTPUT_DIR / "extraction_results.jsonl"
OUTPUT_CSV = RUN_OUTPUT_DIR / "output.csv"

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

def to_int_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group(0)) if m else None


def build_rows(doc: dict) -> list[dict]:
    base = {
        "input_text": doc.get("text"),
        "project_title": None,
        "beneficiary_count": None,
        "beneficiary_group_name": None,
    }

    # Normalize and sort extractions in the order they were produced
    exs = []
    for e in doc.get("extractions", []):
        cls = normalize_class(e.get("extraction_class"))
        val = e.get("extraction_text")
        if val is None or str(val).strip() == "":
            continue
        exs.append({
            "cls": cls,
            "val": val,
            "idx": e.get("extraction_index", 10**9),  # fallback large
        })

    exs.sort(key=lambda x: x["idx"])

    # 1) document-level fields (single)
    for x in exs:
        if x["cls"] == "project_title":
            base["project_title"] = smart_title_case(x["val"])
        elif x["cls"] == "beneficiary_count":
            base["beneficiary_count"] = to_int_or_none(x["val"])
        elif x["cls"] in ("beneficiary_group_name", "beneficiary_group_type"):
            base["beneficiary_group_name"] = smart_title_case(x["val"])

    # 2) explode asset rows by sequence
    rows = []
    current = None
    pending_qty = None
    pending_uom = None

    def new_row(asset_val):
        return {
            "input_text": base["input_text"],
            "project_title": base["project_title"],
            "asset": smart_title_case(asset_val),
            "asset_quantity": None,
            "asset_quantity_uom": None,
            "beneficiary_count": base["beneficiary_count"],
            "beneficiary_group_name": base["beneficiary_group_name"],
        }

    for x in exs:
        cls, val = x["cls"], x["val"]

        if cls == "asset":
            # finalize previous row
            if current is not None:
                rows.append(current)

            # start new row
            current = new_row(val)

            # attach any pending qty/uom that appeared before asset
            if pending_qty is not None:
                current["asset_quantity"] = pending_qty
                pending_qty = None
            if pending_uom is not None:
                current["asset_quantity_uom"] = pending_uom
                pending_uom = None

        elif cls == "asset_quantity":
            qty = to_int_or_none(val)
            if current is None:
                pending_qty = qty
            else:
                current["asset_quantity"] = qty

        elif cls in ("asset_quantity_uom", "uom", "unit_of_measure"):
            uom = smart_title_case(val)
            if current is None:
                pending_uom = uom
            else:
                current["asset_quantity_uom"] = uom

    # finalize last row
    if current is not None:
        rows.append(current)

    # fallback: if no assets found, return one row
    if not rows:
        rows.append({
            "input_text": base["input_text"],
            "project_title": base["project_title"],
            "asset": None,
            "asset_quantity": None,
            "asset_quantity_uom": None,
            "beneficiary_count": base["beneficiary_count"],
            "beneficiary_group_name": base["beneficiary_group_name"],
        })

    return rows


rows = []
with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        rows.extend(build_rows(doc))

df = pd.DataFrame(
    rows,
    columns=[
        "input_text",
        "project_title",
        "asset",
        "asset_quantity",
        "asset_quantity_uom",
        "beneficiary_count",
        "beneficiary_group_name",
    ],
    )
    
df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved: {OUTPUT_CSV}")
