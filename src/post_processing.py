import json
import re
import pandas as pd
from pathlib import Path
import argparse

# -----------------------
# args + paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID

INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_CSV     = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction.csv"

STOP_WORDS = {"of", "with", "at", "in", "on", "for", "to", "and", "or", "the", "a", "an", "by", "from"}

# -----------------------
# helpers
# -----------------------
def smart_title_case(text):
    if text is None:
        return None
    words = str(text).strip().lower().split()
    if not words:
        return None
    out = []
    for i, w in enumerate(words):
        out.append(w.capitalize() if i == 0 or w not in STOP_WORDS else w)
    return " ".join(out)

def normalize_class(name: str) -> str:
    return re.sub(r"\s+", "_", name.strip().lower()) if name else ""

def to_int_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group(0)) if m else None

def fmt_mp(n: int) -> str:
    return f"MP-{n:04d}"

def fmt_prj(mp_code: str, n: int) -> str:
    return f"PRJ-{mp_code}-{n:03d}"

# -----------------------
# main parsing
# -----------------------
rows = []

master_project_counter = 0
master_project_map = {}  # mp_key -> MP-0001

with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for doc_index, line in enumerate(f):
        line = line.strip()
        if not line:
            continue

        doc = json.loads(line)
        text = doc.get("text")

        exs = []
        for e in doc.get("extractions", []):
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if val is None or str(val).strip() == "":
                continue
            exs.append({"cls": cls, "val": val, "idx": e.get("extraction_index", 10**9)})
        exs.sort(key=lambda x: x["idx"])

        # ---- master project ----
        master_project_title = None
        for x in exs:
            if x["cls"] == "master_project_title":
                master_project_title = smart_title_case(x["val"])
                break

        mp_key = master_project_title or (text or "")
        if mp_key not in master_project_map:
            master_project_counter += 1
            master_project_map[mp_key] = fmt_mp(master_project_counter)
        master_project_code = master_project_map[mp_key]

        # ---- beneficiaries (doc-level fallback) ----
        beneficiary_count = None
        beneficiary_group_name = None
        for x in exs:
            if x["cls"] == "beneficiary_count" and beneficiary_count is None:
                beneficiary_count = to_int_or_none(x["val"])
            elif x["cls"] in ("beneficiary_group_name", "beneficiary_group_type") and beneficiary_group_name is None:
                beneficiary_group_name = smart_title_case(x["val"])

        # ---- iterate and build combined rows ----
        project_counter_per_mp = 0
        current_project_code = None
        current_project_title = None

        current_asset = None
        pending_qty = None
        pending_uom = None

        # Track which project_codes got at least one asset row (for "project stub" rows)
        projects_with_assets = set()
        seen_projects_in_doc = []

        for x in exs:
            cls, val = x["cls"], x["val"]

            if cls == "project_title":
                # flush any current asset before switching project
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                project_counter_per_mp += 1
                current_project_title = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter_per_mp)

                seen_projects_in_doc.append((current_project_code, current_project_title))

                # reset pending
                pending_qty = None
                pending_uom = None
                continue

            if cls == "asset":
                # flush previous asset row
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                current_asset = {
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "project_code": current_project_code,
                    "project_title": current_project_title,
                    "beneficiary_count": beneficiary_count,
                    "beneficiary_group_name": beneficiary_group_name,
                    "asset": smart_title_case(val),
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "input_text": text,
                }

                # attach pending qty/uom that came before asset
                if pending_qty is not None:
                    current_asset["asset_quantity"] = pending_qty
                    pending_qty = None
                if pending_uom is not None:
                    current_asset["asset_quantity_uom"] = pending_uom
                    pending_uom = None

                continue

            if cls == "asset_quantity":
                qty = to_int_or_none(val)
                if current_asset is not None:
                    current_asset["asset_quantity"] = qty
                else:
                    pending_qty = qty
                continue

            if cls in ("asset_quantity_uom", "uom", "unit_of_measure"):
                uom = smart_title_case(val)
                if current_asset is not None:
                    current_asset["asset_quantity_uom"] = uom
                else:
                    pending_uom = uom
                continue

        # flush last asset in doc
        if current_asset is not None:
            rows.append(current_asset)
            projects_with_assets.add(current_asset["project_code"])
            current_asset = None

        # If a project had no assets, still emit one row for it (asset fields null)
        for prj_code, prj_title in seen_projects_in_doc:
            if prj_code not in projects_with_assets:
                rows.append({
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "project_code": prj_code,
                    "project_title": prj_title,
                    "beneficiary_count": beneficiary_count,
                    "beneficiary_group_name": beneficiary_group_name,
                    "asset": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "input_text": text,
                })

# -----------------------
# write output
# -----------------------
df = pd.DataFrame(rows)

# Optional: enforce column order
FINAL_COLUMNS = [
    "master_project_code",
    "master_project_title",
    "project_code",
    "project_title",
    "beneficiary_count",
    "beneficiary_group_name",
    "asset",
    "asset_quantity",
    "asset_quantity_uom",
    "input_text",
]
for c in FINAL_COLUMNS:
    if c not in df.columns:
        df[c] = None
df = df[FINAL_COLUMNS]

df.to_csv(OUT_CSV, index=False)
print(f"Saved combined output: {OUT_CSV}")
