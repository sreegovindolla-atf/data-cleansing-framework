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

        # normalize + sort extractions
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

        # ---- beneficiary storage ----
        # global (doc-level) fallback
        global_ben_count = None
        global_ben_group = None

        # project-specific beneficiaries keyed by project_code
        project_ben = {}  # project_code -> {"beneficiary_count":..., "beneficiary_group_name":...}

        # ---- iterate and build combined rows ----
        project_counter_per_mp = 0
        current_project_code = None
        current_project_title = None

        current_asset = None
        pending_qty = None
        pending_uom = None

        projects_with_assets = set()
        seen_projects_in_doc = []  # [(project_code, project_title)]

        for x in exs:
            cls, val = x["cls"], x["val"]

            # -------------------------
            # project boundary
            # -------------------------
            if cls == "project_title":
                # flush any open asset row
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                project_counter_per_mp += 1
                current_project_title = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter_per_mp)
                seen_projects_in_doc.append((current_project_code, current_project_title))

                # init beneficiary bucket for this project
                project_ben[current_project_code] = {
                    "beneficiary_count": None,
                    "beneficiary_group_name": None,
                }

                pending_qty = None
                pending_uom = None
                continue

            # -------------------------
            # beneficiaries
            # (project-specific if we are inside a project; else global fallback)
            # -------------------------
            if cls == "beneficiary_count":
                b = to_int_or_none(val)
                if current_project_code is not None:
                    if project_ben[current_project_code]["beneficiary_count"] is None:
                        project_ben[current_project_code]["beneficiary_count"] = b
                else:
                    if global_ben_count is None:
                        global_ben_count = b
                continue

            if cls in ("beneficiary_group_name", "beneficiary_group_type"):
                g = smart_title_case(val)
                if current_project_code is not None:
                    if project_ben[current_project_code]["beneficiary_group_name"] is None:
                        project_ben[current_project_code]["beneficiary_group_name"] = g
                else:
                    if global_ben_group is None:
                        global_ben_group = g
                continue

            # helper: get beneficiary values for current project (with fallback)
            def _get_ben(prj_code):
                bc = None
                bg = None
                if prj_code in project_ben:
                    bc = project_ben[prj_code].get("beneficiary_count")
                    bg = project_ben[prj_code].get("beneficiary_group_name")
                if bc is None:
                    bc = global_ben_count
                if bg is None:
                    bg = global_ben_group
                return bc, bg

            # -------------------------
            # assets
            # -------------------------
            if cls == "asset":
                # flush previous asset row
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                ben_count, ben_group = _get_ben(current_project_code)

                current_asset = {
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "project_code": current_project_code,
                    "project_title": current_project_title,
                    "beneficiary_count": ben_count,
                    "beneficiary_group_name": ben_group,
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

        # emit stub rows for projects with no assets
        for prj_code, prj_title in seen_projects_in_doc:
            if prj_code not in projects_with_assets:
                # beneficiary fallback resolution
                bc = project_ben.get(prj_code, {}).get("beneficiary_count")
                bg = project_ben.get(prj_code, {}).get("beneficiary_group_name")
                if bc is None:
                    bc = global_ben_count
                if bg is None:
                    bg = global_ben_group

                rows.append({
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "project_code": prj_code,
                    "project_title": prj_title,
                    "beneficiary_count": bc,
                    "beneficiary_group_name": bg,
                    "asset": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "input_text": text,
                })

# -----------------------
# write output
# -----------------------
df = pd.DataFrame(rows)

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
