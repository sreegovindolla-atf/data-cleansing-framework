import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib

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
# SQL Server (Windows Auth)
# -----------------------

def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

TARGET_SCHEMA  = "dbo"
TARGET_TABLE   = "MasterTable_extracted"


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
    if not name:
        return ""
    return re.sub(r"\s+", "_", str(name).strip().lower())

def to_int_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group(0)) if m else None

def fmt_mp(n: int) -> str:
    return f"MP-{n:06d}"

def fmt_prj(mp_code: str, n: int) -> str:
    return f"PRJ-{mp_code}-{n:03d}"

# -----------------------
# IMPORTANT: reconstruct extractions from your "pair/triple" schema
# -----------------------
def parse_langextract_grouped_pairs(doc: dict):
    """
    Your schema stores each logical extraction across multiple rows, keyed by group_index:
      extraction_class == "extraction_class" -> extraction_text contains the field name
      extraction_class == "extraction_text"  -> extraction_text contains the value
      extraction_class == "extraction_index" -> extraction_text contains the logical index
    We reconstruct: {"extraction_class": <field>, "extraction_text": <value>, "extraction_index": <idx>}
    """
    raw = doc.get("extractions", [])
    if not isinstance(raw, list) or not raw:
        return []

    # Detect the "split rows" format
    looks_split = any(isinstance(e, dict) and e.get("extraction_class") in ("extraction_class", "extraction_text", "extraction_index")
                      for e in raw)
    if not looks_split:
        # already normal format
        return raw

    groups = {}  # group_index -> {"cls":..., "val":..., "idx":... , "order":...}
    for e in raw:
        if not isinstance(e, dict):
            continue
        g = e.get("group_index")
        if g is None:
            # if missing group_index, fallback to using extraction_index as unique key
            g = f"idx_{e.get('extraction_index', 10**9)}"

        groups.setdefault(g, {"cls": None, "val": None, "idx": None, "order": e.get("extraction_index", 10**9)})

        kind = e.get("extraction_class")
        txt  = e.get("extraction_text")

        # keep a stable ordering fallback
        groups[g]["order"] = min(groups[g]["order"], e.get("extraction_index", 10**9))

        if kind == "extraction_class":
            groups[g]["cls"] = txt
        elif kind == "extraction_text":
            groups[g]["val"] = txt
        elif kind == "extraction_index":
            # this is the logical index within the prompt output
            try:
                groups[g]["idx"] = int(str(txt).strip())
            except Exception:
                groups[g]["idx"] = None

    out = []
    for g, v in groups.items():
        if v["cls"] is None and v["val"] is None:
            continue
        out.append({
            "extraction_class": v["cls"],
            "extraction_text": v["val"],
            # Prefer the logical idx if present, else fallback to original ordering
            "extraction_index": v["idx"] if v["idx"] is not None else v["order"],
        })

    out.sort(key=lambda x: x.get("extraction_index", 10**9))
    return out

# -----------------------
# main parsing
# -----------------------
if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

rows = []

with open(INPUT_JSONL, "r", encoding="utf-8") as f:
    for doc_index, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        doc = json.loads(line)
        text = doc.get("text")

        master_project_code = fmt_mp(doc_index)

        # use reconstructed extractions
        reconstructed = parse_langextract_grouped_pairs(doc)

        # normalize + sort extractions
        exs = []
        for e in reconstructed:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if val is None or str(val).strip() == "":
                continue
            exs.append({"cls": cls, "val": val, "idx": e.get("extraction_index", 10**9)})

        exs.sort(key=lambda x: x["idx"])

        master_project_title = None
        for x in exs:
            if x["cls"] == "master_project_title":
                master_project_title = smart_title_case(x["val"])
                break

        # ---- beneficiary storage ----
        global_ben_count = None
        global_ben_group = None
        project_ben = {}

        project_counter = 0
        current_project_code = None
        current_project_title = None

        current_asset = None
        pending_qty = None
        pending_uom = None

        projects_with_assets = set()
        seen_projects_in_doc = []

        def get_ben(prj_code):
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

        for x in exs:
            cls, val = x["cls"], x["val"]

            # project boundary
            if cls == "project_title":
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                project_counter += 1
                current_project_title = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter)
                seen_projects_in_doc.append((current_project_code, current_project_title))

                project_ben[current_project_code] = {"beneficiary_count": None, "beneficiary_group_name": None}
                pending_qty = None
                pending_uom = None
                continue

            # beneficiaries
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

            # assets
            if cls == "asset":
                if current_asset is not None:
                    rows.append(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                ben_count, ben_group = get_ben(current_project_code)

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

            if cls in ("asset_quantity_uom", "uom", "unit_of_measure", "unit"):
                uom = smart_title_case(val)
                if current_asset is not None:
                    current_asset["asset_quantity_uom"] = uom
                else:
                    pending_uom = uom
                continue

        if current_asset is not None:
            rows.append(current_asset)
            projects_with_assets.add(current_asset["project_code"])
            current_asset = None

        # stub project if none found
        if not seen_projects_in_doc:
            project_counter = 1
            current_project_code = fmt_prj(master_project_code, project_counter)
            seen_projects_in_doc.append((current_project_code, None))
            project_ben[current_project_code] = {
                "beneficiary_count": global_ben_count,
                "beneficiary_group_name": global_ben_group,
            }

        # stub rows for projects with no assets
        for prj_code, prj_title in seen_projects_in_doc:
            if prj_code not in projects_with_assets:
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

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)

# -----------------------
# write to SQL Server
# -----------------------
engine = get_sql_server_engine()
df.to_sql(
    TARGET_TABLE,
    engine,
    schema=TARGET_SCHEMA,
    if_exists="append",
    index=False,
    chunksize=500,
)

print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("Rows written:", len(df))
print("Non-null counts:\n", df.notna().sum())