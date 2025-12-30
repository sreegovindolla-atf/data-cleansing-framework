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
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

TARGET_SCHEMA = "dbo"
TARGET_TABLE  = "MasterTable_extracted"

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

def to_float_or_none(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else None

def is_missing_or_bad(x):
    if x is None or pd.isna(x):
        return True
    try:
        return float(x) <= 0.0
    except Exception:
        return True

def fmt_mp(n: int) -> str:
    return f"MP-{n:06d}"

def fmt_prj(mp_code: str, n: int) -> str:
    return f"PRJ-{mp_code}-{n:03d}"

# -----------------------
# IMPORTANT: reconstruct extractions from split schema
# -----------------------
def parse_langextract_grouped_pairs(doc: dict):
    raw = doc.get("extractions", [])
    if not isinstance(raw, list) or not raw:
        return []

    looks_split = any(
        isinstance(e, dict) and e.get("extraction_class") in ("extraction_class", "extraction_text", "extraction_index")
        for e in raw
    )

    if not looks_split:
        out = []
        for e in raw:
            if not isinstance(e, dict):
                continue
            out.append({
                "extraction_class": e.get("extraction_class"),
                "extraction_text": e.get("extraction_text"),
                "extraction_index": e.get("extraction_index", 10**9),
            })
        out.sort(key=lambda x: x.get("extraction_index", 10**9))
        return out

    groups = {}
    for e in raw:
        if not isinstance(e, dict):
            continue

        g = e.get("group_index")
        if g is None:
            g = f"idx_{e.get('extraction_index', 10**9)}"

        groups.setdefault(g, {"cls": None, "val": None, "idx": None, "order": e.get("extraction_index", 10**9)})

        kind = e.get("extraction_class")
        txt  = e.get("extraction_text")
        groups[g]["order"] = min(groups[g]["order"], e.get("extraction_index", 10**9))

        if kind == "extraction_class":
            groups[g]["cls"] = txt
        elif kind == "extraction_text":
            groups[g]["val"] = txt
        elif kind == "extraction_index":
            try:
                groups[g]["idx"] = int(str(txt).strip())
            except Exception:
                groups[g]["idx"] = None

    out = []
    for _, v in groups.items():
        if v["cls"] is None and v["val"] is None:
            continue
        out.append({
            "extraction_class": v["cls"],
            "extraction_text": v["val"],
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

        # raw master amount comes from JSONL top-level field
        master_project_amount_actual = to_float_or_none(doc.get("master_project_amount_actual"))

        master_project_code = fmt_mp(doc_index)

        reconstructed = parse_langextract_grouped_pairs(doc)

        exs = []
        for e in reconstructed:
            cls = normalize_class(e.get("extraction_class"))
            val = e.get("extraction_text")
            if val is None or str(val).strip() == "":
                continue
            exs.append({"cls": cls, "val": val, "idx": e.get("extraction_index", 10**9)})
        exs.sort(key=lambda x: x["idx"])

        # master title
        master_project_title = None
        for x in exs:
            if x["cls"] == "master_project_title":
                master_project_title = smart_title_case(x["val"])
                break

        master_project_amount_extracted = None
        for x in exs:
            if x["cls"] == "master_project_amount_extracted":
                master_project_amount_extracted = to_float_or_none(x["val"])
                break

        global_ben_count = None
        global_ben_group = None
        project_ben = {}

        # project-specific amount storage (from JSON: amount_extracted)
        project_amount_extracted_map = {}  # project_code -> float

        project_counter = 0
        current_project_code = None
        current_project_title = None

        current_asset = None
        pending_qty = None
        pending_uom = None

        projects_with_assets = set()
        seen_projects_in_doc = []

        # track row indices so we can backfill even if amount appears later
        row_indices_by_project = {}  # project_code -> list of row indices in rows[]

        def append_row(r: dict):
            rows.append(r)
            prj = r.get("project_code")
            if prj:
                row_indices_by_project.setdefault(prj, []).append(len(rows) - 1)

        def backfill_project_amount_extracted(prj_code: str, amt: float):
            for ridx in row_indices_by_project.get(prj_code, []):
                if rows[ridx].get("project_amount_extracted") is None or pd.isna(rows[ridx].get("project_amount_extracted")):
                    rows[ridx]["project_amount_extracted"] = amt

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

        def get_project_amt_extracted(prj_code):
            return project_amount_extracted_map.get(prj_code)

        for x in exs:
            cls, val = x["cls"], x["val"]

            # project boundary
            if cls == "project_title":
                if current_asset is not None:
                    append_row(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                project_counter += 1
                current_project_title = smart_title_case(val)
                current_project_code = fmt_prj(master_project_code, project_counter)
                seen_projects_in_doc.append((current_project_code, current_project_title))

                project_ben[current_project_code] = {"beneficiary_count": None, "beneficiary_group_name": None}
                project_amount_extracted_map.setdefault(current_project_code, None)

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

            # capture project amount extracted per project (your JSON uses "amount_extracted")
            if cls == "project_amount_extracted":
                a = to_float_or_none(val)
                if current_project_code is not None and a is not None:
                    if project_amount_extracted_map.get(current_project_code) is None:
                        project_amount_extracted_map[current_project_code] = a
                    backfill_project_amount_extracted(current_project_code, a)

                    # if an asset row is currently open, update it too
                    if (
                        current_asset is not None
                        and current_asset.get("project_code") == current_project_code
                        and is_missing_or_bad(current_asset.get("project_amount_extracted"))
                    ):
                        current_asset["project_amount_extracted"] = a
                continue

            # assets
            if cls == "asset":
                if current_asset is not None:
                    append_row(current_asset)
                    projects_with_assets.add(current_asset["project_code"])
                    current_asset = None

                ben_count, ben_group = get_ben(current_project_code)
                amt_extr = get_project_amt_extracted(current_project_code)

                current_asset = {
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_amount_extracted": master_project_amount_extracted,
                    "project_code": current_project_code,
                    "project_title": current_project_title,
                    "beneficiary_count": ben_count,
                    "beneficiary_group_name": ben_group,
                    "asset": smart_title_case(val),
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "project_amount_extracted": amt_extr,
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

        # flush last asset
        if current_asset is not None:
            append_row(current_asset)
            projects_with_assets.add(current_asset["project_code"])
            current_asset = None

        # backfill ALL rows for projects that got amount captured
        for prj_code, amt in project_amount_extracted_map.items():
            if amt is not None:
                backfill_project_amount_extracted(prj_code, amt)

        # stub project if none found
        if not seen_projects_in_doc:
            project_counter = 1
            current_project_code = fmt_prj(master_project_code, project_counter)
            seen_projects_in_doc.append((current_project_code, None))
            project_ben[current_project_code] = {"beneficiary_count": global_ben_count, "beneficiary_group_name": global_ben_group}
            project_amount_extracted_map[current_project_code] = None

        # stub rows for projects with no assets
        for prj_code, prj_title in seen_projects_in_doc:
            if prj_code not in projects_with_assets:
                bc = project_ben.get(prj_code, {}).get("beneficiary_count") or global_ben_count
                bg = project_ben.get(prj_code, {}).get("beneficiary_group_name") or global_ben_group

                append_row({
                    "master_project_code": master_project_code,
                    "master_project_title": master_project_title,
                    "master_project_amount_actual": master_project_amount_actual,
                    "master_project_amount_extracted": master_project_amount_extracted,
                    "project_code": prj_code,
                    "project_title": prj_title,
                    "beneficiary_count": bc,
                    "beneficiary_group_name": bg,
                    "asset": None,
                    "asset_quantity": None,
                    "asset_quantity_uom": None,
                    "project_amount_extracted": project_amount_extracted_map.get(prj_code),
                    "input_text": text,
                })

# -----------------------
# write output
# -----------------------
df = pd.DataFrame(rows)

# Ensure numeric
df["master_project_amount_actual"] = pd.to_numeric(df.get("master_project_amount_actual"), errors="coerce")
df["master_project_amount_extracted"] = pd.to_numeric(df.get("master_project_amount_extracted"), errors="coerce")
df["project_amount_extracted"] = pd.to_numeric(df.get("project_amount_extracted"), errors="coerce")

# number of unique projects per master
project_counts = (
    df[["master_project_code", "project_code"]]
    .drop_duplicates()
    .groupby("master_project_code")["project_code"]
    .nunique()
)
df["_project_count"] = df["master_project_code"].map(project_counts).fillna(1).astype(int)

# -----------------------
# NEW FIELD: project_amount_actual
# project_amount_actual = master_project_amount_actual / number of projects under that master
# (same value repeated per project rows within the master)
# -----------------------
df["project_amount_actual"] = df["master_project_amount_actual"] / df["_project_count"]

# Optional safety: if master is missing/bad, keep project_amount_actual as NaN
df.loc[df["master_project_amount_actual"].apply(is_missing_or_bad), "project_amount_actual"] = pd.NA

df.drop(columns=["_project_count"], inplace=True)

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
    "master_project_amount_actual",
    "master_project_amount_extracted",
    "project_amount_actual",
    "project_amount_extracted",
]

# Backwards-compat name: your rows dict currently uses "project_amount_extracted"
# but you requested "project_amount_extracted" (same), so just ensure it's present.
if "project_amount_extracted" not in df.columns and "project_amount" in df.columns:
    df["project_amount_extracted"] = df["project_amount"]

for c in FINAL_COLUMNS:
    if c not in df.columns:
        df[c] = None

df = df[FINAL_COLUMNS]

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False)

engine = get_sql_server_engine()
df.to_sql(
    TARGET_TABLE,
    engine,
    schema=TARGET_SCHEMA,
    if_exists="replace",
    index=False,
    chunksize=500,
)

print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("Rows written:", len(df))
print("Non-null counts:\n", df.notna().sum())
