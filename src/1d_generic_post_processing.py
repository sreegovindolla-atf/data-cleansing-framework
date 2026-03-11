import json
import re
import argparse
import urllib
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.types import NVARCHAR, Integer, Float, DateTime

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.generic_extraction_config import GENERIC_CONFIG, BASE_OUTPUT_DIR
from utils.post_processing_helpers import normalize_class, _is_blank


# =========================================================
# ARGS
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument("--entity", required=True, help="Example: asset / beneficiary_group / project_type")
parser.add_argument("--run-id", required=True)
parser.add_argument(
    "--upstream-ids-file",
    required=False,
    help="Txt file containing upstream processed indexes from 1a/1b. Read-only."
)
parser.add_argument(
    "--all-from-jsonl",
    action="store_true",
    help="If set, post-process all rows in the entity JSONL instead of only upstream indexes."
)
args = parser.parse_args()

ENTITY = args.entity.strip()
RUN_ID = args.run_id.strip()
ALL_FROM_JSONL = args.all_from_jsonl
UPSTREAM_IDS_FILE = Path(args.upstream_ids_file) if args.upstream_ids_file else None

if ENTITY not in GENERIC_CONFIG:
    raise ValueError(
        f"Unknown entity='{ENTITY}'. Allowed values: {', '.join(sorted(GENERIC_CONFIG.keys()))}"
    )

CFG = GENERIC_CONFIG[ENTITY]["post_processing"]


# =========================================================
# INPUT / OUTPUT PATHS
# =========================================================
RUN_OUTPUT_DIR = BASE_OUTPUT_DIR / RUN_ID
INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_{CFG['input_jsonl_suffix']}"
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_{CFG['output_csv_suffix']}"


# =========================================================
# SQL SERVER CONNECTION
# =========================================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )

engine = get_sql_server_engine()


# =========================================================
# HELPERS
# =========================================================
def safe_text(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def load_ids_txt(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def parse_jsonl(path: Path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"[WARN] Skipping invalid JSONL line {line_no}: {e}")
    return rows


def to_none_if_nullish(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    if s.lower() in {"null", "none", "n/a", "na", "nan", "nil", "-", "--"}:
        return None
    return s


def to_int_or_none(x):
    x = to_none_if_nullish(x)
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if re.fullmatch(r"\d+(\.0+)?", s):
        return int(float(s))
    if re.fullmatch(r"\d+", s):
        return int(s)
    return None


def to_float_or_none(x):
    x = to_none_if_nullish(x)
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return float(s)
    return None


def normalize_asset_text(asset: str):
    if asset is None:
        return None

    s = str(asset).strip()
    if s == "" or s.lower() in {"null", "none", "n/a", "na", "nan", "-"}:
        return None

    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bdaycare\b", "day care", s, flags=re.IGNORECASE)
    s = re.sub(r"\bhealthcare\b", "health care", s, flags=re.IGNORECASE)
    return s.title()


def normalize_uom(uom: str):
    uom = to_none_if_nullish(uom)
    if uom is None:
        return None

    s = str(uom).strip()
    key = s.lower()

    mapping = {
        "l": "Liter",
        "liter": "Liter",
        "litre": "Liter",
        "liters": "Liter",
        "litres": "Liter",
        "gal": "Gallon",
        "gallon": "Gallon",
        "gallons": "Gallon",
        "m3": "Cubic Meter",
        "m³": "Cubic Meter",
        "cubic meter": "Cubic Meter",
        "cubic meters": "Cubic Meter",
        "m2": "Square Meter",
        "m²": "Square Meter",
        "sqm": "Square Meter",
        "sq m": "Square Meter",
        "sq. m": "Square Meter",
        "square meter": "Square Meter",
        "square meters": "Square Meter",
        "kva": "kVA",
        "kv-a": "kVA",
        "k.v.a": "kVA",
        "kw": "kW",
        "kilowatt": "kW",
        "kilowatts": "kW",
        "w": "W",
        "watt": "W",
        "watts": "W",
    }

    canon = mapping.get(key)
    if canon:
        return canon

    canonical_set = {"Liter", "Gallon", "Cubic Meter", "Square Meter", "kVA", "kW", "W"}
    if s in canonical_set:
        return s

    return s


def ensure_target_table_exists(
    df_sample: pd.DataFrame,
    target_schema: str,
    target_table: str,
    dtype_map: dict
) -> None:
    full_name = f"{target_schema}.{target_table}"

    with engine.begin() as conn:
        exists = conn.execute(
            sql_text(
                f"""
                SELECT 1
                WHERE OBJECT_ID('{full_name}', 'U') IS NOT NULL
                """
            )
        ).fetchone()

    if exists:
        return

    print(f"[INFO] Target table does not exist. Creating: {full_name}")

    df_sample.head(0).to_sql(
        name=target_table,
        schema=target_schema,
        con=engine,
        if_exists="append",
        index=False,
        dtype=dtype_map,
        method=None,
    )


# =========================================================
# ENTITY-SPECIFIC ROW BUILDERS
# =========================================================
def build_project_type_row(record: dict) -> dict | None:
    project_code = safe_text(record.get("project_code"))
    if not project_code:
        return None

    exs = record.get("extractions") or []

    project_type = None
    for e in exs:
        cls = normalize_class(e.get("extraction_class"))
        val = e.get("extraction_text")
        if cls == "project_type" and not _is_blank(val):
            project_type = str(val).strip()
            break

    if not project_type:
        return None

    return {
        "project_code": project_code,
        "project_type": project_type,
    }


def build_beneficiary_row(record: dict) -> dict | None:
    project_code = safe_text(record.get("project_code"))
    if not project_code:
        return None

    exs = record.get("extractions") or []

    beneficiary_group = None
    beneficiary_count = 0

    for e in exs:
        cls = normalize_class(e.get("extraction_class"))
        val = e.get("extraction_text")

        if cls == "beneficiary_group" and not _is_blank(val):
            beneficiary_group = str(val).strip()

        elif cls == "beneficiary_count" and not _is_blank(val):
            beneficiary_count = to_int_or_none(val)
            if beneficiary_count is None:
                beneficiary_count = 0

    if not beneficiary_group:
        return None

    return {
        "project_code": project_code,
        "beneficiary_group": beneficiary_group,
        "beneficiary_count": beneficiary_count,
    }


def build_asset_row(record: dict) -> dict | None:
    project_code = safe_text(record.get("project_code"))
    if not project_code:
        return None

    exs = record.get("extractions") or []

    asset = None
    asset_category = None
    asset_quantity = None
    asset_capacity = None
    asset_capacity_uom = None

    for e in exs:
        cls = normalize_class(e.get("extraction_class"))
        val = to_none_if_nullish(e.get("extraction_text"))

        if cls == "asset" and asset is None:
            asset = val
        elif cls == "asset_category" and asset_category is None:
            asset_category = val
        elif cls == "asset_quantity" and asset_quantity is None:
            asset_quantity = val
        elif cls == "asset_capacity" and asset_capacity is None:
            asset_capacity = val
        elif cls == "asset_capacity_uom" and asset_capacity_uom is None:
            asset_capacity_uom = val

    if asset is None:
        asset_category = None
        asset_quantity = None
        asset_capacity = None
        asset_capacity_uom = None

    out = {
        "project_code": project_code,
        "asset": normalize_asset_text(asset),
        "asset_category": asset_category,
        "asset_quantity": to_int_or_none(asset_quantity),
        "asset_capacity": to_float_or_none(asset_capacity),
        "asset_capacity_uom": normalize_uom(asset_capacity_uom),
    }

    if out["asset_capacity"] is None:
        out["asset_capacity_uom"] = None

    if out["asset"] is None:
        out["asset_category"] = None
        out["asset_quantity"] = None
        out["asset_capacity"] = None
        out["asset_capacity_uom"] = None

    return out


def build_output_row(record: dict) -> dict | None:
    if ENTITY == "project_type":
        return build_project_type_row(record)
    if ENTITY == "beneficiary_group":
        return build_beneficiary_row(record)
    if ENTITY == "asset":
        return build_asset_row(record)
    raise ValueError(f"Unsupported entity for post-processing: {ENTITY}")


def get_dtype_map() -> dict:
    if ENTITY == "project_type":
        return {
            "project_code": NVARCHAR(length=255),
            "project_type": NVARCHAR(length=200),
            "updated_ts": DateTime(),
        }

    if ENTITY == "beneficiary_group":
        return {
            "project_code": NVARCHAR(length=255),
            "beneficiary_group": NVARCHAR(length=200),
            "beneficiary_count": Integer(),
            "updated_ts": DateTime(),
        }

    if ENTITY == "asset":
        return {
            "project_code": NVARCHAR(length=255),
            "asset": NVARCHAR(length=400),
            "asset_category": NVARCHAR(length=100),
            "asset_quantity": Integer(),
            "asset_capacity": Float(),
            "asset_capacity_uom": NVARCHAR(length=60),
            "updated_ts": DateTime(),
        }

    raise ValueError(f"Unsupported entity for dtype map: {ENTITY}")


# =========================================================
# LOAD INPUTS
# =========================================================
print(f"[INFO] Entity: {ENTITY}")
print(f"[INFO] RUN_OUTPUT_DIR: {RUN_OUTPUT_DIR}")

pk = CFG["primary_key"]

if ALL_FROM_JSONL:
    print("[INFO] Mode: all_from_jsonl")

    if not INPUT_JSONL.exists():
        print(f"[INFO] JSONL not found for all_from_jsonl mode: {INPUT_JSONL}")
        print("[INFO] Nothing to process.")
        raise SystemExit(0)

    records = parse_jsonl(INPUT_JSONL)
    print(f"[INFO] JSONL rows read: {len(records)}")
    filtered_records = records

else:
    if not UPSTREAM_IDS_FILE:
        raise ValueError(
            "Incremental mode requires --upstream-ids-file. "
            "Either pass it from main.py or use --all-from-jsonl."
        )

    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(
            f"Missing upstream ids file: {UPSTREAM_IDS_FILE}"
        )

    upstream_indexes = load_ids_txt(UPSTREAM_IDS_FILE)
    print(f"[INFO] Mode: incremental")
    print(f"[INFO] Upstream index count: {len(upstream_indexes)}")

    if not upstream_indexes:
        print("[INFO] No indexes found in upstream ids file.")
        print("[INFO] Nothing to post-process.")
        raise SystemExit(0)

    if not INPUT_JSONL.exists():
        raise FileNotFoundError(
            f"Upstream ids file exists and contains indexes, but JSONL is missing: {INPUT_JSONL}"
        )

    records = parse_jsonl(INPUT_JSONL)
    print(f"[INFO] JSONL rows read: {len(records)}")

    filtered_records = []
    for r in records:
        record_index = safe_text(r.get("index"))
        if record_index and record_index in upstream_indexes:
            filtered_records.append(r)

print(f"[INFO] JSONL rows after filter: {len(filtered_records)}")

rows = []
for r in filtered_records:
    out = build_output_row(r)
    if out is not None:
        rows.append(out)

df = pd.DataFrame(rows)

if df.empty:
    print("[INFO] No rows found after filtering / parsing.")
    raise SystemExit(0)


# =========================================================
# BASIC CLEANUP
# =========================================================
df = df[df[pk].notna()].copy()
df[pk] = df[pk].astype(str).str.strip()
df = df[df[pk] != ""].copy()
df = df.drop_duplicates(subset=[pk], keep="first").copy()

df["updated_ts"] = datetime.now().replace(microsecond=0)

final_columns = CFG["output_columns"] + ["updated_ts"]
for c in final_columns:
    if c not in df.columns:
        df[c] = None

df = df[final_columns].copy()

print(f"[INFO] Final rows to upsert: {len(df)}")
print(f"[INFO] Distinct {pk} to upsert: {df[pk].nunique()}")


# =========================================================
# SAVE CSV
# =========================================================
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print(f"[INFO] Saved CSV: {OUT_CSV}")
print(f"[INFO] Final row count: {len(df)}")


# =========================================================
# WRITE TO SQL SERVER (UPSERT BY PRIMARY KEY)
# =========================================================
target_schema = CFG["target_table"].split(".")[0]
target_table = CFG["target_table"].split(".")[-1]

dtype_map = get_dtype_map()

ensure_target_table_exists(
    df_sample=df,
    target_schema=target_schema,
    target_table=target_table,
    dtype_map=dtype_map,
)

pk_values_to_upsert = sorted(set(df[pk].dropna().astype(str)))
if not pk_values_to_upsert:
    print(f"[INFO] No {pk} values to upsert.")
    raise SystemExit(0)

print(
    f"[INFO] Upserting {len(pk_values_to_upsert)} {pk} values into {target_schema}.{target_table}"
)

df_pk = pd.DataFrame({pk: pk_values_to_upsert})

with engine.begin() as conn:
    conn.execute(sql_text("IF OBJECT_ID('tempdb..#pk_ids') IS NOT NULL DROP TABLE #pk_ids;"))
    conn.execute(
        sql_text(
            f"""
            CREATE TABLE #pk_ids (
                [{pk}] NVARCHAR(255) NOT NULL PRIMARY KEY
            );
            """
        )
    )

    df_pk.to_sql(
        "#pk_ids",
        conn,
        if_exists="append",
        index=False,
        method=None,
    )

    conn.execute(
        sql_text(
            f"""
            DELETE T
            FROM {target_schema}.{target_table} AS T
            INNER JOIN #pk_ids AS P
                ON CAST(T.[{pk}] AS NVARCHAR(255)) COLLATE DATABASE_DEFAULT
                 = P.[{pk}] COLLATE DATABASE_DEFAULT;
            """
        )
    )

df.to_sql(
    name=target_table,
    schema=target_schema,
    con=engine,
    if_exists="append",
    index=False,
    dtype=dtype_map,
    chunksize=2000,
    method=None,
)

print(f"[DONE] Upserted into SQL table: {target_schema}.{target_table}")