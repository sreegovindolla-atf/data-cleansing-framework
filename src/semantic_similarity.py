import numpy as np
import pandas as pd
import faiss
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, UnicodeText, DateTime, Float, Boolean
import urllib
import ast
from datetime import datetime, timezone
from sqlalchemy import text as sql_text
import argparse
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.app_config import SEMANTIC_SIMILARITY_CONFIG as CONFIG

# =========================================================
# Args
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument(
    "--source-mode",
    required=True,
    choices=["master projects", "projects"],
    help="db = use embeddings from the master projects table; extracted = use embeddings from the projects table"
)
# Histogram flag (default False)
parser.add_argument(
    "--histogram",
    action="store_true",
    help="If set, uses a lower similarity threshold (0.5) and writes to histogram target table"
)

args = parser.parse_args()

SOURCE_MODE = args.source_mode
HISTOGRAM_MODE = bool(args.histogram)

# -----------------------------
# Config
# -----------------------------
# Default similarity configurations
TOP_K = 20
DEFAULT_SIMILARITY_THRESHOLD = 0.75
HISTOGRAM_THRESHOLD = 0.5

# Target Schema
DEAFULT_TARGET_SCHEMA = "silver"
HISTOGRAM_TARGET_SCHEMA = "histogram"

# Mode dependent configurations
MODE_CFG = CONFIG[SOURCE_MODE]

# Embedding table
EMB_TABLE   = MODE_CFG["emb_table"]

# Target tables
DEFAULT_TARGET_TABLE = MODE_CFG["target_table"]
HISTOGRAM_TARGET_TABLE   = MODE_CFG["histogram_target_table"]

# Source SQL query
SOURCE_SQL = MODE_CFG["source_sql"]

# ADFD similar projects insert SQL query
INSERT_ADFD_SQL = MODE_CFG["insert_adfd_sql"]

# Switch behavior if histogram mode is on
if HISTOGRAM_MODE:
    SIMILARITY_THRESHOLD = HISTOGRAM_THRESHOLD
    TARGET_TABLE = HISTOGRAM_TARGET_TABLE
    TARGET_SCHEMA = HISTOGRAM_TARGET_SCHEMA
else:
    SIMILARITY_THRESHOLD = DEFAULT_SIMILARITY_THRESHOLD  # 0.75
    TARGET_TABLE = DEFAULT_TARGET_TABLE
    TARGET_SCHEMA = DEFAULT_TARGET_SCHEMA

print(
    f"[MODE] source_mode={SOURCE_MODE} | histogram={HISTOGRAM_MODE} "
    f"| threshold={SIMILARITY_THRESHOLD} | target={TARGET_SCHEMA}.{TARGET_TABLE}"
)

FILTER_COLS = ["country_name_en", "donor_name_en", "implementing_org_en"]
SEASONAL_SUBSECTOR = "Seasonal programmes"

# =====================================
# SQL SERVER CONNECTION (WINDOWS AUTH)
# =====================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

engine = get_sql_server_engine()

# -----------------------------
# Load embeddings + join filter cols (DEDUPED)
# -----------------------------
df = pd.read_sql_query(text(SOURCE_SQL), engine).fillna("").reset_index(drop=True)
print(f"[LOAD] Loaded {len(df):,} rows from source table")

# Parse embeddings (stored as NVARCHAR list)
print("[EMB] Parsing embeddings...")
embeddings = np.asarray([ast.literal_eval(s) for s in df["embedding"]], dtype=np.float32)
print(f"[EMB] Parsed embeddings with shape {embeddings.shape}")

# -----------------------------
# Similarity search WITH HARD FILTER (FAISS)
# -----------------------------
ts_inserted = datetime.now(timezone.utc)
out_rows = []

def run_grouped_faiss(df_slice: pd.DataFrame, group_cols: list[str]):
    print(f"[FAISS] Starting FAISS for {len(df_slice):,} rows")
    rows = []

    for _, g in df_slice.groupby(group_cols, dropna=False, sort=False):
        if len(g) < 2:
            continue

        idxs = g.index.to_numpy()
        vecs = embeddings[idxs]

        dim = vecs.shape[1]
        faiss_index = faiss.IndexFlatIP(dim)
        faiss_index.add(vecs)

        scores, nbrs = faiss_index.search(vecs, TOP_K + 1)

        for gi in range(len(g)):
            src_global_i = int(idxs[gi])
            src = df.iloc[src_global_i]

            for rank in range(TOP_K + 1):
                gj = int(nbrs[gi, rank])
                s = float(scores[gi, rank])

                if gj < 0 or gj == gi:
                    continue
                if s < SIMILARITY_THRESHOLD:
                    continue

                sim_global_j = int(idxs[gj])
                sim = df.iloc[sim_global_j]
                
                # Input columns
                title_en = MODE_CFG["title_en"]
                desc_en  = MODE_CFG["desc_en"]
                title_ar = MODE_CFG["title_ar"]
                desc_ar  = MODE_CFG["desc_ar"]

                # Output columns
                out_title_en = MODE_CFG["out_title_en"]
                out_desc_en  = MODE_CFG["out_desc_en"]
                out_title_ar = MODE_CFG["out_title_ar"]
                out_desc_ar  = MODE_CFG["out_desc_ar"]

                out_sim_title_en = MODE_CFG["out_sim_title_en"]
                out_sim_desc_en  = MODE_CFG["out_sim_desc_en"]
                out_sim_title_ar = MODE_CFG["out_sim_title_ar"]
                out_sim_desc_ar  = MODE_CFG["out_sim_desc_ar"]

                row = {
                    "index": src["index"],
                    "source_id": src["source_id"],

                    MODE_CFG["out_title_en"]: src.get(MODE_CFG["title_en"], ""),
                    MODE_CFG["out_desc_en"]:  src.get(MODE_CFG["desc_en"], ""),
                    MODE_CFG["out_title_ar"]: src.get(MODE_CFG["title_ar"], ""),
                    MODE_CFG["out_desc_ar"]:  src.get(MODE_CFG["desc_ar"], ""),

                    "country_name_en": src["country_name_en"],
                    "donor_name_en": src["donor_name_en"],
                    "implementing_org_en": src["implementing_org_en"],
                    "year": src["year"],
                    "subsector_name_en": src["subsector_name_en"],
                    "amount": src["amount"],

                    "similar_index": sim["index"],
                    "similar_source_id": sim["source_id"],

                    MODE_CFG["out_sim_title_en"]: sim.get(MODE_CFG["title_en"], ""),
                    MODE_CFG["out_sim_desc_en"]:  sim.get(MODE_CFG["desc_en"], ""),
                    MODE_CFG["out_sim_title_ar"]: sim.get(MODE_CFG["title_ar"], ""),
                    MODE_CFG["out_sim_desc_ar"]:  sim.get(MODE_CFG["desc_ar"], ""),

                    "similar_country_name_en": sim["country_name_en"],
                    "similar_donor_name_en": sim["donor_name_en"],
                    "similar_implementing_org_en": sim["implementing_org_en"],
                    "similar_year": sim["year"],
                    "similar_subsector_name_en": sim["subsector_name_en"],
                    "similar_amount": sim["amount"],

                    "similarity_score": round(s, 2),
                    "ts_inserted": ts_inserted,
                }

                # add projects-mode extras
                extra_map = MODE_CFG.get("extra_map", {})
                extra_sim_map = MODE_CFG.get("extra_sim_map", {})

                for src_col, out_col in extra_map.items():
                    row[out_col] = src.get(src_col, "")

                for sim_col, out_col in extra_sim_map.items():
                    row[out_col] = sim.get(sim_col, "")

                rows.append(row)


    print(f"[FAISS] Finished FAISS → produced {len(rows):,} similarity rows")

    return rows

# Split seasonal vs non-seasonal
df_seasonal = df[df["subsector_name_en"] == SEASONAL_SUBSECTOR]
df_non_seasonal = df[df["subsector_name_en"] != SEASONAL_SUBSECTOR]
print(f"[SPLIT] Seasonal projects: {len(df_seasonal):,}")
print(f"[SPLIT] Non-seasonal projects: {len(df_non_seasonal):,}")

out_rows = []

# Non-seasonal: same country+donor+implementing org (year can differ)
print("[RUN] Running FAISS for NON-SEASONAL projects")
out_rows.extend(
    run_grouped_faiss(df_non_seasonal, FILTER_COLS)
)
print(f"[RUN] Total rows after non-seasonal: {len(out_rows):,}")

# Seasonal: must match year too
print("[RUN] Running FAISS for SEASONAL projects (year-aware)")
out_rows.extend(
    run_grouped_faiss(df_seasonal, FILTER_COLS + ["year"])
)
print(f"[RUN] Total rows after seasonal: {len(out_rows):,}")

df_out = pd.DataFrame(out_rows)

# -----------------------------
# De-duplicate symmetric pairs (A-B == B-A)
# -----------------------------
# canonical ordering of the pair
# -----------------------------
if SOURCE_MODE == "master projects":
    left_col, right_col = "index", "similar_index"
elif SOURCE_MODE == "projects":
    left_col, right_col = "project_code", "similar_project_code"
else:
    raise ValueError(f"Unknown SOURCE_MODE: {SOURCE_MODE}")

# Canonical ordering to remove symmetric duplicates
df_out["pair_left"]  = np.minimum(df_out[left_col].astype(str), df_out[right_col].astype(str))
df_out["pair_right"] = np.maximum(df_out[left_col].astype(str), df_out[right_col].astype(str))

# Keep best row per pair (highest similarity_score)
df_out = (
    df_out.sort_values(["pair_left", "pair_right", "similarity_score"], ascending=[True, True, False])
         .drop_duplicates(subset=["pair_left", "pair_right"], keep="first")
         .drop(columns=["pair_left", "pair_right"])
         .reset_index(drop=True)
)

print(f"[DEDUP] Rows after symmetric de-dup (mode={SOURCE_MODE}, key={left_col}/{right_col}): {len(df_out):,}")

# -----------------------------
# % difference between amounts
# -----------------------------
df_out["amount_diff_pct"] = (
    (df_out["amount"] - df_out["similar_amount"])
        .abs()
        .div(
            df_out[["amount", "similar_amount"]]
                .abs()
                .max(axis=1)
        )
        .mul(100)
        .round(2)
)

df_out["source_id_match"] = (
    df_out["source_id"].astype(str).str.strip().ne("") &
    df_out["similar_source_id"].astype(str).str.strip().ne("") &
    (df_out["source_id"].astype(str).str.strip() == df_out["similar_source_id"].astype(str).str.strip())
)
print(f"[OUT] source_id_match=True count: {df_out['source_id_match'].sum():,} / {len(df_out):,}")


# -----------------------------
# Save output to CSV (optional)
# -----------------------------
csv_dir = Path("data/outputs/embeddings")
csv_dir.mkdir(parents=True, exist_ok=True)
out_csv = csv_dir / "similarity_projects_flat_filtered.csv"
print("[CSV] Writing CSV output...")
df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
print(f"Saved {out_csv}")

# -----------------------------
# Write output to SQL Server
# -----------------------------
dtype = {
    "index": NVARCHAR(255),
    "source_id": NVARCHAR(255),
    "country_name_en": NVARCHAR(255),
    "donor_name_en": NVARCHAR(255),
    "implementing_org_en": NVARCHAR(255),
    "year": NVARCHAR(50),
    "subsector_name_en": NVARCHAR(255),
    "amount": Float(),

    "similar_index": NVARCHAR(255),
    "similar_source_id": NVARCHAR(255),
    "similar_country_name_en": NVARCHAR(255),
    "similar_donor_name_en": NVARCHAR(255),
    "similar_implementing_org_en": NVARCHAR(255),
    "similar_year": NVARCHAR(50),
    "similar_subsector_name_en": NVARCHAR(255),
    "similar_amount": Float(),

    # -----------------------------
    # Projects-mode extras
    # -----------------------------
    "project_code": NVARCHAR(255),
    "similar_project_code": NVARCHAR(255),

    # -----------------------------
    # Master project text columns
    # -----------------------------
    "master_project_title_en": UnicodeText(),
    "master_project_description_en": UnicodeText(),
    "master_project_title_ar": UnicodeText(),
    "master_project_description_ar": UnicodeText(),

    "similar_master_project_title_en": UnicodeText(),
    "similar_master_project_description_en": UnicodeText(),
    "similar_master_project_title_ar": UnicodeText(),
    "similar_master_project_description_ar": UnicodeText(),

    # -----------------------------
    # Project text columns
    # -----------------------------
    "project_title_en": UnicodeText(),
    "project_description_en": UnicodeText(),
    "project_title_ar": UnicodeText(),
    "project_description_ar": UnicodeText(),

    "similar_project_title_en": UnicodeText(),
    "similar_project_description_en": UnicodeText(),
    "similar_project_title_ar": UnicodeText(),
    "similar_project_description_ar": UnicodeText(),

    # -----------------------------
    # Similarity + computed columns
    # -----------------------------
    "similarity_score": Float(),
    "amount_diff_pct": Float(),
    "source_id_match": Boolean(),

    # -----------------------------
    # Audit columns
    # -----------------------------
    "ts_inserted": DateTime(),
    }

print("[SQL] Writing similarity results to SQL Server...")
df_out.to_sql(
    name=TARGET_TABLE,
    schema=TARGET_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype = dtype
)

print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE} (mode={SOURCE_MODE})")

# -----------------------------
# ADFD projects
# -----------------------------
print(f"[ADFD] Appending ADFD similarity rules for mode={SOURCE_MODE}")
with engine.begin() as conn:
    rows = conn.execute(sql_text(INSERT_ADFD_SQL)).rowcount
print(f"[INFO] Appended ADFD similar projects: {rows} rows")