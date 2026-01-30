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

# -----------------------------
# Config
# -----------------------------
TOP_K = 20
SIMILARITY_THRESHOLD = 0.80

#EMB_TABLE = "silver.project_embeddings"
DAR_EMB_TABLE = "silver.dar_project_embeddings"
DENORM_TABLE = "dbo.denorm_MasterTable"

OUT_SCHEMA = "silver"
#OUT_TABLE  = "similar_projects"
DAR_OUT_TABLE  = "dar_similar_projects"

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
VIEW_SCHEMA = "silver"
#VIEW_NAME = "vw_project_similarity_source"
DAR_VIEW_NAME = "vw_dar_project_similarity_source"

#CREATE_VIEW_SQL = f"""
#CREATE OR ALTER VIEW {VIEW_SCHEMA}.{VIEW_NAME}
#AS
#-- non-ADFD newly split projects
#SELECT
#    a.[index]
#  , b.SourceID                            AS source_id
#  , a.project_code
#  , a.project_title_en
#  , a.project_description_en
#  , a.project_title_ar
#  , a.project_description_ar
#  , b.year
#  , b.CountryNameEnglish                  AS country_name_en
#  , b.DonorNameEnglish                    AS donor_name_en
#  , b.ImplementingOrganizationEnglish     AS implementing_org_en
#  , c.extracted_subsector_en              AS subsector_name_en
#  , a.embedding
#--FROM silver.project_embeddings a
#FROM silver.dar_project_embeddings a
#LEFT JOIN [dbo].[MasterTableDenormalizedCleanedFinal] b
#  ON a.[index] = b.[index]
#LEFT JOIN silver.cleaned_project_attributes c
#    ON a.project_code = c.project_code
#WHERE EXISTS (
#    SELECT 1
#    FROM silver.cleaned_project_attributes c
#    WHERE c.project_code = a.project_code
#)
#UNION ALL
#-- non-ADFD single projects
#SELECT
#    a.[index]
#  , b.SourceID                            AS source_id
#  , a.project_code
#  , a.project_title_en
#  , a.project_description_en
#  , a.project_title_ar
#  , a.project_description_ar
#  , b.year
#  , b.CountryNameEnglish                  AS country_name_en
#  , b.DonorNameEnglish                    AS donor_name_en
#  , b.ImplementingOrganizationEnglish     AS implementing_org_en
#  , b.SubSectorNameEnglish                AS subsector_name_en
#  , a.embedding
#FROM silver.project_embeddings a
#LEFT JOIN [dbo].[MasterTableDenormalizedCleanedFinal] b
#  ON a.[index] = b.[index]
#WHERE NOT EXISTS (
#    SELECT 1
#    FROM silver.cleaned_project_attributes c
#    WHERE c.project_code = a.project_code
#);
#"""

CREATE_DAR_VIEW_SQL = f"""
CREATE OR ALTER VIEW {VIEW_SCHEMA}.{DAR_VIEW_NAME}
AS

SELECT
    a.[index]
  , b.SourceID                            AS source_id
  , a.project_title_en
  , a.project_description_en
  , a.project_title_ar
  , a.project_description_ar
  , b.year
  , b.CountryNameEnglish                  AS country_name_en
  , b.DonorNameEnglish                    AS donor_name_en
  , b.ImplementingOrganizationEnglish     AS implementing_org_en
  , b.SubSectorNameEnglish                AS subsector_name_en
  , a.embedding
FROM silver.dar_project_embeddings a
LEFT JOIN [dbo].[MasterTableDenormalizedCleanedFinal] b
  ON a.[index] = b.[index]
;
"""

print(f"[VIEW] Creating/updating view {VIEW_SCHEMA}.{DAR_VIEW_NAME} ...")
with engine.begin() as conn:
    conn.execute(sql_text(CREATE_DAR_VIEW_SQL))
print("[VIEW] View ready")


sql = f"SELECT * FROM {VIEW_SCHEMA}.{DAR_VIEW_NAME};"
print("[LOAD] Loading embeddings + metadata from VIEW...")
df = pd.read_sql_query(text(sql), engine).fillna("").reset_index(drop=True)
print(f"[LOAD] Loaded {len(df):,} rows from view")

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

                rows.append({
                    "index": src["index"],
                    "source_id": src["source_id"],
                    #"project_code": src["project_code"],
                    "project_title_en": src["project_title_en"],
                    "project_description_en": src["project_description_en"],
                    "project_title_ar": src["project_title_ar"],
                    "project_description_ar": src["project_description_ar"],
                    "country_name_en": src["country_name_en"],
                    "donor_name_en": src["donor_name_en"],
                    "implementing_org_en": src["implementing_org_en"],
                    "year": src["year"],
                    "subsector_name_en": src["subsector_name_en"],

                    "similar_index": sim["index"],
                    "similar_source_id": sim["source_id"],
                    #"similar_project_code": sim["project_code"],
                    "similar_project_title_en": sim["project_title_en"],
                    "similar_project_description_en": sim["project_description_en"],
                    "similar_project_title_ar": sim["project_title_ar"],
                    "similar_project_description_ar": sim["project_description_ar"],
                    "similar_project_country_name_en": sim["country_name_en"],
                    "similar_project_donor_name_en": sim["donor_name_en"],
                    "similar_project_implementing_org_en": sim["implementing_org_en"],
                    "similar_project_year": sim["year"],
                    "similar_project_subsector_name_en": sim["subsector_name_en"],

                    "similarity_score": round(s, 4),
                    "ts_inserted": ts_inserted,
                })

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
df_out["pair_left"]  = np.minimum(df_out["index"].astype(str), df_out["similar_index"].astype(str))
df_out["pair_right"] = np.maximum(df_out["index"].astype(str), df_out["similar_index"].astype(str))

# keep best row per pair (e.g., highest similarity_score)
df_out = (
    df_out.sort_values(["pair_left", "pair_right", "similarity_score"], ascending=[True, True, False])
         .drop_duplicates(subset=["pair_left", "pair_right"], keep="first")
         .drop(columns=["pair_left", "pair_right"])
         .reset_index(drop=True)
)

print(f"[DEDUP] Rows after symmetric de-dup: {len(df_out):,}")


df_out["source_id_match"] = (
    df_out["source_id"].astype(str).str.strip().ne("") &
    df_out["similar_source_id"].astype(str).str.strip().ne("") &
    (df_out["source_id"].astype(str).str.strip() == df_out["similar_source_id"].astype(str).str.strip())
)
print(f"[OUT] source_id_match=True count: {df_out['source_id_match'].sum():,} / {len(df_out):,}")


# -----------------------------
# Save output to CSV (optional)
# -----------------------------
OUT_DIR = Path("data/outputs/embeddings")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "similarity_projects_flat_filtered.csv"
print("[CSV] Writing CSV output...")
df_out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print(f"Saved {OUT_CSV}")

# -----------------------------
# Write output to SQL Server
# -----------------------------
print("[SQL] Writing similarity results to SQL Server...")
df_out.to_sql(
    name=DAR_OUT_TABLE,
    schema=OUT_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype={
        "index": NVARCHAR(255),
        "source_id": NVARCHAR(255),
        #"project_code": NVARCHAR(255),
        "project_title_en": UnicodeText(),
        "project_description_en": UnicodeText(),
        "project_title_ar": UnicodeText(),
        "project_description_ar": UnicodeText(),
        "country_name_en": NVARCHAR(255),
        "donor_name_en": NVARCHAR(255),
        "implementing_org_en": NVARCHAR(255),
        "year": NVARCHAR(50),
        "subsector_name_en": NVARCHAR(255),

        "similar_index": NVARCHAR(255),
        "similar_source_id": NVARCHAR(255),
        #"similar_project_code": NVARCHAR(255),
        "similar_project_title_en": UnicodeText(),
        "similar_project_description_en": UnicodeText(),
        "similar_project_title_ar": UnicodeText(),
        "similar_project_description_ar": UnicodeText(),
        "similar_project_country_name_en": NVARCHAR(255),
        "similar_project_donor_name_en": NVARCHAR(255),
        "similar_project_implementing_org_en": NVARCHAR(255),
        "similar_project_year": NVARCHAR(50),
        "similar_project_subsector_name_en": NVARCHAR(255),

        "similarity_score": Float(),
        "source_id_match": Boolean(),

        "ts_inserted": DateTime(),
    },
)

print(f"Saved to SQL Server: {OUT_SCHEMA}.{DAR_OUT_TABLE}")

# -----------------------------
# ADFD projects
# -----------------------------
#INSERT_ADFD_SQL = f"""
#;WITH adfd AS (
#    SELECT
#        cp.[index],
#        cp.project_code,
#        cp.project_title_en,
#        cp.project_description_en,
#        cp.project_title_ar,
#        cp.project_description_ar,
#        mt.SourceID             AS source_id
#    FROM silver.cleaned_project cp
#    JOIN dbo.MasterTableDenormalizedCleanedFinal mt
#        ON cp.[index] = mt.[index]
#    WHERE cp.[index] LIKE 'ADFD%'
#      AND mt.SourceID IS NOT NULL
#)
#INSERT INTO {OUT_SCHEMA}.{OUT_TABLE} (
#      [index]
#    , source_id
#    , project_code
#    , project_title_en
#    , project_description_en
#    , project_title_ar
#    , project_description_ar
#    , similar_index
#    , similar_source_id
#    , similar_project_code
#    , similar_project_title_en
#    , similar_project_description_en
#    , similar_project_title_ar
#    , similar_project_description_ar
#    , similarity_score
#    , source_id_match
#    , ts_inserted
#)
#SELECT
#    a.[index],
#    a.source_id,
#    a.project_code,
#    a.project_title_en,
#    a.project_description_en,
#    a.project_title_ar,
#    a.project_description_ar,
#
#    b.[index]                AS similar_index,
#    b.source_id               AS similar_source_id,
#    b.project_code           AS similar_project_code,
#    b.project_title_en       AS similar_project_title_en,
#    b.project_description_en AS similar_project_description_en,
#    b.project_title_ar       AS similar_project_title_ar,
#    b.project_description_ar AS similar_project_description_ar,
#
#    CAST(1.0 AS FLOAT)       AS similarity_score,
#    CASE
#        WHEN a.source_id = b.source_id THEN 1
#        ELSE 0
#        END AS source_id_match,
#    CURRENT_TIMESTAMP        AS ts_inserted
#FROM adfd a
#JOIN adfd b
#  ON a.source_id = b.source_id
# AND a.[index] <> b.[index];
#"""
#
#print("[ADFD] Appending ADFD similarity rules...")
#with engine.begin() as conn:
#    rows = conn.execute(sql_text(INSERT_ADFD_SQL)).rowcount
#print(f"[INFO] Appended ADFD similar projects: {rows} rows")
