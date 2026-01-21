import numpy as np
import pandas as pd
import faiss
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, UnicodeText, DateTime, Float
import urllib
import ast
from datetime import datetime, timezone
from sqlalchemy import text as sql_text

# -----------------------------
# Config
# -----------------------------
TOP_K = 20
SIMILARITY_THRESHOLD = 0.80

EMB_TABLE = "silver.project_embeddings"
DENORM_TABLE = "dbo.denorm_MasterTable"

OUT_SCHEMA = "silver"
OUT_TABLE  = "similar_projects"

FILTER_COLS = ["country_name_en", "donor_name_en", "implementing_org_en"]

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
sql = f"""
SELECT 
    a.*
    , b.CountryNameEnglish                  AS country_name_en
    , b.DonorNameEnglish                    AS donor_name_en
    , b.ImplementingOrganizationEnglish     AS implementing_org_en
  FROM {EMB_TABLE} a
  LEFT JOIN {DENORM_TABLE} b
	ON a.[index] = b.[index]
"""

df = pd.read_sql_query(text(sql), engine).fillna("").reset_index(drop=True)

# Parse embeddings (stored as NVARCHAR list)
embeddings = np.asarray([ast.literal_eval(s) for s in df["embedding"]], dtype=np.float32)

# -----------------------------
# Similarity search WITH HARD FILTER (grouped FAISS)
# -----------------------------
ts_inserted = datetime.now(timezone.utc)
out_rows = []

# Only compare within same Country+Donor+ImplementingOrg
for _, g in df.groupby(FILTER_COLS, dropna=False, sort=False):
    if len(g) < 2:
        continue

    idxs = g.index.to_numpy()
    vecs = embeddings[idxs]

    dim = vecs.shape[1]
    faiss_index = faiss.IndexFlatIP(dim)  # cosine similarity if vectors normalized
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

            out_rows.append({
                "index": src["index"],
                "project_code": src["project_code"],
                "project_title_en": src["project_title_en"],
                "project_description_en": src["project_description_en"],
                "project_title_ar": src["project_title_ar"],
                "project_description_ar": src["project_description_ar"],
                "country_name_en": src["country_name_en"],
                "donor_name_en": src["donor_name_en"],
                "implementing_org_en": src["implementing_org_en"],
                "similar_index": sim["index"],
                "similar_project_code": sim["project_code"],
                "similar_project_title_en": sim["project_title_en"],
                "similar_project_description_en": sim["project_description_en"],
                "similar_project_title_ar": sim["project_title_ar"],
                "similar_project_description_ar": sim["project_description_ar"],
                "similar_project_country_name_en": sim["country_name_en"],
                "similar_project_donor_name_en": sim["donor_name_en"],
                "similar_project_implementing_org_en": sim["implementing_org_en"],
                "similarity_score": round(s, 4),
                "ts_inserted": ts_inserted,
            })

df_out = pd.DataFrame(out_rows)

# -----------------------------
# Save output to CSV (optional)
# -----------------------------
OUT_DIR = Path("data/outputs/embeddings")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "similarity_projects_flat_filtered.csv"
df_out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print(f"Saved {OUT_CSV}")

# -----------------------------
# Write output to SQL Server
# -----------------------------
df_out.to_sql(
    name=OUT_TABLE,
    schema=OUT_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype={
        "index": NVARCHAR(255),
        "project_code": NVARCHAR(255),
        "project_title_en": UnicodeText(),
        "project_description_en": UnicodeText(),
        "project_title_ar": UnicodeText(),
        "project_description_ar": UnicodeText(),
        "country_name_en": NVARCHAR(255),
        "donor_name_en": NVARCHAR(255),
        "implementing_org_en": NVARCHAR(255),
        "similar_index": NVARCHAR(255),
        "similar_project_code": NVARCHAR(255),
        "similar_project_title_en": UnicodeText(),
        "similar_project_description_en": UnicodeText(),
        "similar_project_title_ar": UnicodeText(),
        "similar_project_description_ar": UnicodeText(),
        "similar_project_country_name_en": NVARCHAR(255),
        "similar_project_donor_name_en": NVARCHAR(255),
        "similar_project_implementing_org_en": NVARCHAR(255),
        "similarity_score": Float(),
        "ts_inserted": DateTime(),
    },
)

print(f"Saved to SQL Server: {OUT_SCHEMA}.{OUT_TABLE}")

INSERT_ADFD_SQL = f"""
;WITH adfd AS (
    SELECT
        cp.[index],
        cp.project_code,
        cp.project_title_en,
        cp.project_description_en,
        cp.project_title_ar,
        cp.project_description_ar,
        mt.SourceID
    FROM silver.cleaned_project cp
    JOIN dbo.MasterTableDenormalizedCleanedFinal mt
        ON cp.[index] = mt.[index]
    WHERE cp.[index] LIKE 'ADFD%'
      AND mt.SourceID IS NOT NULL
)
INSERT INTO {OUT_SCHEMA}.{OUT_TABLE} (
      [index]
    , project_code
    , project_title_en
    , project_description_en
    , project_title_ar
    , project_description_ar
    , similar_index
    , similar_project_code
    , similar_project_title_en
    , similar_project_description_en
    , similar_project_title_ar
    , similar_project_description_ar
    , similarity_score
    , ts_inserted
)
SELECT
    a.[index],
    a.project_code,
    a.project_title_en,
    a.project_description_en,
    a.project_title_ar,
    a.project_description_ar,

    b.[index]                AS similar_index,
    b.project_code           AS similar_project_code,
    b.project_title_en       AS similar_project_title_en,
    b.project_description_en AS similar_project_description_en,
    b.project_title_ar       AS similar_project_title_ar,
    b.project_description_ar AS similar_project_description_ar,

    CAST(1.0 AS FLOAT)       AS similarity_score,
    CURRENT_TIMESTAMP        AS ts_inserted
FROM adfd a
JOIN adfd b
  ON a.SourceID = b.SourceID
 AND a.[index] <> b.[index];

"""

with engine.begin() as conn:
    rows = conn.execute(sql_text(INSERT_ADFD_SQL)).rowcount

print(f"[INFO] Appended ADFD similar projects: {rows} rows")