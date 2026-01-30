from sentence_transformers import SentenceTransformer
import numpy as np
from pathlib import Path
import pandas as pd
import json
from sqlalchemy import create_engine, text
import urllib
import sys
from sqlalchemy.types import NVARCHAR, UnicodeText, DateTime
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# -----------------------------
# Config
# -----------------------------
MODEL_NAME = "all-MiniLM-L6-v2"
TARGET_SCHEMA = "silver"
#TARGET_TABLE = "project_embeddings"
DAR_TARGET_TABLE = "dar_project_embeddings"

# Text columns used to build embedding
TEXT_COLS = ["project_title_en", "project_description_en", "project_title_ar", "project_description_ar"]
TABLE = "dbo.MasterTableDenormalizedCleanedFinal"

# =====================================
# helpers
# =====================================
def build_text_from_row(row) -> str:
    parts = []
    for c in TEXT_COLS:
        v = row.get(c, None)
        if v is None:
            continue
        v = str(v).strip()
        if v:
            parts.append(v)
    return "\n".join(parts).strip()

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
# Read from SQL Server
# -----------------------------
#sql = f"""
#SELECT DISTINCT
#    [index],
#    project_code,
#    project_title_en,
#    project_description_en,
#    project_title_ar,
#    project_description_ar
#FROM {TABLE}
#WHERE [index] NOT LIKE '%ADFD-%'
#"""

sql = f"""
SELECT DISTINCT
    [index]
    , ProjectTitleEnglish           AS project_title_en
    , DescriptionEnglish            AS project_description_en
    , ProjectTitleArabic            AS project_title_ar
    , DescriptionArabic             AS project_description_ar
FROM {TABLE}
WHERE 1=1
--AND [index] NOT LIKE '%ADFD-%'
--Dar Al Ber
AND DonorID = 30
"""

df_src = pd.read_sql_query(text(sql), engine).fillna("").reset_index(drop=True)

# -----------------------------
# Build texts + REMOVE empty ones (critical)
# -----------------------------
df_src["__text__"] = df_src.apply(build_text_from_row, axis=1)
df_src = df_src[df_src["__text__"].str.len() > 0].reset_index(drop=True)

# -----------------------------
# Build embeddings
# -----------------------------
model = SentenceTransformer(MODEL_NAME)

base_texts = df_src["__text__"].tolist()
base_embeddings = model.encode(
    base_texts,
    normalize_embeddings=True,
    show_progress_bar=True
)
base_embeddings = np.asarray(base_embeddings, dtype=np.float32)

# Safety check (prevents silent mismatch)
if len(base_embeddings) != len(df_src):
    raise ValueError(f"Length mismatch: embeddings={len(base_embeddings)} vs df={len(df_src)}")

# -----------------------------
# Build output dataframe (NO iloc loop needed)
# -----------------------------
#df_out = df_src[["index", "project_code", "project_title_en", "project_description_en", "project_title_ar", "project_description_ar"]].copy()
df_out = df_src[["index", "project_title_en", "project_description_en", "project_title_ar", "project_description_ar"]].copy()
df_out["embedding"] = [json.dumps(e.tolist()) for e in base_embeddings]
df_out["ts_inserted"] = datetime.now(timezone.utc)

# -----------------------------
# Save to CSV
# -----------------------------
RUN_OUTPUT_DIR = Path("data/outputs/embeddings")
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = RUN_OUTPUT_DIR / "dar_project_embeddings.csv"
df_out.to_csv(OUT_CSV, index=False)
print(f"Saved {OUT_CSV}")

# -----------------------------
# Write embeddings to SQL Server
# -----------------------------

df_out.to_sql(
    name=DAR_TARGET_TABLE,
    schema=TARGET_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype={
        "index": NVARCHAR(255),
        #"project_code": NVARCHAR(255),
        "project_title_en": UnicodeText(),        # NVARCHAR(MAX)
        "project_description_en": UnicodeText(),  # NVARCHAR(MAX)
        "project_title_ar": UnicodeText(),        # NVARCHAR(MAX)
        "project_description_ar": UnicodeText(),  # NVARCHAR(MAX)
        "embedding": UnicodeText(),               # NVARCHAR(MAX) 
        "ts_inserted": DateTime(),       # or DateTime(timezone=True)
    }
)

print(f"Saved to SQL Server: {TARGET_SCHEMA}.{DAR_TARGET_TABLE}")