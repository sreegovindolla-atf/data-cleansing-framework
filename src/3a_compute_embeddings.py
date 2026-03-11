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
import argparse
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.app_config import COMPUTE_EMB_CONFIG as CONFIG

# =========================================================
# Args
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument(
    "--source-mode",
    required=True,
    choices=["master projects", "projects"],
    help="db = compute embeddings from the master projects table; extracted = compute embeddings from the projects table"
)

args = parser.parse_args()

SOURCE_MODE = args.source_mode

# -----------------------------
# Config
# -----------------------------
MODEL_NAME = "all-MiniLM-L6-v2"
TARGET_SCHEMA = "silver"
#DAR_TARGET_TABLE = "dar_project_embeddings"


MODE_CFG = CONFIG[SOURCE_MODE]
TARGET_TABLE = MODE_CFG["target_table"]
SOURCE_SQL = MODE_CFG["source_sql"]
TEXT_COLS = MODE_CFG["text_cols"]
OUTPUT_COLS = MODE_CFG["output_cols"]


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

df_src = pd.read_sql_query(text(SOURCE_SQL), engine).fillna("").reset_index(drop=True)

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
df_out = df_src[OUTPUT_COLS].copy()
df_out["embedding"] = [json.dumps(e.tolist()) for e in base_embeddings]
df_out["ts_inserted"] = datetime.now(timezone.utc)

# -----------------------------
# Save to CSV
# -----------------------------
csv_dir = Path("data/outputs/embeddings")
csv_dir.mkdir(parents=True, exist_ok=True)
out_csv = csv_dir / MODE_CFG["out_csv_name"]
df_out.to_csv(out_csv, index=False)
print(f"Saved {out_csv}")

# -----------------------------
# Write embeddings to SQL Server
# -----------------------------
dtype_map = {}
for c in OUTPUT_COLS:
    if c.lower() == "index":
        dtype_map[c] = NVARCHAR(255)
    elif c.endswith("_en") or c.endswith("_ar") or "description" in c.lower() or "title" in c.lower():
        dtype_map[c] = UnicodeText()
    else:
        dtype_map[c] = NVARCHAR(255)

dtype_map.update({
    "embedding": UnicodeText(),
    "ts_inserted": DateTime(),
    "source_mode": NVARCHAR(50),
    "model_name": NVARCHAR(255),
})

df_out.to_sql(
    name=TARGET_TABLE,
    schema=TARGET_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype=dtype_map
)

print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE} (mode={SOURCE_MODE})")