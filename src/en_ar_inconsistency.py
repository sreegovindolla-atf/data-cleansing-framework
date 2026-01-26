import os
import argparse
from pathlib import Path
import urllib.parse

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, Float, Boolean, DateTime
from datetime import datetime, timezone

from sentence_transformers import SentenceTransformer


# -----------------------------
# Config defaults
# -----------------------------
INPUT_TABLE  = "dbo.MasterTableDenormalizedCleanedFinal"
OUTPUT_TABLE = "silver.semantic_en_ar_check"

ID_COL       = "index"
EN_TITLE_COL = "ProjectTitleEnglish"
EN_DESC_COL  = "DescriptionEnglish"
AR_TITLE_COL = "ProjectTitleArabic"
AR_DESC_COL  = "DescriptionArabic"

MODEL_NAME   = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
SIM_THRESHOLD = 0.60
BATCH_SIZE   = 256

BASE_OUT_DIR = Path("data/outputs/semantic_check/")

# =========================================================
# HELPERS
# =========================================================
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


def clean_text(x) -> str:
    if x is None:
        return ""
    x = str(x).strip()
    return "" if x.lower() in {"nan", "none", "null"} else x


def build_text(title, desc) -> str:
    title, desc = clean_text(title), clean_text(desc)
    if title and desc:
        return f"{title}\n{desc}"
    return title or desc


def cosine_similarity(a, b):
    return np.sum(a * b, axis=1)


# =========================================================
# MAIN
# =========================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    RUN_ID = args.run_id
    RUN_DIR = BASE_OUT_DIR / RUN_ID
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    CSV_OUT = RUN_DIR / f"{RUN_ID}_semantic_en_ar_check.csv"

    # -----------------------------
    # Read input data
    # -----------------------------
    query = f"""
    SELECT DISTINCT TOP 4000
        [{ID_COL}]            ,
        [{EN_TITLE_COL}]      AS project_title_en,
        [{EN_DESC_COL}]       AS project_description_en,
        [{AR_TITLE_COL}]      AS project_title_ar,
        [{AR_DESC_COL}]       AS project_description_ar
    FROM {INPUT_TABLE}
    WHERE [index] NOT IN (
    SELECT [index]
    FROM silver.semantic_en_ar_check
    )
    """

    df = pd.read_sql(query, engine)

    df["project_en"] = df.apply(lambda r: build_text(r.project_title_en, r.project_description_en), axis=1)
    df["project_ar"] = df.apply(lambda r: build_text(r.project_title_ar, r.project_description_ar), axis=1)

    df["has_en_text"] = df["project_en"].astype(bool)
    df["has_ar_text"] = df["project_ar"].astype(bool)

    df["semantic_similarity"] = None
    df["is_semantic_mismatch"] = False

    # -----------------------------
    # Embeddings
    # -----------------------------
    model = SentenceTransformer(MODEL_NAME)
    valid = df.has_en_text & df.has_ar_text

    if valid.any():
        en_emb = model.encode(
            df.loc[valid, "project_en"].tolist(),
            normalize_embeddings=True,
            batch_size=BATCH_SIZE,
            show_progress_bar=True
        )

        ar_emb = model.encode(
            df.loc[valid, "project_ar"].tolist(),
            normalize_embeddings=True,
            batch_size=BATCH_SIZE,
            show_progress_bar=True
        )

        sims = cosine_similarity(en_emb, ar_emb)

        df.loc[valid, "semantic_similarity"] = sims
        df.loc[valid, "is_semantic_mismatch"] = sims < SIM_THRESHOLD

    # -----------------------------
    # Metadata
    # -----------------------------
    df["ts_inserted"] = datetime.now(timezone.utc)

    # -----------------------------
    # Write CSV (REPLACE)
    # -----------------------------
    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")

    # -----------------------------
    # Write SQL table (REPLACE)
    # -----------------------------

    df_sql = df[[
                "index"
                , "project_title_en"
                , "project_title_ar"
                , "project_description_en"
                , "project_description_ar"
                , "semantic_similarity"
                , "is_semantic_mismatch"
                , "ts_inserted"
    ]].copy()

    df_sql.to_sql(
        OUTPUT_TABLE.split(".")[1],
        engine,
        schema=OUTPUT_TABLE.split(".")[0],
        if_exists="append",
        index=False,
        dtype={
            "index": NVARCHAR(255),
            "project_title_en": NVARCHAR(None),
            "project_title_ar": NVARCHAR(None),
            "project_description_en": NVARCHAR(None),
            "project_description_ar": NVARCHAR(None),
            "semantic_similarity": Float,
            "is_semantic_mismatch": Boolean,
            "ts_inserted": DateTime,
        }
    )

    print("✅ Semantic consistency check completed")
    print(f"CSV  : {CSV_OUT}")
    print(f"SQL  : {OUTPUT_TABLE}")
    print(f"Run  : {RUN_ID}")


if __name__ == "__main__":
    main()