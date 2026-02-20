import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
import urllib
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# -----------------------------
# Config
# -----------------------------
SCHEMA    = "silver"                       
TABLE     = "similar_projects" 
DONOR_COL = "donor_name_en"

BASE_OUTPUT_DIR = Path("data/outputs/projects/donor_similar_project_excels")
BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def safe_name_for_windows(s: str, max_len: int = 120) -> str:
    """Safe for folder/file names on Windows."""
    if s is None or str(s).strip() == "":
        s = "UNKNOWN_DONOR"
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|\x00-\x1F]+', "_", s)  # illegal chars
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip(". ")  # avoid trailing dot/space
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s


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



# -----------------------------
# Main
# -----------------------------
def main():
    BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    engine = get_sql_server_engine()

    donors_sql = f"""
        SELECT DISTINCT {DONOR_COL}
        FROM {SCHEMA}.{TABLE}
        ORDER BY {DONOR_COL};
    """
    donors_df = pd.read_sql(donors_sql, engine)
    donors = donors_df[DONOR_COL].astype("string").fillna("UNKNOWN_DONOR").tolist()

    for i, donor_raw in enumerate(donors, start=1):
        donor_safe = safe_name_for_windows(donor_raw)

        # Folder: "01. <donor name>"
        folder = BASE_OUTPUT_DIR / f"{i:02d}. {donor_safe}"
        folder.mkdir(parents=True, exist_ok=True)

        donor_sql = f"""
            SELECT *
            FROM {SCHEMA}.{TABLE}
            WHERE {DONOR_COL} = ?
            ORDER BY [index]
        """
        donor_df = pd.read_sql(donor_sql, engine, params=(donor_raw,))

        # File: "<donor name>_similar_projects.xlsx"
        xlsx_path = folder / f"{donor_safe}_similar_projects.xlsx"
        donor_df.to_excel(xlsx_path, index=False)

    print(f"Done. Created {len(donors)} folders under: {BASE_OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()