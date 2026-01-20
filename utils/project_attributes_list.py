import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text as sql_text

import langextract as lx


# ============================================================
# SQL Server connection (Windows Auth)
# ============================================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


# ============================================================
# Load allowed subsectors from SQL
# ============================================================
def load_allowed_subsectors(engine) -> List[str]:
    q = """
    SELECT DISTINCT LTRIM(RTRIM(SubSectorNameEnglish)) AS SubSectorNameEnglish
    FROM dbo.subsectors
    WHERE SubSectorNameEnglish IS NOT NULL
      AND LTRIM(RTRIM(SubSectorNameEnglish)) <> ''
    ORDER BY SubSectorNameEnglish
    """
    df = pd.read_sql(sql_text(q), engine)
    values = [str(x).strip() for x in df["SubSectorNameEnglish"].tolist() if str(x).strip()]
    # De-dupe while preserving order
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out