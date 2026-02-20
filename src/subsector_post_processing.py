import json
import re
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib
import sys
from datetime import datetime, timezone
from sqlalchemy import text as sql_text
from sqlalchemy.types import NVARCHAR, DateTime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sqlalchemy import text as sql_text

from utils.post_processing_helpers import (
    smart_title_case,
    normalize_class,
    to_int_or_none,
    to_float_or_none,
    is_missing_or_bad,
    fmt_mp,
    fmt_prj,
    parse_langextract_grouped_pairs,
    _is_blank,
    json_to_csv
)

from utils.post_processing_sql_queries import QUERIES

# -----------------------
# args + paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs/project_attributes") / RUN_ID

INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.jsonl"
OUT_CSV     = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.csv"

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
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

STG_TARGET_SCHEMA = "silver"
STG_TARGET_TABLE  = "stg_project_attributes"

FINAL_SCHEMA = "silver"
FINAL_TABLE  = "cleaned_project_attributes"

# -----------------------
# helpers for code continuity
# -----------------------
def _trail_int(s: str):
    if s is None:
        return None
    m = re.search(r"(\d+)\s*$", str(s))
    return int(m.group(1)) if m else None

# -----------------------
# main parsing
# -----------------------
if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

engine = get_sql_server_engine()

df = json_to_csv(INPUT_JSONL, OUT_CSV)

df.to_sql(
    STG_TARGET_TABLE,
    engine,
    schema=STG_TARGET_SCHEMA,
    if_exists="replace",
    index=False,
    chunksize=2000,
    method=None
)

OUTPUT_QUERY = """
SELECT
    pa.[index]
    , pa.project_code
    , p.project_title_en
    , p.project_title_ar
    , p.project_description_en
    , p.project_description_ar
    , mt.EmergencyTitle                 As emergency_title_en
    , mt.EmergencyTitleAR               AS emergency_title_ar
    , pa.subsector_en                   AS extracted_subsector_en
    , mt.SubSectorNameEnglish           AS subsector_en
    , sect.SectorNameEnglish            AS extracted_sector_en
    , mt.SectorNameEnglish              AS sector_en
    , cl.groupNameEnglish               AS extracted_cluster_en
    , mtcl.groupNameEnglish             AS cluster_en
    , acat.AssistanceCategoryEnglish    AS extracted_assistance_category_en
    , mt.AssistanceCategoryEnglish      AS assistance_category_en
    , mt.IndicatorEnglish               AS extracted_indicator_en
    , mt.IndicatorEnglish               AS indicator_en
    , pa.target_en                      AS extracted_target_en
    , mt.Target_English                 AS target_en
    , g.NameEnglish                     AS extracted_goal_en
    , mt.NameEnglish                    AS goal_en
    , pa.document_id
    , CURRENT_TIMESTAMP                 AS ts_inserted
FROM silver.stg_project_attributes pa
LEFT JOIN silver.cleaned_project p
    ON pa.project_code = p.project_code
LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal mt
    ON pa.[index] = mt.[index]

-- extracted fields
LEFT JOIN dbo.SubSectors subsect
    ON LOWER(pa.subsector_en) = LOWER(subsect.SubSectorNameEnglish)
-- extracted sector
LEFT JOIN dbo.Sectors sect
    ON subsect.OCFA_Code = sect.OCFA_Code
-- extracted cluster
LEFT JOIN dbo.Clusters cl
    ON subsect.Culster_Code = cl.groupID
-- extracted assistance category
LEFT JOIN dbo.AssistanceCategory acat
    ON subsect.Cat_ID = acat.ID
-- extracted goal
LEFT JOIN dbo.Targets t
    ON LOWER(pa.target_en) = LOWER(t.Target_English)
LEFT JOIN dbo.DevelopmentGoals g
    ON LOWER(t.Goal_ID) = LOWER(g.ID)

-- db fields
LEFT JOIN dbo.SubSectors mtsubsect
    ON LOWER(mt.SubSectorNameEnglish) = LOWER(mtsubsect.SubSectorNameEnglish)
-- db cluster
LEFT JOIN dbo.Clusters mtcl
    ON mtsubsect.Culster_Code = mtcl.groupID
"""

with engine.begin() as conn:
    # drop if exists
    conn.execute(sql_text(f"""
    IF OBJECT_ID('{FINAL_SCHEMA}.{FINAL_TABLE}', 'U') IS NOT NULL
        DROP TABLE {FINAL_SCHEMA}.{FINAL_TABLE};
    """))

    # create + load final
    conn.execute(sql_text(f"""
    SELECT *
    INTO {FINAL_SCHEMA}.{FINAL_TABLE}
    FROM (
        {OUTPUT_QUERY}
    ) q;
    """))


print(f"Saved combined output: {OUT_CSV}")
print(f"Saved to SQL Server: {FINAL_SCHEMA}.{FINAL_TABLE}")
print("Rows written:", len(df))
print("Non-null counts:\n", df.notna().sum())