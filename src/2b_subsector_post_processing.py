import json
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
import urllib
import sys
from sqlalchemy import text as sql_text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.post_processing_helpers import json_to_csv

# -----------------------
# args + paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument(
    "--upstream-ids-file",
    required=False,
    help="Txt file containing processed indexes from 2a for this run."
)
args = parser.parse_args()

RUN_ID = args.run_id.strip()
UPSTREAM_IDS_FILE = Path(args.upstream_ids_file) if args.upstream_ids_file else None

RUN_OUTPUT_DIR = Path("data/outputs/project_attributes") / RUN_ID

INPUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.jsonl"
OUT_CSV = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes.csv"

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
STG_TARGET_TABLE = "stg_project_attributes"

FINAL_SCHEMA = "silver"
FINAL_TABLE = "cleaned_project_attributes"

engine = get_sql_server_engine()

# -----------------------
# helpers
# -----------------------
def load_ids_txt(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def filter_jsonl_by_indexes(input_jsonl: Path, output_jsonl: Path, allowed_indexes: set[str]) -> None:
    kept = 0
    with open(input_jsonl, "r", encoding="utf-8") as fin, open(output_jsonl, "w", encoding="utf-8") as fout:
        for line_no, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] Skipping invalid JSONL line {line_no}")
                continue

            idx = str(rec.get("index", "")).strip()
            if idx and idx in allowed_indexes:
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                kept += 1

    print(f"[INFO] Filtered JSONL rows kept: {kept}")


# -----------------------
# main parsing
# -----------------------
if not INPUT_JSONL.exists():
    raise FileNotFoundError(f"Input JSONL not found: {INPUT_JSONL}")

jsonl_for_processing = INPUT_JSONL
upstream_indexes = None

if UPSTREAM_IDS_FILE:
    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(f"Missing upstream ids file: {UPSTREAM_IDS_FILE}")

    upstream_indexes = load_ids_txt(UPSTREAM_IDS_FILE)
    if not upstream_indexes:
        print(f"[INFO] Upstream indexes file is empty: {UPSTREAM_IDS_FILE}")
        raise SystemExit(0)

    FILTERED_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_project_attributes_filtered.jsonl"
    filter_jsonl_by_indexes(INPUT_JSONL, FILTERED_JSONL, upstream_indexes)
    jsonl_for_processing = FILTERED_JSONL

df = json_to_csv(jsonl_for_processing, OUT_CSV)

if df.empty:
    print("[INFO] No rows found after filtering / parsing.")
    raise SystemExit(0)

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
    , mt.EmergencyTitle                 AS emergency_title_en
    , mt.EmergencyTitleAR               AS emergency_title_ar
    , pa.subsector_en                   AS extracted_subsector_en
    , mt.SubSectorNameEnglish           AS subsector_en
    , sect.SectorNameEnglish            AS extracted_sector_en
    , mt.SectorNameEnglish              AS sector_en
    , cl.groupNameEnglish               AS extracted_cluster_en
    , mtcl.groupNameEnglish             AS cluster_en
    , acat.AssistanceCategoryEnglish    AS extracted_assistance_category_en
    , mt.AssistanceCategoryEnglish      AS assistance_category_en
    , NULL                              AS extracted_indicator_en
    , mt.IndicatorEnglish               AS indicator_en
    , NULL                              AS extracted_target_en
    , mt.Target_English                 AS target_en
    , NULL                              AS extracted_goal_en
    , mt.NameEnglish                    AS goal_en
    , pa.document_id
    , CURRENT_TIMESTAMP                 AS ts_inserted
FROM silver.stg_project_attributes pa
LEFT JOIN silver.cleaned_project p
    ON pa.project_code = p.project_code
LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal mt
    ON pa.[index] = mt.[index]

LEFT JOIN dbo.SubSectors subsect
    ON LOWER(pa.subsector_en) = LOWER(subsect.SubSectorNameEnglish)
LEFT JOIN dbo.Sectors sect
    ON subsect.OCFA_Code = sect.OCFA_Code
LEFT JOIN dbo.Clusters cl
    ON subsect.Culster_Code = cl.groupID
LEFT JOIN dbo.AssistanceCategory acat
    ON subsect.Cat_ID = acat.ID

LEFT JOIN dbo.SubSectors mtsubsect
    ON LOWER(mt.SubSectorNameEnglish) = LOWER(mtsubsect.SubSectorNameEnglish)
LEFT JOIN dbo.Clusters mtcl
    ON mtsubsect.Culster_Code = mtcl.groupID

LEFT JOIN dbo.Targets t
    ON LOWER(pa.target_en) = LOWER(t.Target_English)
LEFT JOIN dbo.DevelopmentGoals g
    ON LOWER(t.Goal_ID) = LOWER(g.ID)
"""

if upstream_indexes is not None:
    with engine.begin() as conn:
        conn.execute(sql_text("""
            IF OBJECT_ID('tempdb..#run_indexes') IS NOT NULL DROP TABLE #run_indexes;
            CREATE TABLE #run_indexes ([index] NVARCHAR(255) NOT NULL PRIMARY KEY);
        """))

        pd.DataFrame({"index": sorted(upstream_indexes)}).to_sql(
            "#run_indexes",
            conn,
            if_exists="append",
            index=False,
            method=None,
        )

        conn.execute(
            sql_text(f"""
                DELETE T
                FROM {FINAL_SCHEMA}.{FINAL_TABLE} AS T
                INNER JOIN #run_indexes AS R
                    ON CAST(T.[index] AS NVARCHAR(255)) COLLATE DATABASE_DEFAULT
                     = R.[index] COLLATE DATABASE_DEFAULT;
            """)
        )

    with engine.begin() as conn:
        conn.execute(
            sql_text(f"""
                INSERT INTO {FINAL_SCHEMA}.{FINAL_TABLE}
                (
                    [index],
                    project_code,
                    project_title_en,
                    project_title_ar,
                    project_description_en,
                    project_description_ar,
                    emergency_title_en,
                    emergency_title_ar,
                    extracted_subsector_en,
                    subsector_en,
                    extracted_sector_en,
                    sector_en,
                    extracted_cluster_en,
                    cluster_en,
                    extracted_assistance_category_en,
                    assistance_category_en,
                    extracted_indicator_en,
                    indicator_en,
                    extracted_target_en,
                    target_en,
                    extracted_goal_en,
                    goal_en,
                    document_id,
                    ts_inserted
                )
                {OUTPUT_QUERY}
            """)
        )
else:
    with engine.begin() as conn:
        conn.execute(sql_text(f"""
            IF OBJECT_ID('{FINAL_SCHEMA}.{FINAL_TABLE}', 'U') IS NOT NULL
                DROP TABLE {FINAL_SCHEMA}.{FINAL_TABLE};
        """))

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