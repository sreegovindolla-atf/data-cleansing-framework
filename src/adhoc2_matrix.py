from sqlalchemy import create_engine, text
import urllib

# ----------------------------------
# SQL Server connection (Windows Auth)
# ----------------------------------
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )

engine = get_sql_server_engine()

# ----------------------------------
# CREATE OR ALTER VIEW
# ----------------------------------
CREATE_VIEW_SQL = """
CREATE OR ALTER VIEW silver.donor_projects_matrix AS
WITH project_counts AS (
    SELECT
          b.DonorNameEnglish
        , cp.[index]
        , COUNT(*) AS project_rows_count
    FROM silver.cleaned_project cp
    LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
      ON cp.[index] = b.[index]
    GROUP BY
          b.DonorNameEnglish
        , cp.[index]
),
donor_matrix AS (
    SELECT
          DonorNameEnglish
        , SUM(CASE WHEN project_rows_count = 1 THEN 1 ELSE 0 END) AS single_projects_count
        , SUM(CASE WHEN project_rows_count > 1 THEN 1 ELSE 0 END) AS multiple_projects_count
        , COUNT(*) AS total_distinct_projects
    FROM project_counts
    GROUP BY DonorNameEnglish
)
SELECT *
FROM donor_matrix;
"""

print("[VIEW] Creating / updating silver.donor_projects_matrix ...")
with engine.begin() as conn:
    conn.execute(text(CREATE_VIEW_SQL))
print("[VIEW] View ready")