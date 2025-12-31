QUERIES = [
    """CREATE TABLE dbo.cleaned_master_project (
           [Index]                          NVARCHAR(50)       NOT NULL,
           master_project_code              NVARCHAR(50)      NULL, 
           master_project_title             NVARCHAR(MAX)      NULL,
           master_project_amount_actual     FLOAT               NULL,
           master_project_amount_extracted  FLOAT               NULL,
           master_project_oda_amount        FLOAT               NULL,
           master_project_ge_amount         FLOAT               NULL,
           input_text                       NVARCHAR(MAX)      NULL       
           )""",

    """INSERT INTO dbo.cleaned_master_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           master_project_title,
           master_project_amount_actual,
           master_project_amount_extracted,
           master_project_oda_amount,
           master_project_ge_amount,
           input_text
       FROM dbo.MasterTable_extracted""",

    """CREATE TABLE dbo.cleaned_project (
           [Index]                          NVARCHAR(50)       NOT NULL,
           project_code                     NVARCHAR(50)      NULL,
           project_title                    NVARCHAR(MAX)      NULL,
           master_project_code              NVARCHAR(50)      NULL, 
           master_project_title             NVARCHAR(MAX)      NULL,
           beneficiary_count                INT                 NULL,
           beneficiary_group_name           NVARCHAR(50)      NULL,
           project_amount_actual            FLOAT               NULL,
           project_amount_extracted         FLOAT               NULL,
           project_oda_amount               FLOAT               NULL,
           project_ge_amount                FLOAT               NULL,
           input_text                       NVARCHAR(MAX)      NULL       
           )""",

    """INSERT INTO dbo.cleaned_project
       SELECT DISTINCT
           [Index],
           project_code,
           project_title,
           master_project_code,
           master_project_title,
           beneficiary_count,
           beneficiary_group_name,
           project_amount_actual,
           project_amount_extracted,
           project_oda_amount,
           project_ge_amount,
           input_text
       FROM dbo.MasterTable_extracted""",

    """CREATE TABLE dbo.cleaned_project_asset (
           [Index]                          NVARCHAR(50)       NOT NULL,
           project_asset_code               NVARCHAR(50)      NULL,
           project_code                     NVARCHAR(50)      NULL,
           project_title                    NVARCHAR(MAX)      NULL,
           master_project_code              NVARCHAR(50)      NULL, 
           master_project_title             NVARCHAR(MAX)      NULL,
           asset                            NVARCHAR(50)                NULL,
           asset_quantity                   INT                 NULL,
           asset_quantity_uom               NVARCHAR(50)                NULL,
           input_text                       NVARCHAR(MAX)      NULL       
           )""",

    """INSERT INTO dbo.cleaned_project_asset
       SELECT DISTINCT
           [Index],
           CONCAT(project_code, '-', asset)         AS project_asset_code,
           project_code,
           project_title,
           master_project_code,
           master_project_title,
           asset,
           asset_quantity,
           asset_quantity_uom,
           input_text
       FROM dbo.MasterTable_extracted"""
]
