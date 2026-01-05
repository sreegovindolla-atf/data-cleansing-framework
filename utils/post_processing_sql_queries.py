QUERIES = [
    """TRUNCATE TABLE dbo.cleaned_master_project""",

    """TRUNCATE TABLE dbo.cleaned_project""",

    """TRUNCATE TABLE dbo.cleaned_project_asset""",

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
