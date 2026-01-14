QUERIES = [
    """TRUNCATE TABLE silver.cleaned_master_project""",

    """TRUNCATE TABLE silver.cleaned_project""",

    """TRUNCATE TABLE silver.cleaned_project_asset""",

    """INSERT INTO silver.cleaned_master_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           master_project_title_en,
           master_project_title_ar,
           input_text,
           master_project_amount_actual,
           master_project_oda_amount,
           master_project_ge_amount,
           master_project_off_amount
       FROM silver.MasterTable_extracted""",

    """INSERT INTO silver.cleaned_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           project_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           beneficiary_count,
           beneficiary_group_name,
           input_text,
           project_amount_actual,
           project_amount_extracted,
           project_oda_amount,
           project_ge_amount,
           project_off_amount
       FROM silver.MasterTable_extracted""",

    """INSERT INTO silver.cleaned_project_asset
       SELECT DISTINCT
           [Index],
           master_project_code,
           project_code,
           CONCAT(project_code, '-', asset)         AS project_asset_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           asset,
           asset_category,
           asset_quantity,
           asset_quantity_uom,
           asset_capacity,
           asset_capacity_uom,
           item,
           item_category,
           item_quantity,
           item_quantity_uom,
           input_text
       FROM silver.MasterTable_extracted"""
]
