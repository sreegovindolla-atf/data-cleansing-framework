QUERIES = [
    """TRUNCATE TABLE silver.cleaned_master_project""",

    """TRUNCATE TABLE silver.cleaned_project""",

    """TRUNCATE TABLE silver.cleaned_project_asset""",

    """INSERT INTO silver.cleaned_master_project
       SELECT DISTINCT
           a.[Index],
           master_project_code,
           master_project_title_en,
           COALESCE(a.master_project_description_en, b.master_project_description_en)   AS master_project_description_en,
           master_project_title_ar,
           COALESCE(a.master_project_description_ar, b.master_project_description_ar)   AS master_project_description_ar,
           input_text,
           master_project_amount_actual,
           FORMAT(master_project_amount_actual, 'N2') AS formatted_master_project_amount_actual,
           master_project_oda_amount,
           FORMAT(master_project_oda_amount, 'N2') AS formatted_master_project_oda_amount,
           master_project_ge_amount,
           FORMAT(master_project_ge_amount, 'N2') AS formatted_master_project_ge_amount,
           master_project_off_amount,
           FORMAT(master_project_off_amount, 'N2') AS formatted_master_project_off_amount
       FROM silver.MasterTable_extracted a
       LEFT JOIN silver.cleaned_master_project_description b
        ON a.[index] = b.[index]""",

    """INSERT INTO silver.cleaned_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           a.project_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           b.project_type,
           beneficiary_count,
           beneficiary_group_name,
           input_text,
           project_amount_actual,
           FORMAT(project_amount_actual, 'N2') AS formatted_project_amount_actual,
           project_amount_extracted,
           FORMAT(project_amount_extracted, 'N2') AS formatted_project_amount_extracted,
           project_oda_amount,
           FORMAT(project_oda_amount, 'N2') AS formatted_project_oda_amount,
           project_ge_amount,
           FORMAT(project_ge_amount, 'N2') AS formatted_project_ge_amount,
           project_off_amount,
           FORMAT(project_off_amount, 'N2') AS formatted_project_off_amount
       FROM silver.MasterTable_extracted a
       LEFT JOIN silver.cleaned_project_type b
        ON a.project_code = b.project_code;""",

    """INSERT INTO silver.cleaned_project_asset
       SELECT DISTINCT
           [Index],
           master_project_code,
           a.project_code,
           CONCAT(a.project_code, '-', 
            CASE 
                WHEN asset is not null THEN asset
                WHEN item is not null THEN item
                ELSE 'Unknown'
            END
           )         
           AS project_asset_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           b.project_type,
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
       FROM silver.MasterTable_extracted a
       LEFT JOIN silver.cleaned_project_type b
        ON a.project_code = b.project_code"""
]


OLLAMA_QUERIES = [
    """TRUNCATE TABLE ollama.cleaned_master_project""",

    """TRUNCATE TABLE ollama.cleaned_project""",

    """TRUNCATE TABLE ollama.cleaned_project_asset""",

    """INSERT INTO ollama.cleaned_master_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           master_project_title_en,
           master_project_title_ar,
           input_text,
           master_project_amount_actual,
           FORMAT(master_project_amount_actual, 'N2') AS formatted_master_project_amount_actual,
           master_project_oda_amount,
           FORMAT(master_project_oda_amount, 'N2') AS formatted_master_project_oda_amount,
           master_project_ge_amount,
           FORMAT(master_project_ge_amount, 'N2') AS formatted_master_project_ge_amount,
           master_project_off_amount,
           FORMAT(master_project_off_amount, 'N2') AS formatted_master_project_off_amount
       FROM ollama.MasterTable_extracted""",

    """INSERT INTO ollama.cleaned_project
       SELECT DISTINCT
           [Index],
           master_project_code,
           a.project_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           b.project_type,
           CASE 
            WHEN beneficiary_group_name like '%famil%' THEN beneficiary_count * 4
            ELSE beneficiary_count
           END AS beneficiary_count,
           beneficiary_group_name,
           input_text,
           project_amount_actual,
           FORMAT(project_amount_actual, 'N2') AS formatted_project_amount_actual,
           project_amount_extracted,
           FORMAT(project_amount_extracted, 'N2') AS formatted_project_amount_extracted,
           project_oda_amount,
           FORMAT(project_oda_amount, 'N2') AS formatted_project_oda_amount,
           project_ge_amount,
           FORMAT(project_ge_amount, 'N2') AS formatted_project_ge_amount,
           project_off_amount,
           FORMAT(project_off_amount, 'N2') AS formatted_project_off_amount
       FROM ollama.MasterTable_extracted a
       LEFT JOIN silver.cleaned_project_type b
        ON a.project_code = b.project_code;""",

    """INSERT INTO ollama.cleaned_project_asset
       SELECT DISTINCT
           [Index],
           master_project_code,
           a.project_code,
           CONCAT(a.project_code, '-', 
            CASE 
                WHEN asset is not null THEN asset
                WHEN item is not null THEN item
                ELSE 'Unknown'
            END
           )         
           AS project_asset_code,
           project_title_en,
           project_title_ar,
           project_description_en,
           project_description_ar,
           b.project_type,
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
       FROM ollama.MasterTable_extracted a
       LEFT JOIN silver.cleaned_project_type b
        ON a.project_code = b.project_code"""
]
