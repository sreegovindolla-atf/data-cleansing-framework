SEMANTIC_SIMILARITY_CONFIG = {
    "master projects": {
        "emb_table": "silver.master_project_embeddings",
        "target_table":  "similar_master_projects",
        "source_sql": """
        SELECT
            a.[index]
          , b.SourceID                            AS source_id
          , a.master_project_title_en             AS master_project_title_en
          , a.master_project_description_en       AS master_project_description_en
          , a.master_project_title_ar             AS master_project_title_ar
          , a.master_project_description_ar       AS master_project_description_ar
          , b.year
          , b.CountryNameEnglish                  AS country_name_en
          , b.DonorNameEnglish                    AS donor_name_en
          , b.ImplementingOrganizationEnglish     AS implementing_org_en
          , b.SubSectorNameEnglish                AS subsector_name_en
          , b.amount
          , a.embedding
        FROM silver.master_project_embeddings a
        LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
          ON a.[index] = b.[index];
        """,

        #Input column names
        "title_en": "master_project_title_en",
        "desc_en": "master_project_description_en",
        "title_ar": "master_project_title_ar",
        "desc_ar": "master_project_description_ar",

        #Output column names
        "out_title_en": "master_project_title_en",
        "out_desc_en": "master_project_description_en",
        "out_title_ar": "master_project_title_ar",
        "out_desc_ar": "master_project_description_ar",

        "out_sim_title_en": "similar_master_project_title_en",
        "out_sim_desc_en": "similar_master_project_description_en",
        "out_sim_title_ar": "similar_master_project_title_ar",
        "out_sim_desc_ar": "similar_master_project_description_ar",
        
        "extra_cols": [],                 # none
        "extra_sim_cols": [],             # none

        "insert_adfd_sql": """
            WITH adfd AS (
            SELECT
                cp.[index]
                , mt.SourceID             AS source_id
                , cp.master_project_title_en
                , COALESCE(cp.master_project_description_en, mt.DescriptionEnglish) AS master_project_description_en 
                , cp.master_project_title_ar
                , COALESCE(cp.master_project_description_ar, mt.DescriptionArabic) AS master_project_description_ar 
                , mt.CountryNameEnglish                  AS country_name_en
                , mt.DonorNameEnglish                    AS donor_name_en
                , mt.ImplementingOrganizationEnglish     AS implementing_org_en
                , mt.year
                , mt.SubSectorNameEnglish                AS subsector_name_en
                , mt.amount
            FROM silver.cleaned_master_project cp
            JOIN dbo.MasterTableDenormalizedCleanedFinal mt
                ON cp.[index] = mt.[index]
            WHERE cp.[index] LIKE 'ADFD%'
            )

            INSERT INTO silver.similar_master_projects (
                  [index]
                , source_id
                , master_project_title_en
                , master_project_description_en
                , master_project_title_ar
                , master_project_description_ar
                , country_name_en
                , donor_name_en
                , implementing_org_en
                , year
                , subsector_name_en
                , amount

                , similar_index
                , similar_source_id
                , similar_master_project_title_en
                , similar_master_project_description_en
                , similar_master_project_title_ar
                , similar_master_project_description_ar
                , similar_country_name_en
                , similar_donor_name_en
                , similar_implementing_org_en
                , similar_year
                , similar_subsector_name_en
                , similar_amount

                , similarity_score
                , source_id_match
                , ts_inserted
            )
            SELECT
                a.[index]
                , a.source_id
                , a.master_project_title_en
                , a.master_project_description_en
                , a.master_project_title_ar
                , a.master_project_description_ar
                , a.country_name_en
                , a.donor_name_en
                , a.implementing_org_en
                , a.year
                , a.subsector_name_en
                , a.amount

                , b.[index]                         AS similar_index
                , b.source_id                       AS similar_source_id
                , b.master_project_title_en         AS similar_master_project_title_en
                , b.master_project_description_en   AS similar_master_project_description_en
                , b.master_project_title_ar         AS similar_master_project_title_ar
                , b.master_project_description_ar   AS similar_master_project_description_ar
                , b.country_name_en                 AS  similar_country_name_en
                , b.donor_name_en                   AS  similar_donor_name_en
                , b.implementing_org_en             AS  similar_implementing_org_en
                , b.year                            AS  similar_year
                , b.subsector_name_en               AS  similar_subsector_name_en
                , b.amount                          AS  similar_amount

                , CAST(1.0 AS FLOAT)       AS similarity_score
                , CASE
                    WHEN a.source_id = b.source_id THEN 1
                    ELSE 0
                    END AS source_id_match
                , CURRENT_TIMESTAMP        AS ts_inserted
            FROM adfd a
            JOIN adfd b
              ON a.source_id = b.source_id
             AND a.[index] <> b.[index];
            """
    },

    "projects": {
        "emb_table": "silver.project_embeddings",
        "target_table":  "similar_projects",
        "source_sql": """
        SELECT
            a.[index]
          , b.SourceID                            AS source_id
          , a.project_code                        AS project_code
          , a.project_title_en                    AS project_title_en
          , a.project_description_en              AS project_description_en
          , a.project_title_ar                    AS project_title_ar
          , a.project_description_ar              AS project_description_ar
          , b.year
          , b.CountryNameEnglish                  AS country_name_en
          , b.DonorNameEnglish                    AS donor_name_en
          , b.ImplementingOrganizationEnglish     AS implementing_org_en
          , b.SubSectorNameEnglish                AS subsector_name_en
          , b.amount
          , a.embedding
        FROM silver.project_embeddings a
        LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
          ON a.[index] = b.[index];
        """,
        
        #Input column names
        "title_en": "project_title_en",
        "desc_en": "project_description_en",
        "title_ar": "project_title_ar",
        "desc_ar": "project_description_ar",

        #Output column names
        "out_title_en": "project_title_en",
        "out_desc_en": "project_description_en",
        "out_title_ar": "project_title_ar",
        "out_desc_ar": "project_description_ar",

        "out_sim_title_en": "similar_project_title_en",
        "out_sim_desc_en": "similar_project_description_en",
        "out_sim_title_ar": "similar_project_title_ar",
        "out_sim_desc_ar": "similar_project_description_ar",

        "extra_map": {"project_code": "project_code"},
        "extra_sim_map": {"project_code": "similar_project_code"},

        "insert_adfd_sql": """
            WITH adfd AS (
            SELECT
                cp.[index]
                , mt.SourceID             AS source_id
                , cp.project_code
                , cp.project_title_en
                , cp.project_description_en 
                , cp.project_title_ar
                , cp.project_description_ar
                , mt.CountryNameEnglish                  AS country_name_en
                , mt.DonorNameEnglish                    AS donor_name_en
                , mt.ImplementingOrganizationEnglish     AS implementing_org_en
                , mt.year
                , mt.SubSectorNameEnglish                AS subsector_name_en
                , mt.amount
            FROM silver.cleaned_project cp
            JOIN dbo.MasterTableDenormalizedCleanedFinal mt
                ON cp.[index] = mt.[index]
            WHERE cp.[index] LIKE 'ADFD%'
            )

            INSERT INTO silver.similar_projects (
                  [index]
                , source_id
                , project_code
                , project_title_en
                , project_description_en
                , project_title_ar
                , project_description_ar
                , country_name_en
                , donor_name_en
                , implementing_org_en
                , year
                , subsector_name_en
                , amount

                , similar_index
                , similar_project_code
                , similar_source_id
                , similar_project_title_en
                , similar_project_description_en
                , similar_project_title_ar
                , similar_project_description_ar
                , similar_country_name_en
                , similar_donor_name_en
                , similar_implementing_org_en
                , similar_year
                , similar_subsector_name_en
                , similar_amount

                , similarity_score
                , source_id_match
                , ts_inserted
            )
            SELECT
                a.[index]
                , a.source_id
                , a.project_code
                , a.project_title_en
                , a.project_description_en
                , a.project_title_ar
                , a.project_description_ar
                , a.country_name_en
                , a.donor_name_en
                , a.implementing_org_en
                , a.year
                , a.subsector_name_en
                , a.amount

                , b.[index]                         AS similar_index
                , b.project_code                    AS similar_project_code
                , b.source_id                       AS similar_source_id
                , b.project_title_en                AS similar_project_title_en
                , b.project_description_en          AS similar_project_description_en
                , b.project_title_ar                AS similar_project_title_ar
                , b.project_description_ar          AS similar_project_description_ar
                , b.country_name_en                 AS similar_country_name_en
                , b.donor_name_en                   AS similar_donor_name_en
                , b.implementing_org_en             AS similar_implementing_org_en
                , b.year                            AS similar_year
                , b.subsector_name_en               AS similar_subsector_name_en
                , b.amount                          AS similar_amount

                , CAST(1.0 AS FLOAT)       AS similarity_score
                , CASE
                    WHEN a.source_id = b.source_id THEN 1
                    ELSE 0
                    END AS source_id_match
                , CURRENT_TIMESTAMP        AS ts_inserted
            FROM adfd a
            JOIN adfd b
              ON a.source_id = b.source_id
             AND a.[index] <> b.[index];
            """
    }
}


COMPUTE_EMB_CONFIG = {
    "master projects": {
        "target_table": "master_project_embeddings",
        "out_csv_name": "master_project_embeddings.csv",
        "text_cols": [
            "master_project_title_en",
            "master_project_description_en",
            "master_project_title_ar",
            "master_project_description_ar",
        ],
        "output_cols": [
            "index",
            "master_project_title_en",
            "master_project_description_en",
            "master_project_title_ar",
            "master_project_description_ar",
        ],
        "source_sql": """
                        SELECT DISTINCT
                            a.[index]
                            , a.master_project_title_en
                            , COALESCE(a.master_project_description_en, b.DescriptionEnglish) AS master_project_description_en      
                            , a.master_project_title_ar         
                            , COALESCE(a.master_project_description_ar, b.DescriptionArabic) AS master_project_description_ar 
                        FROM silver.cleaned_master_project a
                        LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                            ON a.[index] = b.[index]
                        WHERE 1=1
                        AND a.[index] NOT LIKE '%ADFD-%'
                        """
            },
    "projects": {
        "target_table": "project_embeddings",
        "out_csv_name": "project_embeddings.csv",
        "text_cols": [
            "project_title_en",
            "project_description_en",
            "project_title_ar",
            "project_description_ar",
        ],
        "output_cols": [
            "index",
            "project_code",
            "project_title_en",
            "project_description_en",
            "project_title_ar",
            "project_description_ar",
        ],
        "source_sql": """
                        SELECT DISTINCT
                            [index]
                            , project_code
                            , project_title_en
                            , project_description_en     
                            , project_title_ar         
                            , project_description_ar
                        FROM silver.cleaned_project
                        WHERE 1=1
                        AND [index] NOT LIKE '%ADFD-%'
                        """
    }
}