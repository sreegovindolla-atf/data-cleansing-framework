STEP_CONFIG = {
    1: {
        "type": "sql_to_table",
        "description": "Unique ADFD projects",
        "depends_on": [],
        "sql": """
        select
            max(a.[index])                              AS ref_index
            , max(a.master_project_code)                AS ref_master_project_code
            , max(a.master_project_title_en)            AS master_project_title_en
            , max(a.master_project_title_ar)            AS master_project_title_ar
            , max(a.master_project_description_en)      AS master_project_description_en
            , max(a.master_project_description_ar)      AS master_project_description_ar
            , string_agg(cast(b.year as varchar(20)), ', ')                  AS year_list
            , max(b.CountryNameEnglish)                 AS country_en
            , max(b.DonorNameEnglish)                   AS donor_en
            , string_agg(cast(b.ImplementingOrganizationEnglish as varchar(max)), ', ')  AS implementing_org_en
            , string_agg(cast(a.[index] as varchar(max)), ', ')              AS index_list
            , b.SourceID
        from silver.cleaned_master_project a
        left join dbo.MasterTableDenormalizedCleanedFinal b
            on a.[index] = b.[index]
        where a.[index] like '%ADFD%' and b.SourceID is not null
        group by b.SourceID
        """,
        "target_table": "step1_adfd",
        "schema": "silver",
        "if_exists": "replace",
    },

    2: {
        "type": "sql_to_table",
        "description": "Widow transactions as unique projects",
        "depends_on": [1],
        "sql": """
            WITH combo_counts AS (
                SELECT
                    b.DonorNameEnglish,
                    b.CountryNameEnglish, 
                    b.ImplementingOrganizationEnglish,
                    COUNT(*) AS cnt
                FROM silver.cleaned_master_project a
                LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                    ON a.[index] = b.[index]
                -- Eliminate step1 adfd projects
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.step1_adfd_indexes adfd
                        WHERE adfd.[index] = a.[index]
                  )

                GROUP BY
                    b.DonorNameEnglish,
                    b.CountryNameEnglish,
                    b.ImplementingOrganizationEnglish
            )

            -- non-DAB
            SELECT
                a.[index]
                , a.master_project_code
                , a.master_project_title_en
                , a.master_project_title_ar
                , a.master_project_description_en
                , a.master_project_description_ar
                , b.year
                , b.CountryNameEnglish AS country_en
                , b.DonorNameEnglish AS donor_en
                , b.ImplementingOrganizationEnglish AS implementing_org_en
                , b.SourceID
            FROM silver.cleaned_master_project a
            LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                ON a.[index] = b.[index]
            LEFT JOIN combo_counts c
                ON  c.DonorNameEnglish = b.DonorNameEnglish
                AND c.CountryNameEnglish = b.CountryNameEnglish
                AND c.ImplementingOrganizationEnglish = b.ImplementingOrganizationEnglish
            WHERE c.cnt = 1 and a.[index] not like '%DAR%'
                -- Eliminate sponsorships
                and a.master_project_title_en not like '%sponsor%'

            UNION

            --DAB with NULL sourceIDs
            SELECT
                a.[index]
                , a.master_project_code
                , a.master_project_title_en
                , a.master_project_title_ar
                , a.master_project_description_en
                , a.master_project_description_ar
                , b.year
                , b.CountryNameEnglish AS country_en
                , b.DonorNameEnglish AS donor_en
                , b.ImplementingOrganizationEnglish AS implementing_org_en
                , b.SourceID
            FROM silver.cleaned_master_project a
            LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                ON a.[index] = b.[index]
            LEFT JOIN combo_counts c
                ON  c.DonorNameEnglish = b.DonorNameEnglish
                AND c.CountryNameEnglish = b.CountryNameEnglish
                AND c.ImplementingOrganizationEnglish = b.ImplementingOrganizationEnglish
            WHERE c.cnt = 1 and a.[index] like '%DAR%' and b.SourceID is null
                -- Eliminate sponsorships
                and a.master_project_title_en not like '%sponsor%'
        """,
        "target_table": "step2_widow_input",
        "schema": "silver",
        "if_exists": "replace",
    },

    3: {
        "type": "cluster",
        "description": "Sponsorships and Seasonal Projects",
        "depends_on": [1, 2],
        "source_sql": """
            WITH CTE_step3_spn_ss AS (
                SELECT
                    a.[index],
                    a.master_project_title_en,
                    a.master_project_description_en,
                    a.master_project_title_ar,
                    a.master_project_description_ar,
                    b.[year],
                    b.DonorNameEnglish       AS donor_en,
                    b.CountryNameEnglish     AS country_en,
                    b.ImplementingOrganizationEnglish AS implementing_org_en,
                    CONCAT(
                        'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                        'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                        'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                        'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                    ) AS combined_text
                FROM silver.cleaned_master_project a
                LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                    ON a.[index] = b.[index]

                --Eliminate step1 indexes
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.step1_adfd_indexes adfd
                        WHERE adfd.[index] = a.[index]
                  )

                --Eliminate step2 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step2_widow_input_indexes w
                        WHERE w.[index] = a.[index]
                  )
                  AND a.master_project_title_en like '%sponsor%'
                  AND b.SubSectorNameEnglish = 'Seasonal programmes'
            )
            SELECT *
            FROM CTE_step3_spn_ss
        """,
        "input_table": "step3_spn_ss_input",
        "target_table": "step3_spn_ss_similar_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year"],
        "include_emergency": False,
        "schema": "silver",
        "if_exists": "replace",
    },

    4: {
        "type": "cluster",
        "description": "Sponsorships and Emergency Projects",
        "depends_on": [1, 2, 3],
        "source_sql": """
            SELECT
                a.[index],
                a.master_project_title_en,
                a.master_project_description_en,
                a.master_project_title_ar,
                a.master_project_description_ar,
                b.EmergencyTitle,
                b.EmergencyTitleAR,
                b.[year],
                b.DonorNameEnglish       AS donor_en,
                b.CountryNameEnglish     AS country_en,
                b.ImplementingOrganizationEnglish AS implementing_org_en,
                CONCAT(
                    'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                    'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                    'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                    'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                ) AS combined_text
            FROM silver.cleaned_master_project a
            LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                ON a.[index] = b.[index]

            --Eliminate step1 indexes
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.step1_adfd_indexes adfd
                    WHERE adfd.[index] = a.[index]
              )

            --Eliminate step2 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step2_widow_input_indexes w
                    WHERE w.[index] = a.[index]
              )

              --Eliminate step3 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step3_spn_ss_input_indexes s
                    WHERE s.[index] = a.[index]
              )

              --Required Emergency Projects for step4
              AND a.master_project_title_en LIKE '%sponsor%'
              AND (b.EmergencyTitle IS NOT NULL
              OR b.EmergencyTitleAR IS NOT NULL)
              AND b.EmergencyTitleAR <> 'لايوجد'
              AND b.EmergencyTitleAR <> 'لا يوجد'
              AND b.EmergencyTitle <> 'Other'
        """,
        "input_table": "step4_spn_em_input",
        "target_table": "step4_spn_em_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year", "EmergencyTitle"],
        "include_emergency": True,
        "schema": "silver",
        "if_exists": "replace",
    },

    5: {
        "type": "cluster",
        "description": "Only Sponsorships Projects",
        "depends_on": [1, 2, 3, 4],
        "source_sql": """
            WITH CTE_step5_spn AS (
                SELECT
                    a.[index],
                    a.master_project_title_en,
                    a.master_project_description_en,
                    a.master_project_title_ar,
                    a.master_project_description_ar,
                    b.[year],
                    b.DonorNameEnglish       AS donor_en,
                    b.CountryNameEnglish     AS country_en,
                    b.ImplementingOrganizationEnglish AS implementing_org_en,
                    CONCAT(
                        'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                        'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                        'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                        'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                    ) AS combined_text
                FROM silver.cleaned_master_project a
                LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                    ON a.[index] = b.[index]

                --Eliminate step1 indexes
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.step1_adfd_indexes adfd
                        WHERE adfd.[index] = a.[index]
                  )

                --Eliminate step2 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step2_widow_input_indexes w
                        WHERE w.[index] = a.[index]
                  )

                  --Eliminate step3 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step3_spn_ss_input_indexes s
                        WHERE s.[index] = a.[index]
                  )

                  --Eliminate step4 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step4_spn_em_input_indexes e 
                        WHERE e.[index] = a.[index]
                  )
                  AND a.master_project_title_en like '%sponsor%'
            )
            SELECT *
            FROM CTE_step5_spn
        """,
        "input_table": "step5_spn_input",
        "target_table": "step5_spn_similar_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year"],
        "include_emergency": False,
        "schema": "silver",
        "if_exists": "replace",
    },

    6: {
        "type": "cluster",
        "description": "Seasonal and Emergency Projects",
        "depends_on": [1, 2, 3, 4],
        "source_sql": """
            WITH CTE_step6_ss_em AS (
                SELECT
                    a.[index],
                    a.master_project_title_en,
                    a.master_project_description_en,
                    a.master_project_title_ar,
                    a.master_project_description_ar,
                    b.EmergencyTitle,
                    b.EmergencyTitleAR,
                    b.[year],
                    b.DonorNameEnglish       AS donor_en,
                    b.CountryNameEnglish     AS country_en,
                    b.ImplementingOrganizationEnglish AS implementing_org_en,
                    CONCAT(
                        'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                        'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                        'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                        'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                    ) AS combined_text
                FROM silver.cleaned_master_project a
                LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                    ON a.[index] = b.[index]

                --Eliminate step1 indexes
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.step1_adfd_indexes adfd
                        WHERE adfd.[index] = a.[index]
                  )

                --Eliminate step2 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step2_widow_input_indexes w
                        WHERE w.[index] = a.[index]
                  )

                  --Eliminate step3 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step3_spn_ss_input_indexes s
                        WHERE s.[index] = a.[index]
                  )

                  --Eliminate step4 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step4_spn_em_input_indexes e 
                        WHERE e.[index] = a.[index]
                  )
                  AND b.SubSectorNameEnglish = 'Seasonal programmes'
                  AND (b.EmergencyTitle IS NOT NULL
                  OR b.EmergencyTitleAR IS NOT NULL)
                  AND b.EmergencyTitleAR <> 'لايوجد'
                  AND b.EmergencyTitleAR <> 'لا يوجد'
                  AND b.EmergencyTitle <> 'Other'

            )
            SELECT *
            FROM CTE_step6_ss_em
        """,
        "input_table": "step6_ss_em_input",
        "target_table": "step6_ss_em_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year", "EmergencyTitle"],
        "include_emergency": True,
        "schema": "silver",
        "if_exists": "replace",
    },

    7: {
        "type": "cluster",
        "description": "Only Seasonal Projects",
        "depends_on": [1, 2, 3, 4, 5, 6],
        "source_sql": """
            WITH CTE_step7_seasonal AS (
                SELECT
                    a.[index],
                    a.master_project_title_en,
                    a.master_project_description_en,
                    a.master_project_title_ar,
                    a.master_project_description_ar,
                    b.[year],
                    b.DonorNameEnglish       AS donor_en,
                    b.CountryNameEnglish     AS country_en,
                    b.ImplementingOrganizationEnglish AS implementing_org_en,
                    CONCAT(
                        'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                        'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                        'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                        'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                    ) AS combined_text
                FROM silver.cleaned_master_project a
                LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                    ON a.[index] = b.[index]

                --Eliminate step1 indexes
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.step1_adfd_indexes adfd
                        WHERE adfd.[index] = a.[index]
                  )

                --Eliminate step2 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step2_widow_input_indexes w
                        WHERE w.[index] = a.[index]
                  )

                  --Eliminate step3 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step3_spn_ss_input_indexes ss
                        WHERE ss.[index] = a.[index]
                  )

                  --Eliminate step4 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step4_spn_em_input_indexes se
                        WHERE se.[index] = a.[index]
                  )

                  --Eliminate step5 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step5_spn_input_indexes s
                        WHERE s.[index] = a.[index]
                  )

                  --Eliminate step6 indexes
                  AND NOT EXISTS (
                        SELECT 1
                        FROM silver.step6_ss_em_input_indexes sse
                        WHERE sse.[index] = a.[index]
                  )
                  AND b.SubSectorNameEnglish = 'Seasonal programmes'
            )
            SELECT *
            FROM CTE_step7_seasonal
        """,
        "input_table": "step7_seasonal_input",
        "target_table": "step7_seasonal_similar_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year"],
        "include_emergency": False,
        "schema": "silver",
        "if_exists": "replace",
    },

    8: {
        "type": "cluster",
        "description": "Only Emergency Projects",
        "depends_on": [1, 2, 3, 4, 5, 6, 7],
        "source_sql": """
            SELECT
                a.[index],
                a.master_project_title_en,
                a.master_project_description_en,
                a.master_project_title_ar,
                a.master_project_description_ar,
                b.EmergencyTitle,
                b.EmergencyTitleAR,
                b.[year],
                b.DonorNameEnglish       AS donor_en,
                b.CountryNameEnglish     AS country_en,
                b.ImplementingOrganizationEnglish AS implementing_org_en,
                CONCAT(
                    'EN_TITLE: ', COALESCE(a.master_project_title_en,''), ' | ',
                    'EN_DESC: ',  COALESCE(a.master_project_description_en,''), ' || ',
                    'AR_TITLE: ', COALESCE(a.master_project_title_ar,''), ' | ',
                    'AR_DESC: ',  COALESCE(a.master_project_description_ar,'')
                ) AS combined_text
            FROM silver.cleaned_master_project a
            LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
                ON a.[index] = b.[index]
            
            --Eliminate step1 indexes
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.step1_adfd_indexes adfd
                    WHERE adfd.[index] = a.[index]
              )
            
             --Eliminate step2 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step2_widow_input_indexes w
                    WHERE w.[index] = a.[index]
              )
            
              --Eliminate step3 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step3_spn_ss_input_indexes ss
                    WHERE ss.[index] = a.[index]
              )
            
              --Eliminate step4 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step4_spn_em_input_indexes se
                    WHERE se.[index] = a.[index]
              )
            
              --Eliminate step5 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step5_spn_input_indexes s
                    WHERE s.[index] = a.[index]
              )
            
              --Eliminate step6 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step6_ss_em_input_indexes sse
                    WHERE sse.[index] = a.[index]
              )
            
              --Eliminate step7 indexes
              AND NOT EXISTS (
                    SELECT 1
                    FROM silver.step7_seasonal_input_indexes ssn
                    WHERE ssn.[index] = a.[index]
              )
            
              --Required Emergency Projects for step8
              AND (b.EmergencyTitle IS NOT NULL
              OR b.EmergencyTitleAR IS NOT NULL)
              AND b.EmergencyTitleAR <> 'لايوجد'
              AND b.EmergencyTitleAR <> 'لا يوجد'
              AND b.EmergencyTitle <> 'Other'
        """,
        "input_table": "step8_emergency_input",
        "target_table": "step8_emergency_similar_clusters",
        "group_cols": ["donor_en", "country_en", "implementing_org_en", "year", "EmergencyTitle"],
        "include_emergency": True,
        "schema": "silver",
        "if_exists": "replace",
    },
}