from pathlib import Path
import langextract as lx

# =========================================================
# COMMON PATHS
# =========================================================
BASE_OUTPUT_DIR = Path("data/outputs")


GENERIC_CONFIG = {
    "asset": {
        # -----------------------------
        # Extraction settings
        # -----------------------------
        "extraction": {
            "output_jsonl_suffix": "asset_extraction.jsonl",
            "output_json_suffix": "asset_extraction.json",
            "cache_suffix": "asset_cache.pkl",
            "index_key": "project_code",
            "upstream_filter_column": "index",
            "checkpoint_every": 50,
            "force_refresh_supported": True,
            "use_schema_constraints": False,
            "model_id": "gpt-4.1-mini",

            "source_query": """
                SELECT
                    cp.[index]
                    , pt.project_code
                    , cp.project_title_en
                    , cp.project_title_ar
                    , cp.project_description_en
                    , cp.project_description_ar
                    , pt.project_type
                FROM silver.cleaned_project_type pt
                LEFT JOIN silver.cleaned_project cp
                    ON cp.project_code = pt.project_code
                WHERE pt.project_type = 'Repair / Maintenance'
                AND NOT EXISTS (
                    SELECT 1
                    FROM silver.cleaned_project_asset_extracted pa
                    WHERE pa.project_code = pt.project_code
                )
            """,

            "prompt": """
Extract the following fields from the input text:
- asset
- asset_category
- asset_quantity
- asset_capacity
- asset_capacity_uom

CONTEXT:
Asset must be a place/structure, not a movable item, not supplies, not cash, not a program, and not a software/system.

STRICT RULES:
- Return EXACTLY ONE value for asset and EXACTLY ONE value for asset_category.
- If the project does NOT involve building/constructing/rehabilitating a physical structure, then:
  asset = "NULL"
  asset_category = "NULL"

WHAT COUNTS AS A VALID ASSET (examples):
- Hospital Building, Clinic Building, School Building, Classrooms, Health Center, Training Center,
  Rehabilitation Center, Community Center, Mosque, Water Well, Facility, Center.

WHAT MUST NEVER BE RETURNED AS AN ASSET (even if mentioned):
- Vehicles/transport: ambulance, car, bus, truck, boat
- Non-physical aid: cash assistance, vouchers, e-vouchers, electronic voucher system, food assistance
- Consumables/NFIs: blankets, clothes, hygiene kits/supplies, food baskets/parcels, medicines, textbooks/books
- Equipment/IT: computers, devices, machines, equipment, "computerized information system", software, platforms, databases
- Programs/projects/initiatives: initiatives, campaigns, training activities

ASSET TEXT RULES:
- asset must be concise (2-8 words), English only.
- If multiple valid physical structures exist, choose the PRIMARY / MOST DOMINANT one.
- Use common standard spelling and avoid variants.

asset_category RULES:
- asset_category MUST be exactly one of the allowed values below (match text exactly).
- Do NOT invent new values.

Allowed values:
- Facility / Building
- Center
- Well
- Mosque

asset_quantity RULES:
- asset_quantity = the number of assets being built or constructed (e.g., 1, 2, etc.)

asset_capacity RULES:
- Extract capacity ONLY when it represents a technical/physical/storage/output capacity.
- Do NOT extract people occupancy or attendance capacity.
- asset_capacity: number only
- asset_capacity_uom: unit only, singular
- If absent, return "NULL"

OUTPUT FORMAT:
Return EXACTLY 5 extractions, in this order:
1) extraction_class: asset
2) extraction_class: asset_category
3) extraction_class: asset_quantity
4) extraction_class: asset_capacity
5) extraction_class: asset_capacity_uom
            """.strip(),

            "examples": [
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Construction of a 40-bed hospital in Baralyn Island\n"
                        "EN_DESC: The project includes constructing a new hospital building and upgrading clinics.\n"
                        "AR_TITLE: بناء مستشفى بسعة 40 سريرًا\n"
                        "AR_DESC: يشمل المشروع بناء مستشفى جديد وترقية العيادات.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(extraction_class="asset", extraction_text="Hospital building"),
                        lx.data.Extraction(extraction_class="asset_category", extraction_text="Facility / Building"),
                        lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),
                        lx.data.Extraction(extraction_class="asset_capacity", extraction_text="40"),
                        lx.data.Extraction(extraction_class="asset_capacity_uom", extraction_text="Bed"),
                    ],
                ),
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Rehabilitation of damaged classrooms\n"
                        "EN_DESC: Repair and renovation works for a school building.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(extraction_class="asset", extraction_text="School building"),
                        lx.data.Extraction(extraction_class="asset_category", extraction_text="Facility / Building"),
                        lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),
                        lx.data.Extraction(extraction_class="asset_capacity", extraction_text="NULL"),
                        lx.data.Extraction(extraction_class="asset_capacity_uom", extraction_text="NULL"),
                    ],
                ),
            ],

            # Which columns are needed to build input text
            "text_builder": {
                "type": "bilingual_basic"
            },

            # Entity-specific extraction cleanup hooks
            "post_extract_rules": {
                "normalize_null_extraction_text": True,
                "asset_null_forces_category_null": True,
            },
        },

        # -----------------------------
        # Post-processing settings
        # -----------------------------
        "post_processing": {
            "input_jsonl_suffix": "asset_extraction.jsonl",
            "output_csv_suffix": "asset.csv",
            "target_table": "silver.cleaned_project_asset_extracted",
            "primary_key": "project_code",

            # final output column order
            "output_columns": [
                "project_code",
                "asset",
                "asset_category",
                "asset_quantity",
                "asset_capacity",
                "asset_capacity_uom",
            ],

            # extraction_class -> final column mapping
            "class_map": {
                "asset": "asset",
                "asset_category": "asset_category",
                "asset_quantity": "asset_quantity",
                "asset_capacity": "asset_capacity",
                "asset_capacity_uom": "asset_capacity_uom",
            },

            # output SQL datatypes
            "sql_types": {
                "project_code": 100,
                "asset": 255,
                "asset_category": 100,
                "asset_quantity": 50,
                "asset_capacity": 50,
                "asset_capacity_uom": 100,
            },

            "normalizers": {
                "asset": "normalize_class",
                "asset_category": "normalize_class",
                "asset_quantity": "identity",
                "asset_capacity": "identity",
                "asset_capacity_uom": "normalize_class",
            },
        },
    },

    "beneficiary_group": {
        "extraction": {
            "output_jsonl_suffix": "beneficiary_group_extraction.jsonl",
            "output_json_suffix": "beneficiary_group_extraction.json",
            "cache_suffix": "beneficiary_group_cache.pkl",
            "index_key": "project_code",
            "upstream_filter_column": "index",
            "checkpoint_every": 50,
            "force_refresh_supported": True,
            "use_schema_constraints": False,
            "model_id": "gpt-4.1-mini",

            "source_query": """
                SELECT
                    cp.[index]
                    , cp.project_code
                    , cp.project_title_en
                    , cp.project_title_ar
                    , cp.project_description_en
                    , cp.project_description_ar
                FROM silver.cleaned_project cp
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.cleaned_project_beneficiary_group b
                    WHERE b.project_code = cp.project_code
                )
            """,

            "prompt": """
Extract the following fields from the input text:
- beneficiary_group
- beneficiary_count

ALLOWED BENEFICIARY GROUPS
- Affected People
- Households / Families
- Women & girls
- Students
- Persons with disabilities
- Orphans
RULES:
- Return EXACTLY one value for beneficiary_group.
- beneficiary_group must be concise and in English.
- beneficiary_group MUST BE one of the values from ALLOWED BENEFICIARY GROUPS ONLY
- If not found, return "NULL".
- beneficiary_count should be numeric only if clearly mentioned.
- If beneficiary_count is not explicitly available or you are unsure, return 'Affected People'

OUTPUT FORMAT:
Return EXACTLY 2 extractions, in this order:
1) extraction_class: beneficiary_group
2) extraction_class: beneficiary_count
            """.strip(),

            "examples": [
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Food support for 120 orphan children\n"
                        "EN_DESC: Distribution of food support to 120 orphan children.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Orphan children"),
                        lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="120"),
                    ],
                ),
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Medical assistance for families\n"
                        "EN_DESC: Health support to vulnerable families.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(extraction_class="beneficiary_group", extraction_text="Families"),
                        lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="NULL"),
                    ],
                ),
            ],

            "text_builder": {
                "type": "bilingual_basic"
            },

            "post_extract_rules": {},
        },

        "post_processing": {
            "input_jsonl_suffix": "beneficiary_group_extraction.jsonl",
            "output_csv_suffix": "beneficiary_group.csv",
            "target_table": "silver.cleaned_project_beneficiary_group",
            "primary_key": "project_code",

            "output_columns": [
                "project_code",
                "beneficiary_group",
                "beneficiary_count",
            ],

            "class_map": {
                "beneficiary_group": "beneficiary_group",
                "beneficiary_count": "beneficiary_count",
            },

            "sql_types": {
                "project_code": 100,
                "beneficiary_group": 255,
                "beneficiary_count": 50,
            },

            "normalizers": {
                "beneficiary_group": "normalize_class",
                "beneficiary_count": "identity",
            },
        },
    },

    "project_type": {
        "extraction": {
            "output_jsonl_suffix": "project_type_extraction.jsonl",
            "output_json_suffix": "project_type_extraction.json",
            "cache_suffix": "project_type_cache.pkl",
            "index_key": "project_code",
            "upstream_filter_column": "index",
            "checkpoint_every": 50,
            "force_refresh_supported": True,
            "use_schema_constraints": False,
            "model_id": "gpt-4.1-mini",

            "source_query": """
                SELECT
                    cp.[index]
                    , cp.project_code
                    , cp.project_title_en
                    , cp.project_title_ar
                    , cp.project_description_en
                    , cp.project_description_ar
                FROM silver.cleaned_project cp
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.cleaned_project_type pt
                    WHERE pt.project_code = cp.project_code
                )
            """,

            "prompt": """
Extract the following field from the input text:
- project_type

RULES (STRICT):
- Return EXACTLY one value for project_type.
- project_type MUST be exactly one of the allowed values below.
- Do NOT invent new values. Do NOT return NULL.
- Choose the most dominant intervention type described.

DECISION GUIDANCE:
- If the project involves installing, setting up, commissioning, equipping, supplying and installing,
  sponsoring, seasonal, agricultural, economic development, support individuals and communities,
  classify it as:
  "Program Implementation / Operation"
- Choose "New Construction" ONLY if it explicitly describes constructing a NEW physical structure.
- Choose "Repair / Maintenance" when about rehabilitation/renovation/repair of an existing structure.
- Choose "Service Delivery" when the core intervention is ongoing delivery of services.
- Choose "Training / Capacity Building" when the core intervention is training/workshops/capacity building.
- If unclear, choose "Program Implementation / Operation".

ALLOWED VALUES:
- New Construction
- Repair / Maintenance
- Service Delivery
- Training / Capacity Building
- Program Implementation / Operation

OUTPUT FORMAT:
Return a single extraction with:
- extraction_class: project_type
- extraction_text: <one allowed value>
            """.strip(),

            "examples": [
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Kabul University Mosque - Construction\n"
                        "EN_DESC: Construction of a mosque within Kabul University.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(
                            extraction_class="project_type",
                            extraction_text="New Construction",
                        ),
                    ],
                ),
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: School Building Rehabilitation\n"
                        "EN_DESC: Rehabilitation and minor repairs to damaged classrooms.\n"
                    ),
                    extractions=[
                        lx.data.Extraction(
                            extraction_class="project_type",
                            extraction_text="Repair / Maintenance",
                        ),
                    ],
                ),
                lx.data.ExampleData(
                    text=(
                        "EN_TITLE: Seasonal Programs Outside the State\n"
                        "EN_DESC: Seasonal programs outside the State\n"
                    ),
                    extractions=[
                        lx.data.Extraction(
                            extraction_class="project_type",
                            extraction_text="Program Implementation / Operation",
                        ),
                    ],
                ),
            ],

            "text_builder": {
                "type": "bilingual_basic"
            },

            "post_extract_rules": {},
        },

        "post_processing": {
            "input_jsonl_suffix": "project_type_extraction.jsonl",
            "output_csv_suffix": "project_type.csv",
            "target_table": "silver.cleaned_project_type",
            "primary_key": "project_code",

            "output_columns": [
                "project_code",
                "project_type",
            ],

            "class_map": {
                "project_type": "project_type",
            },

            "sql_types": {
                "project_code": 100,
                "project_type": 100,
            },

            "normalizers": {
                "project_type": "identity",
            },
        },
    },
}