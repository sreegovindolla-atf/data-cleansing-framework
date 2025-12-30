import textwrap

# Project prompt focuses ONLY on project and beneficiary fields (no beneficiary fields here anymore)
PROJECT_PROMPT = textwrap.dedent("""\
Extract only these fields as labeled spans from the input text.

Single (optional) — applies to the whole input text:
- master_project_title

Repeatable (one per project inside the master text):
- project_title
- beneficiary_count
- beneficiary_group_name

Rules:
- You may output MULTIPLE project_title entries if multiple projects are listed in the same master text.
- beneficiary_count and beneficiary_group_name are PROJECT-LEVEL fields:
  - If the text contains beneficiaries for a specific project, extract them near that project.
  - If the text contains only one beneficiary set for the whole input and multiple projects are listed,
    attach that same beneficiary_count and beneficiary_group_name to each project (repeat them).
- If beneficiary info is not explicitly present, DO NOT output it.
- extraction_text must be string/int/float only (never null, never a list/dict).
- Omit fields not explicitly present.
""")

# Asset prompt focuses ONLY on project and asset fields (no beneficiary fields here anymore)
ASSET_PROMPT = textwrap.dedent("""\
Extract only these fields from the input text as labeled spans.

Repeatable (can repeat multiple times):
- project_title            (used as a section marker for assets)
- asset_quantity
- asset
- asset_quantity_uom

Rules:
- You may output MULTIPLE project_title entries if multiple projects are listed in the text.
- Assets listed AFTER a project_title belong to that project, until the next project_title appears.
- You may output MULTIPLE asset rows if multiple assets are listed.
- Each asset_quantity must correspond to the nearest matching asset in the text.
- extraction_text must be string/int/float only (never null, never a list/dict).
- Omit fields not explicitly present.
""")

# Combined prompt (for both project beneficiary and project assests),
PROMPT = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text.

Single per document:
- master_project_title

Repeatable (one per project):
- project_title
- beneficiary_count
- beneficiary_group_name

Repeatable (can repeat multiple times for a project):
- asset_quantity (number linked to an asset)
- asset (the item being delivered/built)
- asset_quantity_uom (unit of measure for the asset_quantity)

Mandatory fields:
- master_project_title  
- project_title

CRITICAL RULES:
1) Project Splitting Rules (very important):
- The input text may describe ONE master project that contains MULTIPLE sub-projects.

- Treat master_project_title as the overall umbrella/theme.

- Extract one project_title for EACH distinct sub-project ONLY when the text clearly describes multiple independent actions or initiatives that could reasonably be tracked, funded, or implemented separately.

- Financial disambiguation rule (important):
    - When a master project description lists multiple actions AND explicitly mentions separate monetary amounts, use the number of distinct monetary amounts as a strong signal for how many project_title entries to extract.
    - If two distinct amounts are clearly associated with two different actions, extract two project_title entries.
    - Do NOT create additional project_title entries for actions that do not have their own explicit amount; instead, group such actions with the most relevant amount-bearing project or keep them under the master scope.

- Do NOT split projects based solely on punctuation, formatting, or keywords such as "+", "/", ",", parentheses, or the presence of action verbs.

- Split the master project into multiple project_title entries ONLY when:
  - The text describes separate actions applied to different targets or entities
    (e.g., different buildings, locations, beneficiary groups, or deliverables), OR
  - The text enumerates multiple activities that are conceptually independent
    (i.e., removing one activity would still leave a complete and meaningful project).

- Do NOT create multiple project_title entries when:
  - Multiple words or verbs describe a single continuous scope of work,
  - Multiple components together form one unified intervention,
  - Multiple actions are tightly coupled and jointly describe how one project is executed.

- When in doubt, prefer fewer project_title entries rather than over-splitting.

2) Asset Rules:
- Asset pairing rules:
   - Each asset_quantity must correspond to the nearest matching asset in the text.
   - Each asset_quantity_uom must correspond to that asset_quantity.

- Asset tangibility rules (very important):

    - Extract ONLY tangible, concrete assets that can be physically delivered, constructed, installed, transported, or directly quantified.

    - Do NOT extract intangible activities, services, or abstract support mechanisms as assets, such as:
      - services, training, education, awareness, sensitization
      - support, assistance, facilitation, extension services
      - financial mechanisms, financing models, advisory services, capacity building

    - Exception:
      - If a financial or material instrument is explicitly described as a concrete deliverable
        (e.g., "cash grants", "agricultural finance disbursed", "loans provided", "vouchers", "e-vouchers"),
        THEN it may be extracted as an asset.
      - In such cases, prefer the most concrete representation (e.g., "Agricultural Finance", "Cash Grant", "Voucher")
        and ignore accompanying abstract services.

    - When multiple items are listed and some are tangible while others are intangible,
      extract ONLY the tangible assets and omit the intangible ones.

3) Beneficiary count extraction rules:
    - If beneficiary_count is expressed as “X per [unit]” (e.g., “60 students per school”) 
    and the text states the number of those units (e.g., “4 schools”), 
    then output the total beneficiaries as X × number_of_units (e.g., 60×4 = 240)

4) Do NOT extract components when a parent package/kit is present:
   - If the text explicitly mentions a parent container such as a package/basket/kit/parcel/mosque/school/hospital (e.g., "food package", "food basket", "winter package", "hygiene kit", "relief kit", "food parcel", "a mosque", "a school", "a hospital"),
     and then lists its contents (e.g., in parentheses or after words like "contains", "including", "consisting of", "composed of", "includes", "containing", "construction of"),
     THEN extract ONLY the parent container as the asset.
   - Do NOT extract the listed contents/items (e.g., rice, sugar, oil, pasta, sauce, flour, salt, tea, ablution seat, beds, rooms, etc.) as separate assets in that case.
   - Exception: If the contents are explicitly stated as separate deliveries outside the package (e.g., "food package + rice + sugar distributed separately"),
     then you may extract them separately.

5) When to extract individual items:
   - If there is NO parent container mentioned (no package/basket/kit/parcel/hospital/mosque/school), and the text lists items being delivered (e.g., "distributed rice, sugar, oil, constructed ablution seats, beds, rooms"),
     then extract each item as its own asset (with quantities/uom if present).

6) Output format:
   - extraction_text MUST be a string, integer, or float (never null, never a list/dict).
   - If a field is not explicitly present, DO NOT output an extraction for it.
   - Do not use attributes for now (leave attributes empty).
    - Return ONLY valid JSON (no markdown, no commentary).
    - The top-level JSON MUST have exactly these keys:
      - "text": the original input text (string)
      - "extractions": a list of objects

    Each item in "extractions" MUST be an object with:
    - "extraction_class": string
    - "extraction_text": string or number (never null, never list/dict)
    - "extraction_index": integer (order in which you found it; start at 0 and increment)

7) MANDATORY FALLBACK RULE (critical):

- master_project_title and project_title MUST ALWAYS be present.
- If the input text is short, generic, or does not explicitly describe a project:
    - Use the full input text verbatim as master_project_title.
    - Use the same value as project_title.
- Do NOT return an empty extractions list.
- At minimum, always return:
    - one master_project_title
    - one project_title
    
""")
