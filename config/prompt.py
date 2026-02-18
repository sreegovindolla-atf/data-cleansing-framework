import textwrap
from typing import List

PROMPT = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text.

Single per document (Master project fields):
- The following combination of columns indicate one master project. These fields must NOT repeat more than once per document.
  - master_project_title_en
  - master_project_description_en
  - master_project_title_ar
  - master_project_description_ar

Repeatable (Project fields):
- The following combination of columns indicate one project. One master project can have one or more projects.
  - project_title_en
  - project_title_ar
  - project_description_en
  - project_description_ar
  - beneficiary_count
  - beneficiary_group_name
  - project_amount_extracted

Repeatable (Project Asset/Item fields):
- The following combination of columns indicate one asset/item for a project. One project can have one or more assets/items.
  Assets (physical constructions or major deliverables):
  - asset
  - asset_category
  - asset_quantity
  - asset_capacity
  - asset_capacity_uom
  Items (non-asset tangible goods or consumables):
  - item
  - item_category
  - item_quantity
  - item_quantity_uom

Mandatory fields:
- master_project_title_en
- master_project_description_en
- master_project_title_ar
- master_project_description_ar
- project_title_en
- project_title_ar
- project_description_en
- project_description_ar

Order of extraction: Extract the fields in the following order
  - Master Project fields
  - Project fields
  - Project Asset/Item fields

==================================================
INPUT STRUCTURE (CRITICAL – READ CAREFULLY):
==================================================

The input text may contain BOTH English and Arabic versions of the same record.
It will be provided in a LABELED format such as:

TITLE_EN: <English Project Title>
DESC_EN: <English Project Description>

TITLE_AR: <Arabic Project Title>
DESC_AR: <Arabic Project Description>

- Either language MAY be missing.
- Treat English and Arabic as two references describing the SAME project(s).

LABEL-ONLY NOISE (ABSOLUTE):
- The strings TITLE_EN:, DESC_EN:, TITLE_AR:, DESC_AR: are labels, not content.
- If a label appears with no meaningful text after it, it is noise.
- NEVER output extraction_text values that are labels or label bundles, e.g. "TITLE_AR:", "DESC_AR:", "TITLE_AR: DESC_AR:", "TITLE_EN: DESC_EN:".
- If only label-only noise is available, treat the field as missing and use fallback from meaningful content only.

==================================================
BILINGUAL CANONICALIZATION RULES (CRITICAL):
==================================================

You must produce TWO aligned outputs for titles/descriptions:
- English fields: master_project_title_en, master_project_description_en, project_title_en, project_description_en,
                  beneficiary_group_name, asset, asset_category, asset_quantity_uom, asset_capacity_uom, item, item_category,
                  item_quantity_uom
- Arabic fields:  master_project_title_ar, master_project_description_ar, project_title_ar, project_description_ar
- English fields must be in English only and Arabic fields must be in Arabic only.
- Never mix Arabic and English words in the same extracted field.

1) One meaning, two languages:
- English and Arabic outputs MUST represent the SAME meaning and scope.
- Do NOT output different project meanings across languages.

2) Conflict resolution (important):
- If EN and AR conflict:
  - Prefer the version that is MORE specific, complete, and contextually correct.
  - Use that as the canonical meaning.
  - Then ensure BOTH language outputs reflect that same meaning:
    - English fields in English
    - Arabic fields in Arabic (translate/paraphrase as needed)

3) Completeness:
- If attributes (asset/item/beneficiary/capacity/quantities/etc.) are missing in EN but present in AR,
  extract them using the Arabic evidence.
- If missing in AR but present in EN, extract them using the English evidence.

4) Translation fallback (mandatory):
- If Arabic title/description is missing but English is present:
  - Still output the *_ar fields by translating the canonical meaning to Arabic.
- If English title/description is missing but Arabic is present:
  - Still output the English fields by translating the canonical meaning to English.

5) ARABIC FIELDS MUST BE PURE ARABIC (STRICT, NO EXCEPTIONS):
- master_project_title_ar, master_project_description_ar, project_title_ar, project_description_ar MUST contain ONLY Arabic script.
- They MUST NOT contain any Latin letters (A-Z), numbers in Latin formatting, or Latin abbreviations.
- If the source contains Latin brand names (e.g., "Sunshow"), you MUST transliterate them into Arabic letters.
  Example: Sunshow -> سانشو (or سَنشو)
- If the source contains units in Latin (gallon, liter, kVA, kW, W), translate them into Arabic:
  gallon -> جالون
  liter  -> لتر
  kVA    -> كيلو فولت أمبير
  kW     -> كيلوواط
  W      -> واط
- Use Arabic-Indic digits (٠١٢٣٤٥٦٧٨٩) in *_ar fields.

==================================================
CRITICAL RULES:
==================================================

1) Project Splitting Rules (very important):
- The input text may describe ONE master project that contains MULTIPLE projects.

- Treat Master Project fields as the overall umbrella/theme.

- Extract one set of Project fields for EACH distinct project ONLY when the text clearly describes multiple independent actions or initiatives that could reasonably be tracked, funded, or implemented separately.

- Financial disambiguation rule (important):
    - When a master project description lists multiple actions AND explicitly mentions separate monetary amounts, use the number of distinct monetary amounts as a strong signal for how many sets of Project fields to extract.
    - If two distinct amounts are clearly associated with two different actions, extract two sets of Project fields.
    - Do NOT create additional set of Project fields for actions that do not have their own explicit amount; instead, group such actions with the most relevant amount-bearing project or keep them under the master scope.

- Split the Master Project into multiple Projects ONLY when:
  - The text describes separate actions applied to different targets or entities
    (e.g., different buildings, locations, beneficiary groups, or deliverables), OR
  - The text enumerates multiple activities that are conceptually independent
    (i.e., removing one activity would still leave a complete and meaningful project).

- NON-SPLIT RULE (IMPORTANT):
- Do NOT create multiple Projects when:
  - Multiple words or verbs describe a single continuous scope of work,
  - Multiple components together form one unified intervention,
  - Multiple actions are tightly coupled and jointly describe how one project is executed.
  - Do NOT split projects based solely on punctuation, formatting, or keywords such as "+", "/", ",", parentheses, or the presence of action verbs.
- Do NOT split into multiple projects when the text describes the SAME type of intervention
  delivered to multiple target groups or destinations (e.g., families / schools / mosques)
  as part of one delivery or distribution activity.
- Also do NOT split when the text lists multiple variants/specifications of the same deliverable
  (e.g., different tank capacities like 2000 liters and 500 gallons) unless the text explicitly
  states they are separate projects, separate implementations, or separate budgets.

- Text after "; Description:" MAY be used to extract titles of Master Project and Project if it provides clearer meaning.
- HOWEVER:
  - Master Project and Project titles MUST NOT include
    the literal string "Description:" or the delimiter "; Description:".
  - When using description text for titles, extract ONLY the meaningful
    content and REMOVE the label completely.

- When in doubt, prefer fewer Projects rather than over-splitting.

2) Asset Rules:
- What qualifies as an asset:
- Extract an asset ONLY when the text describes:
 - Physical construction or creation of a fixed structure, such as:
    construction / building / rehabilitation / expansion of:
      mosques
      schools
      hospitals
      houses
      clinics
      wells
      roads
      buildings
      shelters

  - In these cases:
    - asset = the most specific description (e.g., Small Mosque, Grand Mosque, Primary School Building, Relay Station, etc.)
    - asset_category = the generic categorical type (e.g., Mosque, School, Hospital, Stations, etc.)
    - asset_quantity = the number of assets being built or constructed(e.g., 1, 2, etc.)

    - If a project includes BOTH:
    (a) construction of new assets AND
    (b) maintenance, expansion, rehabilitation, or upgrading of existing assets,
    THEN you MUST create separate asset records.

Asset category rules:
  - asset_category MUST be singular, generic, standardized
  - asset_category MUST NOT contain adjectives, sizes, locations, or qualifiers.

Capacity extraction rules:
  - Extract capacity ONLY when it represents a technical/physical/equipment/storage/output capacity, such as:
   - tanks/containers/storage/area: gallons, liters, L, m3, cubic meters, m2
   - generators/electrical: kVA, kva, kW, kw, watts, W
   - similar measurable technical capacities explicitly tied to an asset/system
   - For any single asset, extract at most one capacity.
   - If the same asset's capacity is expressed multiple times or in different units (e.g., liters and gallons), select only one capacity-unit pair and ignore the rest.

  - Capacity should be extracted when phrased like:
   - "capacity of 522 gallons"
   - "capacity: 3 kVA"
   - "water tank ... capacity of 1000 liters"

  - DO NOT extract capacity when it describes people occupancy or attendance capacity, because that is beneficiary_count.
   - Exclude units/terms like: worshippers, people, persons, attendees, students, patients, families, households (when used as “capacity of X …”).
   - Examples to exclude:
     - "prayer hall with a capacity of 60 worshippers"  -> asset_capacity = NULL, asset_capacity_uom = NULL
     - "mosque with a capacity of 60 worshippers"       -> asset_capacity = NULL, asset_capacity_uom = NULL
   - Hard exclusion: If the word immediately following the capacity number is a human-group term (worshippers/people/persons/etc.), do not extract capacity.

  - Output formatting constraints:
    - asset_capacity: return only the number (e.g., 522, 3, 1000)
    - asset_capacity_uom: return only the unit (e.g., Gallon, kVA, Liter); return in the singluar form
    - If capacity is not present or excluded by rules, return null for both fields.

3) Item Rules:
- What qualifies as an item:
  - Extract an item when the text describes tangible goods or consumables that are:
      movable
      distributable
      not permanent physical constructions

  - Examples of items:
      furniture
      food baskets
      rice, wheat, flour
      medical equipment
      school supplies
      treatment, medicines
      relief kits (when not part of a larger constructed asset)

  - In these cases:
    - item = the most specific description (e.g., Wooden Furniture, Winter Food Basket)
    - item_category = a generic categorical type (e.g., Furniture, Food, etc.)
    - item_quantity = the number of items being distributed, etc. (e.g., 100, 2000, etc.)
    - item_quantity_uom = the unit of measurement for the items (e.g., Kilograms, Liters, Unit, etc.)

4) Asset vs Item Decision Rule (CRITICAL):
  - If something is constructed or built, treat it as an asset.
  - If something is distributed or provided, treat it as an item.
  - Never extract the same thing as both an asset and an item.

5) Beneficiary count extraction rules:
    - If beneficiary_count is expressed as “X per [unit]” (e.g., “60 students per school”) 
    and the text states the number of those units (e.g., “4 schools”), 
    then output the total beneficiaries as X x number_of_units (e.g., 60x4 = 240)

6) Do NOT extract components when a parent package/kit is present:
   - If the text explicitly mentions a parent container such as a package/basket/kit/parcel/mosque/school/hospital (e.g., "food package", "food basket", "winter package", "hygiene kit", "relief kit", "food parcel", "a mosque", "a school", "a hospital"),
     and then lists its contents (e.g., in parentheses or after words like "contains", "including", "consisting of", "composed of", "includes", "containing", "construction of"),
     THEN extract ONLY the parent container as the asset.
   - Do NOT extract the listed contents/items (e.g., rice, sugar, oil, pasta, sauce, flour, salt, tea, ablution seat, beds, rooms, etc.) as separate assets in that case.
   - Exception: If the contents are explicitly stated as separate deliveries outside the package (e.g., "food package + rice + sugar distributed separately"),
     then you may extract them separately.

7) When to extract individual items:
   - If there is NO parent container mentioned (no package/basket/kit/parcel/hospital/mosque/school), and the text lists items being delivered (e.g., "distributed rice, sugar, oil, constructed ablution seats, beds, rooms"),
     then extract each item as its own item (with quantities/uom if present).

8) Output format:
- Output must be in the following format:
{
  "extractions": [...],
  "text": <string>,
  "index": <string>,
  "master_project_amount_actual": <float>,
  "master_project_oda_amount": <float>,
  "master_project_ge_amount": <float>,
  "master_project_off_amount": <float>,
  "document_id": <string>
}
   - extraction_text MUST be a string, integer, or float (never null, never a list/dict).
   - If a field is not explicitly present, DO NOT output an extraction for it EXCEPT for mandatory fields, which must always be produced using fallback rules.
    - Return ONLY valid JSON (no markdown, no commentary).
    - The top-level JSON MUST have exactly these keys:
      - "text": the original input text (string)
      - "extractions": a list of objects

    FINAL VALIDATION (MANDATORY):
    Before returning JSON:
    1) Remove any extraction whose extraction_text is label-only noise.
    2) Ensure Master Project fields are extracted EXACTLY ONCE in one extraction. If duplicates exist, keep the best non-noise candidate and delete the rest.
    3) If you have already output a master_project_* field once, DO NOT output it again later in the list. Only keep the earliest occurrence.
    4) Do not repeat the entire extraction list; do not restart extraction from the top.
    5) Do not restart extraction. Once you output masterproject/project/item/asset fields, do not output master fields again.

10) Amount extraction rules (IMPORTANT):

AMOUNT RULES (IMPORTANT):
- Extract the project amount when they are explicitly mentioned in the text (Title or Description).
- extraction_text must be numeric (int/float). Do NOT include currency symbols or words.
  Examples:
    "AED 1,200,000" -> 1200000
    "USD 2.5 million" -> 2500000
- These may appear at master level or per project; extract whatever is stated.

11) Project Title Description Rules:
- For EACH extracted project_title_en, also extract a project_description_en.
- project_description_en MUST be a comprehensive, self-contained description of the project scope.
- It SHOULD clearly explain:
  - what is being done (construction, distribution, rehabilitation, provision, etc.)
  - what is being delivered or built (assets/items)
  - key quantities or capacities (if present)
  - intended purpose or outcome (if stated)
  - relevant beneficiaries or target group (if clearly tied to the project)
  - and any other information that is available

- Use BOTH:
  - the TITLE SECTION, and
  - the relevant parts of the DESCRIPTION SECTION
  to construct the most accurate and complete description for that specific project.

- If the master text contains multiple projects:
  - Each project_description_en must include ONLY the information relevant to that project.
  - Do NOT mix scopes across different projects.

- Do NOT include:
  - the literal strings "Description:" or "; Description:"
  - unrelated background or generic text

- If the text is very short or minimal:
  - project_description_en may closely resemble project_title_en,
    but MUST still be a complete sentence describing the project.

- project_description_en is MANDATORY for each project_title_en.

""")


from typing import List

def build_project_attr_prompt(allowed_subsectors: List[str], allowed_mdg_targets: List[str], allowed_sdg_targets: List[str]) -> str:
#def build_project_attr_prompt(allowed_subsectors: List[str]) -> str:
    subsector_bullets = "\n".join([f"- {v}" for v in allowed_subsectors])
    mdg_target_bullets = "\n".join([f"- {v}" for v in allowed_mdg_targets])
    sdg_target_bullets = "\n".join([f"- {v}" for v in allowed_sdg_targets])

    return f"""
      Extract the following fields from the input text:
      - subsector_en
      - target_en
      
      SUBSECTOR RULES:
      Subsector Definition: The subsector represents the PRIMARY development objective of the project,
      not the delivery method, funding mechanism, or all activities mentioned.
      - mandatory field
      - subsector_en MUST be exactly one of the allowed values below (match text exactly).
      - Do NOT invent categories.
      - Do NOT return multiple values.
      - Choose the subsector that best describes why the project exists and the objective of the project.
      - Do NOT classify based on beneficiaries alone.
      - Do NOT classify by inputs or delivery method

        EMERGENCY RULE (STRICT):
        - You will be given a field called emergency_title.
        - First, decide whether emergency_title has a meaningful value.

        A meaningful emergency_title:
        - is NOT null, empty, "None", "null", "n/a"
        - is NOT an Arabic value meaning "None" or "Not Applicable" or "Nothing"

        If emergency_title HAS a meaningful value:
        - subsector MUST be selected from values that start with "Emergency"
        - NEVER select a non-Emergency subsector

        If emergency_title DOES NOT have a meaningful value:
        - subsector MUST NOT start with "Emergency"
        - NEVER select an Emergency subsector
      
      Allowed subsector values:
      {subsector_bullets}

      TARGET RULES:
      Framework selection:
      - If project_year <= 2015: select target_en ONLY from the MDG target list.
      - If project_year > 2015: select target_en ONLY from the SDG target list.
      - NEVER select from the wrong list.
      
      Closed-set enforcement:
      - target_en MUST be copied EXACTLY from ONE of the allowed values below.
      - Do NOT paraphrase, summarize, shorten, or rewrite target text.
      - Do NOT invent new targets.
      - Do NOT combine multiple targets.
      - Return EXACTLY ONE target_en value.
      - Do NOT return NULL. It is a mandatory field.
      
      Matching rule:
      - Select the SINGLE allowed target that best matches the PRIMARY intended outcome.
      
      Classification constraints:
      - Do NOT classify based on beneficiaries alone.
      - Do NOT classify by activities, inputs, or delivery mechanisms.
      - Targets represent intended OUTCOMES only.

      Allowed MDG target values (use only if project_year <= 2015):
      {mdg_target_bullets}

      Allowed SDG target values (use only if project_year > 2015):
      {sdg_target_bullets}
      """.strip()