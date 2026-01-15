import textwrap

PROMPT = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text.

Single per document:
- master_project_title_en
- master_project_title_ar

Repeatable (one per project OR master-level when applicable):
- project_title_en
- project_title_ar
- project_description_en
- project_description_ar
- beneficiary_count
- beneficiary_group_name
- project_amount_extracted

Repeatable (can repeat multiple times per project):
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
- master_project_title_ar
- project_title_en
- project_title_ar
- project_description_en
- project_description_ar

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

==================================================
BILINGUAL CANONICALIZATION RULES (CRITICAL):
==================================================

You must produce TWO aligned outputs for titles/descriptions:
- English fields: master_project_title_en, project_title_en, project_description_en
- Arabic fields:  master_project_title_ar, project_title_ar, project_description_ar

1) One meaning, two languages:
- English and Arabic outputs MUST represent the SAME meaning and scope.
- Do NOT output different project meanings across languages.

2) Consistency:
- If EN and AR agree, keep both aligned.
- Do NOT duplicate extractions per language beyond the *_ar fields.

3) Conflict resolution (important):
- If EN and AR conflict:
  - Prefer the version that is MORE specific, complete, and contextually correct.
  - Use that as the canonical meaning.
  - Then ensure BOTH language outputs reflect that same meaning:
    - English fields in English
    - Arabic fields in Arabic (translate/paraphrase as needed)

4) Completeness:
- If attributes (asset/item/beneficiary/capacity/quantities/etc.) are missing in EN but present in AR,
  extract them using the Arabic evidence.
- If missing in AR but present in EN, extract them using the English evidence.

5) Script purity (strict):
- master_project_title_en / project_title_en / project_description_en MUST be English only.
- master_project_title_ar / project_title_ar / project_description_ar MUST be Arabic only.
- Never mix Arabic and English words in the same extracted field.

6) Translation fallback (mandatory):
- If Arabic title/description is missing but English is present:
  - Still output the *_ar fields by translating the canonical meaning to Arabic.
- If English title/description is missing but Arabic is present:
  - Still output the English fields by translating the canonical meaning to English.

7) ARABIC FIELDS MUST BE PURE ARABIC (STRICT, NO EXCEPTIONS):
- master_project_title_ar, project_title_ar, project_description_ar MUST contain ONLY Arabic script.
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
- The input text may describe ONE master project that contains MULTIPLE sub-projects.

- Treat master_project_title_en as the overall umbrella/theme.

- Extract one project_title_en for EACH distinct sub-project ONLY when the text clearly describes multiple independent actions or initiatives that could reasonably be tracked, funded, or implemented separately.

- Financial disambiguation rule (important):
    - When a master project description lists multiple actions AND explicitly mentions separate monetary amounts, use the number of distinct monetary amounts as a strong signal for how many project_title_en entries to extract.
    - If two distinct amounts are clearly associated with two different actions, extract two project_title_en entries.
    - Do NOT create additional project_title_en entries for actions that do not have their own explicit amount; instead, group such actions with the most relevant amount-bearing project or keep them under the master scope.

- Split the master project into multiple project_title_en entries ONLY when:
  - The text describes separate actions applied to different targets or entities
    (e.g., different buildings, locations, beneficiary groups, or deliverables), OR
  - The text enumerates multiple activities that are conceptually independent
    (i.e., removing one activity would still leave a complete and meaningful project).

- NON-SPLIT RULE (IMPORTANT):
- Do NOT create multiple project_title_en entries when:
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

- Text after "; Description:" MAY be used to extract
  master_project_title_en and project_title_en if it provides clearer meaning.

- HOWEVER:
  - master_project_title_en and project_title_en MUST NOT include
    the literal string "Description:" or the delimiter "; Description:".
  - When using description text for titles, extract ONLY the meaningful
    content and REMOVE the label completely.

- When in doubt, prefer fewer project_title_en entries rather than over-splitting.

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
    - asset = the most specific description (e.g., Small Mosque Construction, Grand Mosque, Primary School Building Maintenance, Existing Relay Station, etc.)
    - asset_category = the generic categorical type (e.g., Mosque, School, Hospital, Stations, etc.)
    - asset_quantity = the number of assets being built or constructed(e.g., 1, 2, etc.)

   - Examples:
    Small Mosque Construction → Mosque
    Grand Mosque → Mosque
    Girls' Primary School Maintenance → School
    Residential House → House

    - If a project includes BOTH:
    (a) construction of new assets AND
    (b) maintenance, expansion, rehabilitation, or upgrading of existing assets,
    THEN you MUST create separate asset records.

  - The asset name MUST be explicitly differentiated as follows:
    • Use "<Asset Name> - Construction" for new builds
    • Use "<Asset Name> - Maintenance" for expansion, rehabilitation, upgrading, or equipment addition

  - NEVER use the same asset name for both construction and maintenance activities.
  - This rule is MANDATORY even if the base asset type is the same.

Asset category rules:
  - asset_category MUST be singular, generic, standardized
  - asset_category MUST NOT contain adjectives, sizes, locations, or qualifiers.

Capacity extraction rules:
  - Extract capacity ONLY when it represents a technical/physical/equipment/storage/output capacity, such as:
   - tanks/containers/storage: gallons, liters, L, m3, cubic meters
   - generators/electrical: kVA, kva, kW, kw, watts, W
   - similar measurable technical capacities explicitly tied to an asset/system
   - For any single asset, extract at most one capacity.
   - If the same asset's capacity is expressed multiple times or in different units (e.g., liters and gallons), select only one capacity-unit pair and ignore the rest.

  - Capacity should be extracted when phrased like:
   - "capacity of 522 gallons"
   - "capacity: 3 kVA"
   - "water tank ... capacity of 1000 liters"

  - DO NOT extract capacity when it describes people occupancy or attendance capacity, because that is beneficiary_count instead.
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

  - Examples:
      Wooden Furniture → Furniture
      Food Basket → Food
      Medical Equipment → Healthcare
      Treatment → Healthcare

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
     then extract each item as its own asset (with quantities/uom if present).

8) Output format:
   - extraction_text MUST be a string, integer, or float (never null, never a list/dict).
   - If a field is not explicitly present, DO NOT output an extraction for it EXCEPT mandatory fields (master_project_title_en, project_title_en), which must always be produced using fallback rules.
   - Do not use attributes for now (leave attributes empty).
    - Return ONLY valid JSON (no markdown, no commentary).
    - The top-level JSON MUST have exactly these keys:
      - "text": the original input text (string)
      - "extractions": a list of objects

    Each item in "extractions" MUST be an object with:
    - "extraction_class": string
    - "extraction_text": string or number (never null, never list/dict)
    - "extraction_index": integer (order in which you found it; start at 0 and increment)

9) MANDATORY FALLBACK RULE (critical):

- master_project_title_en and project_title_en MUST ALWAYS be present.
- If the input text is short, generic, or does not explicitly describe a project:
    - Use the full input text verbatim as master_project_title_en.
    - Use the same value as project_title_en.
- Do NOT return an empty extractions list.
- At minimum, always return:
    - one master_project_title_en
    - one project_title_en

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