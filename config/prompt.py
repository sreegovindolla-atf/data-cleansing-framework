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

Repeatable (one per project OR master-level when applicable):
- project_title
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
- master_project_title  
- project_title

==================================================
INPUT STRUCTURE (CRITICAL – READ CAREFULLY):
==================================================

- The input text ALWAYS follows this structure:

<Project Title>; Description: <Project Description>

- The text BEFORE "; Description:" is the TITLE SECTION
- The text AFTER  "; Description:" is the DESCRIPTION SECTION

==================================================
CRITICAL RULES:
==================================================

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

- Text after "; Description:" MAY be used to extract
  master_project_title and project_title if it provides clearer meaning.

- HOWEVER:
  - master_project_title and project_title MUST NOT include
    the literal string "Description:" or the delimiter "; Description:".
  - When using description text for titles, extract ONLY the meaningful
    content and REMOVE the label completely.

- When in doubt, prefer fewer project_title entries rather than over-splitting.

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
    - asset = the most specific description (e.g., Small Mosque, Grand Mosque, Primary School Building, etc.)
    - asset_category = the generic categorical type (e.g., Mosque, School, Hospital, etc.)
    - asset_quantity = the number of assets being built or constructed(e.g., 1, 2, etc.)

   - Examples:
    Small Mosque → Mosque
    Grand Mosque → Mosque
    Girls' Primary School → School
    Residential House → House

Asset category rules:
  - asset_category MUST be singular, generic, standardized
  - asset_category MUST NOT contain adjectives, sizes, locations, or qualifiers.

Capacity extraction rules:
  - Extract capacity ONLY when it represents a technical/physical/equipment/storage/output capacity, such as:
   - tanks/containers/storage: gallons, liters, L, m3, cubic meters
   - generators/electrical: kVA, kva, kW, kw, watts, W
   - similar measurable technical capacities explicitly tied to an asset/system

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
   - If a field is not explicitly present, DO NOT output an extraction for it EXCEPT mandatory fields (master_project_title, project_title), which must always be produced using fallback rules.
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

- master_project_title and project_title MUST ALWAYS be present.
- If the input text is short, generic, or does not explicitly describe a project:
    - Use the full input text verbatim as master_project_title.
    - Use the same value as project_title.
- Do NOT return an empty extractions list.
- At minimum, always return:
    - one master_project_title
    - one project_title

10) Amount extraction rules (IMPORTANT):

AMOUNT RULES (IMPORTANT):
- Extract the project amount when they are explicitly mentioned in the text (Title or Description).
- extraction_text must be numeric (int/float). Do NOT include currency symbols or words.
  Examples:
    "AED 1,200,000" -> 1200000
    "USD 2.5 million" -> 2500000
- These may appear at master level or per project; extract whatever is stated.
""")
