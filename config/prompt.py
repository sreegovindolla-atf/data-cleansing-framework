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
  - project_amount_extracted

Repeatable (Project Item fields):
- The following combination of columns indicate one item for a project. One project can have one or more items.
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

Order of extraction:
- Master Project fields
- Project fields
- Project Item fields

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
- NEVER output extraction_text values that are labels or label bundles.
- If only label-only noise is available, treat the field as missing and use fallback from meaningful content only.

==================================================
BILINGUAL CANONICALIZATION RULES (CRITICAL):
==================================================

You must produce TWO aligned outputs for titles/descriptions:
- English fields: master_project_title_en, master_project_description_en, project_title_en, project_description_en, item, item_category, item_quantity_uom
- Arabic fields:  master_project_title_ar, master_project_description_ar, project_title_ar, project_description_ar
- English fields must be in English only and Arabic fields must be in Arabic only.
- Never mix Arabic and English words in the same extracted field.

1) One meaning, two languages:
- English and Arabic outputs MUST represent the SAME meaning and scope.

2) Conflict resolution:
- If EN and AR conflict:
  - Prefer the version that is more specific, complete, and contextually correct.
  - Use that as the canonical meaning.
  - Then ensure both language outputs reflect that same meaning.

3) Completeness:
- If attributes are missing in EN but present in AR, extract them using Arabic evidence.
- If missing in AR but present in EN, extract them using English evidence.

4) Translation fallback:
- If Arabic title/description is missing but English is present:
  - Still output the *_ar fields by translating the canonical meaning to Arabic.
- If English title/description is missing but Arabic is present:
  - Still output the English fields by translating the canonical meaning to English.

5) ARABIC FIELDS MUST BE PURE ARABIC:
- master_project_title_ar, master_project_description_ar, project_title_ar, project_description_ar MUST contain only Arabic script.

==================================================
CRITICAL RULES:
==================================================

1) Project Splitting Rules:
- The input text may describe one master project that contains multiple projects.
- Treat Master Project fields as the overall umbrella/theme.
- Extract one set of Project fields for each distinct project only when the text clearly describes multiple independent actions or initiatives.

- Financial disambiguation rule:
  - When a master project description lists multiple actions AND explicitly mentions separate monetary amounts, use the number of distinct monetary amounts as a strong signal for how many sets of Project fields to extract.

- Split the Master Project into multiple Projects ONLY when:
  - The text describes separate actions applied to different targets or entities, OR
  - The text enumerates multiple activities that are conceptually independent.

- NON-SPLIT RULE:
- Do NOT create multiple Projects when:
  - Multiple words or verbs describe a single continuous scope of work,
  - Multiple components together form one unified intervention,
  - Multiple actions are tightly coupled and jointly describe how one project is executed.

- When in doubt, prefer fewer Projects rather than over-splitting.

2) Item Rules:
- What qualifies as an item:
  - Extract an item when the text describes tangible goods or consumables that are:
      movable
      distributable
      not permanent physical constructions

- Examples of items:
  - furniture
  - food baskets
  - rice, wheat, flour
  - medical equipment
  - school supplies
  - treatment, medicines
  - relief kits

- In these cases:
  - item = the most specific description
  - item_category = a generic categorical type
  - item_quantity = the number of items being distributed
  - item_quantity_uom = the unit of measurement for the items

3) Do NOT extract components when a parent package/kit is present:
- If the text explicitly mentions a parent container such as a package/basket/kit/parcel,
  and then lists its contents, then extract only the parent container as the item.
- Do NOT extract the listed contents as separate items in that case.

4) Amount extraction rules:
- Extract the project amount when explicitly mentioned in the text.
- extraction_text must be numeric only.

5) Project Title Description Rules:
- For each extracted project_title_en, also extract a project_description_en.
- project_description_en must be a comprehensive, self-contained description of the project scope.
- Use both the title section and the relevant parts of the description section.

6) Output format:
- extraction_text must be a string, integer, or float.
- If a field is not explicitly present, do not output an extraction for it except for mandatory fields.
- Return only valid JSON.

FINAL VALIDATION:
1) Remove label-only noise.
2) Ensure master_project_* fields are extracted exactly once.
3) Do not repeat the entire extraction list.
4) Do not restart extraction from the top.
""")


from typing import List

#def build_project_attr_prompt(allowed_subsectors: List[str], allowed_mdg_targets: List[str], allowed_sdg_targets: List[str]) -> str:
def build_project_attr_prompt(allowed_subsectors: List[str]) -> str:
    subsector_bullets = "\n".join([f"- {v}" for v in allowed_subsectors])
    #mdg_target_bullets = "\n".join([f"- {v}" for v in allowed_mdg_targets])
    #sdg_target_bullets = "\n".join([f"- {v}" for v in allowed_sdg_targets])

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
      """.strip()