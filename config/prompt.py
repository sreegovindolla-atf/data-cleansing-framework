import textwrap

PROJECT_PROMPT = """
Extract only these fields:

Single (optional):
- master_project_title

Repeatable (one per project inside the master text):
- project_title

Rules:
- You may output MULTIPLE project rows if multiple projects are listed in the same master_project
- Create one project_title per distinct project extracted
- extraction_text must be string only
"""

ASSET_PROMPT = """
Extract only these fields from the input text:

Single (optional):
- beneficiary_count
- beneficiary_group_name

Repeatable (can repeat multiple times):
- asset_quantity
- asset
- asset_quantity_uom

Rules:
- You may output MULTIPLE asset rows if multiple assets are listed.
- extraction_text must be string/int/float only (no null, no lists/dicts).
- Omit fields not explicitly present.
"""


PROMPT = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text:

Fields (can repeat):
- project_title
- beneficiary_count
- beneficiary_group_name
- asset_quantity (number linked to an asset)
- asset (the item being delivered/built)
- asset_quantity_uom (unit of measure for the asset_quantity)

Fields (single per document):
- master_project_title

Mandatory fields:
- master_project_title
- project_title

Rules:
- You may output MULTIPLE project_titles if multiple projects are listed within the same master project.
- You may output MULTIPLE asset_quantity/asset/asset_quantity_uom if multiple assets are listed.
- Each asset_quantity must correspond to the nearest matching asset in the text.
- extraction_text MUST be a string, integer, or float (never null, never a list/dict).
- If a field other than mandatory fields is not explicitly present, DO NOT output an extraction for it.
- Do not use attributes for now (leave attributes empty).
""")