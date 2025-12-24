import textwrap

PROMPT = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text:

Fields (can repeat):
- asset_quantity (number linked to an asset)
- asset (the item being delivered/built)
- asset_quantity_uom (unit of measure for the asset_quantity)

Fields (single per document):
- project_title
- beneficiary_count
- beneficiary_group_name

Mandatory fields:
- project_title

Rules:
- You may output MULTIPLE asset_quantity/asset/asset_quantity_uom if multiple assets are listed.
- Each asset_quantity must correspond to the nearest matching asset in the text.
- extraction_text MUST be a string, integer, or float (never null, never a list/dict).
- If a field other than mandatory fields is not explicitly present, DO NOT output an extraction for it.
- Do not use attributes for now (leave attributes empty).
""")