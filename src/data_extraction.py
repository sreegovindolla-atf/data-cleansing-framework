from dotenv import load_dotenv
load_dotenv()

import langextract as lx
import textwrap
import os
import pandas as pd
from pathlib import Path

# 1. Define the prompt and extraction rules
prompt = textwrap.dedent("""\
Extract the following fields as labeled spans from the input text:

- project_title
- asset
- asset_quantity
- beneficiary_count
- beneficiary_group_name

Rules:
- extraction_text MUST be a string, integer, or float (never null, never a list/dict).
- If a field is not explicitly present, DO NOT output an extraction for it.
- Do not use attributes for now (leave attributes empty).
""")

# 2. Provide a high-quality example to guide the model
examples = [
    lx.data.ExampleData(
        text=" 3 classroom school made of bricks with zinc roof with 3 bathrooms + furnishing (120 students) - 150 square meters",
        extractions=[
            lx.data.Extraction(
                extraction_class="project title",
                extraction_text="Construction of School"
                #attributes={}
            ),
            lx.data.Extraction(
                extraction_class="asset",
                extraction_text="School"
                #attributes={}
            ),
            lx.data.Extraction(
                extraction_class="asset quantity",
                extraction_text="3"
                #attributes={}
            ),
            lx.data.Extraction(
                extraction_class="beneficiary count",
                extraction_text="120",
                #attributes={}
            ),
            lx.data.Extraction(
                extraction_class="beneficiary group type",
                extraction_text="students",
                #attributes={}
            ),
        ]
    )
]

# The input text to be processed

INPUT_DIR = Path("data/input")
INPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = INPUT_DIR / "denorm_mastertable.csv"
TEXT_COLUMN = "ProjectTitleEnglish"

df_inputs = pd.read_csv(INPUT_CSV)

input_texts = (
    df_inputs[TEXT_COLUMN]
    .dropna()              # remove nulls
    .astype(str)           # ensure string
    .str.strip()           # trim spaces
)

# remove empty strings
input_texts = input_texts[input_texts != ""]

# take only first 100 values
input_texts = input_texts.head(5).tolist()

print(f"Loaded {len(input_texts)} input texts for testing")


all_results = []


for i, text in enumerate(input_texts, start=1):
    result = lx.extract(
        text_or_documents=text,
        prompt_description=prompt,
        examples=examples,
        model_id="gpt-4o",  # Automatically selects OpenAI provider
        api_key=os.environ.get('OPENAI_API_KEY'),
        fence_output=True,
        use_schema_constraints=False
    )

    all_results.append(result)

# Save the results to a JSONL file
lx.io.save_annotated_documents(all_results, output_name="extraction_results.jsonl", output_dir=".")

# Generate the visualization from the file
#html_content = lx.visualize("extraction_results.jsonl")
#with open("langextract_output.html", "w", encoding="utf-8", errors="replace") as f:
#    f.write(html_content)