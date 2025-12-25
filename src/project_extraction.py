from dotenv import load_dotenv
load_dotenv()

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import langextract as lx
import textwrap
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
import json
from config.examples.infrastructure_projects import EXAMPLES as INFRA
from config.examples.distribution_projects import EXAMPLES as DIST
from config.prompt import PROMPT, PROJECT_PROMPT

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Read examples from config files
EXAMPLES = INFRA + DIST

# The input text to be processed

input_texts = [
    "Construction a residential village and houses for poor people + distribution 250 food package, for poor families, it contains of rice, sugar, oil, sauce and pasta."
    , "Construction classes for memorizing the Quran + Sponsorship 2 episodes for students to memorizing the Quran."
    , "Construction of (2) Schools + maintenance of (9) schools with four attached offices "
    , "Construction of a hospital in Hrjessea + Berbera"
    , "Reconstruction of the first phase of the port of Mukalla / hospitals / electricity / water (July 2016: renovating and furnishing Radio Mukalla (150,995.46) AED + renovating and furnishing Mukalla Court (134.260) AED + rebuilding and rehabilitation of the "
    , "Founded in 2015, Grassland Cameroon is a private limited liability social enterprise providing rural agricultural finance to smallholder farmers while enhancing local food supply chains.  It delivers a combination of asset-based finance, extension services, and post harvest support to rural maize farmers in Cameroon. These innovative financial and training tools have enabled the farmers to increase their yields, and as a result, improve their livelihoods. "
    , "Send a land convoy of 15-5 (3) truck to Jordan moved 75 tons blankets and food items as follows: Cardboard pass 62 310 2077 10385 UAE and Saudi, Quilt 1 × 10 66 990 13200 Bendel Emirati, Cardboard pass 22 896 477 3816 UAE and Saudi, Quilt 1 × 10 961 440 "
    , "WFP e-voucher programme : For nearly 17,000 vulnerable Palestinians, mostly women, children and elderly, TBHF & WFP have partnered to provide vital food assistance that keeps hope alive."
    , "Dispatching the twentieth ship via Al Fujairah harbour to transport (10,000) food parcels + school stationery+ school bags + toothbrushes"
    , "Dispatching the twenty-second ship to transport (847) tons, including 13,000 food parcels + 1000 tents + 300 tons of flour + 126 tons of dates  "
]


all_results = []


for i, text in enumerate(input_texts, start=1):
    result = lx.extract(
        text_or_documents=text,
        prompt_description=PROJECT_PROMPT,
        examples=EXAMPLES,
        model_id="gpt-4o",  # Automatically selects OpenAI provider
        api_key=os.environ.get('OPENAI_API_KEY'),
        fence_output=True,
        use_schema_constraints=False
    )

    all_results.append(result)


# Save the results to a JSONL file
lx.io.save_annotated_documents(all_results, output_name="extraction_results.jsonl", output_dir=RUN_OUTPUT_DIR)

# Writing into a json file for output analysis
JSONL_PATH = RUN_OUTPUT_DIR / "extraction_results.jsonl"
JSON_PATH  = RUN_OUTPUT_DIR / "extraction_results.json"

docs = []
with open(JSONL_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            docs.append(json.loads(line))

with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(docs, f, ensure_ascii=False, indent=2)

# Generate the visualization from the file
#html_content = lx.visualize("extraction_results.jsonl")
#with open("langextract_output.html", "w", encoding="utf-8", errors="replace") as f:
#    f.write(html_content)