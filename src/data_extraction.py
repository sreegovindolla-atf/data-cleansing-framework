from dotenv import load_dotenv
load_dotenv()

import sys
import os
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import langextract as lx

# ---- config imports ----
# You said: "I want to do the extraction only once for all fields"
# So we use ONE prompt (PROMPT) and ONE combined examples list.
from config.examples.infrastructure_projects import EXAMPLES as INFRA_EXAMPLES
from config.examples.distribution_projects import EXAMPLES as DIST_EXAMPLES

from config.prompt import PROMPT  # combined prompt: master_project_title, project_title, beneficiary_*, asset_*


# -----------------------
# args + output paths
# -----------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
args = parser.parse_args()

RUN_ID = args.run_id
RUN_OUTPUT_DIR = Path("data/outputs") / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Single extraction output (ONE pass)
OUT_JSONL = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.jsonl"
OUT_JSON  = RUN_OUTPUT_DIR / f"{RUN_ID}_combined_extraction_results.json"


# -----------------------
# input texts
# -----------------------
input_texts = [
    "Construction a residential village and houses for poor people + distribution 250 food package, for poor families, it contains of rice, sugar, oil, sauce and pasta.",
    "Construction classes for memorizing the Quran + Sponsorship 2 episodes for students to memorizing the Quran.",
    "Construction of (2) Schools + maintenance of (9) schools with four attached offices ",
    "Construction of a hospital in Hrjessea + Berbera",
    "Reconstruction of the first phase of the port of Mukalla / hospitals / electricity / water (July 2016: renovating and furnishing Radio Mukalla (150,995.46) AED + renovating and furnishing Mukalla Court (134.260) AED + rebuilding and rehabilitation of the ",
    "Founded in 2015, Grassland Cameroon is a private limited liability social enterprise providing rural agricultural finance to smallholder farmers while enhancing local food supply chains.  It delivers a combination of asset-based finance, extension services, and post harvest support to rural maize farmers in Cameroon. These innovative financial and training tools have enabled the farmers to increase their yields, and as a result, improve their livelihoods. ",
    "Send a land convoy of 15-5 (3) truck to Jordan moved 75 tons blankets and food items as follows: Cardboard pass 62 310 2077 10385 UAE and Saudi, Quilt 1 × 10 66 990 13200 Bendel Emirati, Cardboard pass 22 896 477 3816 UAE and Saudi, Quilt 1 × 10 961 440 ",
    "WFP e-voucher programme : For nearly 17,000 vulnerable Palestinians, mostly women, children and elderly, TBHF & WFP have partnered to provide vital food assistance that keeps hope alive.",
    "Dispatching the twentieth ship via Al Fujairah harbour to transport (10,000) food parcels + school stationery+ school bags + toothbrushes",
    "Dispatching the twenty-second ship to transport (847) tons, including 13,000 food parcels + 1000 tents + 300 tons of flour + 126 tons of dates  ",
    "Artesian well with electric pump 1000 liter water tank | Beneficiaries: 500 people Depth from 200 meters | Shelf life is 15 years",
    "Distribution of (1060) food basket from Najjar complex warehouse and logistic operations of the team in the governorates Hadramout / Marib / Mahra / Shabwa",
    "Emergency Response - Food Boxes to support 1,195,746 beneficiaries in Gaza (WFP-1st Payment-Gaza)",
]

# Combined examples
EXAMPLES = INFRA_EXAMPLES + DIST_EXAMPLES


# -----------------------
# extraction loop (ONE prompt, ONE pass)
# -----------------------
all_results = []

for i, text in enumerate(input_texts, start=1):
    result = lx.extract(
        text_or_documents=text,
        prompt_description=PROMPT,
        examples=EXAMPLES,
        model_id="gpt-4o",
        api_key=os.environ.get("OPENAI_API_KEY"),
        fence_output=True,
        use_schema_constraints=False,
    )
    all_results.append(result)


# -----------------------
# save outputs (single file)
# -----------------------
lx.io.save_annotated_documents(all_results, output_name=OUT_JSONL.name, output_dir=RUN_OUTPUT_DIR)

# also write pretty JSON for debugging
def jsonl_to_pretty_json(jsonl_path: Path, json_path: Path):
    docs = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

jsonl_to_pretty_json(OUT_JSONL, OUT_JSON)

print(f"Saved extraction: {OUT_JSONL}")
print(f"Saved debug JSON: {OUT_JSON}")