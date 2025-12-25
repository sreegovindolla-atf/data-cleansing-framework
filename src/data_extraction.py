from dotenv import load_dotenv
load_dotenv()

import langextract as lx
import textwrap
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
import json
from config.examples.infrastructure import EXAMPLES as INFRA
from config.examples.distribution import EXAMPLES as DIST
from config.prompt import PROMPT, SPLIT_PROMPT, ASSET_PROMPT

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
    " 4-classroom school made of bricks with a zinc roof + administration room + 3 bathrooms + furnishing - accommodates 160 students - 288 square meters"
    , " A mosque of 80 worshipers made of brick with a zinc roof + a lighthouse + a loudspeaker + mats + 1 bathroom - 50 square meters"
    , " A mosque of 90 worshipers made of brick with a zinc roof, with a minaret + mats + 2 bathrooms + a place for ablution - 56 square meters"
    , " A school consisting of 1 classroom made of bricks with a zinc roof, accommodating 40 students, furnishing + 1 bathroom, 50 square meters."
    , " Artesian well with electric pump 1000 liter water tank | Beneficiaries: 500 people Depth from 200 meters | Shelf life is 15 years"
    , " Borehole with electric pump 1000 liter water tank | 3 faucets | Depth 30-40 meters | Life span 5-10 years | Beneficiaries: 10 families"
    , " Distribution of (1060) food basket from Najjar complex warehouse and logistic operations of the team in the governorates Hadramout / Marib / Mahra / Shabwa"
    , "15 tons of tents, 5 tons of blankets and food supplies for the people of Socotra affected by Cyclone Mkono, 10,000 food baskets containing essential foodstuffs (9 items) for the people of Socotra affected by Cyclone Mkono"
    , " Providing a poor family with food (50 kilograms of rice + 1 carton of pasta + 2 kilograms of milk + 5 liters of oil + 5 kilograms of sugar + 1 kilogram of salt)"
    , " Providing a winter package for families consisting of a food basket (10 kilograms of flour, 3 liters of oil, 3 kilograms of salt, 3 kilograms of pasta, 5 kilograms of potatoes, 3 kilograms of sugar, 1 kilogram of tea) + a blanket + an oil heater + provid"
    , " Shipments of 35 MT of Ready-to-use Therapeutic Food (RUTF) to Support United Nations Childrens Operations in Nigeria through the International Humanitarian City (IHC)"
    , "Around 16,362 food baskets consisting of rice, burghul, beans, lentils, sugar, tea, vegetable ghee, tomato paste, salt, flour and shuriyya were processed and distributed to displaced Iraqis and Syrian refugees in camps and shelters and outside them on the"
    , "Consist of 2 flights, sent 26 tonnes of food aid to support Sudanese refugees in Chad affected by the conflict. In addition to logistic and support operations."
    , "Dispatching C 17 aircraft to transport (100) tons of foodstuff, presented by Khalifa Foundation (relied from 2015)"
    , "Emergency Response - Food Boxes to support 1,195,746 beneficiaries in Gaza (WFP-1st Payment-Gaza)"
    , "Fourth response, the UAE sent 1,000 tons of food were distributed to drought affected people in Mogadishu, Somalia. In addition to logistics operations."
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
        prompt_description=PROMPT,
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