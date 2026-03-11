import os
import json
import argparse
from pathlib import Path

import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from openai import OpenAI

# --------------------------------------------------
# ARGS
# --------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=False)
parser.add_argument(
    "--upstream-ids-file",
    required=False,
    help="Txt file containing processed indexes from 2a for this run."
)
args = parser.parse_args()

RUN_ID = args.run_id.strip() if args.run_id else None
UPSTREAM_IDS_FILE = Path(args.upstream_ids_file) if args.upstream_ids_file else None

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set")

client = OpenAI(api_key=OPENAI_API_KEY)

STORE_NAMES_IN_EXTRACTED_COLUMNS = True
PRINT_SAMPLE_ROWS = 5
PRINT_PROGRESS_EVERY = 100

print("===================================================")
print("SDG mapping selection + update script starting...")
print(f"Model: {MODEL}")
print(f"STORE_NAMES_IN_EXTRACTED_COLUMNS = {STORE_NAMES_IN_EXTRACTED_COLUMNS}")
print(f"RUN_ID = {RUN_ID}")
print("===================================================\n")

# --------------------------------------------------
# SQL SERVER CONNECTION
# --------------------------------------------------
def get_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

engine = get_engine()


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def load_ids_txt(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


# --------------------------------------------------
# LLM PROMPT
# --------------------------------------------------
SYSTEM_PROMPT = """
You are selecting the single best SDG mapping for a project.

Rules:
- You MUST choose exactly one option from the provided candidates.
- Do NOT invent new SDG values.
- Base your decision ONLY on the project title and description.
- Prefer the candidate that best matches the project's primary intent and measurable outcome.
- If multiple fit, prefer the most specific indicator (most directly measurable).
- Output valid JSON only. No extra text.
"""

JSON_SCHEMA = {
    "name": "sdg_selection",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "sdg_goal": {"type": "string"},
            "sdg_goal_name": {"type": "string"},
            "sdg_target": {"type": "string"},
            "sdg_target_name": {"type": "string"},
            "sdg_indicator": {"type": "string"},
            "sdg_indicator_name": {"type": "string"},
        },
        "required": [
            "sdg_goal", "sdg_goal_name",
            "sdg_target", "sdg_target_name",
            "sdg_indicator", "sdg_indicator_name"
        ],
    },
}

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
PROJECT_SQL = """
SELECT b.year, a.*
FROM [ForeignAidDatabase_2019].[silver].[cleaned_project_attributes] a
LEFT JOIN dbo.MasterTableDenormalizedCleanedFinal b
    ON a.[index] = b.[index]
WHERE b.year > 2015
"""

MAPPING_SQL = """
SELECT
    a.subsector,
    a.sdg_goal,
    b.NameEnglish              AS sdg_goal_name,
    a.sdg_target,
    c.Target_English           AS sdg_target_name,
    a.sdg_indicator,
    d.IndicatorEnglish         AS sdg_indicator_name
FROM silver.sdg_subsector_mapping a
LEFT JOIN dbo.DevelopmentGoals b
    ON a.sdg_goal = b.ID
LEFT JOIN dbo.Targets c
    ON a.sdg_target = c.Target_ID
LEFT JOIN dbo.Indicators d
    ON a.sdg_indicator = d.IndicatorCode
WHERE a.subsector IS NOT NULL
  AND LTRIM(RTRIM(a.subsector)) <> ''
  AND a.sdg_goal IS NOT NULL
  AND a.sdg_target IS NOT NULL
  AND a.sdg_indicator IS NOT NULL
"""

projects = pd.read_sql(PROJECT_SQL, engine)

if UPSTREAM_IDS_FILE:
    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(f"Missing upstream ids file: {UPSTREAM_IDS_FILE}")

    upstream_indexes = load_ids_txt(UPSTREAM_IDS_FILE)
    if not upstream_indexes:
        print(f"[INFO] Upstream indexes file is empty: {UPSTREAM_IDS_FILE}")
        raise SystemExit(0)

    before_count = len(projects)
    projects["index"] = projects["index"].astype(str).str.strip()
    projects = projects[projects["index"].isin(upstream_indexes)].copy()

    print(f"[INFO] Upstream indexes count: {len(upstream_indexes)}")
    print(f"[INFO] Projects after upstream filter: {len(projects)} / {before_count}")

print(f"Projects loaded: {len(projects):,} rows")
if projects.empty:
    print("No projects to process.")
    raise SystemExit(0)

print(projects[["index", "year", "extracted_subsector_en"]].head(PRINT_SAMPLE_ROWS).to_string(index=False), "\n")

mapping = pd.read_sql(MAPPING_SQL, engine)
print(f"Mapping loaded: {len(mapping):,} rows")
print(mapping.head(PRINT_SAMPLE_ROWS).to_string(index=False), "\n")

projects["subsector_key"] = projects["extracted_subsector_en"].astype(str).str.strip().str.lower()
mapping["subsector_key"] = mapping["subsector"].astype(str).str.strip().str.lower()

merged = projects.merge(mapping, on="subsector_key", how="inner")
print(f"Joined rows: {len(merged):,}")
print(f"Unique projects matched: {merged['index'].nunique():,}")
print("Sample joined rows:")
print(
    merged[["index", "extracted_subsector_en", "sdg_goal", "sdg_target", "sdg_indicator"]]
    .head(PRINT_SAMPLE_ROWS)
    .to_string(index=False),
    "\n"
)

if merged.empty:
    raise RuntimeError("No matches after joining subsector. Check subsector text alignment.")

# --------------------------------------------------
# GROUP CANDIDATES PER PROJECT
# --------------------------------------------------
def norm(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())

def candidate_key(c: dict) -> tuple:
    return (
        norm(c.get("sdg_goal")),
        norm(c.get("sdg_target")),
        norm(c.get("sdg_indicator")),
    )

print("Grouping candidates per project + de-duplicating...")
grouped = {}
for _, r in merged.iterrows():
    idx = r["index"]
    if idx not in grouped:
        grouped[idx] = {
            "project": {
                "index": idx,
                "extracted_subsector_en": r.get("extracted_subsector_en", ""),
                "project_title_en": r.get("project_title_en", ""),
                "project_description_en": r.get("project_description_en", ""),
            },
            "candidates": []
        }

    grouped[idx]["candidates"].append({
        "sdg_goal": str(r["sdg_goal"]),
        "sdg_goal_name": str(r.get("sdg_goal_name") or ""),
        "sdg_target": str(r["sdg_target"]),
        "sdg_target_name": str(r.get("sdg_target_name") or ""),
        "sdg_indicator": str(r["sdg_indicator"]),
        "sdg_indicator_name": str(r.get("sdg_indicator_name") or ""),
    })

total_before = 0
total_after = 0
multi_candidate_projects = 0

for idx in grouped:
    before = len(grouped[idx]["candidates"])
    total_before += before

    seen = set()
    uniq = []
    for c in grouped[idx]["candidates"]:
        k = candidate_key(c)
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    grouped[idx]["candidates"] = uniq

    after = len(uniq)
    total_after += after
    if after > 1:
        multi_candidate_projects += 1

print(f"Projects grouped: {len(grouped):,}")
print(f"Candidates total before dedupe: {total_before:,}")
print(f"Candidates total after dedupe : {total_after:,}")
print(f"Projects with >1 candidate    : {multi_candidate_projects:,}\n")

# --------------------------------------------------
# LLM SELECTION
# --------------------------------------------------
print("Running LLM selection + preparing updates...")
updates = []
llm_calls = 0
fallbacks = 0
invalid_choices = 0

for n, (idx, data) in enumerate(grouped.items(), start=1):
    project = data["project"]
    candidates = data["candidates"]

    if not candidates:
        continue

    if len(candidates) == 1:
        chosen = candidates[0]
    else:
        payload = {
            "title": project["project_title_en"],
            "description": project["project_description_en"],
            "subsector": project["extracted_subsector_en"],
            "candidates": candidates
        }

        try:
            llm_calls += 1
            resp = client.responses.create(
                model=MODEL,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
                ],
                response_format={"type": "json_schema", "json_schema": JSON_SCHEMA},
                temperature=0
            )
            chosen = json.loads(resp.output_text)
        except Exception as e:
            fallbacks += 1
            chosen = candidates[0]
            if n <= 3:
                print(f"LLM error on index={idx}, fallback to first candidate. Error={type(e).__name__}: {e}")

        cand_keys = {candidate_key(c) for c in candidates}
        if candidate_key(chosen) not in cand_keys:
            invalid_choices += 1
            chosen = candidates[0]

        chosen_k = candidate_key(chosen)
        chosen = next((c for c in candidates if candidate_key(c) == chosen_k), candidates[0])

    updates.append({
        "index": project["index"],
        "sdg_goal": chosen["sdg_goal"],
        "sdg_target": chosen["sdg_target"],
        "sdg_indicator": chosen["sdg_indicator"],
        "sdg_goal_name": chosen["sdg_goal_name"],
        "sdg_target_name": chosen["sdg_target_name"],
        "sdg_indicator_name": chosen["sdg_indicator_name"],
    })

    if n % PRINT_PROGRESS_EVERY == 0:
        print(
            f"Progress: {n:,}/{len(grouped):,} projects processed "
            f"(LLM calls={llm_calls:,}, fallbacks={fallbacks:,}, invalid={invalid_choices:,})"
        )

print(f"Total projects processed: {len(updates):,}")
print(f"LLM calls made          : {llm_calls:,}")

out_df = pd.DataFrame(updates)
if out_df.empty:
    print("Nothing to update (no matched candidates).")
    raise SystemExit(0)

print("Sample chosen outputs:")
print(out_df.head(PRINT_SAMPLE_ROWS).to_string(index=False), "\n")

# --------------------------------------------------
# UPDATE TARGET TABLE
# --------------------------------------------------
if STORE_NAMES_IN_EXTRACTED_COLUMNS:
    print("Updating extracted_*_en with NAMES (goal_name/target_name/indicator_name)...")
    UPDATE_SQL = """
    UPDATE silver.cleaned_project_attributes
    SET
        extracted_goal_en = :sdg_goal_name,
        extracted_target_en = :sdg_target_name,
        extracted_indicator_en = :sdg_indicator_name
    WHERE [index] = :index
    """
else:
    print("Updating extracted_*_en with IDS/CODES (sdg_goal/sdg_target/sdg_indicator)...")
    UPDATE_SQL = """
    UPDATE silver.cleaned_project_attributes
    SET
        extracted_goal_en = :sdg_goal,
        extracted_target_en = :sdg_target,
        extracted_indicator_en = :sdg_indicator
    WHERE [index] = :index
    """

with engine.begin() as conn:
    for i, row in enumerate(updates, start=1):
        conn.execute(text(UPDATE_SQL), row)
        if i <= 3:
            print(f"Updated sample row {i}: index={row['index']}")
    print(f"Updated {len(updates):,} rows in silver.cleaned_project_attributes.")