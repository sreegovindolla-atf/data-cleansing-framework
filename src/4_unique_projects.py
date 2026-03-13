import argparse
import re
import urllib
from collections import defaultdict
from datetime import datetime

import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Integer, NVARCHAR
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.unique_projects_config import STEP_CONFIG


# =========================================================
# Global configuration
# =========================================================
SIM_THR = 0.75
TOP_K = 30
TARGET_SCHEMA = "silver"


# =========================================================
# SQL Server connection
# =========================================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


engine = get_sql_server_engine()


# =========================================================
# Embedding model
# =========================================================
#model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
#Use this for more accuracy
model = "all-MiniLM-L6-v2"

# =========================================================
# Argument parser
# =========================================================
def parse_args():
    """
    Parse optional command-line arguments.

    --steps 5
    -> run step 5 and all of its dependencies

    No argument
    -> run all steps
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--steps",
        nargs="+",
        type=int,
        help="Optional list of step numbers to run. Dependencies will be included automatically."
    )
    return parser.parse_args()


# =========================================================
# Utility helpers
# =========================================================
def l2_normalize(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return x / norms


def connected_components(edges, nodes):
    parent = {n: n for n in nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for a, b in edges:
        union(a, b)

    comps = defaultdict(list)
    for n in nodes:
        comps[find(n)].append(n)

    return list(comps.values())


def pick_longest(sub_df: pd.DataFrame, col: str):
    if col not in sub_df.columns:
        return None

    vals = sub_df[col].fillna("").astype(str).tolist()
    vals = [v.strip() for v in vals if v.strip()]
    return max(vals, key=len) if vals else None


def build_dtype_map(include_emergency: bool):
    dtype_map = {
        "index_list": NVARCHAR(None),
        "master_project_title_en": NVARCHAR(None),
        "master_project_description_en": NVARCHAR(None),
        "master_project_title_ar": NVARCHAR(None),
        "master_project_description_ar": NVARCHAR(None),
        "country_en": NVARCHAR(255),
        "donor_en": NVARCHAR(255),
        "implementing_org_en": NVARCHAR(255),
        "year": Integer(),
        "created_at": DateTime(),
    }

    if include_emergency:
        dtype_map["EmergencyTitle"] = NVARCHAR(None)
        dtype_map["EmergencyTitleAR"] = NVARCHAR(None)

    return dtype_map


def normalize_index_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()
    return value if value else None


def extract_indexes_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a normalized helper dataframe containing one row per index.

    This is required because:
    - some steps return one row per original index
    - some steps aggregate multiple indexes into index_list
    - later SQL exclusions need a simple table with one index per row
    """
    index_values = []

    # Case 1: direct index column exists
    if "index" in df.columns:
        for v in df["index"].tolist():
            norm = normalize_index_value(v)
            if norm:
                index_values.append(norm)

    # Case 2: aggregated comma-separated index_list exists
    elif "index_list" in df.columns:
        for raw in df["index_list"].fillna("").astype(str).tolist():
            parts = re.split(r"\s*,\s*", raw.strip())
            for p in parts:
                norm = normalize_index_value(p)
                if norm:
                    index_values.append(norm)

    # Case 3: aggregated reference index
    elif "ref_index" in df.columns:
        for v in df["ref_index"].tolist():
            norm = normalize_index_value(v)
            if norm:
                index_values.append(norm)

    # Deduplicate
    if index_values:
        index_df = pd.DataFrame({"index": sorted(set(index_values))})
    else:
        index_df = pd.DataFrame(columns=["index"])

    return index_df


def write_index_helper_table(df: pd.DataFrame, base_table: str, schema: str, if_exists: str = "replace"):
    """
    Write a helper table with one index per row.

    Example:
    step3_spn_ss_input -> step3_spn_ss_input_indexes

    This is used by later SQL steps in NOT EXISTS clauses.
    """
    helper_table = f"{base_table}_indexes"
    index_df = extract_indexes_from_df(df)

    index_df.to_sql(
        name=helper_table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        dtype={"index": NVARCHAR(255)},
        chunksize=500,
    )

    print(f"[DONE] Helper index table written to {schema}.{helper_table} | rows={len(index_df)}")


# =========================================================
# Step executors
# =========================================================
def run_sql_to_table_step(step_no: int, cfg: dict):
    """
    Execute a SQL query and write its output to a table.

    Used for steps like:
    - step1_adfd_input
    - step2_widow_input
    """
    sql = cfg["sql"]
    target_table = cfg["target_table"]
    schema = cfg.get("schema", TARGET_SCHEMA)
    if_exists = cfg.get("if_exists", "replace")

    print(f"\n[STEP {step_no}] {cfg.get('description', '')}")
    print(f"[INFO] Running SQL and writing output to {schema}.{target_table}")

    df = pd.read_sql(sql, engine)

    # Write SQL result to target table
    df.to_sql(
        name=target_table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=500,
    )

    print(f"[DONE] Step {step_no} output written to {schema}.{target_table} | rows={len(df)}")

    write_index_helper_table(df, target_table, schema, if_exists=if_exists)


def cluster_step(step_no: int, cfg: dict):
    """
    Execute a cluster-type step.

    Flow:
    1. Run source_sql
    2. Write raw result to input_table
    3. Write input_table_indexes helper table
    4. Cluster within each group
    5. Write clustered result to target_table
    """
    source_sql = cfg["source_sql"]
    input_table = cfg["input_table"]
    target_table = cfg["target_table"]
    group_cols = cfg["group_cols"]
    include_emergency = cfg.get("include_emergency", False)
    schema = cfg.get("schema", TARGET_SCHEMA)
    if_exists = cfg.get("if_exists", "replace")

    print(f"\n[STEP {step_no}] {cfg.get('description', '')}")
    print(f"[INFO] Preparing raw input table {schema}.{input_table}")

    df = pd.read_sql(source_sql, engine)

    df.to_sql(
        name=input_table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=500,
    )

    print(f"[DONE] Raw input written to {schema}.{input_table} | rows={len(df)}")

    write_index_helper_table(df, input_table, schema, if_exists=if_exists)

    if df.empty:
        print(f"[WARN] No rows returned for step {step_no}. Skipping clustering.")

        empty_columns = [
            "index_list",
            "master_project_title_en",
            "master_project_description_en",
            "master_project_title_ar",
            "master_project_description_ar",
            "year",
            "country_en",
            "donor_en",
            "implementing_org_en",
            "created_at",
        ]

        if include_emergency:
            empty_columns.extend(["EmergencyTitle", "EmergencyTitleAR"])

        empty_out_df = pd.DataFrame(columns=empty_columns)

        empty_out_df.to_sql(
            name=target_table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=500,
        )

        print(f"[DONE] Empty clustered output written to {schema}.{target_table}")
        return

    rows_out = []

    # Group rows by configured business keys before similarity clustering
    for _, g in df.groupby(group_cols, dropna=False):
        g = g.reset_index(drop=True)

        # If the group has only one row, no similarity check is needed
        if len(g) == 1:
            out_row = {
                "index_list": [g.loc[0, "index"]],
                "master_project_title_en": g.loc[0, "master_project_title_en"],
                "master_project_description_en": g.loc[0, "master_project_description_en"],
                "master_project_title_ar": g.loc[0, "master_project_title_ar"],
                "master_project_description_ar": g.loc[0, "master_project_description_ar"],
                "year": g.loc[0, "year"],
                "country_en": g.loc[0, "country_en"],
                "donor_en": g.loc[0, "donor_en"],
                "implementing_org_en": g.loc[0, "implementing_org_en"],
            }

            if include_emergency:
                out_row["EmergencyTitle"] = g.loc[0, "EmergencyTitle"]
                out_row["EmergencyTitleAR"] = g.loc[0, "EmergencyTitleAR"]

            rows_out.append(out_row)
            continue

        # Create embeddings for text similarity
        texts = g["combined_text"].fillna("").tolist()
        emb = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        emb = l2_normalize(emb).astype("float32")

        # Use FAISS inner-product search on normalized vectors
        faiss_index = faiss.IndexFlatIP(emb.shape[1])
        faiss_index.add(emb)

        sims, nbrs = faiss_index.search(emb, min(TOP_K, len(g)))

        # Build similarity graph edges above threshold
        edges = set()
        for i in range(len(g)):
            for jpos in range(1, nbrs.shape[1]):  # skip self at position 0
                j = int(nbrs[i, jpos])
                s = float(sims[i, jpos])

                if s >= SIM_THR and i != j:
                    edges.add((min(i, j), max(i, j)))

        # Merge all linked rows into connected components
        clusters = connected_components(edges, nodes=list(range(len(g))))

        # Collapse each cluster to a single representative output row
        for cluster in clusters:
            sub = g.iloc[cluster].copy()

            out_row = {
                "index_list": sub["index"].tolist(),
                "master_project_title_en": pick_longest(sub, "master_project_title_en"),
                "master_project_description_en": pick_longest(sub, "master_project_description_en"),
                "master_project_title_ar": pick_longest(sub, "master_project_title_ar"),
                "master_project_description_ar": pick_longest(sub, "master_project_description_ar"),
                "year": sub["year"].iloc[0],
                "country_en": sub["country_en"].iloc[0],
                "donor_en": sub["donor_en"].iloc[0],
                "implementing_org_en": sub["implementing_org_en"].iloc[0],
            }

            if include_emergency:
                out_row["EmergencyTitle"] = pick_longest(sub, "EmergencyTitle")
                out_row["EmergencyTitleAR"] = pick_longest(sub, "EmergencyTitleAR")

            rows_out.append(out_row)

    # Build final clustered dataframe
    out_df = pd.DataFrame(rows_out)

    if not out_df.empty:
        # Store clustered index list as comma-separated string
        out_df["index_list"] = out_df["index_list"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else str(x)
        )
        out_df["year"] = pd.to_numeric(out_df["year"], errors="coerce").astype("Int64")

    # Add audit column
    out_df["created_at"] = datetime.utcnow()

    dtype_map = build_dtype_map(include_emergency=include_emergency)

    out_df.to_sql(
        name=target_table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        dtype=dtype_map,
        chunksize=500,
    )

    print(f"[DONE] Clustered output written to {schema}.{target_table} | rows={len(out_df)}")


# =========================================================
# Dependency resolution
# =========================================================
def resolve_steps(requested_steps):
    """
    Resolve dependencies recursively and return steps
    in the correct execution order.
    """
    resolved = []
    visited = set()

    def visit(step_no):
        if step_no in visited:
            return

        if step_no not in STEP_CONFIG:
            raise ValueError(f"Invalid step: {step_no}")

        for dep in STEP_CONFIG[step_no].get("depends_on", []):
            visit(dep)

        visited.add(step_no)
        resolved.append(step_no)

    for step in requested_steps:
        visit(step)

    return resolved


def run_step(step_no: int):
    """
    Dispatch execution based on step type.
    """
    cfg = STEP_CONFIG[step_no]
    step_type = cfg["type"]

    if step_type == "sql_to_table":
        run_sql_to_table_step(step_no, cfg)
    elif step_type == "cluster":
        cluster_step(step_no, cfg)
    else:
        raise ValueError(f"Unknown step type for step {step_no}: {step_type}")


# =========================================================
# Main
# =========================================================
def main():
    """
    Main entry point.

    Examples:
    python unique_projects.py
    python unique_projects.py --steps 5
    python unique_projects.py --steps 7 8
    """
    args = parse_args()

    # If no steps specified, run everything
    if args.steps is None:
        requested_steps = sorted(STEP_CONFIG.keys())
    else:
        invalid_steps = [s for s in args.steps if s not in STEP_CONFIG]
        if invalid_steps:
            raise ValueError(
                f"Invalid step(s): {invalid_steps}. Valid steps are: {sorted(STEP_CONFIG.keys())}"
            )
        requested_steps = args.steps

    # Automatically include dependencies
    steps_to_run = resolve_steps(requested_steps)

    print(f"[INFO] Requested steps: {requested_steps}")
    print(f"[INFO] Steps to run with dependencies: {steps_to_run}")

    # Execute steps in resolved order
    for step_no in steps_to_run:
        run_step(step_no)

    print("\n[INFO] Pipeline completed successfully.")


if __name__ == "__main__":
    main()