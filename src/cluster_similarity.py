import numpy as np
import pandas as pd
import faiss
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, UnicodeText, DateTime, Float
import urllib
import ast
from datetime import datetime, timezone
import argparse
import sys
from collections import defaultdict
from itertools import combinations

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.app_config import SEMANTIC_SIMILARITY_CONFIG as CONFIG

# =========================================================
# Args
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument(
    "--source-mode",
    required=True,
    choices=["master projects", "projects"],
    help="master projects = cluster master projects; projects = cluster projects"
)
parser.add_argument(
    "--histogram",
    action="store_true",
    help="If set, uses lower threshold (0.5) and writes to histogram schema/table"
)
args = parser.parse_args()

SOURCE_MODE = args.source_mode
HISTOGRAM_MODE = bool(args.histogram)

# =========================================================
# Config
# =========================================================
TOP_K = 50
DEFAULT_SIMILARITY_THRESHOLD = 0.75
HISTOGRAM_THRESHOLD = 0.5

DEFAULT_TARGET_SCHEMA = "silver"
HISTOGRAM_TARGET_SCHEMA = "histogram"

MODE_CFG = CONFIG[SOURCE_MODE]

SOURCE_SQL = MODE_CFG["source_sql"]

DEFAULT_CLUSTER_TARGET_TABLE = MODE_CFG["cluster_target_table"]
HISTOGRAM_CLUSTER_TARGET_TABLE = MODE_CFG["histogram_cluster_target_table"]

if HISTOGRAM_MODE:
    SIMILARITY_THRESHOLD = HISTOGRAM_THRESHOLD
    TARGET_SCHEMA = HISTOGRAM_TARGET_SCHEMA
    TARGET_TABLE = HISTOGRAM_CLUSTER_TARGET_TABLE
else:
    SIMILARITY_THRESHOLD = DEFAULT_SIMILARITY_THRESHOLD
    TARGET_SCHEMA = DEFAULT_TARGET_SCHEMA
    TARGET_TABLE = DEFAULT_CLUSTER_TARGET_TABLE

print(
    f"[MODE] source_mode={SOURCE_MODE} | histogram={HISTOGRAM_MODE} "
    f"| threshold={SIMILARITY_THRESHOLD} | target={TARGET_SCHEMA}.{TARGET_TABLE}"
)

FILTER_COLS = ["country_name_en", "donor_name_en", "implementing_org_en"]
SEASONAL_SUBSECTOR = "Seasonal programmes"

# =========================================================
# SQL SERVER CONNECTION (WINDOWS AUTH)
# =========================================================
def get_sql_server_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SREESPOORTHY\\SQLEXPRESS01;"
        "DATABASE=ForeignAidDatabase_2019;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

engine = get_sql_server_engine()

# =========================================================
# Load source + embeddings
# =========================================================
df = pd.read_sql_query(text(SOURCE_SQL), engine).fillna("").reset_index(drop=True)
print(f"[LOAD] Loaded {len(df):,} rows from source table")

print("[EMB] Parsing embeddings...")
embeddings = np.asarray([ast.literal_eval(s) for s in df["embedding"]], dtype=np.float32)
print(f"[EMB] Parsed embeddings with shape {embeddings.shape}")

faiss.normalize_L2(embeddings)

TITLE_EN = MODE_CFG["title_en"]
DESC_EN  = MODE_CFG["desc_en"]
TITLE_AR = MODE_CFG["title_ar"]
DESC_AR  = MODE_CFG["desc_ar"]

# This is the unique id used to represent a project
# - master projects: "index"
# - projects: project_code
if SOURCE_MODE == "projects" and "project_code" in df.columns:
    ENTITY_ID_COL = "project_code"
else:
    ENTITY_ID_COL = "index"

print(f"[ID] Using ENTITY_ID_COL={ENTITY_ID_COL}")

# =========================================================
# COMPLETE-LINKAGE CLUSTERING USING THRESHOLD GRAPH
# =========================================================
def build_edges_for_group(g: pd.DataFrame, global_indices: np.ndarray):
    if len(g) < 2:
        return {}

    idxs = global_indices
    vecs = embeddings[idxs]
    dim = vecs.shape[1]

    faiss_index = faiss.IndexFlatIP(dim)
    faiss_index.add(vecs)

    scores, nbrs = faiss_index.search(vecs, TOP_K + 1)

    edges = defaultdict(dict)

    for gi in range(len(g)):
        i_global = int(idxs[gi])
        for rank in range(TOP_K + 1):
            gj = int(nbrs[gi, rank])
            s = float(scores[gi, rank])
            if gj < 0 or gj == gi:
                continue
            if s < SIMILARITY_THRESHOLD:
                continue
            j_global = int(idxs[gj])

            prev = edges[i_global].get(j_global)
            if prev is None or s > prev:
                edges[i_global][j_global] = s
            prev2 = edges[j_global].get(i_global)
            if prev2 is None or s > prev2:
                edges[j_global][i_global] = s

    return edges

def complete_linkage_clusters(nodes: list[int], edges: dict[int, dict[int, float]]):
    degrees = {n: len(edges.get(n, {})) for n in nodes}
    remaining = sorted(nodes, key=lambda n: degrees[n], reverse=True)

    clusters = []
    assigned = set()

    for seed in remaining:
        if seed in assigned:
            continue

        cluster = [seed]
        assigned.add(seed)

        cand = set(edges.get(seed, {}).keys()) - assigned
        cand = sorted(cand, key=lambda n: degrees.get(n, 0), reverse=True)

        for v in cand:
            if v in assigned:
                continue
            ok = True
            v_edges = edges.get(v, {})
            for u in cluster:
                if u not in v_edges: 
                    ok = False
                    break
            if ok:
                cluster.append(v)
                assigned.add(v)

        clusters.append(cluster)

    return clusters

# =========================================================
# Cluster avg similarity (pairwise average inside cluster)
# =========================================================
def compute_avg_pair_similarity(cluster: list[int]) -> float | None:
    """
    Returns average cosine similarity over all unique pairs in the cluster.
    For singleton clusters, returns None.
    """
    n = len(cluster)
    if n < 2:
        return None

    sims = []
    for i, j in combinations(cluster, 2):
        sims.append(float(np.dot(embeddings[i], embeddings[j])))

    return round(float(np.mean(sims)), 4)

# =========================================================
# Run within hard-filter groups (seasonal vs non-seasonal)
# =========================================================
ts_inserted = datetime.now(timezone.utc)

df_seasonal = df[df["subsector_name_en"] == SEASONAL_SUBSECTOR]
df_non_seasonal = df[df["subsector_name_en"] != SEASONAL_SUBSECTOR]
print(f"[SPLIT] Seasonal: {len(df_seasonal):,} | Non-seasonal: {len(df_non_seasonal):,}")

def iter_groups(df_slice: pd.DataFrame, group_cols: list[str]):
    for key, g in df_slice.groupby(group_cols, dropna=False, sort=False):
        yield key, g

all_clusters_global = []

def process_slice(df_slice: pd.DataFrame, group_cols: list[str], label: str):
    print(f"[RUN] {label} groups by {group_cols}")
    groups = 0
    nodes_total = 0
    clusters_total = 0

    for _, g in iter_groups(df_slice, group_cols):
        groups += 1

        if len(g) < 2:
            all_clusters_global.append([int(g.index[0])])
            clusters_total += 1
            nodes_total += 1
            continue

        idxs = g.index.to_numpy().astype(int)

        edges = build_edges_for_group(g, idxs)

        nodes = idxs.tolist()
        clusters = complete_linkage_clusters(nodes, edges)

        all_clusters_global.extend(clusters)
        clusters_total += len(clusters)
        nodes_total += len(nodes)

    print(f"[RUN] {label}: groups={groups:,} nodes={nodes_total:,} clusters={clusters_total:,}")

process_slice(df_non_seasonal, FILTER_COLS, "NON-SEASONAL")
process_slice(df_seasonal, FILTER_COLS + ["year"], "SEASONAL")

# =========================================================
# Build cluster output schema + avg_similarity_score
# =========================================================
def fmt_cluster_id(n: int) -> str:
    return f"CL-{n:05d}"

rows = []
cluster_num = 0

for cluster in all_clusters_global:
    cluster_num += 1
    cid = fmt_cluster_id(cluster_num)

    # ✅ compute ONCE per cluster
    avg_sim = compute_avg_pair_similarity(cluster)

    for gi in cluster:
        r = df.iloc[int(gi)]
        rows.append({
            "cluster_id": cid,
            "index": r.get("index", ""),
            "source_id": r.get("source_id", ""),
            "project_title_en": r.get(TITLE_EN, ""),
            "project_title_ar": r.get(TITLE_AR, ""),
            "project_description_en": r.get(DESC_EN, ""),
            "project_description_ar": r.get(DESC_AR, ""),
            "country_name_en": r.get("country_name_en", ""),
            "implementing_org_en": r.get("implementing_org_en", ""),
            "donor_name_en": r.get("donor_name_en", ""),
            "year": r.get("year", ""),
            "avg_similarity_score": avg_sim,
            "ts_inserted": ts_inserted,
        })

df_clusters = pd.DataFrame(rows)

print(f"[OUT] cluster rows: {len(df_clusters):,} | clusters: {df_clusters['cluster_id'].nunique():,}")

df_clusters = df_clusters.sort_values(["cluster_id", "index"]).reset_index(drop=True)

# =========================================================
# Save to CSV
# =========================================================
csv_dir = Path("data/outputs/embeddings")
csv_dir.mkdir(parents=True, exist_ok=True)
out_csv = csv_dir / f"complete_linkage_clusters_{SOURCE_MODE.replace(' ', '_')}.csv"
print("[CSV] Writing CSV output...")
df_clusters.to_csv(out_csv, index=False, encoding="utf-8-sig")
print(f"Saved {out_csv}")

# =========================================================
# Write to SQL Server
# =========================================================
dtype = {
    "cluster_id": NVARCHAR(50),
    "index": NVARCHAR(255),
    "source_id": NVARCHAR(255),
    "project_title_en": UnicodeText(),
    "project_title_ar": UnicodeText(),
    "project_description_en": UnicodeText(),
    "project_description_ar": UnicodeText(),
    "country_name_en": NVARCHAR(255),
    "implementing_org_en": NVARCHAR(255),
    "donor_name_en": NVARCHAR(255),
    "year": NVARCHAR(50),
    "avg_similarity_score": Float(),
    "ts_inserted": DateTime(),
}

print("[SQL] Writing clusters to SQL Server...")
df_clusters.to_sql(
    name=TARGET_TABLE,
    schema=TARGET_SCHEMA,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=200,
    method=None,
    dtype=dtype
)
print(f"Saved to SQL Server: {TARGET_SCHEMA}.{TARGET_TABLE} (mode={SOURCE_MODE})")