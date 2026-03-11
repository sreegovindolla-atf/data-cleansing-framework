import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib
from sentence_transformers import SentenceTransformer
import faiss
from collections import defaultdict
from sqlalchemy.types import NVARCHAR, Integer, DateTime
from datetime import datetime

SIM_THR = 0.75
TOP_K = 30  # neighbors per row within group

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

SQL = """
SELECT *
FROM silver.step3_spn_ss;
"""

df = pd.read_sql(SQL, engine)

# --- Embedding model (multilingual for EN+AR) ---
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

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

rows_out = []

group_cols = ["donor_en", "country_en", "implementing_org_en", "year"]
for key, g in df.groupby(group_cols, dropna=False):
    g = g.reset_index(drop=True)

    # if group is tiny, skip heavy work
    if len(g) == 1:
        rows_out.append({
            "index_list": [g.loc[0, "index"]],
            "year": g.loc[0, "year"],
            "country_en": g.loc[0, "country_en"],
            "donor_en": g.loc[0, "donor_en"],
            "implementing_org_en": g.loc[0, "implementing_org_en"],
            # placeholders for summaries (filled later)
            "master_project_title_en": g.loc[0, "master_project_title_en"],
            "master_project_description_en": g.loc[0, "master_project_description_en"],
            "master_project_title_ar": g.loc[0, "master_project_title_ar"],
            "master_project_description_ar": g.loc[0, "master_project_description_ar"],
        })
        continue

    texts = g["combined_text"].fillna("").tolist()
    emb = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
    emb = l2_normalize(emb).astype("float32")

    index = faiss.IndexFlatIP(emb.shape[1])  # cosine via inner product after L2 norm
    index.add(emb)

    sims, nbrs = index.search(emb, min(TOP_K, len(g)))

    edges = set()
    for i in range(len(g)):
        for jpos in range(1, nbrs.shape[1]):  # skip self at position 0
            j = int(nbrs[i, jpos])
            s = float(sims[i, jpos])
            if s >= SIM_THR:
                a = int(i); b = int(j)
                if a != b:
                    edges.add((min(a,b), max(a,b)))

    clusters = connected_components(edges, nodes=list(range(len(g))))

    # output one row per cluster
    for cluster in clusters:
        sub = g.iloc[cluster].copy()
        idx_list = sub["index"].tolist()

        # for now keep "best" title/desc as the longest; replace with LLM summaries next
        def pick_longest(col):
            vals = sub[col].fillna("").astype(str).tolist()
            vals = [v.strip() for v in vals if v.strip()]
            return max(vals, key=len) if vals else None

        rows_out.append({
            "index_list": idx_list,
            "master_project_title_en": pick_longest("master_project_title_en"),
            "master_project_description_en": pick_longest("master_project_description_en"),
            "master_project_title_ar": pick_longest("master_project_title_ar"),
            "master_project_description_ar": pick_longest("master_project_description_ar"),
            "year": sub["year"].iloc[0],
            "country_en": sub["country_en"].iloc[0],
            "donor_en": sub["donor_en"].iloc[0],
            "implementing_org_en": sub["implementing_org_en"].iloc[0],
        })

out_df = pd.DataFrame(rows_out)

# Make index_list a stable string for SQL (or keep JSON)
out_df["index_list"] = out_df["index_list"].apply(lambda x: "[" + ",".join(map(str, x)) + "]")

TARGET_SCHEMA = "silver"
TARGET_TABLE  = "step3_spn_ss_similar_clusters"

out_df["year"] = out_df["year"].astype("Int64")

out_df["created_at"] = datetime.utcnow()

dtype_map = {
    "index_list": NVARCHAR(None),  # NVARCHAR(MAX)
    "master_project_title_en": NVARCHAR(None),
    "master_project_description_en": NVARCHAR(None),
    "master_project_title_ar": NVARCHAR(None),
    "master_project_description_ar": NVARCHAR(None),
    "country_en": NVARCHAR(255),
    "donor_en": NVARCHAR(255),
    "implementing_org_en": NVARCHAR(255),
    "year": Integer(),
    "created_at": DateTime() 
}

out_df.to_sql(
    name=TARGET_TABLE,
    con=engine,
    schema=TARGET_SCHEMA,
    if_exists="replace", 
    index=False,
    dtype=dtype_map,
    chunksize=500
)

print("Data written successfully to SQL Server")