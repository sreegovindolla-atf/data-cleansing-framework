from sentence_transformers import SentenceTransformer
import numpy as np
from pathlib import Path
import pandas as pd
import json
import faiss

# -----------------------------
# Config
# -----------------------------
MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 2
SIMILARITY_THRESHOLD = 0.80

# -----------------------------
# Helper functions
# -----------------------------
def build_text(title: str, description: str) -> str:
    return f"{title}\n{description}".strip()

# -----------------------------
# Base projects (10)
# -----------------------------
base_projects = [
    {
        "project_index": "MBRGI-2023-0004",
        "title": "The UAE's One Billion Meals campaign has distributed 12.6 million meals benefiting 75,147 people.",
        "description": "The UAE's One Billion Meals campaign has distributed 12.6 million meals benefiting 75,147 people."
    },
    {
        "project_index": "ARC-2016-006",
        "title": "Building houses",
        "description": "Building houses"
    },
    {
        "project_index": "ARC-2017-061",
        "title": "sponsoring orphans",
        "description": "sponsoring orphans"
    },
    {
        "project_index": "ADFD-2010-107",
        "title": "Radio stations",
        "description": "Radio stations"
    },
    {
        "project_index": "ADFD-2014-085",
        "title": "Sh. Zayed Canal - Wouth Alwadi",
        "description": "Sh. Zayed Canal - Wouth Alwadi"
    },
    {
        "project_index": "ADFD-2000-005-R",
        "title": "Phosphate Factory in Al-Aqaba",
        "description": "Phosphate Factory in Al-Aqaba"
    },
    {
        "project_index": "ADFD-2000-022",
        "title": "Integration Health project",
        "description": "Integration Health project"
    },
    {
        "project_index": "ADFD-2007-046",
        "title": "Baku Bybass highway",
        "description": "Baku Bybass highway"
    },
    {
        "project_index": "ADFD-2010-080",
        "title": "Sewerag project in ehden  and korah distric",
        "description": "Project's Objectives:  The project aims to establish sewerage system in the villages of Ehden and Al Korah districts, as currently the wastewater are disposed in the sea. the total benefited population are around 25,000 people. The project contributes in "
    },
    {
        "project_index": "ADFD-2020-0125 ",
        "title": "30 MW Solar Power Plant Project in Blitta",
        "description": "30 MW Solar Power Plant Project in Blitta"
    },
]

# -----------------------------
# Query projects (10)
# -----------------------------
query_projects = [
    {
        "index": "ADFD-2002-007-R",
        "title": "Phosphat Factory in Al-Aqaba",
        "description": "Phosphat Factory in Al-Aqaba"
    },
    {
        "index": "ADFD-2024-0002",
        "title": "Integrated Health project",
        "description": "Integrated Health project"
    },
    {
        "index": "ADFD-2016-025",
        "title": "Baku Baypass High Way",
        "description": "Baku Baypass High Way"
    },
    {
        "index": "ADFD-2014-030",
        "title": "Sewerag project in ehden  and korah distric",
        "description": "Sewerag project in ehden  and korah distric"
    },
    {
        "index": "ADFD-2021-0072",
        "title": "Sheikh Mohamed Bin Zayed Al Nahyan Solar PV Complex",
        "description": "30 MW Solar Power Plant Project in Blitta"
    },
    {
        "index": "MBRGI-2023-0005",
        "title": "The UAE's One Billion Meals campaign has distributed 2.2 million meals benefiting 8,105 people.",
        "description": "The UAE's One Billion Meals campaign has distributed 2.2 million meals benefiting 8,105 people."
    },
    {
        "index": "ARC-2016-021",
        "title": "Building houses",
        "description": "Building houses"
    },
    {
        "index": "ARC-2018-001",
        "title": "Orphan sponsorship",
        "description": "Orphan sponsorship"
    },
    {
        "index": "ADFD-2012-060",
        "title": "Radio stations",
        "description": "Radio stations"
    },
    {
        "index": "ADFD-2017-140",
        "title": "Sh. Zayed Canal - South Alwadi",
        "description": "Sh. Zayed Canal - South Alwadi"
    },
]

# -----------------------------
# Build embeddings
# -----------------------------
model = SentenceTransformer(MODEL_NAME)

# Base embeddings
base_texts = [build_text(p["title"], p["description"]) for p in base_projects]
base_embeddings = model.encode(
    base_texts,
    normalize_embeddings=True,
    show_progress_bar=True
)

# Query embeddings
query_texts = [build_text(p["title"], p["description"]) for p in query_projects]
query_embeddings = model.encode(
    query_texts,
    normalize_embeddings=True,
    show_progress_bar=True
)

# FAISS expects float32 arrays
base_embeddings = np.asarray(base_embeddings, dtype=np.float32)
query_embeddings = np.asarray(query_embeddings, dtype=np.float32)

# -----------------------------
# Save base embeddings to CSV
# -----------------------------
rows = []
for project, embedding in zip(base_projects, base_embeddings):
    rows.append({
        "project_index": project["project_index"],
        "project_title": project["title"],
        "project_description": project["description"],
        "embedding": json.dumps(embedding.tolist())
    })

df_embeddings = pd.DataFrame(rows)

RUN_OUTPUT_DIR = Path("data/outputs/embeddings")
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = RUN_OUTPUT_DIR / "base_project_embeddings.csv"
df_embeddings.to_csv(OUT_CSV, index=False)
print(f"Saved {OUT_CSV}")

# -----------------------------
# Build FAISS index
# -----------------------------
dim = base_embeddings.shape[1]
index = faiss.IndexFlatIP(dim)  # Inner Product
index.add(base_embeddings)      # add base vectors

# -----------------------------
# Similarity search with FAISS
# -----------------------------
scores, base_idxs = index.search(query_embeddings, TOP_K)

# -----------------------------
# Print results + build output CSV
# -----------------------------
similarity_output_rows = []

for q_idx, q in enumerate(query_projects):
    similar_project_indices = []

    print("\n" + "=" * 80)
    print(f"QUERY: {q['title']}")

    for rank in range(TOP_K):
        score = float(scores[q_idx][rank])
        b_idx = int(base_idxs[q_idx][rank])

        # Safety: FAISS can return -1 if nothing is found
        if b_idx < 0:
            continue

        if score < SIMILARITY_THRESHOLD:
            continue

        base_project = base_projects[b_idx]
        similar_project_indices.append(base_project["project_index"])

    similarity_output_rows.append({
        "index": q["index"],
        "project_title": q["title"],
        "project_description": q["description"],
        "similar_project_indices": json.dumps(similar_project_indices)
    })

# Save similarity output CSV
OUT_SIM_CSV = RUN_OUTPUT_DIR / "similar_project_indices.csv"
pd.DataFrame(similarity_output_rows).to_csv(OUT_SIM_CSV, index=False)
print(f"\nSaved {OUT_SIM_CSV}")