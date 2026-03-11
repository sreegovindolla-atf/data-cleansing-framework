import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import urllib

# =====================================
# SQL SERVER CONNECTION (WINDOWS AUTH)
# =====================================
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

sns.set_theme(style="whitegrid")

# -----------------------------------
# Config for histogram
# -----------------------------------
threshold = 0.75
bins = np.arange(0.45, 1.0001, 0.005)

# Define the 4 plots
plots = [
    {
        "title": "Master Projects (including similarity_score = 1)",
        "sql": """
            SELECT similarity_score
            FROM histogram.similar_master_projects
        """
    },
    {
        "title": "Master Projects (excluding similarity_score = 1)",
        "sql": """
            SELECT similarity_score
            FROM histogram.similar_master_projects
            WHERE similarity_score <> 1
        """
    },
    {
        "title": "Projects (including similarity_score = 1)",
        "sql": """
            SELECT similarity_score
            FROM histogram.similar_projects
        """
    },
    {
        "title": "Projects (excluding similarity_score = 1)",
        "sql": """
            SELECT similarity_score
            FROM histogram.similar_projects
            WHERE similarity_score <> 1
        """
    },
]

# -----------------------------------
# Draw 4 histograms
# -----------------------------------
for i, p in enumerate(plots, start=1):
    df = pd.read_sql(p["sql"], engine)

    print(f"[{i}/4] {p['title']} | Total pairs: {len(df):,}")

    plt.figure(figsize=(11, 6))

    sns.histplot(
        data=df,
        x="similarity_score",
        bins=bins,
        kde=False,
        color="#D4AF37",
        edgecolor="white",
        linewidth=0.5
    )

    # Threshold line
    plt.axvline(threshold, linestyle="--", linewidth=2, color="#8B4513", label=f"Threshold = {threshold:.2f}")

    plt.title(p["title"], fontsize=14)
    plt.xlabel("Similarity Score")
    plt.ylabel("Number of Similar Project Pairs")
    plt.legend()
    plt.tight_layout()
    plt.show()