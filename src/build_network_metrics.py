import os
from io import StringIO
from pathlib import Path

import pandas as pd
import numpy as np
import psycopg2
import networkx as nx
import matplotlib.pyplot as plt

# ---------------------------------------------------
# 1. Database connection parameters
# ---------------------------------------------------
# Non-sensitive parameters are hardcoded. The password is read from an
# environment variable so it never appears in the source code or GitHub.

USER = "postgres"
HOST = "127.0.0.1"
PORT = "5432"
DBNAME = "crypto_sql"

PASSWORD = os.environ.get("PGPASSWORD")   # must be set externally

if PASSWORD is None:
    raise ValueError(
        "Environment variable PGPASSWORD is not set. "
        "Set it in PowerShell, e.g.:  $env:PGPASSWORD = 'your_password'"
    )

# psycopg2 connection (used for both reading and writing)
conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
)

# ---------------------------------------------------
# 2. Configuration: correlation threshold and output paths
# ---------------------------------------------------
# Only edges with |corr| >= CORR_THRESHOLD will be included in the network.
# You can tune this value depending on how dense/sparse you want the graphs.
CORR_THRESHOLD = 0.5

# Directory for figures / reports
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True, parents=True)

# ---------------------------------------------------
# 3. Load monthly correlations from database
# ---------------------------------------------------
query = """
SELECT month, asset_i, asset_j, corr
FROM monthly_correlations
WHERE corr IS NOT NULL
"""

df = pd.read_sql(query, conn)

# Ensure month is a proper datetime
df["month"] = pd.to_datetime(df["month"])

# ---------------------------------------------------
# 4. Build monthly graphs and compute network metrics
# ---------------------------------------------------
records = []

for month, df_month in df.groupby("month"):
    # Filter edges by absolute correlation threshold
    df_edges = df_month[np.abs(df_month["corr"]) >= CORR_THRESHOLD]

    # If there are no edges above the threshold, skip this month
    if df_edges.empty:
        continue

    # Build an undirected graph
    G = nx.Graph()

    # Add edges with weight = correlation
    for _, row in df_edges.iterrows():
        G.add_edge(row["asset_i"], row["asset_j"], weight=row["corr"])

    n_nodes = G.number_of_nodes()
    n_edges = G.number_of_edges()

    if n_nodes == 0:
        # Nothing to compute for this month
        continue

    # Basic metrics
    density = nx.density(G)

    degrees = np.array([d for _, d in G.degree()])
    avg_degree = float(degrees.mean()) if len(degrees) > 0 else 0.0
    max_degree = int(degrees.max()) if len(degrees) > 0 else 0

    # Average clustering coefficient
    clustering_dict = nx.clustering(G, weight=None)
    avg_clustering = float(np.mean(list(clustering_dict.values()))) if clustering_dict else 0.0

    # Size of the largest connected component
    lcc_size = len(max(nx.connected_components(G), key=len))

    records.append(
        {
            "month": month.date(),   # store as plain date
            "n_assets": n_nodes,
            "n_edges": n_edges,
            "density": density,
            "avg_degree": avg_degree,
            "max_degree": max_degree,
            "avg_clustering": avg_clustering,
            "lcc_size": lcc_size,
        }
    )

if records:
    metrics_df = pd.DataFrame(records)
    metrics_df = metrics_df.sort_values("month").reset_index(drop=True)
else:
    metrics_df = pd.DataFrame(
        columns=[
            "month",
            "n_assets",
            "n_edges",
            "density",
            "avg_degree",
            "max_degree",
            "avg_clustering",
            "lcc_size",
        ]
    )

# ---------------------------------------------------
# 5. Write network metrics to PostgreSQL using COPY
# ---------------------------------------------------
cur = conn.cursor()

cur.execute("""
    DROP TABLE IF EXISTS temporal_network_metrics;
    CREATE TABLE temporal_network_metrics (
        month          DATE PRIMARY KEY,
        n_assets       INTEGER,
        n_edges        INTEGER,
        density        DOUBLE PRECISION,
        avg_degree     DOUBLE PRECISION,
        max_degree     INTEGER,
        avg_clustering DOUBLE PRECISION,
        lcc_size       INTEGER
    );
""")

if not metrics_df.empty:
    # Prepare CSV in memory
    buffer = StringIO()
    metrics_df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_sql = """
        COPY temporal_network_metrics (
            month,
            n_assets,
            n_edges,
            density,
            avg_degree,
            max_degree,
            avg_clustering,
            lcc_size
        )
        FROM STDIN WITH (FORMAT csv)
    """
    cur.copy_expert(copy_sql, buffer)

conn.commit()

# ---------------------------------------------------
# 6. Generate simple time-series plots for key metrics
# ---------------------------------------------------
if not metrics_df.empty:
    # Convert month back to datetime for plotting
    metrics_df["month"] = pd.to_datetime(metrics_df["month"])

    # Plot network density over time
    plt.figure(figsize=(8, 4))
    plt.plot(metrics_df["month"], metrics_df["density"], marker="o")
    plt.xlabel("Month")
    plt.ylabel("Network density")
    plt.title(f"Network density over time (|corr| >= {CORR_THRESHOLD})")
    plt.xticks(rotation=45)
    plt.tight_layout()
    density_path = REPORTS_DIR / "network_density_over_time.png"
    plt.savefig(density_path)
    plt.close()

    # Plot average degree over time
    plt.figure(figsize=(8, 4))
    plt.plot(metrics_df["month"], metrics_df["avg_degree"], marker="o")
    plt.xlabel("Month")
    plt.ylabel("Average degree")
    plt.title(f"Average degree over time (|corr| >= {CORR_THRESHOLD})")
    plt.xticks(rotation=45)
    plt.tight_layout()
    degree_path = REPORTS_DIR / "average_degree_over_time.png"
    plt.savefig(degree_path)
    plt.close()

cur.close()
conn.close()

print("Done building temporal_network_metrics.")
print("Rows inserted:", len(metrics_df))
print(f"Figures saved in: {REPORTS_DIR.resolve()}")
