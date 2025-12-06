import os
from io import StringIO

import pandas as pd
import numpy as np
import psycopg2

# ---------------------------------------------------
# 1. Database connection parameters
# ---------------------------------------------------
# Non-sensitive parameters are hardcoded. The password is read from an
# environment variable so it never appears in the source code or GitHub.

USER = "postgres"
HOST = "127.0.0.1"     # force TCP on localhost
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
# 2. Load daily log-returns from database
# ---------------------------------------------------
query = """
SELECT asset_address, day, log_return
FROM ohlc_daily
WHERE log_return IS NOT NULL
"""

df = pd.read_sql(query, conn)

df["day"] = pd.to_datetime(df["day"])
df["month"] = df["day"].dt.to_period("M").dt.to_timestamp()

# ---------------------------------------------------
# 3. Compute monthly correlations
# ---------------------------------------------------
min_days_per_asset = 10  # minimum number of daily points per month per asset
records = []

for month, df_month in df.groupby("month"):
    # Pivot: rows = days, columns = assets, values = log_returns
    pivot = df_month.pivot(index="day", columns="asset_address", values="log_return")

    # Filter out assets with insufficient data in this month
    valid_counts = pivot.count()
    keep_cols = valid_counts[valid_counts >= min_days_per_asset].index
    pivot = pivot[keep_cols]

    # If fewer than 2 assets survive, skip this month
    if pivot.shape[1] < 2:
        continue

    # Asset × asset correlation matrix
    corr_matrix = pivot.corr()

    # Give distinct names to the row and column axes to avoid
    # duplicate column names when resetting index.
    corr_matrix.index.name = "asset_i"
    corr_matrix.columns.name = "asset_j"

    # Convert to long format: columns = [asset_i, asset_j, corr]
    corr_long = corr_matrix.stack().reset_index(name="corr")

    # Remove self-correlations
    corr_long = corr_long[corr_long["asset_i"] != corr_long["asset_j"]]

    # Add month
    corr_long["month"] = month

    records.append(corr_long)

if records:
    monthly_corr = pd.concat(records, ignore_index=True)
else:
    monthly_corr = pd.DataFrame(columns=["asset_i", "asset_j", "corr", "month"])

# Ensure 'month' is a plain date object for PostgreSQL DATE
if not monthly_corr.empty:
    monthly_corr["month"] = pd.to_datetime(monthly_corr["month"]).dt.date
    # Reorder columns to a fixed order
    monthly_corr = monthly_corr[["month", "asset_i", "asset_j", "corr"]]

# ---------------------------------------------------
# 4. Write correlation data to PostgreSQL using COPY
# ---------------------------------------------------
cur = conn.cursor()

# Drop and recreate the target table
cur.execute("""
    DROP TABLE IF EXISTS monthly_correlations;
    CREATE TABLE monthly_correlations (
        month      DATE NOT NULL,
        asset_i    TEXT NOT NULL,
        asset_j    TEXT NOT NULL,
        corr       DOUBLE PRECISION
    );
""")

if not monthly_corr.empty:
    # Convert the DataFrame to CSV in memory (without header)
    buffer = StringIO()
    monthly_corr.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Use COPY for fast bulk insert
    copy_sql = """
        COPY monthly_correlations (month, asset_i, asset_j, corr)
        FROM STDIN WITH (FORMAT csv)
    """
    cur.copy_expert(copy_sql, buffer)

# Commit changes and close
conn.commit()
cur.close()
conn.close()

print("Done building monthly_correlations.")
print("Rows inserted:", len(monthly_corr))
