# Crypto Asset Temporal Correlation Network  
*A full SQL + Python analytics pipeline for multi-asset crypto time-series*

---

## 📌 Overview

This project builds a **complete data pipeline** for analyzing the temporal correlation structure of crypto assets.  
Starting from raw OHLC time-series for hundreds of tokens, the pipeline:

1. **Ingests & cleans CSV data** into PostgreSQL  
2. **Computes daily log-returns**  
3. **Builds monthly cross-asset correlation matrices**  
4. **Constructs temporal correlation networks** (one network per month)  
5. **Computes key network metrics**  
6. **Generates analytical charts** in the `reports/` directory  

This repository demonstrates practical skills in:

- SQL & relational data modeling  
- Python data processing (pandas, numpy)  
- ETL orchestration  
- Network analysis (NetworkX)  
- Data visualization (matplotlib)  
- Production-style pipeline design  

The structure and methodology match what is expected from a **Data Scientist / Data Analyst** working in finance, crypto analytics, or consultancy.

---

## 🗂️ Repository Structure
```
crypto-sql-portfolio/
│
├── sql/
│ ├── 01_create_raw.sql
│ ├── 02_load_data.sql
│ ├── 03_build_daily.sql
│ └── 04_quality_checks.sql
│
├── src/
│ ├── build_monthly_correlations.py # Python ETL for correlation matrices
│ ├── build_network_metrics.py # Network analytics pipeline
│ └── utils/ # Helper modules (optional)
│
├── data/
│ └── ohlc_subset/ # Raw OHLC CSV files
│
├── reports/
│ ├── network_density_over_time.png
│ ├── average_degree_over_time.png
│ └── (other visualizations)
│
└── README.md
```
---

## 🛠️ Pipeline Architecture

### **1. Data ingestion (SQL + PowerShell)**  
Raw CSV files containing OHLC crypto time-series are loaded into PostgreSQL using:

- a PowerShell batch loader  
- the SQL script `02_load_data.sql`  

This produces the table:

ohlc_raw(asset_address, ts, ts_unix, price_open, price_high, price_low, price_close)


---

### **2. Feature engineering: daily log-returns**

`03_build_daily.sql` computes:

- daily close price  
- daily log-return per asset  

Output table:

monthly_correlations(month, asset_i, asset_j, corr)


---

### **4. Temporal network construction**

`build_network_metrics.py` creates:

- one graph per month  
- nodes = assets  
- edges = asset pairs with |corr| ≥ threshold (default: 0.5)

For each monthly network, it computes:

- number of nodes  
- number of edges  
- network density  
- average degree  
- max degree  
- average clustering coefficient  
- size of largest connected component  

These metrics are saved in the table:

temporal_network_metrics(
month,
n_assets,
n_edges,
density,
avg_degree,
max_degree,
avg_clustering,
lcc_size
)


---

## 📊 Key Visualizations

Two automatically generated plots (and more can be added):

- **Network density over time**  
- **Average degree over time**

These reveal how the dependency structure of crypto assets evolves month-by-month.

Images are saved in:

reports/
network_density_over_time.png
average_degree_over_time.png


---

## 🚀 Running the Pipeline

### **Environment variable for database password**

```powershell
$env:PGPASSWORD = "your_password"
```

Build monthly correlations
python src/build_monthly_correlations.py

Build temporal network metrics
python src/build_network_metrics.py


📚 Skills Demonstrated
🔹 Data Engineering

Clean relational data model

Bulk ingestion with COPY

ETL pipeline design

Automated transformations

🔹 Data Science

Time-series processing

Correlation modeling

Network construction & metrics

Exploratory data analysis

🔹 Software Engineering

Modular Python scripts

Secure credential handling (no passwords in code)

Reproducible directory structure

Clear separation between SQL and Python logic

🧭 Possible Extensions

Add a Streamlit dashboard (interactive network visualizer)

Add community detection (Louvain, Leiden)

Use dynamic graph models (temporal clustering, centrality trends)

Add anomaly detection on correlation structure

📄 License

MIT License (or others depending on your preference)

👤 Author

Leonardo Di Gaetano, PhD
Data Scientist | Network Science | Time-Series Analytics

