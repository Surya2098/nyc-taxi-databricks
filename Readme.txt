# 🚖 NYC Taxi Trip Analysis with PySpark on Databricks

This project showcases a scalable big data analysis pipeline for NYC Yellow Taxi trip data using **PySpark** on **Databricks Community Edition**.

> 📂 Dataset: `yellow-tripdata-2025-02.csv`  
> 💻 Platform: Databricks Community Edition  
> 🧰 Tech Stack: PySpark, Spark SQL, Delta Lake, Parquet

---

## 📌 Project Objectives

- ✅ Ingest millions of trip records efficiently using PySpark.
- ✅ Engineer meaningful features like trip duration and rush hour flags.
- ✅ Perform group-based aggregations on key trip metrics.
- ✅ Optimize queries through partitioning, filter pushdown, and caching.
- ✅ Save processed data in **Parquet** and **Delta Lake** formats for downstream use.

---

## 📊 Key Features

### 1. Feature Engineering
- Trip duration calculated from pickup & drop-off timestamps.
- Rush hour flag (7–9 AM, 4–6 PM) for traffic analysis.

### 2. Aggregations
- Grouped by `VendorID` and `rush_hour` flag.
- Metrics include:
  - Average trip distance
  - Average trip duration
  - Average passenger count

### 3. Performance Optimization
- Data **repartitioned** on `VendorID`
- **In-memory caching** enabled
- Efficient **filter pushdown** for fast queries

### 4. Storage Optimization
- Saved output to **Parquet** and **Delta Lake** formats
- Registered Delta Table to run SQL analytics

---

## 🛠️ Technologies Used

| Tool | Purpose |
|------|---------|
| **Databricks CE** | Interactive Spark development |
| **PySpark** | Distributed data processing |
| **Spark SQL** | Querying structured data |
| **Delta Lake** | Transactional big data storage |
| **Parquet** | Columnar storage format |

---

## 🚀 How to Run This Project

1. Sign in to [Databricks Community Edition](https://community.cloud.databricks.com/)
2. Upload the dataset `yellow-tripdata-2025-02.csv` to `FileStore`
3. Import the notebook file (`nyc_taxi_project.py`)
4. Follow each code block to:
   - Load the data
   - Perform feature engineering
   - Run aggregations
   - Save as Parquet & Delta formats
5. Optional: Register Delta table and run SQL queries

---

## 📂 File Structure

```bash
nyc-taxi-databricks/
│
├── nyc_taxi_project.py        # Databricks notebook (exported as source)
├── README.md                  # You're reading it!
└── yellow-tripdata-2025-02.csv # Input dataset (should be uploaded in Databricks)
