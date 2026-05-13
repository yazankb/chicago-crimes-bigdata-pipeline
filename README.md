# Big Data Project — Team 2
## Chicago Crimes Analysis Pipeline (2001–Present)

**Team Members:**
- **Yazan Kbaili** (50%) — Data ingestion, Hive optimizations, ML pipeline, dashboard deployment, report
- **Ali Salloum** (50%) — PostgreSQL schema, EDA, hyperparameter tuning, Superset charts, presentation

---

## Overview

End-to-end big data analytics pipeline analyzing **8.5 million Chicago crime records** (2001–2026) to predict arrest likelihood. Built using the CRISP-DM methodology on the IU Hadoop cluster with HDFS, Hive, Spark, and Superset.

**Final result:** Gradient Boosted Trees model with **AUC-PR 0.8176, F1 0.8710** — deployed in a 3-tab Superset dashboard.

## Dataset

| Attribute | Value |
|-----------|-------|
| Source | [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) |
| Records | 8,549,662 |
| Columns | 22 |
| CSV Size | ~1.9 GB |
| HDFS (Parquet+Snappy) | 509 MB |
| Target | `arrest` (Boolean — 25% TRUE, 75% FALSE) |
| Time Span | 2001–2026 |
| Geospatial | latitude, longitude (24 districts, 77 community areas) |

## Pipeline Architecture

```
Socrata API / CSV
    │
    ▼
PostgreSQL (Staging)
    │  • Schema validation, bulk COPY, indexes
    ▼
Sqoop (Parquet+Snappy, 4 mappers, 54s)
    │
    ▼
HDFS → Hive (team2_projectdb)
    │  • crimes (external, Parquet)
    │  • crimes_optimized (partitioned by year, bucketed by district)
    │
    ▼
PySpark ML Pipeline (YARN)
    │  • SinCosTransformer (cyclical time encoding)
    │  • GeoToECEFTransformer (WGS84 geospatial encoding)
    │  • StringIndexer + OHE (categoricals)
    │  • VarianceThresholdSelector
    │  • RF vs GBT with GridSearch + 3-fold CV
    │
    ▼
Superset Dashboard (3 tabs)
    • Data Description | EDA Insights | ML Modeling
```

## Project Stages

| Stage | Description | Key Deliverables |
|-------|-------------|------------------|
| **I** | Data Collection & Storage | Socrata API download, PostgreSQL staging, Sqoop import (Parquet+Snappy: 509 MB, 54s), Hive tables |
| **II** | Hive Optimizations & EDA | Partitioned+bucketed table (26 partitions, 22 buckets), 7 EDA insights, initial Superset dashboard |
| **III** | ML Pipeline | Custom PySpark transformers, RF + GBT training with hyperparameter tuning, model evaluation |
| **IV** | Presentation & Delivery | Full Superset dashboard with 3 tabs, risk assessment, final report, pitch presentation |

## ML Model Results

| Model | AUC-ROC | AUC-PR | Accuracy | F1 |
|-------|---------|--------|----------|-----|
| Random Forest (tuned) | 0.8738 | 0.7968 | 0.8492 | 0.8256 |
| **GBT (tuned)** | **0.8844** | **0.8176** | **0.8809** | **0.8710** |

**Winner:** Gradient Boosted Trees (`maxDepth=6`, `stepSize=0.1`)

## EDA Insights

1. **Arrest rate by district**: 2.25× variation — District 11 highest (40.89%), District 16 lowest (18.19%)
2. **Crime types**: THEFT (1.82M) and BATTERY (1.56M) dominate — top 5 types = 65% of incidents
3. **Hourly pattern**: Peaks at 12:00 (490K) and 18:00; lowest at 5 AM (120K)
4. **Arrest rate trend**: Declining from 29.21% (2001) to ~12–16% (2020s)
5. **Monthly volume**: Stable year-round (~10% variation); Feb lowest, May/Dec highest
6. **Community areas**: Arrest rate ranges from ~10% to 36.68% (Area 23)
7. **Domestic vs non-domestic**: Non-domestic (26.32%) > domestic (19.22%)

## Quick Start

### Prerequisites
- IU Hadoop Cluster (`hadoop-01.uni.innopolis.ru`)
- PostgreSQL client tools + `psycopg2`
- Sqoop, Hive, Spark 3 on cluster
- Python 3.6 + PySpark

### Run Everything

```bash
bash main.sh
```

### Or Run Individual Stages

```bash
# Stage I: Data collection & ingestion
bash scripts/data_collection.sh
python3 scripts/build_db.py
bash scripts/sqoop_import.sh
beeline -n team2 -p 'V2P1hy6zjPqWoXMm' -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/" -f sql/hive_create.sql
bash scripts/verify.sh

# Stage II: EDA
bash scripts/stage2.sh

# Stage III: ML Pipeline (spark-submit on YARN)
bash scripts/stage3.sh

# Stage IV: Dashboard data loading
bash scripts/stage4.sh
```

## Repository Structure

```
chicago-crimes-bigdata-pipeline/
├── main.sh                 # Master orchestrator
├── scripts/                # All pipeline scripts (bash + Python)
├── sql/                    # DDL and queries (PostgreSQL + Hive)
├── notebooks/              # Jupyter notebooks (EDA + ML)
├── docs/                   # Reports and documentation
│   ├── MS_Final_Report.md  # Comprehensive final report
│   ├── latex_report/       # LaTeX source for compiled report
│   └── MS_Stage*_Report.md # Per-stage reports
├── output/                 # Results (CSVs, charts, model predictions)
├── models/                 # Saved ML models
├── presentation/           # Pitch presentation
├── data/                   # Raw data (not tracked in git)
└── secrets/                # Credentials (not tracked in git)
```

## Superset Dashboard

Available at `http://hadoop-03.uni.innopolis.ru:8808` (3 tabs):

| Tab | Content |
|-----|---------|
| Data Description | Dataset overview, schema, class balance, data cleaning |
| EDA Insights | 7 charts with conclusions from exploratory analysis |
| ML Modeling | Feature pipeline, grid search params, model comparison, confusion matrices |

## Risk Assessment

Three synthetic scenarios tested: Optimistic (40% arrest rate), Pessimistic (10%), and Shifted (distribution change). GBT remains robust across all scenarios. Mitigation strategies include annual retraining, drift monitoring, and human-in-the-loop review.

## Tech Stack

| Component | Tool |
|-----------|------|
| Data Source | Socrata SODA API |
| Staging | PostgreSQL 14 |
| Ingestion | Apache Sqoop |
| Storage | HDFS + Parquet + Snappy |
| Warehouse | Apache Hive (partitioned + bucketed) |
| Compute | Apache Spark 3 on YARN |
| Visualization | Apache Superset |
| Language | Python 3.6, PySpark, bash |

## Deliverables

- [x] Public GitHub repository with `main.sh` orchestrator
- [x] Comprehensive CRISP-DM report (LaTeX + PDF)
- [x] Superset dashboard (3 tabs)
- [x] Pitch presentation (PDF)
- [x] Presentation recording

## Business Context

**Problem:** City agencies need data-driven prioritization of patrol and response resources. Manual analysis of 8.5M+ crime records is infeasible.

**Objective:** Predict arrest likelihood from crime incident attributes to enable proactive resource deployment and improve clearance rates.

**Output:** Interactive Superset dashboard and a tuned GBT model (AUC-PR 0.8176) for operational decision support.

---

*Project completed: May 2026*
