# Stage IV Report — Presentation & Delivery

**Team:** Team 2 — Chicago Crimes Pipeline
**Authors:** Yazan Kbaili, Ali Salloum
**Date:** May 2026

## 1. Overview

Stage IV creates a comprehensive Apache Superset dashboard that presents the full project lifecycle: data description, EDA insights from Stage II, and ML modeling results from Stage III. The dashboard tells a cohesive story about crime patterns in Chicago and arrest prediction modeling.

## 2. Data Infrastructure

Hive tables (`ml_evaluation`, `ml_predictions_rf`, `ml_predictions_gbt`, `ml_features`, `ml_gridsearch`) were already created by the Stage III pipeline. Stage IV only handles PostgreSQL loading for Superset charting.

### 2.1 PostgreSQL Tables

Five new tables were loaded into `team2_projectdb` on PostgreSQL:

| Table | Rows | Purpose |
|-------|------|---------|
| `ml_evaluation` | 2 | Model comparison metrics |
| `dataset_info` | 5 | Record counts for all project tables |
| `schema_info` | 22 | Column names and data types of `crimes` table |
| `ml_features` | 4 | Feature extraction pipeline summary |
| `ml_gridsearch` | 4 | Hyperparameter tuning grid search params |

Loaded via `scripts/load_postgres.py` (psycopg2) since `psql` is not available on the cluster.

## 3. Superset Dashboard

### 3.1 Dashboard Structure

The dashboard "Chicago Crimes Analysis" is organized into three tabs:

**Tab 1: Data Description**
- Dataset overview — record counts per table
- Column schema — data types of all 22 columns
- Data samples — raw crime records preview
- Data cleaning summary — null handling, type casting

**Tab 2: EDA Insights**
- 7 charts from Stage II (q1–q7) with conclusions:
  - Arrest rate by district (Q1): District 11 highest (40.89%)
  - Top 15 crime types (Q2): THEFT (1.82M), BATTERY (1.56M)
  - Crimes by hour (Q3): Peaks at 12:00, 18:00
  - Arrest rate trend (Q4): 29.21% → ~12-16% declining
  - Crimes by month (Q5): Seasonal summer peaks
  - Arrest by community area (Q6): Wide variation across 77 areas
  - Domestic vs non-domestic (Q7): Non-domestic (26.32%) > domestic (19.22%)

**Tab 3: ML Modeling**
- Feature pipeline overview — 4 feature groups (categorical, numerical, cyclical, geospatial)
- Grid search hyperparameters — RF (numTrees, maxDepth) and GBT (maxDepth, stepSize)
- Model comparison bar chart — AUC-ROC, AUC-PR, Accuracy, F1
- Confusion matrices — RF vs GBT prediction distributions
- Best model: **GBT** (AUC-PR=0.8176, F1=0.8710)

### 3.2 Publishing

The dashboard is published from the Superset UI (Dashboard > ⋮ > Publish), making it visible to all users.

## 4. Automation

| Script | Purpose |
|--------|---------|
| `scripts/stage4.sh` | Orchestrator — loads PostgreSQL, runs pylint (Hive tables from Stage III) |
| `scripts/load_postgres.py` | Python psycopg2 loader for PostgreSQL tables |

Manual steps (Superset UI) are documented in the script output.

## 5. Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Dashboard tool | Apache Superset | Required by project; already deployed on cluster |
| Hive tables | Created by Stage III | Pipeline owns data output; Stage IV only consumes |
| Data source for ML charts | Hive + PostgreSQL | Hive for ML data (Stage III outputs), PostgreSQL for data description |
| PostgreSQL loader | psycopg2 (Python) | psql not available on cluster; psycopg2 available |
| Dashboard structure | 3 tabs | Logical separation: data → insights → predictions |

## 6. Outputs

| Artifact | Path |
|----------|------|
| Dashboard screenshot | `output/dashboard_stage4.jpg` |
| Stage IV automation | `scripts/stage4.sh` |
| PostgreSQL loader | `scripts/load_postgres.py` |
| Stage IV checklist | `CHECKLIST_STAGE4.md` |
| Stage IV report | `docs/MS_Stage4_Report.md` |
