# Stage III Report — Predictive Data Analytics

**Team:** Team 2 — Chicago Crimes Pipeline
**Authors:** Yazan Kbaili, Ali Salloum
**Date:** May 2026

## 1. Overview

Stage III implements a full ML pipeline for predicting arrest likelihood using PySpark on the Hadoop YARN cluster. The pipeline reads from the optimized Hive table (`team2_projectdb.crimes_optimized`, partitioned by year, bucketed by district), engineers features using custom transformers, and trains two classification models with hyperparameter tuning via grid search and cross-validation.

## 2. ML Pipeline Architecture

```
Hive (crimes_optimized, 8.5M rows)
  │
  ▼
Feature Engineering (SQL)
  │  • Extract hour, day_of_week, month from epoch timestamp
  │  • Cast coordinates to double
  │  • Handle nulls in categorical/numerical columns
  │
  ▼
Custom Transformers (pyspark.ml)
  │  • SinCosTransformer (hour period=24, day period=7, month period=12)
  │  • GeoToECEFTransformer (lat/lon → ECEF x,y,z)
  │
  ▼
Categorical Encoding
  │  • StringIndexer + OneHotEncoder for: primary_type, iucr,
  │    location_description, beat
  │
  ▼
VectorAssembler → VarianceThresholdSelector
  │
  ▼
Train/Test Split (80/20) → JSON to HDFS
  │
  ├── Model 1: Random Forest
  │     ParamGrid: numTrees [20, 50], maxDepth [5, 10]
  │     CrossValidator: 3 folds, metric = AUC-PR
  │     → Best model → predict → evaluate → save
  │
  └── Model 2: GBT
        ParamGrid: maxDepth [3, 6], stepSize [0.05, 0.1]
        CrossValidator: 3 folds, metric = AUC-PR
        → Best model → predict → evaluate → save

Model Comparison → evaluation.csv
```

## 3. Feature Engineering Details

### 3.1 Cyclical Time Encoding (SinCosTransformer)

Custom `pyspark.ml.Transformer` that encodes cyclical features into two components:

| Feature | Period | Output Columns |
|---------|--------|----------------|
| hour_of_day | 24 | hour_sin, hour_cos |
| day_of_week | 7 | day_sin, day_cos |
| month | 12 | month_sin, month_cos |

Formula: `sin(2π × value / period)`, `cos(2π × value / period)`

### 3.2 Geospatial Encoding (GeoToECEFTransformer)

Custom `pyspark.ml.Transformer` that converts geodetic coordinates (latitude, longitude) to Earth-Centered Earth-Fixed (ECEF) coordinates (x, y, z) using WGS84 ellipsoid parameters:

- Semi-major axis (a): 6,378,137 m
- First eccentricity squared (e²): 6.69437999014 × 10⁻³
- Altitude: 0 (mean sea level)

Output: `ecef_x`, `ecef_y`, `ecef_z` (in meters)

### 3.3 Feature Pipeline Stages

1. SinCosTransformer × 3 (hour, day_of_week, month)
2. GeoToECEFTransformer (lat, lon → ecef_x, ecef_y, ecef_z)
3. StringIndexer × 4 (primary_type, iucr, location_description, beat)
4. OneHotEncoder × 4
5. VectorAssembler (all features → raw_features vector)
6. VarianceThresholdSelector (removes zero-variance features)

Final feature vector includes:
- OHE-encoded categoricals (4 groups)
- Numerical: district, community_area, x_coordinate, y_coordinate, domestic
- Cyclical sin/cos: 6 columns
- ECEF: 3 columns

## 4. Hyperparameter Tuning Methodology

### 4.1 Random Forest

| Hyperparameter | Values Tested | Description |
|----------------|---------------|-------------|
| numTrees | [20, 50] | Number of trees in the forest |
| maxDepth | [5, 10] | Maximum depth of each tree |

- Grid size: 2 × 2 = 4 combinations
- Cross-validation: 3 folds
- Total training runs: 12
- Optimization metric: AUC-PR

### 4.2 Gradient Boosted Trees

| Hyperparameter | Values Tested | Description |
|----------------|---------------|-------------|
| maxDepth | [3, 6] | Maximum depth of each tree |
| stepSize | [0.05, 0.1] | Learning rate for each boosting iteration |

- Grid size: 2 × 2 = 4 combinations
- Cross-validation: 3 folds
- Total training runs: 12
- Optimization metric: AUC-PR

### 4.3 Why AUC-PR?

The dataset is imbalanced (25% arrest rate). AUC-PR is more informative than AUC-ROC for imbalanced classification because it focuses on the positive class (arrests) and does not inflate scores due to true negatives.

## 5. Results

### Model Comparison

| Model | AUC-ROC | AUC-PR | Accuracy | F1 |
|-------|---------|--------|----------|-----|
| Random Forest (tuned: numTrees=50, maxDepth=10) | 0.8738 | 0.7968 | 0.8492 | 0.8256 |
| GBT (tuned: maxDepth=6, stepSize=0.1) | **0.8844** | **0.8176** | **0.8809** | **0.8710** |

### Best Model

**Gradient Boosted Trees** is the winner:
- AUC-PR (0.8176) — highest precision-recall for imbalanced data
- F1 (0.8710) — best precision/recall balance
- AUC-ROC (0.8844) — strong overall classification
- Optimal hyperparameters: maxDepth=6, stepSize=0.1

### Comparison with Stage II Pilot

| Metric | Stage II (10%, no tuning) | Stage III (1M, tuned) | Change |
|--------|--------------------------|----------------------|--------|
| RF AUC-PR | 0.8352 | 0.7968 | −0.0384 |
| GBT AUC-PR | 0.8418 | 0.8176 | −0.0242 |
| RF F1 | 0.8742 | 0.8256 | −0.0486 |
| GBT F1 | 0.8817 | 0.8710 | −0.0107 |

The slight decrease from Stage II pilot is expected: the stage II model was evaluated on a smaller, potentially less diverse 10% sample (~846K records). The Stage III model uses a different 1M sample with more varied data, making the task harder. Importantly, GBT degrades less than RF when scaling to more diverse data, reinforcing it as the better model.

## 6. Outputs

### HDFS / Local

| Artifact | HDFS Path | Local Path |
|----------|-----------|------------|
| Train data | `project/data/train` | `data/train.json` |
| Test data | `project/data/test` | `data/test.json` |
| RF model | `project/models/model1` | `models/model1` |
| GBT model | `project/models/model2` | `models/model2` |
| RF predictions | `project/output/model1_predictions` | `output/model1_predictions.csv` |
| GBT predictions | `project/output/model2_predictions` | `output/model2_predictions.csv` |
| Comparison | `project/output/evaluation` | `output/evaluation.csv` |

### Hive Tables (created by pipeline for Stage IV dashboard)

| Hive Table | Type | Source |
|------------|------|--------|
| `ml_evaluation` | External (CSV) | `project/output/evaluation` |
| `ml_predictions_rf` | External (CSV) | `project/output/model1_predictions` |
| `ml_predictions_gbt` | External (CSV) | `project/output/model2_predictions` |
| `ml_features` | Managed | Static INSERT (feature groups) |
| `ml_gridsearch` | Managed | Static INSERT (grid search params) |

All tables are in the `team2_projectdb` Hive database and queryable via HiveServer2 or Superset/Hive connector.

## 7. Automation

All steps are automated via `scripts/stage3.sh`:
1. Runs `spark-submit --master yarn scripts/ml_pipeline.py`
2. Copies HDFS outputs to local repository
3. Runs `pylint` quality check

## 8. Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Spark master | yarn | Required by project spec; distributed execution on cluster |
| Data source | Hive `crimes_optimized` | Uses existing partitioned/bucketed table from Stage II |
| Models | RF + GBT | Best performers in Stage II (LR dropped due to poor AUC-PR 0.38) |
| Custom transformers | SinCos + ECEF | Required by project spec for cyclical and geospatial features |
| CV folds | 3 | Balances evaluation quality with runtime on 8.5M records |
| Parallelism | 3 | Matches numFolds for efficient CV execution |
| Optimization metric | AUC-PR | Best for imbalanced dataset (25% positive class) |
