# ML Model Comparison Report — Chicago Crimes Arrest Prediction

## Problem
Predict arrest likelihood (binary: TRUE/FALSE) from crime context features.
Dataset: 8.5M records, ~25% arrest rate (imbalanced).

## Stage II — Pilot Results (10% sample, no tuning)

| Model | AUC-ROC | AUC-PR | Accuracy | F1-Score |
|-------|---------|--------|----------|----------|
| Logistic Regression | 0.6529 | 0.3813 | 0.7491 | 0.6666 |
| Random Forest (50 trees) | 0.9031 | 0.8352 | 0.8839 | 0.8742 |
| GBT Classifier (30 iters) | **0.9069** | **0.8418** | **0.8891** | **0.8817** |

## Stage III — Final Results (1M sampled, hyperparameter tuning on YARN)

| Model | AUC-ROC | AUC-PR | Accuracy | F1-Score |
|-------|---------|--------|----------|----------|
| Random Forest (tuned: numTrees=50, maxDepth=10) | 0.8738 | 0.7968 | 0.8492 | 0.8256 |
| Gradient Boosted Trees (tuned: maxDepth=6, stepSize=0.1) | **0.8844** | **0.8176** | **0.8809** | **0.8710** |

## Winner: GBT Classifier
- **AUC-PR (0.8176)**: The priority metric for imbalanced data
- **F1 (0.8710)**: Best precision-recall tradeoff
- **AUC-ROC (0.8844)**: Strong ranking ability
- **Best hyperparams**: maxDepth=6, stepSize=0.1

## Key Improvements in Stage III
- **YARN distributed mode** — runs on cluster (not local)
- **Hive table source** — reads `team2_projectdb.crimes_optimized`
- **Custom SinCosTransformer** for cyclical time features (hour, day_of_week, month)
- **Custom GeoToECEFTransformer** for geospatial encoding (lat/lon → ECEF x,y,z)
- **Hyperparameter tuning** via ParamGridBuilder + 3-fold CrossValidator
  - RF: numTrees [20, 50], maxDepth [5, 10]
  - GBT: maxDepth [3, 6], stepSize [0.05, 0.1]
- **Feature selection** via VarianceThresholdSelector
- **Optimization metric**: AUC-PR (best for imbalanced data)
