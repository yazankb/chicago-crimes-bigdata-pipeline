"""Stage III - ML Pipeline: Predictive Data Analytics with Hyperparameter Tuning"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.feature import (StringIndexer, OneHotEncoder,
                                 VectorAssembler, VarianceThresholdSelector)
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                    MulticlassClassificationEvaluator)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


class SinCosTransformer(Transformer, HasInputCol, HasOutputCol,
                        DefaultParamsReadable, DefaultParamsWritable):
    """Custom transformer: encodes cyclical feature into sin+cos components."""

    def __init__(self, inputCol=None, outputCol=None, period=24):
        super().__init__()
        self._setDefault(inputCol=None, outputCol=None)
        if inputCol is not None:
            self._set(inputCol=inputCol)
        if outputCol is not None:
            self._set(outputCol=outputCol)
        self.period = period

    def _transform(self, df):
        inp = self.getInputCol()
        out = self.getOutputCol()
        pi = F.acos(F.lit(0)) * 2
        return (df
                .withColumn(f"{out}_sin",
                            F.sin(F.col(inp) * pi * 2 / self.period))
                .withColumn(f"{out}_cos",
                            F.cos(F.col(inp) * pi * 2 / self.period)))


class GeoToECEFTransformer(Transformer, DefaultParamsReadable,
                           DefaultParamsWritable):
    """Custom transformer: converts (lat, lon) to ECEF (x, y, z) coordinates."""

    def __init__(self, latCol="latitude", lonCol="longitude",
                 outputPrefix="ecef"):
        super().__init__()
        self.latCol = latCol
        self.lonCol = lonCol
        self.outputPrefix = outputPrefix

    def _transform(self, df):
        lat_r = F.radians(F.col(self.latCol).cast(DoubleType()))
        lon_r = F.radians(F.col(self.lonCol).cast(DoubleType()))
        a = 6378137.0
        e_sq = 6.69437999014e-3
        n = a / F.sqrt(F.lit(1.0) - e_sq * F.sin(lat_r) * F.sin(lat_r))
        alt = F.lit(0.0)
        p = self.outputPrefix
        return (df
                .withColumn(f"{p}_x", (n + alt) * F.cos(lat_r) * F.cos(lon_r))
                .withColumn(f"{p}_y", (n + alt) * F.cos(lat_r) * F.sin(lon_r))
                .withColumn(f"{p}_z",
                            (n * (F.lit(1.0) - e_sq) + alt) * F.sin(lat_r)))


def main():
    """Run the full Stage III ML pipeline: feature engineering, training, tuning."""
    # 1. SparkSession with YARN + Hive
    spark = (SparkSession.builder
             .appName("team2 - Stage III ML Pipeline")
             .master("yarn")
             .config("hive.metastore.uris",
                     "thrift://hadoop-02.uni.innopolis.ru:9883")
             .config("spark.sql.warehouse.dir", "project/hive/warehouse")
             .config("spark.sql.avro.compression.codec", "snappy")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 2. Read Hive optimized table
    print("=" * 60)
    print("READING team2_projectdb.crimes_optimized")
    print("=" * 60)
    df = spark.read.table("team2_projectdb.crimes_optimized")
    total = df.count()
    print(f"Total records: {total}")
    df.createOrReplaceTempView("crimes")

    # 3. Feature engineering
    print("\n=== FEATURE ENGINEERING ===")
    feature_df = spark.sql("""
        SELECT
            primary_type, iucr, location_description, beat,
            CAST(district AS INT) AS district,
            CAST(community_area AS INT) AS community_area,
            CAST(x_coordinate AS DOUBLE) AS x_coordinate,
            CAST(y_coordinate AS DOUBLE) AS y_coordinate,
            CAST(latitude AS DOUBLE) AS latitude,
            CAST(longitude AS DOUBLE) AS longitude,
            CASE WHEN domestic = 'true' OR domestic = true
                 THEN 1.0 ELSE 0.0 END AS domestic,
            HOUR(from_unixtime(`date` / 1000)) AS hour_of_day,
            DAYOFWEEK(from_unixtime(`date` / 1000)) AS day_of_week,
            MONTH(from_unixtime(`date` / 1000)) AS month,
            CASE WHEN arrest = true THEN 1.0 ELSE 0.0 END AS label
        FROM crimes
        WHERE `date` IS NOT NULL AND latitude IS NOT NULL
          AND longitude IS NOT NULL
    """)
    # Sample ~1M records for faster prototyping
    total_feat = feature_df.count()
    sample_frac = min(1_000_000 / total_feat, 1.0)
    feature_df = feature_df.sample(False, sample_frac, seed=42)
    feature_df.cache()
    print(f"Feature rows: {total_feat} → sampled: {feature_df.count()}")

    for c in ["primary_type", "iucr", "location_description", "beat"]:
        feature_df = feature_df.withColumn(
            c, F.when(F.col(c).isNull(), "Unknown")
              .otherwise(F.col(c)).cast("string"))
    for c in ["district", "community_area", "x_coordinate", "y_coordinate"]:
        feature_df = feature_df.withColumn(
            c, F.when(F.col(c).isNull(), -1)
              .otherwise(F.col(c)).cast("double"))

    # 4. Build feature pipeline with custom transformers
    print("\n=== BUILDING FEATURE PIPELINE ===")

    hour_enc = SinCosTransformer(inputCol="hour_of_day",
                                  outputCol="hour", period=24)
    day_enc = SinCosTransformer(inputCol="day_of_week",
                                 outputCol="day", period=7)
    month_enc = SinCosTransformer(inputCol="month",
                                   outputCol="month", period=12)
    geo_enc = GeoToECEFTransformer(latCol="latitude", lonCol="longitude")

    cat_cols = ["primary_type", "iucr", "location_description", "beat"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx",
                               handleInvalid="keep") for c in cat_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx",
                               outputCol=f"{c}_ohe") for c in cat_cols]

    numeric_cols = ["district", "community_area",
                    "x_coordinate", "y_coordinate", "domestic"]
    cyclical_cols = ["hour_sin", "hour_cos", "day_sin",
                     "day_cos", "month_sin", "month_cos"]
    ecef_cols = ["ecef_x", "ecef_y", "ecef_z"]
    all_feat = ([f"{c}_ohe" for c in cat_cols]
                + numeric_cols + cyclical_cols + ecef_cols)

    assembler = VectorAssembler(inputCols=all_feat,
                                 outputCol="raw_features",
                                 handleInvalid="keep")
    selector = VarianceThresholdSelector(
        featuresCol="raw_features", outputCol="features",
        varianceThreshold=0.0)

    pipeline = Pipeline(stages=[hour_enc, day_enc, month_enc, geo_enc]
                        + indexers + encoders + [assembler, selector])
    print("Fitting pipeline...")
    pipeline_model = pipeline.fit(feature_df)
    transformed = pipeline_model.transform(feature_df)
    data = transformed.select("features", "label").na.drop()
    n_feat = data.select("features").first()[0].size
    print(f"Final: {data.count()} rows, {n_feat} features")

    # 5. Train / test split
    print("\n=== SPLIT: 80% TRAIN / 20% TEST ===")
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    print(f"Train: {train.count()}, Test: {test.count()}")

    print("Saving to HDFS project/data/ ...")
    (train.select("features", "label").coalesce(1)
     .write.mode("overwrite").format("json").save("project/data/train"))
    (test.select("features", "label").coalesce(1)
     .write.mode("overwrite").format("json").save("project/data/test"))

    # 6. Evaluators
    roc_eval = BinaryClassificationEvaluator(labelCol="label",
                                              metricName="areaUnderROC")
    pr_eval = BinaryClassificationEvaluator(labelCol="label",
                                             metricName="areaUnderPR")
    acc_eval = MulticlassClassificationEvaluator(labelCol="label",
                                                  metricName="accuracy")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label",
                                                 metricName="f1")

    results = []

    # 7. Model 1 — Random Forest + Grid Search + CV
    print("\n" + "=" * 60)
    print("MODEL 1: RANDOM FOREST — GRID SEARCH + 3-FOLD CV")
    print("=" * 60)

    rf = RandomForestClassifier(featuresCol="features", labelCol="label",
                                 seed=42, maxBins=500)
    rf_grid = (ParamGridBuilder()
               .addGrid(rf.numTrees, [20, 50])
               .addGrid(rf.maxDepth, [5, 10])
               .build())
    rf_cv = CrossValidator(estimator=rf, estimatorParamMaps=rf_grid,
                            evaluator=pr_eval, parallelism=3,
                            numFolds=3, seed=42)

    print("Training CV (27 runs)...")
    rf_cv_model = rf_cv.fit(train)
    rf_best = rf_cv_model.bestModel
    print(f"Best: numTrees={rf_best.getNumTrees}, "
          f"maxDepth={rf_best.getOrDefault('maxDepth')}")

    rf_pred = rf_best.transform(test)
    rf_roc = roc_eval.evaluate(rf_pred)
    rf_pr = pr_eval.evaluate(rf_pred)
    rf_acc = acc_eval.evaluate(rf_pred)
    rf_f1 = f1_eval.evaluate(rf_pred)
    print(f"Test: AUC-ROC={rf_roc:.4f}  AUC-PR={rf_pr:.4f}  "
          f"Acc={rf_acc:.4f}  F1={rf_f1:.4f}")

    rf_best.write().overwrite().save("project/models/model1")
    print("Saved: project/models/model1")

    (rf_pred.select("label", "prediction").coalesce(1)
     .write.mode("overwrite").format("csv")
     .option("sep", ",").option("header", "true")
     .save("project/output/model1_predictions"))
    print("Saved: project/output/model1_predictions")
    results.append(("Random Forest (tuned)", rf_roc, rf_pr, rf_acc, rf_f1))

    # 8. Model 2 — GBT + Grid Search + CV
    print("\n" + "=" * 60)
    print("MODEL 2: GRADIENT BOOSTED TREES — GRID SEARCH + 3-FOLD CV")
    print("=" * 60)

    gbt = GBTClassifier(featuresCol="features", labelCol="label",
                         seed=42, maxBins=500)
    gbt_grid = (ParamGridBuilder()
                .addGrid(gbt.maxDepth, [3, 6])
                .addGrid(gbt.stepSize, [0.05, 0.1])
                .build())
    gbt_cv = CrossValidator(estimator=gbt, estimatorParamMaps=gbt_grid,
                             evaluator=pr_eval, parallelism=3,
                             numFolds=3, seed=42)

    print("Training CV (27 runs)...")
    gbt_cv_model = gbt_cv.fit(train)
    gbt_best = gbt_cv_model.bestModel
    print(f"Best: maxDepth={gbt_best.getOrDefault('maxDepth')}, "
          f"stepSize={gbt_best.getOrDefault('stepSize'):.3f}")

    gbt_pred = gbt_best.transform(test)
    gbt_roc = roc_eval.evaluate(gbt_pred)
    gbt_pr = pr_eval.evaluate(gbt_pred)
    gbt_acc = acc_eval.evaluate(gbt_pred)
    gbt_f1 = f1_eval.evaluate(gbt_pred)
    print(f"Test: AUC-ROC={gbt_roc:.4f}  AUC-PR={gbt_pr:.4f}  "
          f"Acc={gbt_acc:.4f}  F1={gbt_f1:.4f}")

    gbt_best.write().overwrite().save("project/models/model2")
    print("Saved: project/models/model2")

    (gbt_pred.select("label", "prediction").coalesce(1)
     .write.mode("overwrite").format("csv")
     .option("sep", ",").option("header", "true")
     .save("project/output/model2_predictions"))
    print("Saved: project/output/model2_predictions")
    results.append(("Gradient Boosted Trees (tuned)",
                    gbt_roc, gbt_pr, gbt_acc, gbt_f1))

    # 9. Model comparison
    print("\n" + "=" * 60)
    print("MODEL COMPARISON")
    print("=" * 60)
    header = f"{'Model':<35} {'AUC-ROC':<10} {'AUC-PR':<10} "
    header += f"{'Accuracy':<10} {'F1':<10}"
    print(header)
    print("-" * 75)
    for name, roc, pr, acc, f1 in results:
        print(f"{name:<35} {roc:<10.4f} {pr:<10.4f} "
              f"{acc:<10.4f} {f1:<10.4f}")

    best_i = max(range(len(results)), key=lambda i: results[i][2])
    print(f"\n*** Best: {results[best_i][0]} "
          f"(AUC-PR={results[best_i][2]:.4f}) ***")

    rows = [(n.replace(" (tuned)", ""),
             float(r), float(p), float(a), float(f))
            for n, r, p, a, f in results]
    comp = spark.createDataFrame(
        rows, ["model", "auc_roc", "auc_pr", "accuracy", "f1"])
    (comp.coalesce(1).write.mode("overwrite").format("csv")
     .option("sep", ",").option("header", "true")
     .save("project/output/evaluation"))
    print("Saved: project/output/evaluation")

    # 10. Create Hive external tables on pipeline outputs
    print("\n=== CREATING HIVE EXTERNAL TABLES ===")
    spark.sql("USE team2_projectdb")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ml_evaluation (
            model STRING, auc_roc DOUBLE, auc_pr DOUBLE,
            accuracy DOUBLE, f1 DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar'=',', 'quoteChar'='"', 'escapeChar'='\\\\'
        )
        STORED AS TEXTFILE
        LOCATION '/user/team2/project/output/evaluation'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """)
    print("  [x] ml_evaluation")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ml_predictions_rf (
            label DOUBLE, prediction DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar'=',', 'quoteChar'='"', 'escapeChar'='\\\\'
        )
        STORED AS TEXTFILE
        LOCATION '/user/team2/project/output/model1_predictions'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """)
    print("  [x] ml_predictions_rf")

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ml_predictions_gbt (
            label DOUBLE, prediction DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar'=',', 'quoteChar'='"', 'escapeChar'='\\\\'
        )
        STORED AS TEXTFILE
        LOCATION '/user/team2/project/output/model2_predictions'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """)
    print("  [x] ml_predictions_gbt")

    # Verify
    tbls = spark.sql("SHOW TABLES IN team2_projectdb")
    ml_tbls = [r.tableName for r in tbls.select("tableName").collect()
               if "ml_" in r.tableName]
    print(f"  Hive ML tables: {ml_tbls}")

    feature_df.unpersist()
    print("\n" + "=" * 60)
    print("STAGE III ML PIPELINE COMPLETE")
    print("=" * 60)
    print("HDFS outputs:")
    print("  data/train, data/test")
    print("  models/model1, models/model2")
    print("  output/model1_predictions")
    print("  output/model2_predictions")
    print("  output/evaluation")
    print("Hive tables:")
    print("  ml_evaluation, ml_predictions_rf, ml_predictions_gbt")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
