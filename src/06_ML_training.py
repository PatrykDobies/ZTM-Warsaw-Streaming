import mlflow
import mlflow.spark
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

storage_account_name = "ztmstreaming"

# Load from secret
try:
    storage_account_key = dbutils.secrets.get(scope="ztm-scope", key="adls-key")
    print("ADLS Key retrieved successfully.")
except Exception as e:
    raise e

# Configurate access to Data Lake Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Path to Gold Layer
gold_spatial_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/gold/spatial_heatmap"

# Loading data from gold layer
df = spark.read.format("delta").load(gold_spatial_path)

ml_data = df.select(
    col("grid_lat").cast("double"),
    col("grid_lon").cast("double"),
    col("hour_of_day").cast("integer"),
    col("traffic_density").cast("double").alias("label")
) \
    .filter(col("traffic_density") < 50) # reject bus depots to reduce RMSE

# Feature engineering
assembler = VectorAssembler(
    inputCols=["grid_lat", "grid_lon", "hour_of_day"],
    outputCol="features"
)

# Split data strategy (80/20 - K-Fold Cross-Validation)
(train_data, test_data) = ml_data.randomSplit([0.8, 0.2], seed=42)

print(f"Total Records: {ml_data.count()}")
print(f"Training Set: {train_data.count()} | Test Set: {test_data.count()}")

# MLOps setup
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
experiment_name = f"/Users/{current_user}/ZTM_Traffic_Prediction_MLOps"
mlflow.set_experiment(experiment_name)

# Training and Tuning
with mlflow.start_run(run_name="RandomForest_GridSearch_CV"):
    
    print("Initializing Pipeline...")
    
    rf = RandomForestRegressor(featuresCol="features", labelCol="label")
    
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Hyperparameter Grid for Tuning
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxDepth, [5, 10]) \
        .build()

    # K-Fold Cross-Validation (3 Folds)
    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=RegressionEvaluator(labelCol="label", metricName="rmse"),
                              numFolds=3)

    print("Training with 3-Fold Cross-Validation...")
    cv_model = crossval.fit(train_data)
    
    # Evaluation
    print("Evaluating best model")
    best_model = cv_model.bestModel
    predictions = best_model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"RMSE: {rmse:.4f}, R2: {r2:.4f}")
    
    # Logging
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    
    best_rf_stage = best_model.stages[-1]
    mlflow.log_param("best_numTrees", best_rf_stage.getNumTrees)
    
    mlflow.spark.log_model(best_model, "traffic_density_model_tuned")
    print("Model logged to MLflow.")

# Preview
print("Sample Predictions:")
display(predictions.select("hour_of_day", "grid_lat", "grid_lon", "label", "prediction").limit(10))
