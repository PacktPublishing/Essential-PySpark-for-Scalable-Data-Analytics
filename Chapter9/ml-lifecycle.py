# Databricks notebook source
# MAGIC %md
# MAGIC ##Machine Learning Lifecycle Management using MLflow
# MAGIC ###1. MLflow Experiment Tracking
# MAGIC **Note:** Model Registry functionality is not available in Databricks Community Edition.<br>
# MAGIC Either use the paid version of Databricks or st-up your own open-source MLFlow with Model Registry outside of Databricks environment, by following instructions given here: https://www.mlflow.org/docs/latest/tracking.html#backend-stores

# COMMAND ----------

# MAGIC %md ####Linear Regression with Corssvalidation

# COMMAND ----------

# MAGIC %sql USE feature_store

# COMMAND ----------

# MAGIC %md Make sure your local mlflow server is running

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

mlflow.set_tracking_uri("databricks")
print(mlflow.tracking.get_tracking_uri())

mlflow.set_experiment("/Users/snudurupati@outlook.com/linregexp")
experiment = mlflow.get_experiment_by_name("/Users/snudurupati@outlook.com/linregexp")
print("Experiment Id: {}".format(experiment.experiment_id))
print("Artifact Location: {}".format(experiment.artifact_location))

retail_features = spark.read.table("retail_features")
retail_df = retail_features.selectExpr("cust_age as label", "selected_features as features")
train_df, test_df = retail_df.randomSplit([0.9, 0.1])

evaluator = RegressionEvaluator(labelCol="label", metricName="rmse")

lr = LinearRegression(maxIter=10)
param_grid = (ParamGridBuilder()
    .addGrid(lr.regParam, [0.1, 0.01]) 
    .addGrid(lr.fitIntercept, [False, True])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .build())

csv = CrossValidator(estimator=lr,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(),
                          numFolds=2)

with mlflow.start_run() as active_run:
  lr_model = csv.fit(train_df)
  test_metric = evaluator.evaluate(lr_model.transform(test_df))
  mlflow.log_metric(evaluator.getMetricName(), test_metric) 
  mlflow.spark.log_model(spark_model=lr_model.bestModel, artifact_path='best-model') 

(lr_model.transform(test_df)
    .select("features", "label", "prediction")
    .display())

# COMMAND ----------

# MAGIC %md ###2. Registering Model with Model Registry, a central model repository

# COMMAND ----------

run_id.info

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_name = "linear-regression-model"
artifact_path = "best_model"
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)
registered_model = mlflow.register_model(model_uri=model_uri, name=model_name, await_registration_for=120)

#Add model and model version descriptions to Model Registry
client.update_model_version(
  name=registered_model.name,
  version=registered_model.version,
  description="This predicts the age of a customer using transaction history."
)

#Transition a model version to Staging/Prod/Archived
client.transition_model_version_stage(
  name=registered_model.name,
  version=registered_model.version,
  stage='Staging',
)

#Get model version details
model_version = client.get_model_version(
  name=registered_model.name,
  version=registered_model.version,
)

#Load a specific model version from the Model Registry
model_uri = "models:/{model_name}/staging".format(model_name=model_name)

spark_model = mlflow.spark.load_model(model_uri)

# COMMAND ----------

# MAGIC %md ##3. ML Model Serving and Inferencing
# MAGIC ###Offline Inferencing

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct

# Load model as a Spark UDF.
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=active_run.info.run_id, artifact_path="best-model")
spark_udf = mlflow.pyfunc.spark_udf(spark, model_uri)

predictions_df = retail_df.withColumn("predictions", spark_udf(struct("features")))
predictions_df.write.format(“delta”).save(“/tmp/retail_predictions”)
