# Databricks notebook source
# MAGIC %md ##Scaling-out Single-node Pyhon ML Libraries using Apache Spark
# MAGIC ###1. Scaling-out Exploratory Data Analysis
# MAGIC ####EDA using Pandas

# COMMAND ----------

# MAGIC %md ####Prerequisites
# MAGIC 1. MLFlow - If your are using Databricks, make sure to use Databricks Runtime 7.3 ML or higher. If not using Databricks, make sure to install and configure MLflow in your environment.
# MAGIC 2. Spark-sklearn package - Install databricks:spark-sklearn:0.2.3 using the Library interface within Databricks cluster configuration. https://docs.databricks.com/libraries/workspace-libraries.html#maven-or-spark-package

# COMMAND ----------

import pandas as pd
from sklearn.datasets import load_boston

boston_data = load_boston()
boston_pd = pd.DataFrame(boston_data.data, columns=boston_data.feature_names)
features = boston_data.feature_names
boston_pd['MEDV'] = boston_data.target
boston_pd.info()

# COMMAND ----------

boston_pd.head()

# COMMAND ----------

boston_pd.shape

# COMMAND ----------

boston_pd.isnull().sum()

# COMMAND ----------

boston_pd.describe()

# COMMAND ----------

# MAGIC %md 
# MAGIC ####EDA using PySpak

# COMMAND ----------

boston_df = spark.createDataFrame(boston_pd)
boston_df.display()

# COMMAND ----------

print((boston_df.count(), len(boston_df.columns)))

# COMMAND ----------

boston_df.where(boston_df.AGE.isNull()).count()

# COMMAND ----------

boston_df.describe().display()

# COMMAND ----------

# MAGIC %md ###Scale-out Model inferencing

# COMMAND ----------

import mlflow
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

X = boston_pd[features]
y = boston_pd['MEDV']

with mlflow.start_run() as run1:
  lr = LinearRegression()
  lr_model = lr.fit(X,y)
  mlflow.sklearn.log_model(lr_model, "model")

# COMMAND ----------

print(run1.info)

# COMMAND ----------

import mlflow.pyfunc

model_uri = "runs:/" + run1.info.run_id + "/model"
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

# COMMAND ----------

print(features)

# COMMAND ----------

from pyspark.sql.functions import struct
 
predicted_df = boston_df.withColumn("prediction", pyfunc_udf(struct('CRIM','ZN','INDUS','CHAS','NOX','RM','AGE','DIS','RAD','TAX','PTRATIO', 'B', 'LSTAT')))
predicted_df.display()

# COMMAND ----------

# MAGIC %md ###Embarrassingly Parallel Model Training
# MAGIC ####Distributed Hyperparameter Tuning

# COMMAND ----------

#Standard single-node grid search
from sklearn.datasets import load_digits
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
digits_pd = load_digits()
X = digits_pd.data 
y = digits_pd.target
parameter_grid = {"max_depth": [2, None],
              "max_features": [1, 2, 5],
              "min_samples_split": [1, 2, 5],
              "min_samples_leaf": [2, 3, 5],
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"],
              "n_estimators": [5, 10, 15, 20]}
grid_search = GridSearchCV(RandomForestClassifier(), param_grid=parameter_grid)
model = grid_search.fit(X, y)
model.best_estimator_

# COMMAND ----------

#Grid search in parallel using Spark-sklearn

from sklearn.datasets import load_digits
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

digits_pd = load_digits()
X = digits_pd.data 
y = digits_pd.target
parameter_grid = {"max_depth": [2, None],
              "max_features": [1, 2, 5],
              "min_samples_split": [2, 3, 5],
              "min_samples_leaf": [1, 2, 5],
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"],
              "n_estimators": [5, 10, 15, 20]}
grid_search = GridSearchCV(RandomForestClassifier(), param_grid=parameter_grid)
best_model = grid_search.fit(X, y)

# COMMAND ----------

# MAGIC %md ####Pandas UDF

# COMMAND ----------

# MAGIC %md ###Koalas - Pandas like API for PySpark

# COMMAND ----------

import databricks.koalas as ks

boston_data = load_boston()
features = boston_data.feature_names
boston_pd['MEDV'] = boston_data.target

boston_pd = ks.DataFrame(boston_data.data, columns=boston_data.feature_names)

boston_pd.info()
boston_pd.head()
boston_pd.isnull().sum()
boston_pd.describe()