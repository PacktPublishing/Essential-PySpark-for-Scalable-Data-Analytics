# Databricks notebook source
# MAGIC %md ###Supervised Learning
# MAGIC 1. ####Regression
# MAGIC #####Linear Regression

# COMMAND ----------

# MAGIC %sql 
# MAGIC use feature_store;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql select * from retail_features

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

retail_features = spark.read.table("retail_features")
train_df = retail_features.selectExpr("cust_age as label", "selected_features as features")

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

lr_model = lr.fit(train_df)

print("Coefficients: %s" % str(lr_model.coefficients))
print("Intercept: %s" % str(lr_model.intercept))

summary = lr_model.summary
print("RMSE: %f" % summary.rootMeanSquaredError)
print("r2: %f" % summary.r2)

# COMMAND ----------

# MAGIC %md #####Decision Tree Regression

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor

retail_features = spark.read.table("retail_features").selectExpr("cust_age as label", "selected_features as features")

(train_df, test_df) = retail_features.randomSplit([0.8, 0.2])

dtree = DecisionTreeRegressor(featuresCol="features")

model = dtree.fit(train_df)

predictions = model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("RMSE for test data = %g" % rmse)

#print(model.toDebugString)
display(model)

# COMMAND ----------

print(model.toDebugString)

# COMMAND ----------

# MAGIC %md #### 2. Classification
# MAGIC #####Logistic Regression - Binary Classification

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression

train_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

lr = LogisticRegression(maxIter=10, regParam=0.9, elasticNetParam=0.6)

pipeline = Pipeline(stages=[string_indexer, lr])

model = pipeline.fit(train_df)

lr_model = model.stages[1]
summary = lr_model.summary

print("Coefficients: " + str(lr_model.coefficientMatrix))
print("Intercepts: " + str(lr_model.interceptVector))

print("areaUnderROC: " + str(summary.areaUnderROC))
summary.roc.display()

# COMMAND ----------

# MAGIC %md #####Logistic Regression - Multinomial Classification

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression

train_df = spark.read.table("retail_features").selectExpr("country_indexed as label", "selected_features as features")


mlr = LogisticRegression(maxIter=10, regParam=0.5, elasticNetParam=0.3, family="multinomial")


mlr_model = mlr.fit(train_df)


print("Coefficients: " + str(mlr_model.coefficientMatrix))
print("Intercepts: " + str(mlr_model.interceptVector))

print("areaUnderROC: " + str(summary.areaUnderROC))
summary.roc.display()

# COMMAND ----------

# MAGIC %md #####Decision Tree Classifier

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

retail_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

(train_df, test_df) = retail_df.randomSplit([0.8, 0.2])

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

dtree = DecisionTreeClassifier(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[string_indexer, dtree])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g " % (accuracy))

dtree_model = model.stages[1]

#print(dtree_model.toDebugString)
display(dtree_model)

# COMMAND ----------

# MAGIC %md #####Naive Bayes Classifier

# COMMAND ----------

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

retail_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

(train_df, test_df) = retail_df.randomSplit([0.8, 0.2])

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

nb = NaiveBayes(smoothing=0.9, modelType="gaussian")

pipeline = Pipeline(stages=[string_indexer, nb])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Model accuracy = %f" % accuracy)

# COMMAND ----------

# MAGIC %md #####Linear SVM Binary Classification

# COMMAND ----------

from pyspark.ml.classification import LinearSVC

train_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

svm = LinearSVC(maxIter=10, regParam=0.1)

pipeline = Pipeline(stages=[string_indexer, svm])

model = pipeline.fit(train_df)

svm_model = model.stages[1]

# Print the coefficients and intercept for linear SVC
print("Coefficients: " + str(svm_model.coefficients))
print("Intercept: " + str(svm_model.intercept))

# COMMAND ----------

# MAGIC %md ####Tree Ensembles
# MAGIC #####Random Forest Regression

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

retail_features = spark.read.table("retail_features").selectExpr("cust_age as label", "selected_features as features")

(train_df, test_df) = retail_features.randomSplit([0.8, 0.2])

rf = RandomForestRegressor(labelCol="label", featuresCol="features", numTrees=5)

rf_model = rf.fit(train_df)

predictions = rf_model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("RMSE for test data = %g" % rmse)

print(rf_model.toDebugString)

# COMMAND ----------

# MAGIC %md #####Random Forest Classification

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

retail_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

(train_df, test_df) = retail_df.randomSplit([0.8, 0.2])

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=5)

pipeline = Pipeline(stages=[string_indexer, rf])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g " % (accuracy))

rf_model = model.stages[1]

print(rf_model.toDebugString)

# COMMAND ----------

# MAGIC %md #####Regression using Gradient Boosted Trees

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

retail_features = spark.read.table("retail_features").selectExpr("cust_age as label", "selected_features as features")

(train_df, test_df) = retail_features.randomSplit([0.8, 0.2])

gbt = GBTRegressor(labelCol="label", featuresCol="features", maxIter=5)

gbt_model = gbt.fit(train_df)

predictions = gbt_model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("RMSE for test data = %g" % rmse)

print(gbt_model.toDebugString)

# COMMAND ----------

# MAGIC %md #####Classification using GBT

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

retail_df = spark.read.table("retail_features").selectExpr("gender", "selected_features as features")

(train_df, test_df) = retail_df.randomSplit([0.8, 0.2])

string_indexer = StringIndexer(inputCol="gender", outputCol="label", handleInvalid="skip" )

gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=5)

pipeline = Pipeline(stages=[string_indexer, gbt])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g " % (accuracy))

gbt_model = model.stages[1]

print(gbt_model.toDebugString)