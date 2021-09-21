# Databricks notebook source
# MAGIC %md ###Unsupervised Learning
# MAGIC 1. ####Clustering
# MAGIC #####K-means clustering

# COMMAND ----------

# MAGIC %sql 
# MAGIC use feature_store;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql select * from retail_features

# COMMAND ----------

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

retail_features = spark.read.table("retail_features")
train_df = retail_features.selectExpr("selected_features as features")

kmeans = KMeans(k=3, featuresCol='features')
kmeans_model = kmeans.fit(train_df)

predictions = kmeans_model.transform(train_df)

evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette measure using squared euclidean distance = " + str(silhouette))

# Shows the result.
cluster_centers = kmeans_model.clusterCenters()
print(cluster_centers)

# COMMAND ----------

# MAGIC %md #####Hierarchial Clustering via Bisecting K-means

# COMMAND ----------

from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator

retail_features = spark.read.table("retail_features")
train_df = retail_features.selectExpr("selected_features as features")

bkmeans = BisectingKMeans(k=3, featuresCol='features')
bkmeans_model = kmeans.fit(train_df)

predictions = bkmeans_model.transform(train_df)

evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette measure using squared euclidean distance = " + str(silhouette))

cluster_centers = kmeans_model.clusterCenters()
print(cluster_centers)

# COMMAND ----------

predictions.display()

# COMMAND ----------

# MAGIC %md #####Topic Modeling using Latent Dirichlet Allocation

# COMMAND ----------

from pyspark.ml.clustering import LDA
train_df = spark.read.table("retail_features").selectExpr("selected_features as features")

lda = LDA(k=2, maxIter=1)
lda_model = lda.fit(train_df)

topics = lda_model.describeTopics(3)
topics.display()

transformed = lda_model.transform(dataset)
transformed.display()

# COMMAND ----------

# MAGIC %md #####Topic Modeling using Latent Dirichlet Allocation

# COMMAND ----------

from pyspark.ml.clustering import GaussianMixture

train_df = spark.read.table("retail_features").selectExpr("selected_features as features")

gmm = GaussianMixture(k=3, featuresCol='features')
gmm_model = gmm.fit(train_df)

gmm_model.gaussiansDF.display()

# COMMAND ----------

# MAGIC %md #### 2. Associan Rules
# MAGIC #####Collaborative Filtering - Alternating Least Squares

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

ratings_df = (spark.read.table("retail_features").selectExpr("CAST(invoice_num AS INT) as user_id", 
                          "CAST(stock_code AS INT) as item_id", "CAST(quantity AS INT) as rating")
              .where("user_id is NOT NULL AND item_id is NOT NULL"))
                
ratings_df.display()
#(train_df, test_df) = ratings_df.randomSplit([0.8, 0.2])

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

ratings_df = (spark.read.table("retail_features").selectExpr("CAST(invoice_num AS INT) as user_id", 
                          "CAST(stock_code AS INT) as item_id", "CAST(quantity AS INT) as rating")
              .where("user_id is NOT NULL AND item_id is NOT NULL"))
                
#ratings_df.display()
(train_df, test_df) = ratings_df.randomSplit([0.7, 0.3])
als = ALS(maxIter=3, regParam=0.03, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")
als_model = als.fit(train_df)

predictions = model.transform(test_df)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

user_recs = als_model.recommendForAllUsers(5)
user_recs.display()

item_recs = als_model.recommendForAllItems(5)
item_recs.display()