# Databricks notebook source
# MAGIC %md ##Feature Engineering
# MAGIC Load pre-processed dataset

# COMMAND ----------

preproc_data = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/delta/retail_ml.delta")
preproc_data.display() #use .show() outside of Databricks.

# COMMAND ----------

# MAGIC %md ###Step 1: Feature Extraction
# MAGIC Use CountVectorizer to extract feature vector from the `description` column.

# COMMAND ----------

from pyspark.sql.functions import split, trim
from pyspark.ml.feature import CountVectorizer

cv_df = preproc_data.withColumn("desc_array", split(trim("description"), " ")) #.where("description is NOT NULL")
cv = CountVectorizer(inputCol="desc_array", outputCol="description_vec", vocabSize=2, minDF=2.0)
cv_model = cv.fit(cv_df)

train_df = cv_model.transform(cv_df)
train_df.display()

# COMMAND ----------

from pyspark.ml.feature import Word2Vec

w2v_df = preproc_data.withColumn("desc_array", split(trim("description"), "\t"))

word2Vec = Word2Vec(vectorSize=2, minCount=0, inputCol="desc_array", outputCol="desc_vec")
w2v_model = word2Vec.fit(w2v_df)

train_df = w2v_model.transform(w2v_df)
train_df.display()

# COMMAND ----------

# MAGIC %md ###Step 2: Feature Transformation
# MAGIC Use Tokenizer to tokenize text into individual terms.

# COMMAND ----------

from pyspark.ml.feature import Tokenizer

tokenizer = Tokenizer(inputCol="description", outputCol="desc_terms")

tokenized_df = tokenizer.transform(preproc_data)
tokenized_df.select("description", "desc_terms").display()

# COMMAND ----------

# MAGIC %md **StopWordsRemover** for removing very commonly occuring words which do not carry much meaning.

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

stops_remover = StopWordsRemover(inputCol="desc_terms", outputCol="desc_nostops")
stops_df = stops_remover.transform(tokenized_df)
stops_df.select("desc_terms", "desc_nostops").display()

# COMMAND ----------

# MAGIC %md Convert array of strings to a feature vector.

# COMMAND ----------

from pyspark.ml.feature import Word2Vec

word2Vec = Word2Vec(vectorSize=2, minCount=0, inputCol="desc_nostops", outputCol="desc_vec")
w2v_model = word2Vec.fit(stops_df)

w2v_df = w2v_model.transform(stops_df)
w2v_df.display()

# COMMAND ----------

# MAGIC %md Assigned label indices to string columns using StringIndexer 

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

string_indexer = StringIndexer(inputCol="country_code", outputCol="country_indexed", handleInvalid="skip" )
indexed_df = string_indexer.fit(w2v_df).transform(w2v_df)
indexed_df.select("country_code", "country_indexed").display()

# COMMAND ----------

# MAGIC %md Transforming label indices into binary vectors using OneHotEncoder

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

ohe = OneHotEncoder(inputCol="country_indexed", outputCol="country_ohe")
ohe_df = ohe.fit(indexed_df).transform(indexed_df)
ohe_df.select("country_code", "country_ohe").display()

# COMMAND ----------

# MAGIC %md Run StringIndexer on `quantity` column as well.

# COMMAND ----------

qty_indexer = StringIndexer(inputCol="quantity", outputCol="quantity_indexed", handleInvalid="skip" )
qty_df = qty_indexer.fit(ohe_df).transform(ohe_df)
qty_df.select("quantity", "quantity_indexed").display()

# COMMAND ----------

# MAGIC %md Binarization of continous numerical data.

# COMMAND ----------

from pyspark.ml.feature import Binarizer

binarizer = Binarizer(threshold=10, inputCol="unit_price", outputCol="binarized_price")
binarized_df = binarizer.transform(qty_df)
binarized_df.select("quantity", "binarized_price").display()

# COMMAND ----------

# MAGIC %md Transforming date/time columns

# COMMAND ----------

from pyspark.sql.functions import month

month_df = binarized_df.withColumn("invoice_month", month("invoice_time"))
month_indexer = StringIndexer(inputCol="invoice_month", outputCol="month_indexed", handleInvalid="skip" )
month_df = month_indexer.fit(month_df).transform(month_df)
month_df.select("invoice_month", "month_indexed").display()

# COMMAND ----------

# MAGIC %md #####Assembling Individual Features into a Feature Vector

# COMMAND ----------

month_df.printSchema()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vec_assembler = VectorAssembler(
    inputCols=["desc_vec", "country_ohe", "binarized_price", "month_indexed", "quantity_indexed"],
    outputCol="features")

features_df = vec_assembler.transform(month_df)

features_df.select("features").display()

# COMMAND ----------

# MAGIC %md ### Step3: Feature Scaling
# MAGIC Feature Scaling using StandardScaler

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

std_scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaled_df = std_scaler.fit(features_df).transform(features_df)
scaled_df.select("scaled_features").display()

# COMMAND ----------

# MAGIC %md ###Part 4: Feature Selection
# MAGIC Chi Square Selector

# COMMAND ----------

from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.linalg import Vectors

chisq_selector = ChiSqSelector(numTopFeatures=1, featuresCol="scaled_features",
                         outputCol="selected_features", labelCol="cust_age")

result_df = chisq_selector.fit(scaled_df).transform(scaled_df)

result_df.select("selected_features").display()

# COMMAND ----------

# MAGIC %md Feature Selection using VectorSclicer

# COMMAND ----------

from pyspark.ml.feature import VectorSlicer

vec_slicer = VectorSlicer(inputCol="scaled_features", outputCol="selected_features", indices=[1])
result_df = vec_slicer.transform(scaled_df)
result_df.select("scaled_features", "selected_features").display()

# COMMAND ----------

# MAGIC %md ###Delta Lake as Feature Store

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS feature_store ")

(result_df
   .write
   .format("delta")
   .mode("overwrite")
   .option("location", "/FileStore/shared_uploads/delta/retail_features.delta")
   .saveAsTable("feature_store.retail_features"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store;
# MAGIC CREATE TABLE IF NOT EXISTS feature_store.retail_features
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/shared_uploads/delta/retail_features.delta';

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM feature_store.retail_features;