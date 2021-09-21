# Databricks notebook source
# MAGIC %md ###Change Data Capture

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/online_retail/

# COMMAND ----------

# MAGIC %md ####Step 1: Initial Load - Batch Processing

# COMMAND ----------

(spark
   .read
     .option("header", "true")
     .option("inferSchema", "true")
     .csv("/FileStore/shared_uploads/online_retail/online_retail.csv")
   .write
     .mode("overwrite")
     .format("delta")
     .option("path", "/tmp/data-lake/online_retail.delta")
     .saveAsTable("online_retail"))

# COMMAND ----------

# MAGIC %sql select * from online_retail

# COMMAND ----------

# MAGIC %md ####Step2: Change Data Capture

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

retailSchema = ( StructType()
  .add('InvoiceNo', StringType()) 
  .add('StockCode', StringType())
  .add('Description', StringType()) 
  .add('Quantity', IntegerType()) 
  .add('InvoiceDate', StringType()) 
  .add('UnitPrice', DoubleType()) 
  .add('CustomerID', IntegerType()) 
  .add('Country', StringType())     
)

# COMMAND ----------

retail_stream_df = (spark
                 .readStream
                 .schema(retailSchema)
                 .csv("/FileStore/shared_uploads/online_retail/")
            )

# COMMAND ----------

# MAGIC %md ###Define upsert method

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "/tmp/data-lake/online_retail.delta")

def upsertToDelta(microBatchOutputDF, batchId):
  deltaTable.alias("a").merge(
      microBatchOutputDF.dropDuplicates(["InvoiceNo", "InvoiceDate"]).alias("b"),
      "a.InvoiceNo = b.InvoiceNo and a.InvoiceDate = b.InvoiceDate") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

(retail_stream_df.writeStream 
  .format("delta") 
  .foreachBatch(upsertToDelta)
  .outputMode("update") 
  .start())

# COMMAND ----------

display(retail_stream_df)

# COMMAND ----------

# MAGIC %md ####Clean-up 
# MAGIC **Databricks Community Edition** has a limitation of 10000 files and 10 GB of storage in DBFS.<br>
# MAGIC So it is prudent to clean-up any intermediate datasets created on DBFS that we do not intent to use at a later time.

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data-lake/