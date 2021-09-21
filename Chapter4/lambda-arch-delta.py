# Databricks notebook source
# MAGIC %md ###Simplified Lambda Architecture with Delta Lake

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/online_retail/

# COMMAND ----------

# MAGIC %md ####Step 1: Initial Load - Batch Processing

# COMMAND ----------

retail_batch_df = (spark
                 .read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv("/FileStore/shared_uploads/online_retail/online_retail.csv")
            )

# COMMAND ----------

(retail_batch_df
       .write
       .mode("overwrite")
       .format("delta")
       .option("path", "/tmp/data-lake/online_retail.delta")
       .saveAsTable("online_retail")
)

# COMMAND ----------

# MAGIC %sql select * from online_retail

# COMMAND ----------

# MAGIC %md ####Step2: Incremental Load - Streaming Ingestion

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

display(retail_stream_df)

# COMMAND ----------

# MAGIC %md Write the streaming data to the same Delta lake table that was defined earlier

# COMMAND ----------

(retail_stream_df
       .writeStream
       .outputMode("append")
       .format("delta")
       .option("checkpointLocation", "/tmp/data-lake/online_retail.delta/")
       .start("/tmp/data-lake/online_retail.delta")
)

# COMMAND ----------

# MAGIC %sql select * from online_retail

# COMMAND ----------

# MAGIC %md ####Clean-up 
# MAGIC **Databricks Community Edition** has a limitation of 10000 files and 10 GB of storage in DBFS.<br>
# MAGIC So it is prudent to clean-up any intermediate datasets created on DBFS that we do not intent to use at a later time.

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data-lake/