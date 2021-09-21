# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/online_retail/

# COMMAND ----------

# MAGIC %md ####Define stream schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

eventSchema = ( StructType()
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

# MAGIC %md ####Read file stream from data lake

# COMMAND ----------

stream_df = (spark.readStream
                    .format("csv")
                    .option("header", "true")
                    .schema(eventSchema)
                    .option("maxFilesPerTrigger", 1)
                    .load("/FileStore/shared_uploads/online_retail/"))

display(stream_df)

# COMMAND ----------

# MAGIC %md ####Process stream and persist to data lake in Delta format

# COMMAND ----------

(stream_df.writeStream
              .format("delta")
              .option("checkpointLocation", "/tmp/delta/retail_stream.delta/checkpoint")
              .start("/tmp/delta/retail_stream.delta"))