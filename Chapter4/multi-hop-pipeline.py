# Databricks notebook source
from pyspark.sql.functions import window, max, count, current_timestamp, to_timestamp

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

raw_stream_df = (spark
                 .readStream
                 .schema(retailSchema)
                 .option("header", True)
                 .csv("/FileStore/shared_uploads/online_retail/"))

(raw_stream_df
   .writeStream
     .format("delta")
     .option("checkpointLocation", "/tmp/delta/raw_stream.delta/checkpoint")
     .start("/tmp/delta/raw_stream.delta/"))

# COMMAND ----------

integrated_stream_df = (raw_stream_df
                          .withColumn("InvoiceTime", to_timestamp("InvoiceDate", 'dd/M/yy HH:mm')))

(integrated_stream_df
   .writeStream
     .format("delta")
     .option("checkpointLocation", "/tmp/delta/int_stream.delta/checkpoint")
     .start("/tmp/delta/int_stream.delta/"))

# COMMAND ----------

aggregated_stream_df = (integrated_stream_df
                          .withWatermark("InvoiceTime", "1 minutes")
                          .groupBy("InvoiceNo", window("InvoiceTime", "30 seconds", "10 seconds", "0 seconds"))
                          .agg(max("InvoiceTime").alias("event_time"),
                               count("InvoiceNo").alias("order_count")))
(aggregated_stream_df
   .writeStream
     .format("delta")
     .option("checkpointLocation", "/tmp/delta/agg_stream.delta/checkpoint")
     .start("/tmp/delta/agg_stream.delta/"))

# COMMAND ----------

# MAGIC %md ####Clean-up 
# MAGIC **Databricks Community Edition** has a limitation of 10000 files and 10 GB of storage in DBFS.<br>
# MAGIC So it is prudent to clean-up any intermediate datasets created on DBFS that we do not intent to use at a later time.

# COMMAND ----------

# MAGIC %fs rm -r /tmp/delta