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
                 .csv("/FileStore/shared_uploads/online_retail/")
                 .withColumn("InvoiceTime", to_timestamp("InvoiceDate", 'dd/M/yy HH:mm'))
                 #.withColumn("InvoiceTime", current_timestamp())
                )

# COMMAND ----------

display(raw_stream_df)

# COMMAND ----------

aggregated_df = (raw_stream_df
                  .withWatermark("InvoiceTime", "1 minutes")
                  .groupBy("InvoiceNo", window("InvoiceTime", "30 seconds", "10 seconds", "0 seconds"))
                  .agg(max("InvoiceTime").alias("event_time"),
                       count("InvoiceNo").alias("order_count"))
                )

# COMMAND ----------

(aggregated_df
   .writeStream
   .queryName("aggregated_df")
   .format("memory")
   .outputMode("complete")
   .start())

# COMMAND ----------

# MAGIC %sql SELECT * FROM aggregated_df