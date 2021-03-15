# Databricks notebook source
(spark
   .read
     .option("header", True)
     .option("inferSchema", True)
     .csv("/FileStore/shared_uploads/online_retail/")
   .write
     .format("delta")
     .save("/FileStore/shared_uploads/delta/online_retail")
)

# COMMAND ----------

# MAGIC %fs ls /FileStore/shared_uploads/delta/online_retail

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/delta/online_retail/_delta_log/

# COMMAND ----------

spark.read.json("/FileStore/shared_uploads/delta/online_retail/_delta_log/").show()
