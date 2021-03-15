# Databricks notebook source
# MAGIC %fs ls FileStore/shared_uploads/enriched_retail

# COMMAND ----------

retail_enriched = spark.read.option("header", True).csv("FileStore/shared_uploads/enriched_retail")
retail_enriched.createOrReplaceTempView("retail_enriched")
