# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##Examples of Spark SQL constructs

# COMMAND ----------

# MAGIC %md  ### Data Surces
# MAGIC 1. File-based Data Source

# COMMAND ----------

# MAGIC %sql SELECT * FROM delta.`/FileStore/shared_uploads/delta/retail_features.delta` LIMIT 10;

# COMMAND ----------

# MAGIC %md 1.1 CSV Data Source

# COMMAND ----------

spark.read.csv("/FileStore/ConsolidatedCities.csv").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW csv_able
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true",
# MAGIC   path "/FileStore/ConsolidatedCities.csv"
# MAGIC );
# MAGIC SELECT * FROM csv_able LIMIT 5;

# COMMAND ----------

# MAGIC %md  2. JDBC Data Source

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW jdbcTable
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://localhost:3306/pysparkdb",
# MAGIC   dbtable "authors",
# MAGIC   user 'username', --use actual username
# MAGIC   password 'password' --use actual password
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM jdbcTable;

# COMMAND ----------

# MAGIC %md ### Spark SQL DDL
# MAGIC Create database and table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS feature_store.retail_features
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/shared_uploads/delta/retail_features.delta';

# COMMAND ----------

# MAGIC %fs ls /FileStore/shared_uploads/delta/retail_features.delta

# COMMAND ----------

# MAGIC %md Alter table

# COMMAND ----------

# MAGIC %sql ALTER TABLE feature_store.retail_features RENAME TO feature_store.etailer_features;

# COMMAND ----------

# MAGIC %sql ALTER TABLE feature_store.etailer_features ADD COLUMN (new_col String);

# COMMAND ----------

# MAGIC %md Drop table

# COMMAND ----------

# MAGIC %sql 
# MAGIC --TRUNCATE TABLE feature_store.etailer_features;
# MAGIC --DROP TABLE feature_store.etailer_features;
# MAGIC -- DROP DATABASE feature_store;

# COMMAND ----------

# MAGIC %md ### Sparl SQL DML
# MAGIC INSERT Statement

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO feature_store.retail_features
# MAGIC SELECT * FROM delta.`/FileStore/shared_uploads/delta/retail_features.delta`;

# COMMAND ----------

# MAGIC %md DELETE Statement

# COMMAND ----------

# MAGIC %sql DELETE FROM feature_store.retail_features WHERE country_code = 'FR';

# COMMAND ----------

# MAGIC %md SELECT Statament

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   birth_month AS emp_year,
# MAGIC   max(m.last_name),
# MAGIC   max(m.first_name),
# MAGIC   avg(s.salary) AS avg_salary
# MAGIC FROM
# MAGIC   author_salary s
# MAGIC   JOIN mysql_authors m ON m.uid = s.id
# MAGIC GROUP BY birth_month
# MAGIC ORDER BY avg_salary DESC

# COMMAND ----------

# MAGIC %md Join Hints

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(m) */
# MAGIC   birth_month AS emp_year,
# MAGIC   max(m.last_name),
# MAGIC   max(m.first_name),
# MAGIC   avg(s.salary) AS avg_salary
# MAGIC FROM
# MAGIC   author_salary s
# MAGIC   JOIN mysql_authors m ON m.uid = s.id
# MAGIC GROUP BY birth_month
# MAGIC ORDER BY avg_salary DESC