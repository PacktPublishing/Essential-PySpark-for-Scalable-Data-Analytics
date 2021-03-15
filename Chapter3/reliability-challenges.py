# Databricks notebook source
# MAGIC %md ####Atomic Transactions

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

(spark
   .read
     .csv("/FileStore/shared_uploads/online_retail/")
   .write
     .mode("overwrite")
     .format("parquet")
     .save("/tmp/retail.parquet")
)

# COMMAND ----------

# MAGIC %fs ls /tmp/retail.parquet

# COMMAND ----------

(spark
   .read
     .format("parquet")
     .load("dbfs:/tmp/retail.parquet/part-00006-tid-6775149024880509486-a83d662e-809e-4fb7-beef-208d983f0323-212-1-c000.snappy.parquet")
   .count()
)

# COMMAND ----------

(spark
   .read
     .csv("/FileStore/shared_uploads/online_retail/")
   .write
     .mode("overwrite")
     .format("delta")
     .save("/tmp/retail.delta")
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/retail.delta/

# COMMAND ----------

(spark
   .read
     .format("delta")
   .load("dbfs:/tmp/retail.delta/part-00006-2fef3c0c-ce30-4d2b-8865-3e563c0d94c6-c000.snappy.parquet").count()
)

# COMMAND ----------

# MAGIC %md ####Schema Enforcement

# COMMAND ----------

# MAGIC %fs rm -r /tmp/customer

# COMMAND ----------

from pyspark.sql.functions import lit

df1 = spark.range(3).withColumn("customer_id", lit("1"))
(df1
   .write
     .format("parquet")
     .mode("overwrite")
   .save("/tmp/customer")
)
df2 = spark.range(2).withColumn("customer_id", lit(2))
(df2
   .write
     .format("parquet")
     .mode("append")
   .save("/tmp/customer")
)

# COMMAND ----------

df3 = spark.read.parquet("/tmp/customer")
df3.show()

# COMMAND ----------

# MAGIC %md ##Delta Advantages
# MAGIC ###Schema Validation

# COMMAND ----------

from pyspark.sql.functions import lit

df1 = spark.range(3).withColumn("customer_id", lit("1"))
(df1
   .write
     .format("delta")
     .mode("overwrite")
   .save("/tmp/delta/customer")
)
df2 = spark.range(2).withColumn("customer_id", lit(2))
(df2
   .write
     .format("delta")
     .mode("append")
   .save("/tmp/delta/customer")
)

# COMMAND ----------

# MAGIC %md ###Schema Evolution

# COMMAND ----------

from pyspark.sql.functions import lit

df1 = spark.range(3)
(df1
   .write
     .format("delta")
     .mode("overwrite")
   .save("/tmp/delta/customer"))

df2 = spark.range(2).withColumn("customer_id", lit(2))
(df2
   .write
     .format("delta")
     .option("mergeSchema", True)
     .mode("append")
   .save("/tmp/delta/customer"))

# COMMAND ----------

# MAGIC %md ###UPDATES & DELETES

# COMMAND ----------

from pyspark.sql.functions import lit

df1 = spark.range(5).withColumn("customer_id", lit(2))
df1.write.format("delta").mode("overwrite").save("/tmp/df1")
spark.read.format("delta").load("/tmp/df1").show()

# COMMAND ----------

# MAGIC %md UPDATE command

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/tmp/delta/df1` SET customer_id = 5 WHERE id > 2;
# MAGIC 
# MAGIC SELECT * FROM delta.`/tmp/df1`;

# COMMAND ----------

# MAGIC %md DELETE command

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/tmp/df1` WHERE id = 4;
# MAGIC 
# MAGIC SELECT * FROM delta.`/tmp/df1`;

# COMMAND ----------

# MAGIC %fs ls /tmp/df1/_delta_log/

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY delta.`/tmp/df1`

# COMMAND ----------

# MAGIC %sql SELECT * from delta.`/tmp/df1` VERSION AS OF 0

# COMMAND ----------

# MAGIC %md Rollbacks

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT OVERWRITE delta.`/tmp/df1`
# MAGIC SELECT * from delta.`/tmp/df1` VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql SELECT * from delta.`/tmp/df1`

# COMMAND ----------

# MAGIC %md ####Clean-up 
# MAGIC **Databricks Community Edition** has a limitation of 10000 files and 10 GB of storage in DBFS.<br>
# MAGIC So it is prudent to clean-up any intermediate datasets created on DBFS that we do not intent to use at a later time.

# COMMAND ----------

# MAGIC %fs rm -r /tmp/
