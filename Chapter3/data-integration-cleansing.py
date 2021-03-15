# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/online_retail/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType

schema = (StructType()
            .add("InvoiceNo", StringType(), True)
            .add("StockCode", StringType(), True)
            .add("Description", StringType(), True)
            .add("Quantity", StringType(), True)
            .add("InvoiceDate", StringType(), True)
            .add("UnitPrice", StringType(), True)
            .add("CustomerID", StringType(), True)
            .add("Country", StringType(), True)
         )

df1 = (spark.read
               .option("header", True)
             .csv("dbfs:/FileStore/shared_uploads/online_retail/online_retail.csv"))
df2 = (spark.read
               .option("header", True)
             .csv("dbfs:/FileStore/shared_uploads/online_retail/online_retail_II.csv"))
df1.printSchema()
df2.printSchema()

# COMMAND ----------

retail_df = df1.union(df2)
retail_df.show()

# COMMAND ----------

df3 = (spark.read
             .option("header", True)
             .option("delimiter", ";")
            .csv("/FileStore/shared_uploads/countries_codes.csv"))

country_df = (df3
   .withColumnRenamed("OFFICIAL LANG CODE", "CountryCode")
   .withColumnRenamed("ISO2 CODE", "ISO2Code")
   .withColumnRenamed("ISO3 CODE", "ISO3Code")
   .withColumnRenamed("LABEL EN", "CountryName")
   .withColumnRenamed("Geo Shape", "GeoShape")
   .drop("ONU CODE")
   .drop("IS ILOMEMBER")
   .drop("IS RECEIVING QUEST")
   .drop("LABEL FR")
   .drop("LABEL SP")
   .drop("geo_point_2d")
)

integrated_df = retail_df.join(country_df, retail_df.Country == country_df.CountryName, "left_outer")

# COMMAND ----------

country_df.show()
integrated_df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType

schema = (StructType()
            .add("age", StringType(), True)
            .add("workclass", StringType(), True)
            .add("fnlwgt", StringType(), True)
            .add("education", StringType(), True)
            .add("education-num", StringType(), True)
            .add("marital-status", StringType(), True)
            .add("occupation", StringType(), True)
            .add("relationship", StringType(), True)
            .add("race", StringType(), True)
            .add("gender", StringType(), True)
            .add("capital-gain", StringType(), True)
            .add("capital-loss", StringType(), True)
            .add("hours-per-week", StringType(), True)
            .add("native-country", StringType(), True)
         )

# COMMAND ----------

#%fs rm -r /FileStore/shared_uploads/delta/retail_enriched.delta

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

income_df = spark.read.schema(schema).csv("/FileStore/shared_uploads/adult.data").withColumn("idx", monotonically_increasing_id())

retail_dfx = integrated_df.withColumn("RetailIDx", monotonically_increasing_id())
income_dfx = income_df.withColumn("IncomeIDx", monotonically_increasing_id()) 
retail_enriched_df = retail_dfx.join(income_dfx, retail_dfx.RetailIDx == income_dfx.IncomeIDx, "left_outer")

display(retail_enriched_df)

# COMMAND ----------

'''(retail_enriched_df
   .coalesce(1)
   .write
     .format("delta")
     .mode("overwrite")
    .save("/FileStore/shared_uploads/retail.delta"))
    '''

# COMMAND ----------

# MAGIC %md ###Data Federation

# COMMAND ----------

# MAGIC %md MySQL Source

# COMMAND ----------

dataframe_mysql = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/pysparkdb",
    driver = "org.mariadb.jdbc.Driver",
    dbtable = "authors",
    user="root",
    password="root").load()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE mysql_authors IF NOT EXISTS
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://localhost:3306/pysparkdb",
# MAGIC   dbtable "authors",
# MAGIC   user "root",
# MAGIC   password "root"
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM mysql_authors

# COMMAND ----------

from pyspark.sql.functions import rand, col

authors_df = spark.range(16).withColumn("salary", rand(10)*col("id")*10000)
authors_df.write.format("csv").saveAsTable("author_salary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   m.last_name,
# MAGIC   m.first_name,
# MAGIC   s.salary
# MAGIC FROM
# MAGIC   author_salary s
# MAGIC   JOIN mysql_authors m ON m.uid = s.id
# MAGIC ORDER BY s.salary DESC

# COMMAND ----------

# MAGIC %md ##Data Cleansing
# MAGIC ###Selecting the Right Data

# COMMAND ----------

retail_enriched_df.printSchema()

# COMMAND ----------

 retail_clean_df = (retail_enriched_df
                    .drop("Country")
                    .drop("ISO2Code")
                    .drop("ISO3Code")
                    .drop("RetailIDx")
                    .drop("idx")
                    .drop("IncomeIDx")
                   )

# COMMAND ----------

(retail_clean_df
   .select("InvoiceNo", "InvoiceDate", "StockCode")
   .groupBy("InvoiceNo", "InvoiceDate", "StockCode")
   .count()
   .show()
)

# COMMAND ----------

(retail_clean_df
   .select("*")
   .where("InvoiceNo in ('536373', '536382', '536387') AND StockCode in ('85123A', '22798', '21731')")
   .display()
)

# COMMAND ----------

retail_nodupe = retail_clean_df.drop_duplicates(["InvoiceNo", "InvoiceDate", "StockCode"])

# COMMAND ----------

(retail_nodupe
   .select("InvoiceNo", "InvoiceDate", "StockCode")
   .groupBy("InvoiceNo", "InvoiceDate", "StockCode")
   .count()
   .where("count > 1")
   .show()
)

# COMMAND ----------

retail_nodupe.printSchema()

# COMMAND ----------

retail_final_df = (retail_nodupe.selectExpr(
                                 "InvoiceNo AS invoice_num", "StockCode AS stock_code", "description AS description",
                                    "Quantity AS quantity", "InvoiceDate AS invoice_date", 
                                    "CAST(UnitPrice AS DOUBLE) AS unit_price", "CustomerID AS customer_id",
                                    "CountryCode AS country_code", "CountryName AS country_name", "GeoShape AS geo_shape",
                                    "age", "workclass AS work_class", "fnlwgt AS final_weight", "education", 
                                    "'education-num' AS education_num", "'marital-status' AS marital_status", 
                                    "occupation", "relationship", "race", "gender", "'capital-gain' AS capital_gain",
                                    "'capital-loss' AS capital_loss", "'hours-per-week' AS hours_per_week",
                                    "'native-country' AS native_country")
                  )
#retail_final_df.display()

# COMMAND ----------

retail_final_df.count()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",True)

# COMMAND ----------

# MAGIC %md Limiting the write operation to 10K rows because of Databricks Community hard limit of 10GB storage space.

# COMMAND ----------

retail_final_df.limit(10000).write.format("delta").save("dbfs:/FileStore/shared_uploads/delta/retail_silver.delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/delta/retail_silver.delta

# COMMAND ----------

# MAGIC %md ####Clean-up 
# MAGIC **Databricks Community Edition** has a limitation of 10000 files and 10 GB of storage in DBFS.<br>
# MAGIC So it is prudent to clean-up any intermediate datasets created on DBFS that we do not intent to use at a later time.

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/shared_uploads/delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE author_salary;
