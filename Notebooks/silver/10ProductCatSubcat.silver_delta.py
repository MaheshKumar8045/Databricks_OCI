# Databricks notebook source
# MAGIC %md
# MAGIC ###Schema Conversion (not needed)

# COMMAND ----------

df_Bronz_prodcatsubcat = spark.table("M_prodcatsubcat")

# COMMAND ----------

# DBTITLE 1,Not Needed
# ##Schema Conversion
# from pyspark.sql.functions import col, to_date
# df_schema=df_Bronz_prodcatsubcat.withColumn("DOB", to_date("DOB", "dd-MM-yyyy"))

# COMMAND ----------

record_count = df_Bronz_prodcatsubcat.count()
print(record_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct Function 

# COMMAND ----------

df_distinct = df_Bronz_prodcatsubcat.distinct()

# COMMAND ----------

count_distinct = df_distinct.count()
print(count_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove Duplicate records based on customerID 
# MAGIC #(not needed)

# COMMAND ----------

# record_count = df_schema.count()
# print(record_count)

# COMMAND ----------

# df_DimCustomer = df_distinct.drop_duplicates(["customer_ID"])

# COMMAND ----------

# count_distinct = df_distinct.count()
# print(count_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite Dim PodCatSubCat

# COMMAND ----------

# Check if the "silver" database exists
database_exists = spark.sql("SHOW DATABASES LIKE 'silver'").count() > 0

# Create the "silver" database if it does not exist
if not database_exists:
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Create the Delta table using the DataFrame
df_distinct.write.format("delta").mode("overwrite").saveAsTable("silver.DimProdCatSubCat")
