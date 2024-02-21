# Databricks notebook source
# MAGIC %md
# MAGIC ### Customer Schema Conversion

# COMMAND ----------

df_Bronz_Customer = spark.table("M_Customer")

# COMMAND ----------

##Schema Conversion
from pyspark.sql.functions import col, to_date
df_schema=df_Bronz_Customer.withColumn("DOB", to_date("DOB", "dd-MM-yyyy"))

# COMMAND ----------

df_schema.printSchema()

# COMMAND ----------

record_count = df_schema.count()
print(record_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct Function 

# COMMAND ----------

df_distinct = df_schema.distinct()

# COMMAND ----------

count_distinct = df_distinct.count()
print(count_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicate records based on customerID

# COMMAND ----------

record_count = df_distinct.count()
print(record_count)

# COMMAND ----------

df_DimCustomer = df_distinct.drop_duplicates(["customer_ID"])

# COMMAND ----------

count_distinct_DimCustomer = df_distinct.count()
print(count_distinct_DimCustomer)

# COMMAND ----------

# Check if the "silver" database exists
database_exists = spark.sql("SHOW DATABASES LIKE 'silver'").count() > 0

# Create the "silver" database if it does not exist
if not database_exists:
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")


# COMMAND ----------

# MAGIC %md
# MAGIC Insert or Update

# COMMAND ----------

from delta.tables import DeltaTable

# Check if the "silver.DimCustomer" exists
if spark.catalog._jcatalog.tableExists("silver.dimcustomer"):
    print("Table exists")
    existing_table = DeltaTable.forName(spark,"silver.dimcustomer")
    # Perform the merge operation
    existing_table.alias("existing") \
        .merge(df_DimCustomer.alias("new"), "new.customer_id = existing.customer_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    print("Table does not exist")
    # Create the Delta table using the DataFrame
    df_DimCustomer.write.format("delta").saveAsTable("silver.dimcustomer")
