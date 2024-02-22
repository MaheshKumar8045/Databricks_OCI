# Databricks notebook source
# MAGIC %md
# MAGIC ### Customer Schema Conversion

# COMMAND ----------

df_Bronz_transactions = spark.table("M_transactions")

# COMMAND ----------

##Schema Conversion
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, FloatType

df_schema = df_Bronz_transactions.withColumn("TRAN_DATE", to_date("TRAN_DATE", "dd-MM-yyyy"))\
    .withColumn("PROD_SUBCAT_CODE", col("PROD_SUBCAT_CODE").cast(IntegerType()))\
    .withColumn("PROD_CAT_CODE", col("PROD_CAT_CODE").cast(IntegerType()))\
    .withColumn("QTY", col("QTY").cast(IntegerType()))\
    .withColumn("RATE", col("RATE").cast(FloatType()))\
    .withColumn("TAX", col("TAX").cast(FloatType()))\
    .withColumn("TOTAL_AMT", col("TOTAL_AMT").cast(FloatType()))

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

df_DimTransactions = df_distinct.drop_duplicates(["Transaction_ID"])

# COMMAND ----------

count_distinct_DimTransactions = df_DimTransactions.count()
print(count_distinct_DimTransactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Below code reduces number of transactions but refines data to remove bad records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Negitive values from Transaction ID

# COMMAND ----------

# df_DimTransactions = df_DimTransactions.filter(col("Transaction_ID") >= 0)

# COMMAND ----------

# display(df_DimTransactions)

# COMMAND ----------

# df_DimTransactions.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Transaction_ID with =! 6 digits

# COMMAND ----------

# from pyspark.sql.functions import length

# df_DimTransactions = df_DimTransactions.filter(length(col("Transaction_ID")) == 6)

# COMMAND ----------

# display(df_DimTransactions)

# COMMAND ----------

df_DimTransactions.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DIM Transactions

# COMMAND ----------

# Check if the "silver" database exists
database_exists = spark.sql("SHOW DATABASES LIKE 'silver'").count() > 0

# Create the "silver" database if it does not exist
if not database_exists:
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")


# COMMAND ----------

from delta.tables import DeltaTable

# Check if the "silver.DimTransactions" exists
if spark.catalog._jcatalog.tableExists("silver.dimtransactions"):
    print("Table exists")
    existing_table = DeltaTable.forName(spark,"silver.dimTransactions")
    # Perform the merge operation
    existing_table.alias("existing") \
        .merge(df_DimTransactions.alias("new"), "new.Transaction_id = existing.Transaction_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    print("Table does not exist")
    # Create the Delta table using the DataFrame
    df_DimTransactions.write.format("delta").saveAsTable("silver.dimTransactions")
