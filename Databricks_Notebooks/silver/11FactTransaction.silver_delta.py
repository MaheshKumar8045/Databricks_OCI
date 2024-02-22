# Databricks notebook source
# MAGIC %md
# MAGIC ###Joining all Dim to Fact

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading tables as spark df

# COMMAND ----------

df_silverTransactions = spark.table("silver.dimTransactions")
df_silverCustomer = spark.table("silver.dimCustomer")
df_silverprodcatsubcat = spark.table("silver.dimprodcatsubcat")
df_silverStore = spark.table("silver.dimStore")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joinin df to Create Fact Transactions

# COMMAND ----------

from pyspark.sql.functions import col

# Renaming the column "store_type" in df_silverStore
df_silverStore_renamed = df_silverStore.withColumnRenamed("store_type", "dup_store_type")
df_silverprodcatsubcat_renamed = df_silverprodcatsubcat.withColumnRenamed("prod_cat_code", "dup_prod_cat_code")

# Performing the joins with renamed columns
df_FactTransactions1 = df_silverTransactions.join(df_silverCustomer, df_silverTransactions["Cust_ID"] == df_silverCustomer["Customer_ID"], "left_outer")
df_FactTransactions2 = df_FactTransactions1.join(df_silverStore_renamed, df_FactTransactions1["store_type"] == df_silverStore_renamed["dup_store_type"], "left_outer")
df_FactTransactions3 = df_FactTransactions2.join(df_silverprodcatsubcat_renamed, (df_FactTransactions2["Prod_SubCAT_Code"] == df_silverprodcatsubcat_renamed["prod_sub_cat_code"]) & (df_FactTransactions2["PROD_CAT_CODE"] == df_silverprodcatsubcat_renamed["dup_prod_cat_code"]), "left_outer")


# COMMAND ----------

df_FactTransactions=df_FactTransactions3

# COMMAND ----------

count = df_FactTransactions.count()
print(count)

# COMMAND ----------

df_gold = df_FactTransactions.select(
    "TRANSACTION_ID",
    "CUST_ID",
    "TRAN_DATE",
    "PROD_SUBCAT_CODE",
    "PROD_CAT_CODE",
    "PROD_SUBCAT",
    "PROD_CAT",
    "QTY",
    "RATE",
    "TAX",
    "TOTAL_AMT",
    "STORE_TYPE",
    "CUSTOMER_ID",
    "GENDER",
    "CITY_CODE",
    "CUSTOMER_NAME",
    "CREATED_MODIFIED_DATE"
)

# # Display the first few rows of the resulting DataFrame
# df_gold.show()

# COMMAND ----------

# Check if the "gold" database exists
database_exists = spark.sql("SHOW DATABASES LIKE 'gold'").count() > 0

# Create the "gold" database if it does not exist
if not database_exists:
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Create the Delta table using the DataFrame
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold.FactTransactions")
