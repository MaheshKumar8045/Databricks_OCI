# Databricks notebook source
# MAGIC %md
# MAGIC ### Transaction to Storetype

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_DimStore AS
# MAGIC SELECT STORE_TYPE, COUNT(TRANSACTION_ID) AS TRANSACTIONS_COUNT
# MAGIC FROM m_Transactions
# MAGIC GROUP BY STORE_TYPE;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.DimStore
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_DimStore;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dimstore
