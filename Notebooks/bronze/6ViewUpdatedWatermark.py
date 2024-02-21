# Databricks notebook source
# MAGIC %md
# MAGIC ### Running Previous file sor variable references

# COMMAND ----------

# MAGIC %run ./2ConnectionwithSourceServers/

# COMMAND ----------

# MAGIC %run ./3IncrementalLoading/

# COMMAND ----------

# MAGIC %run ./5UpdateCFGTableWithLatestTimestampsForNextRun/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updated Watermarks

# COMMAND ----------

# Customer
cfg_df_customer.show()

# COMMAND ----------

# Transactions
cfg_df_Transactions.show()
