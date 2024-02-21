# Databricks notebook source
# MAGIC %md
# MAGIC ### View All records (Old and Incremental) effected by merge statement 

# COMMAND ----------

# DBTITLE 1,Customer
# MAGIC %sql 
# MAGIC -- Customer
# MAGIC select * from M_Customer
# MAGIC order by CREATED_MODIFIED_DATE desc
# MAGIC limit 10;

# COMMAND ----------

# DBTITLE 1,Transactions
# MAGIC %sql 
# MAGIC -- Transactions
# MAGIC select * from M_Transactions
# MAGIC order by TRAN_DATE desc
# MAGIC limit 10;

# COMMAND ----------

# DBTITLE 1,ProdCatSubCat
# MAGIC %sql
# MAGIC -- M_ProdCatSubCat
# MAGIC select * from M_ProdCatSubCat
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change Data Feed (Change and Version)

# COMMAND ----------

# DBTITLE 1,Customer
dfCustomerCDF=spark.read.format("delta")\
    .option("readchangefeed","true")\
    .option("startingversion",0)\
    .table("M_Customer")
display(dfCustomerCDF)

# COMMAND ----------

# DBTITLE 1,Transactions
dfTransactionsCDF = spark.read.format("delta") \
    .option("readchangefeed", "true") \
    .option("startingversion", 0) \
    .table("M_Transactions")
display(dfTransactionsCDF)

# COMMAND ----------

# DBTITLE 1,ProdCatSubCat
dfProdCatSubCatCDF=spark.read.format("delta")\
    .option("readchangefeed","true")\
    .option("startingversion",0)\
    .table("M_ProdCatSubCat")
display(dfProdCatSubCatCDF)
