# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Empty Delta Tables

# COMMAND ----------

# DBTITLE 1,Table for Customer
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS M_Customer
# MAGIC (
# MAGIC     CUSTOMER_ID INT,
# MAGIC     DOB DATE,
# MAGIC     GENDER STRING,
# MAGIC     CITY_CODE STRING,
# MAGIC     CUSTOMER_NAME STRING,
# MAGIC     CREATED_MODIFIED_DATE TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES(delta.enableChangeDataFeed=true);

# COMMAND ----------

# DBTITLE 1,Table for Transactions
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS M_Transactions
# MAGIC (
# MAGIC     TRANSACTION_ID int,
# MAGIC     CUST_ID int,
# MAGIC     TRAN_DATE timestamp,
# MAGIC     PROD_SUBCAT_CODE string,
# MAGIC     PROD_CAT_CODE string,
# MAGIC     QTY string,
# MAGIC     RATE string,
# MAGIC     TAX string,
# MAGIC     TOTAL_AMT int,
# MAGIC     STORE_TYPE string
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES(delta.enableChangeDataFeed=true);

# COMMAND ----------

# DBTITLE 1,Table for Prod_cat_subcat
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS M_ProdCatSubCat (
# MAGIC   prod_cat_code INT,
# MAGIC   prod_cat STRING,
# MAGIC   prod_sub_cat_code INT,
# MAGIC   prod_subcat STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES(delta.enableChangeDataFeed=true);
