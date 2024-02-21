# Databricks notebook source
# MAGIC %run ./2ConnectionwithSourceServers/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking for Last Modified Date in cfg tables (Incremental Load)

# COMMAND ----------

# DBTITLE 1,Customers 
# Customers 
if cfg_df_customer.count() > 0:
    print("Data Exist's perform Incremantal Load")
    last_modified_Customer = cfg_df_customer.filter(cfg_df_customer["DEST_TABLE"] == "M_Delta_Customer").first()[4]
    Customer_df=Customer_df.filter(Customer_df["CREATED_MODIFIED_DATE"]>last_modified_Customer)

Customer_Watermark_Value=Customer_df.agg({"CREATED_MODIFIED_DATE": "max"}).collect()[0][0]

Customer_df.createOrReplaceTempView("Customer_Oracle")

# COMMAND ----------

# DBTITLE 1,Transactions
# Transactions
if cfg_df_Transactions.count() > 0:
    print("Data Exist's perform Incremantal Load")
    last_modified_Transactions = cfg_df_Transactions.filter(cfg_df_Transactions["DEST_TABLE"] == "M_Delta_Transactions").first()[4]
    Transactions_df=Transactions_df.filter(Transactions_df["TRAN_DATE"]>last_modified_Transactions)
    
    
Transactions_Watermark_Value=Transactions_df.agg({"TRAN_DATE": "max"}).collect()[0][0]

Transactions_df.createOrReplaceTempView("Transactions_Oracle")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwriting into Delta (Full Load)

# COMMAND ----------

# DBTITLE 1,ProductCatSubcat
ProdCatSubCat_df.write.format("delta").mode("overwrite").saveAsTable("M_ProdCatSubCat")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Merge Function to upsert the incremental records

# COMMAND ----------

# DBTITLE 1,Customer
# MAGIC %sql
# MAGIC --Customer
# MAGIC MERGE INTO M_Customer 
# MAGIC USING Customer_Oracle 
# MAGIC ON M_Customer.CUSTOMER_ID = Customer_Oracle.CUSTOMER_ID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Transactions
# MAGIC %sql
# MAGIC --Transactions
# MAGIC MERGE INTO M_Transactions
# MAGIC USING Transactions_Oracle
# MAGIC ON M_Transactions.Transaction_ID=Transactions_Oracle.Transaction_ID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
