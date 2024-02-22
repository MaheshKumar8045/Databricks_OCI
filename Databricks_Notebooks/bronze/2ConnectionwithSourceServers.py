# Databricks notebook source
# MAGIC %md
# MAGIC ###Connecting to sqlserver Database using JDBC Divers for cfg Tables

# COMMAND ----------

sqlServerName="adbworkshopserver.database.windows.net"
sqlDB="AADT"
sqlPORT=1433
User="serveradmin"
passsword="Welcome@123"
sqlServerURL="jdbc:sqlserver://{0}:{1};database={2}".format(sqlServerName,sqlPORT,sqlDB)
connectionProperties= {
"user":User,
"password":passsword,
"driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



# COMMAND ----------

# DBTITLE 1,Reading CFG Tables from SQL (Not needed)
# #read cfg_databricks_customer
# cfg_df_customer=spark.read.format("jdbc")\
# .option("url",sqlServerURL)\
# .option("dbtable","cfg_databricks_customer")\
# .option("user",User)\
# .option("password",passsword)\
# .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
# .load()

# #read cfg_databricks_Transactions
# cfg_df_Transactions=spark.read.format("jdbc")\
# .option("url",sqlServerURL)\
# .option("dbtable","cfg_databricks_transactions")\
# .option("user",User)\
# .option("password",passsword)\
# .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
# .load()


# COMMAND ----------

# DBTITLE 1,Reading CFG Tables from DBFS
# Read cfg_df_customer table
cfg_df_customer = spark.read.table("cfg_Customer")

# Read cfg_df_Transactions table
cfg_df_Transactions = spark.read.table("cfg_Transactions")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Connecting to Oracle Autonomous Database using JDBC Divers for Customer and Transaction Tables

# COMMAND ----------


DB_URL_TLS="""jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-sanjose-1.oraclecloud.com))(connect_data=(service_name=gbdc777c2001e9f_transactiondbadb_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"""

jdbcDriver="oracle.jdbc.driver.OracleDriver"


# COMMAND ----------

# DBTITLE 1,Reading Tables from Oracle
# Reading Customer_Raw table from Oracle
Customer_df=spark.read\
    .format("jdbc")\
    .option("url",DB_URL_TLS)\
    .option("dbtable","Customer_Raw")\
    .option("user","ADMIN")\
    .option("password","Welcome123456")\
    .option("fetchsize",500000)\
    .option("driver",jdbcDriver)\
    .load()

# Reading Transactions_Raw table from Oracle
Transactions_df=spark.read\
    .format("jdbc")\
    .option("url",DB_URL_TLS)\
    .option("dbtable","Transactions_Raw")\
    .option("user","ADMIN")\
    .option("password","Welcome123456")\
    .option("fetchsize",500000)\
    .option("driver",jdbcDriver)\
    .load()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Product-Category-Subcategory from dbfs

# COMMAND ----------

#read Product-Category-Subcategory
schema="prod_cat_code integer,prod_cat string,prod_sub_cat_code integer,prod_subcat string"
ProdCatSubCat_df=spark.read.option("header",True).schema(schema).csv("dbfs:/FileStore/tables/Product_Category_Subcategory.csv")
