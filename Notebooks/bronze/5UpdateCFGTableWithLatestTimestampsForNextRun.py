# Databricks notebook source
# DBTITLE 1,Running Previous files for variable reference
# MAGIC %run ./3IncrementalLoading/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Old Watermark Value is Available

# COMMAND ----------

# Customer
print("Original Customer_Watermark_Value:", Customer_Watermark_Value)
# Transactions
print("Original Transactions_Watermark_Value:", Transactions_Watermark_Value)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update cfg Table With Latest Timestamps

# COMMAND ----------

# Customer
import pymssql
conn = pymssql.connect(server='adbworkshopserver.database.windows.net', user='serveradmin@adbworkshopserver.database.windows.net',
password='Welcome@123', database='AADT')
cursor = conn.cursor()

if cfg_df_customer.count() > 0:
    Var = "update"
    if Customer_Watermark_Value is not None:
        Customer_Watermark_Value = str(Customer_Watermark_Value).split(".")[0]
        print(Customer_Watermark_Value)
        SqlString_Customer = "update cfg_databricks_customer set WATERMARK_VALUE='" + Customer_Watermark_Value + "' where Dest_Table='M_Delta_Customer'"
        cursor.execute(SqlString_Customer)
        print(SqlString_Customer)
    else:
        print("Customer_Watermark_Value is Null. Skip the update.")
else:
    Customer_Customer_Watermark_Value = str(Customer_Watermark_Value).split(".")[0]
    print(Customer_Watermark_Value)
    SqlString_Customer = "INSERT INTO cfg_databricks_customer (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
                         "VALUES('Oracle', 'ADMIN.Customer_Raw', 'M_Delta_Customer', 'CREATED_MODIFIED_DATE', CONVERT(datetime, '" + str(Customer_Watermark_Value) + "', 120), 'Incremental', 'Succeeded')"
    print(SqlString_Customer)
    cursor.execute(SqlString_Customer)

conn.commit()
conn.close()

# COMMAND ----------

# Transactions
import pymssql
conn = pymssql.connect(server='adbworkshopserver.database.windows.net', user='serveradmin@adbworkshopserver.database.windows.net',
password='Welcome@123', database='AADT')
cursor = conn.cursor()

if cfg_df_Transactions.count() > 0 :
    Var="update"
    if  Transactions_Watermark_Value is not None:    
            Transactions_Watermark_Value=str(Transactions_Watermark_Value).split(".")[0]
            print(Transactions_Watermark_Value)
            SqlString_Transactions="update cfg_databricks_transactions set WATERMARK_VALUE='"+Transactions_Watermark_Value+"' where Dest_Table='M_Delta_Transactions'"
            cursor.execute(SqlString_Transactions)
            print(SqlString_Transactions)
    else:
            print("Transactions_Watermark_Value is Null. Skip the update.")
else:
    Transactions_Watermark_Value=str(Transactions_Watermark_Value).split(".")[0]
    print(Transactions_Watermark_Value)
    SqlString_Transactions= "INSERT INTO cfg_databricks_transactions (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
                            "VALUES('Oracle', 'ADMIN.Transactions_Raw', 'M_Delta_Transactions', 'TRAN_DATE', '" + str(Transactions_Watermark_Value) + "', 'Incremental', 'Succeeded')"
    print(SqlString_Transactions)
    cursor.execute(SqlString_Transactions)

conn.commit()
conn.close()
