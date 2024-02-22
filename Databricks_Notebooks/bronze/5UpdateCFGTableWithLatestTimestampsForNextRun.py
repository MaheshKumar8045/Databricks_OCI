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


if cfg_df_customer.count() > 0 :
    Var="update"
    if  Customer_Watermark_Value is not None:    
        Customer_Watermark_Value=str(Customer_Watermark_Value).split(".")[0]
        print(Customer_Watermark_Value)
        # Update the table using Spark SQL
        spark.sql(f"UPDATE TABLE cfg_customer SET WATERMARK_VALUE='{Customer_Watermark_Value}' WHERE Dest_Table='M_Delta_Customer'")
        # Print the SQL statement
        print(f"UPDATE TABLE cfg_customer SET WATERMARK_VALUE='{Customer_Watermark_Value}' WHERE Dest_Table='M_Delta_Customer'")
    else:
        print("Customer_Watermark_Value is Null. Skip the update.")
else:
    Customer_Watermark_Value=str(Customer_Watermark_Value).split(".")[0]
    print(Customer_Watermark_Value)
    # Insert into the table using Spark SQL
    spark.sql(f"INSERT INTO TABLE cfg_customer (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
              f"VALUES('Oracle', 'ADMIN.Customer_Raw', 'M_Delta_Customer', 'TRAN_DATE', '{Customer_Watermark_Value}', 'Incremental', 'Succeeded')")
    # Print the SQL statement
    print(f"INSERT INTO TABLE cfg_customer (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
          f"VALUES('Oracle', 'ADMIN.Customer_Raw', 'M_Delta_Customer', 'TRAN_DATE', '{Customer_Watermark_Value}', 'Incremental', 'Succeeded')")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cfg_customer

# COMMAND ----------



if cfg_df_Transactions.count() > 0 :
    Var="update"
    if  Transactions_Watermark_Value is not None:    
        Transactions_Watermark_Value=str(Transactions_Watermark_Value).split(".")[0]
        print(Transactions_Watermark_Value)
        # Update the table using Spark SQL
        spark.sql(f"UPDATE TABLE cfg_transactions SET WATERMARK_VALUE='{Transactions_Watermark_Value}' WHERE Dest_Table='M_Delta_Transactions'")
        # Print the SQL statement
        print(f"UPDATE TABLE cfg_transactions SET WATERMARK_VALUE='{Transactions_Watermark_Value}' WHERE Dest_Table='M_Delta_Transactions'")
    else:
        print("Transactions_Watermark_Value is Null. Skip the update.")
else:
    Transactions_Watermark_Value=str(Transactions_Watermark_Value).split(".")[0]
    print(Transactions_Watermark_Value)
    # Insert into the table using Spark SQL
    spark.sql(f"INSERT INTO TABLE cfg_transactions (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
              f"VALUES('Oracle', 'ADMIN.Transactions_Raw', 'M_Delta_Transactions', 'TRAN_DATE', '{Transactions_Watermark_Value}', 'Incremental', 'Succeeded')")
    # Print the SQL statement
    print(f"INSERT INTO TABLE cfg_transactions (SOURCE, SOURCE_TABLE, DEST_TABLE, WATERMARK_COLUMN, WATERMARK_VALUE, LOAD_FLAG, STATUS) " \
          f"VALUES('Oracle', 'ADMIN.Transactions_Raw', 'M_Delta_Transactions', 'TRAN_DATE', '{Transactions_Watermark_Value}', 'Incremental', 'Succeeded')")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cfg_transactions
