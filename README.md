# Databricks_OCI
In this we have three tables Customer_Raw, Transactions_Raw, ProdCatSubCat (download from resource folder and rename in oracle as follows)
in which M_Customer, M_Transactions are in Oracle autonomous DB, and M_ProdCatSubCat is in loacl dbfs
then we create and use cfg table to lookup for increamental records

Notebooks:
Notebooks are coded to run above tables but they need to be customised if different tables are used

Prerequisetes:
Cluster config 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12) or above
OJDBC driver for oracle intigration and credentials/secrets
(Optional if using Azure Sql for CFG tables) pymssql from PyPI

Running the notebooks: (Add Triggers if needed)
Create work flow seperately and run in sequence MasterWorkflow= BronzeFlow->SilverFlow->GoldFlow
Tasks under Bronze_Flow: Run in sequence  1CustomerTransactionsEmptyDeltaTables->6ViewUpdatedWatermark->4ViewUpsertedTables(optional)
Tasks under Silver_Flow: Run in parellel 7,8,9,10, then run notebook 11 depends on 7,8,9,10
Tasks under Gold_Flow: Run notebook Gold_Aggregates

Reports:
After connecting with powerbi 
Query silver.facttransactions table and generate reports