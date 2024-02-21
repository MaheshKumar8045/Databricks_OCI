-- Databricks notebook source
-- MAGIC %md
-- MAGIC Total sales amount by store type:

-- COMMAND ----------

SELECT
    STORE_TYPE,
    SUM(TOTAL_AMT) AS TOTAL_SALES_AMOUNT
FROM gold.FactTransactions
GROUP BY STORE_TYPE


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Total sales quantity by product category:

-- COMMAND ----------

SELECT prod_cat, SUM(QTY) AS TOTAL_QUANTITY, SUM(TOTAL_AMT) AS TOTAL_AMOUNT
FROM gold.facttransactions
GROUP BY prod_cat

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Average transaction amount by city:

-- COMMAND ----------

SELECT CITY_CODE, AVG(TOTAL_AMT) AS AVG_TRANSACTION_AMOUNT
FROM gold.facttransactions
GROUP BY CITY_CODE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Number of transactions by gender:

-- COMMAND ----------

SELECT GENDER, COUNT(TRANSACTION_ID) AS TRANSACTION_COUNT
FROM gold.facttransactions
GROUP BY GENDER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Total sales amount by customer name:

-- COMMAND ----------

SELECT CUSTOMER_NAME, SUM(TOTAL_AMT) AS TOTAL_SALES
FROM gold.facttransactions
GROUP BY CUSTOMER_NAME
