# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/operations

# COMMAND ----------

# Step 1: manage raw data to th bronze table
raw_to_bronze(moviePath)
# Step 2: manage bronze table to silver table
bronze_to_silver(spark)
# Step 3: update the silver table
silver_update(spark)