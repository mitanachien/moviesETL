# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# TODO
username = "ChiJen_Chien"

# COMMAND ----------

moviePath = f"/FileStore/{username}/dataengineering/movie/"
classicPipelinePath = f"/FileStore/{username}/dataengineering/classic/"

#landingPath = classicPipelinePath + "landing/"
rawPath = classicPipelinePath + "raw/"
bronzePath = classicPipelinePath + "bronze/"
movieSilverPath = classicPipelinePath + "silver/movie/"
genreSilverPath = classicPipelinePath + "silver/genre/"
silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
goldPath = classicPipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS FileStore_{username}")
spark.sql(f"USE FileStore_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

#%run ./utilities
from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
from pyspark.sql.functions import from_unixtime, dayofmonth, month, hour
from delta import DeltaTable
from datetime import datetime
import time