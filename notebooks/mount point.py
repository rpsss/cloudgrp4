# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://conteneurgrp4@stockagegrp4.blob.core.windows.net",
mount_point = "/mnt/",
extra_configs = {"fs.azure.account.key.stockagegrp4.blob.core.windows.net":dbutils.secrets.get(scope = "grp4scope", key = "grp4secret2")})

# COMMAND ----------

dfTest = spark.read.csv("/mnt/test.csv")
dfTest.drop("_c0")

# COMMAND ----------

dfTrain = spark.read.csv("/mnt/train.csv")
dfTrain.drop("_c0")

# COMMAND ----------

dfVal = spark.read.csv("/mnt/val.csv")
dfVal.drop("_c0")

# COMMAND ----------

dfTrain.write.saveAsTable("TrainTable")


# COMMAND ----------

dfVal.write.saveAsTable("ValTable")


# COMMAND ----------

dfTest.write.saveAsTable("TestTable")


# COMMAND ----------

tables = spark.catalog.listTables()
for table in tables:
   print(table.name)

# COMMAND ----------


