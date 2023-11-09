# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://conteneurgrp4@stockagegrp4.blob.core.windows.net",
mount_point = "/mnt/",
extra_configs = {"fs.azure.account.key.stockagegrp4.blob.core.windows.net":dbutils.secrets.get(scope = "grp4scope", key = "grp4secret2")})

# COMMAND ----------

dfTest = spark.read.csv("/mnt/test.csv", header=True, inferSchema=True)
dfTest.drop("_c0")

dfTest = dfTest.withColumnRenamed("N°_département_(BAN)","N°_département_BAN")
dfTest = dfTest.withColumnRenamed("Code_postal_(BAN)","Code_postal_BAN")
dfTest = dfTest.withColumnRenamed("Nom__commune_(Brut)","Nom__commune_Brut")
dfTest = dfTest.withColumnRenamed("Code_INSEE_(BAN)","Code_INSEE_BAN")
dfTest = dfTest.withColumnRenamed("Code_postal_(brut)","Code_postal_brut")

# COMMAND ----------

dfTrain = spark.read.csv("/mnt/train.csv", header=True, inferSchema=True)
dfTrain.drop("_c0")

dfTrain = dfTrain.withColumnRenamed("N°_département_(BAN)","N°_département_BAN")
dfTrain = dfTrain.withColumnRenamed("Code_postal_(BAN)","Code_postal_BAN")
dfTrain = dfTrain.withColumnRenamed("Nom__commune_(Brut)","Nom__commune_Brut")
dfTrain = dfTrain.withColumnRenamed("Code_INSEE_(BAN)","Code_INSEE_BAN")
dfTrain = dfTrain.withColumnRenamed("Code_postal_(brut)","Code_postal_brut")

# COMMAND ----------

dfVal = spark.read.csv("/mnt/val.csv", header=True, inferSchema=True)
dfVal.drop("_c0")


dfVal = dfVal.withColumnRenamed("N°_département_(BAN)","N°_département_BAN")
dfVal = dfVal.withColumnRenamed("Code_postal_(BAN)","Code_postal_BAN")
dfVal = dfVal.withColumnRenamed("Nom__commune_(Brut)","Nom__commune_Brut")
dfVal = dfVal.withColumnRenamed("Code_INSEE_(BAN)","Code_INSEE_BAN")
dfVal = dfVal.withColumnRenamed("Code_postal_(brut)","Code_postal_brut")

# COMMAND ----------

dfTrain.write.mode("overwrite").saveAsTable("TrainTable")


# COMMAND ----------

dfVal.write.mode("overwrite").saveAsTable("ValTable")


# COMMAND ----------

dfTest.write.mode("overwrite").saveAsTable("TestTable")


# COMMAND ----------

tables = spark.catalog.listTables()
for table in tables:
   print(table.name)

# COMMAND ----------


