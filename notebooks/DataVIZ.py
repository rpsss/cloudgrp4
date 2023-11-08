# Databricks notebook source
dfTrain = spark.read.table("traintable")

# COMMAND ----------

display(dfTrain)
