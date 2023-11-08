# Databricks notebook source
tables = spark.catalog.listTables()
for table in tables:
   print(table.name)

# COMMAND ----------

dfTrain = spark.read.table("traintable")
dfVal = spark.read.table("valtable")
dfTest = spark.read.table("testtable")

# COMMAND ----------

colToDrop = ['Configuration_installation_chauffage_n°2', 'Facteur_couverture_solaire_saisi', 'Cage_d\'escalier', 'Type_générateur_froid', 'Type_émetteur_installation_chauffage_n°2', 'Surface_totale_capteurs_photovoltaïque', 'Type_énergie_n°3', 'Type_générateur_n°1_installation_n°2', 'Description_générateur_chauffage_n°2_installation_n°2', 'Facteur_couverture_solaire', 'Qualité_isolation_plancher_haut_comble_aménagé', 'Qualité_isolation_plancher_haut_toit_terrase', 'Surface_habitable_immeuble', 'Nom_commune_(Brut)', 'N°_département_(BAN)', 'Type_bâtiment']


# COMMAND ----------

dfTrainClean = dfTrain.drop(colToDrop, axis=1, inplace=True)
dfTestClean = dfTest.drop(colToDrop, axis=1, inplace=True)
dfValClean = dfVal.drop(colToDrop, axis=1, inplace=True)

# COMMAND ----------

dfTrain.count()
