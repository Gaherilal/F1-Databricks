# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

v_result