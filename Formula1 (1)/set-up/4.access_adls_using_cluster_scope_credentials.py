# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.f1dlpaliwal.dfs.core.windows.net",
#     "63kLx7oNj0apTCMcZdRMOXkCJVH0q/mceEAWQA6FRZjPpNmJuHSNzeqJvgHYnei57V9PqsNm07r9+AStoIMwZw=="
# )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlpaliwal.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlpaliwal.dfs.core.windows.net/circuits.csv"))