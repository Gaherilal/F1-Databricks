# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlpaliwal.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1dlpaliwal.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1dlpaliwal.dfs.core.windows.net", "sp=rl&st=2024-08-01T06:51:47Z&se=2024-08-01T14:51:47Z&spr=https&sv=2022-11-02&sr=c&sig=hcCcmLxK1gBb7bqNTy0YHGSCq20PU0tlT1kIQXrnHJ4%3D")

# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.f1dlpaliwal.dfs.core.windows.net",
#     "63kLx7oNj0apTCMcZdRMOXkCJVH0q/mceEAWQA6FRZjPpNmJuHSNzeqJvgHYnei57V9PqsNm07r9+AStoIMwZw=="
# )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlpaliwal.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlpaliwal.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

pip install databricks-cli
