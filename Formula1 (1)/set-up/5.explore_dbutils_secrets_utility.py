# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

formula1_account_key =dbutils.secrets.get(scope= "formula1-scope",key="formula1dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.f1dlpaliwal.dfs.core.windows.net",formula1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlpaliwal.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlpaliwal.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/amazon/data20K/_common_metadata"))

# COMMAND ----------

# dbutils.secrets.listScopes()
dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope="formula1-scope" , key="formula1dl-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlpaliwal.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1dlpaliwal.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1dlpaliwal.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlpaliwal.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlpaliwal.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

