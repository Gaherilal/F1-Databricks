# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = "4b2ffc85-154c-4e7c-aeba-158e02661ec1"
tenant_id = "89bbc8b6-9f88-4e35-a3bd-15a8fa916d6d"
client_secrets = "cAo8Q~OYRlUWZ-aNqhvM2mFBqAFn4YiSpI6U6dc7"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl.dfs.core.windows.net", client_secrets)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secrets,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1dlpaliwal.dfs.core.windows.net/",
  mount_point = "/mnt/f1dlpaliwal/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dlpaliwal/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/f1dlpaliwal/demo/races"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

