# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):

    #Secret value
    client_id = "4b2ffc85-154c-4e7c-aeba-158e02661ec1"
    tenant_id = "89bbc8b6-9f88-4e35-a3bd-15a8fa916d6d"
    client_secrets = "cAo8Q~OYRlUWZ-aNqhvM2mFBqAFn4YiSpI6U6dc7"

    #Spark Configeration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secrets,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    

    #Mount the storage Account
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mount())



# COMMAND ----------

mount_adls("f1dlpaliwal","raw")

# COMMAND ----------

mount_adls("f1dlpaliwal","processed")

# COMMAND ----------

mount_adls("f1dlpaliwal","presentation")

# COMMAND ----------

# client_id = "4b2ffc85-154c-4e7c-aeba-158e02661ec1"
# tenant_id = "89bbc8b6-9f88-4e35-a3bd-15a8fa916d6d"
# client_secrets = "cAo8Q~OYRlUWZ-aNqhvM2mFBqAFn4YiSpI6U6dc7"

# COMMAND ----------

mount_adls("f1dlpaliwal","processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mounting other Containor
# MAGIC

# COMMAND ----------

mount_adls("f1dlpaliwal","raw1")

# COMMAND ----------

dbutils.fs.unmount("/mnt/f1dlpaliwal/processed1")

# COMMAND ----------

mount_adls("f1dlpaliwal","processed1")

# COMMAND ----------

mount_adls("f1dlpaliwal","presentation1")

# COMMAND ----------

dbutils.fs.ls("/mnt/f1dlpaliwal/processed1")

# COMMAND ----------

mount_adls("f1dlpaliwal","demo1")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/f1dlpaliwal/demo1")
display(dbutils.fs.mounts())