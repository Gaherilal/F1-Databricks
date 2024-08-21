# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json("/mnt/f1dlpaliwal/raw/constructors.json",schema=constructor_schema)

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                                            .withColumnRenamed("constructorRef","constructor_ref") \
                                            .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/f1dlpaliwal/processed/constructors")

# COMMAND ----------

# MAGIC %fs ls /mnt/f1dlpaliwal/processed/constructors