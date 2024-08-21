# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.csv("dbfs:/mnt/f1dlpaliwal/raw/circuits.csv",header=True,schema=circuits_schema)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().display()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls /mnt/f1dlpaliwal/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

circuit_selected_df = circuits_df.select("circuitID","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("country").alias("race_country"),("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final_df = circuits_renamed_df.withColumn("ingestin_date",current_timestamp()) \
    .withColumn("env",lit("Production"))

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dlpaliwal/processed1/circuits"))

# COMMAND ----------

df = spark.read.parquet("/mnt/f1dlpaliwal/processed1/circuits")
display(df)

# COMMAND ----------

