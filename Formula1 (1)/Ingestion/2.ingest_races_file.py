# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)

])

# COMMAND ----------

races_df = spark.read.csv("/mnt/f1dlpaliwal/raw/races.csv",header=True,schema=races_schema)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,to_timestamp,concat

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp(),)\
    .withColumn("races_timestamp",to_timestamp(concat(col("date") , lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("races_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC
# MAGIC %fs ls /mnt/f1dlpaliwal/processed/races

# COMMAND ----------

di\\splay(spark.read.parquet("/mnt/f1dlpaliwal/processed/races"))

# COMMAND ----------

