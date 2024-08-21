# Databricks notebook source
# MAGIC  %md
# MAGIC ## Spark Join Transformation
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id < 70")\
    .withColumnRenamed("name","circuit_name")
display(circuit_df)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name","race_name")

# COMMAND ----------

race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"inner") \
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.race_country.alias("country"),races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_id ) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outer Join

# COMMAND ----------

# Left Outer Join
race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"left") \
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.race_country.alias("country"),races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# right outer join
race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"right") \
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.race_country.alias("country"),races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# full outer join
race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"full") \
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.race_country.alias("country"),races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semi Join

# COMMAND ----------

race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"semi")

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti Join

# COMMAND ----------

race_circuits_id =  circuit_df.join(races_df,circuit_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Join

# COMMAND ----------

race_circuits_id = races_df.crossJoin(circuit_df)

# COMMAND ----------

display(race_circuits_id)

# COMMAND ----------

# Cross Join is same as cross product
int (races_df.count()) * int(circuit_df.count())

# COMMAND ----------

