# Databricks notebook source
# MAGIC %md
# MAGIC ### Access datafram using SQL
# MAGIC ### Objective
# MAGIC - 1.Create temporary view on datafram
# MAGIC - 2.Access view from SQL cell
# MAGIC - 3.Access view from Python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_result_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_result_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View
# MAGIC - 1.Create global view on datafram
# MAGIC - 2.Access view from SQL cell
# MAGIC - 3.Access view from Python cell
# MAGIC - 4.Acces view from another notebook
# MAGIC

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_result

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_result").display()

# COMMAND ----------

