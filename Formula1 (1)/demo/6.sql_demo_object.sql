-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO


-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

USE DEMO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES

-- COMMAND ----------

describe EXTENDED race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

create table race_result_sql
as
select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_result_sql

-- COMMAND ----------

drop table demo.race_result_sql

-- COMMAND ----------

show tables in demo  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_python_ext_py")

-- COMMAND ----------

desc extended demo.race_results_python_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP, 
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/f1dlpaliwal/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLE IN DEMO;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_result
AS
SELECT * FROM demo.race_results_python
WHERE race_year=2020

-- COMMAND ----------

select * from v_race_result

-- COMMAND ----------

create or replace global temp view gv_race_result
as
select * from demo.race_results_python where race_year=2012

-- COMMAND ----------

select * from global_temp.gv_race_result

-- COMMAND ----------

