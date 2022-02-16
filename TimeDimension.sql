-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Intro
-- MAGIC 
-- MAGIC This notebook creates a time dimension as a Delta Lake table and registers it in the Hive Metastore.
-- MAGIC 
-- MAGIC It is assumed that the reader is familiar with connecting to the desired storage account.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import explode, sequence
-- MAGIC 
-- MAGIC (
-- MAGIC   spark.sql(f"select explode(sequence(to_timestamp('1900-01-01 00:00'), to_timestamp('1900-01-01 23:59'), interval 1 minute)) as t")
-- MAGIC     .createOrReplaceTempView('time')
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Select Time Dimension Columns
-- https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

select
  cast(date_format(t, 'HHmm') as int) as Id,
  date_format(t, 'hh:mm a') as Time,
  date_format(t, 'hh') as Hour,
  date_format(t, 'HH:mm') as Time24,
  date_format(t, 'kk') as Hour24,
  date_format(t, 'a') as AmPm
from
  time

-- COMMAND ----------

-- DBTITLE 1,Save as Delta Table
-- use the query developed above to load the time dimension into a Delta Lake table
create database if not exists gold;

create or replace table gold.dim_time
using delta
location 'abfss://datalake@stsharedckdev001.dfs.core.windows.net/gold/dim_time'
as select
  cast(date_format(t, 'HHmm') as int) as Id,
  date_format(t, 'hh:mm a') as Time,
  date_format(t, 'hh') as Hour,
  date_format(t, 'HH:mm') as Time24,
  date_format(t, 'kk') as Hour24,
  date_format(t, 'a') as AmPm
from
  time

-- COMMAND ----------

-- DBTITLE 1,Optimize
optimize gold.dim_time

-- COMMAND ----------

-- DBTITLE 1,Examine the Time Dimension
select * from gold.dim_time
