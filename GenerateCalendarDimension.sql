-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Intro
-- MAGIC 
-- MAGIC This notebook creates a calendar dimension (Also known as date dimension) as a Delta Lake table and registers it in the Hive Metastore. The calendar dimension can then be used to perform data warehouse style queries in the data lake.
-- MAGIC 
-- MAGIC It is assumed that the reader is familiar with connecting to the desired storage account.
-- MAGIC 
-- MAGIC The date range can be changed to meet your needs in the cell below. The date range could also be made dynamic.

-- COMMAND ----------

-- DBTITLE 1,Generate Raw Dates
-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import explode, sequence, to_date
-- MAGIC 
-- MAGIC beginDate = '2000-01-01'
-- MAGIC endDate = '2050-12-31'
-- MAGIC 
-- MAGIC (
-- MAGIC   spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
-- MAGIC     .createOrReplaceTempView('dates')
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Examine dates temporary view
select * from dates     

-- COMMAND ----------

-- DBTITLE 1,Select Calendar Dimension Columns
-- Use this cell to develop a query that returns the desired columns. This cell can be deleted when development is completed.
select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateInt,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) AS DayOfWeek,
  weekday(calendarDate) + 1 as DayOfWeekStartMonday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  dayofyear(calendarDate) as DayOfYear,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear,
  /* Use fiscal periods needed by organization fiscal calendar */
  case
    when month(calendarDate) >= 10 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearOctToSep,
  (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep,
  case
    when month(calendarDate) >= 7 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearJulToJun,
  (month(calendarDate) + 5) % 12 + 1 AS FiscalMonthJulToJun
from
  dates
order by
  calendarDate

-- COMMAND ----------

-- DBTITLE 1,Save as Delta Table
-- use the query developed above to load the calendar dimension into a Delta Lake table
create or replace table dim_calendar
using delta
location 'abfss://datalake@stsharedckdev001.dfs.core.windows.net/gold/dim_calendar'
as select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateInt,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) as DayOfWeek,
  weekday(calendarDate) + 1 as DayOfWeekStartMonday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  dayofyear(calendarDate) as DayOfYear,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear,
  /* Use fiscal periods needed by organization fiscal calendar */
  case
    when month(calendarDate) >= 10 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearOctToSep,
  (month(calendarDate) + 2) % 12 + 1 as FiscalMonthOctToSep,
  case
    when month(calendarDate) >= 7 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearJulToJun,
  (month(calendarDate) + 5) % 12 + 1 as FiscalMonthJulToJun
from
  dates

-- COMMAND ----------

-- DBTITLE 1,Optimize
optimize dim_calendar zorder by (calendarDate)

-- COMMAND ----------

-- DBTITLE 1,Examine the Calendar Dimension
select * from dim_calendar
