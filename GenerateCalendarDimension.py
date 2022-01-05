# Databricks notebook source
# DBTITLE 1,Generate Raw Dates
from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2000-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# DBTITLE 1,Examine dates temporary view
# MAGIC %sql select * from dates                     

# COMMAND ----------

# DBTITLE 1,Select dimCalendar Columns
# MAGIC %sql
# MAGIC 
# MAGIC -- Use this SQL notebook cell to develop query and evaluate the results. This cell can be deleted when development is completed. 
# MAGIC select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
# MAGIC   CalendarDate,
# MAGIC   year(calendarDate) AS CalendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as CalendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(calendarDate) AS DayOfWeek,
# MAGIC   weekday(calendarDate) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as DayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as DayOfYear,
# MAGIC   weekofyear(calendarDate) as WeekOfYearIso,
# MAGIC   quarter(calendarDate) as QuarterOfYear,
# MAGIC   /* Use fiscal periods needed by organization fiscal calendar */
# MAGIC   case
# MAGIC     when month(calendarDate) >= 10 then year(calendarDate) + 1
# MAGIC     else year(calendarDate)
# MAGIC   end as FiscalYearOctToSep,
# MAGIC   (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep,
# MAGIC   case
# MAGIC     when month(calendarDate) >= 7 then year(calendarDate) + 1
# MAGIC     else year(calendarDate)
# MAGIC   end as FiscalYearJulToJun,
# MAGIC   (month(calendarDate) + 5) % 12 + 1 AS FiscalMonthJulToJun
# MAGIC from
# MAGIC   dates
# MAGIC order by
# MAGIC   calendarDate

# COMMAND ----------

# DBTITLE 1,Return Query Results as Data Frame
dimDateDf = spark.sql(f"""select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
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
order by
  calendarDate
""")

# COMMAND ----------

# DBTITLE 1,Save as Delta Table
tableName = 'dimcalendar'

(
  dimDateDf.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .option('path', f'/mnt/datalake/silver/{tableName}')
    .saveAsTable(tableName)
)

# COMMAND ----------

# DBTITLE 1,Examine the Calendar Dimension
# MAGIC %sql select * from dimcalendar
