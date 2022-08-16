# Databricks notebook source
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# COMMAND ----------

# need to query KPIs accessibility, attainability, service_quality, downtime
# tables fromd delta_hem: modem_accessibility_daily, modem_attainability_daily, modem_service_quality_daily, 
# will use KPI_full_day (excludes maintenance window)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_ADD(Max(event_date), -15)
# MAGIC FROM delta_hem.modem_accessibility_daily;

# COMMAND ----------

# DBTITLE 1,Set 15 days history review window
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW stt_date
# MAGIC AS 
# MAGIC SELECT DATE_ADD(Max(event_date), -15)
# MAGIC FROM delta_hem.modem_accessibility_daily;

# COMMAND ----------

# DBTITLE 1,Extract features
df_mop_validation = spark.sql(
    '''SELECT a.mac,
       b.cmts,
       b.phub,
       b.shub,
       b.modem_manufacturer,
       b.model,
       b.downtime_full_day,
       b.accessibility_perc_full_day,
       c.attainability_pct,
--        d.utilization_max,
--        d.utilization_95,
--        d.utilization_98,
--        d.us_util_index_percentage,
       e.codeword_error_rate,
       e.cmts_cm_us_signal_noise_avg,
       b.event_date,
       CASE
         WHEN b.system = 'SS' THEN 'Legacy'
         WHEN b.system = 'MAESTRO' THEN 'Ignite'
       end AS system
FROM   (SELECT DEFAULT.mop_inf_dly_live.mac
        FROM   DEFAULT.mop_inf_dly_live
        WHERE  DEFAULT.mop_inf_dly_live.mac IS NOT NULL
        GROUP  BY DEFAULT.mop_inf_dly_live.mac) AS a
       LEFT JOIN (SELECT *
                  FROM   delta_hem.modem_accessibility_daily
                  WHERE  event_date > (SELECT *
                                       FROM   stt_date)
                         AND cmts IS NOT NULL
                         AND phub IS NOT NULL
                         AND shub IS NOT NULL
                         AND modem_manufacturer IS NOT NULL
                         AND model IS NOT NULL
                         AND event_date IS NOT NULL) AS b
              ON a.mac = b.mac
       LEFT JOIN (SELECT *
                  FROM   delta_hem.modem_attainability_daily
                  WHERE  event_date > (SELECT *
                                       FROM   stt_date)) AS c
              ON a.mac = c.cm_mac_addr
--        LEFT JOIN (SELECT *
--                   FROM   delta_hem.ccap_us_port_util_daily_fct
--                   WHERE  date_key > (SELECT *
--                                      FROM   stt_date)) AS d
--               ON b.cmts = d.cmts_host_name
       LEFT JOIN (SELECT *
                  FROM   delta_hem.modem_service_quality_daily
                  WHERE  event_date > (SELECT *
                                       FROM   stt_date)) AS e
              ON a.mac = e.mac         
         ''')

# COMMAND ----------

df_mop_validation.display(5)

# COMMAND ----------

# DBTITLE 1,Create temp view of table
df_mop_validation.createOrReplaceTempView("vw_mop_validation")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.mop_validation;
# MAGIC CREATE TABLE IF NOT EXISTS default.mop_validation
# MAGIC (
# MAGIC SELECT * 
# MAGIC FROM vw_mop_validation
# MAGIC );

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


