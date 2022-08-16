# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import *

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# COMMAND ----------

df_dim_node_detail = spark.sql("""SELECT * FROM  connectedhome.dim_node_detail""")
df_fact_ds_md_utilization = spark.sql("""SELECT * FROM  connectedhome.fact_ds_md_utilization""")
df_fact_node_attainability = spark.sql("""SELECT * FROM  connectedhome.fact_node_attainability""")
df_fact_node_churn = spark.sql("""SELECT * FROM  connectedhome.fact_node_churn""")
df_fact_node_ibro_revenue = spark.sql("""SELECT * FROM  connectedhome.fact_node_ibro_revenue""")
df_fact_us_port_utilization = spark.sql("""SELECT * FROM  connectedhome.fact_us_port_utilization""")

# COMMAND ----------

df_list = [df_dim_node_detail,
           df_fact_ds_md_utilization,
           df_fact_node_attainability,
           df_fact_node_churn,
           df_fact_node_ibro_revenue,
           df_fact_us_port_utilization 
]

# COMMAND ----------

df_fact_node_attainability.display()

# COMMAND ----------


