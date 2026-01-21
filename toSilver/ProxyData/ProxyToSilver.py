import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

df_bronze = spark.read.format("delta").load("Files/Proxy_data/Dynamic_proxy_data")
pdf = df_bronze.toPandas()

if isinstance(pdf.columns, pd.MultiIndex):
    pdf.columns = pdf.columns.get_level_values(0)

pdf['Date'] = pd.to_datetime(pdf['Date']).dt.date

today = datetime.now() 

target_start = (today - relativedelta(months=5)).replace(day=1).date()
target_end = ((today - relativedelta(months=1)).replace(day=1) - relativedelta(days=1)).date()

full_range = pd.date_range(start=target_start, end=target_end)
pdf_complete = pd.DataFrame({'Date': full_range.date})

pdf_final = pd.merge(pdf_complete, pdf, on='Date', how='left')


cols_to_fill = [c for c in pdf_final.columns if c != 'Date']
pdf_final[cols_to_fill] = pdf_final[cols_to_fill].ffill().bfill()

df_silver_proxies = spark.createDataFrame(pdf_final)
df_silver_proxies = df_silver_proxies.withColumn("Date", F.to_date("Date"))

df_silver_proxies.write.mode("overwrite").format("delta").save("abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/Proxy_data/proxy_data_delta_table")