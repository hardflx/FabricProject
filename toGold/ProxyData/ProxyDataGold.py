from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_proxies = spark.read.format("delta").load("abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/Proxy_data/proxy_data_delta_table")

win = Window.orderBy("Date")

fact_market_daily = df_proxies.withColumn(
    "spy_daily_change", 
    F.coalesce(F.col("SPY") - F.lag("SPY", 1).over(win), F.lit(0.0))
).withColumn(
    "xli_daily_change", 
    F.coalesce(F.col("XLI") - F.lag("XLI", 1).over(win), F.lit(0.0))
)

final_market_df = fact_market_daily.select(
    F.col("Date").cast("date"),
    F.col("Vol_VIX").alias("vol_vix").cast("double"), 
    F.col("spy_daily_change").cast("double"),
    F.col("xli_daily_change").cast("double")
).orderBy("Date")

final_market_df.write.mode("overwrite").format("delta").saveAsTable("Finance.FactMarketDaily")