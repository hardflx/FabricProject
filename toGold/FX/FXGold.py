from pyspark.sql import functions as F

df_fx = spark.read.format("delta").load("abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/Daily_FX/Daily_FX_delta_table")

fact_daily_fx = df_fx.select(
    F.col("Date").cast("date"),
    F.col("Exchange_Rate").cast("double")
).orderBy("Date")

fact_daily_fx.write.mode("overwrite").format("delta").saveAsTable("FX.FactDailyFX")