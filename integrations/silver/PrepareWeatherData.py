from pyspark.sql.functions import explode, col, arrays_zip

bronze_path = "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/fa6ef4f1-de78-4ea3-8d12-9ef83a59653e/Files/Weather_data/New_York_weather_data"
bronze_df = spark.read.format("delta").load(bronze_path)

silver_df = (bronze_df
    .select(explode(arrays_zip(
        col("daily.time"), 
        col("daily.temperature_2m_max"), 
        col("daily.temperature_2m_min"),
        col("daily.precipitation_sum")
    )).alias("weather_data"))
    .select(
        col("weather_data.time").alias("time"), 
        col("weather_data.temperature_2m_max").cast("float").alias("temperature_2m_max"), 
        col("weather_data.temperature_2m_min").cast("float").alias("temperature_2m_min"),
        col("weather_data.precipitation_sum").cast("float").alias("precipitation_sum")
    )
)

save_path = f"Files/Weather_data/weather_silver"
silver_df.write.format("delta").mode("overwrite").save(save_path)