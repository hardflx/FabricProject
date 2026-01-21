from pyspark.sql import functions as F
from pyspark.sql.window import Window
from notebookutils import mssparkutils

base_silver_path = "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/NYC_taxi_data/delta_tables_for_each_borough/"
borough_folders = [f.name for f in mssparkutils.fs.ls(base_silver_path)]

borough_dfs = []

for borough in borough_folders:
    borough_name = borough.strip("/")
    suffix = borough_name.replace('_', '').replace(' ', '')
    source_path = f"{base_silver_path}{borough_name}"

    df_raw = spark.read.format("delta").load(source_path)

    df_clean = df_raw.filter(
        (F.col("total_amount") > 0) & (F.col("total_amount") <= 200)
    ).withColumn(
        "formatted_time", 
        F.concat(F.lpad(F.col("time").cast("string"), 2, "0"), F.lit(":00:00"))
    )

    hourly_counts = df_clean.groupBy("Date", "formatted_time").count()

    window_spec = Window.partitionBy("Date").orderBy(F.col("count").desc(), F.col("formatted_time").asc())
    
    peak_hour_df = hourly_counts.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .select(F.col("Date"), F.col("formatted_time").alias("peak_trip_hour"))

    daily_stats = df_clean.groupBy("Date").agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("total_amount"), 2).alias("average_trip_cost"),
        F.round(F.sum("total_amount"), 2).alias("total_amount_earned"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips_earned"),
        F.round(F.avg("trip_distance"), 2).alias("average_trip_distance"),
        F.round(F.avg("passenger_count"), 1).alias("average_passengers")
    )

    final_borough_stats = daily_stats.join(peak_hour_df, on="Date", how="left")

    for col_name in final_borough_stats.columns:
        if col_name != "Date":
            final_borough_stats = final_borough_stats.withColumnRenamed(col_name, f"{col_name}_{suffix}")

    borough_dfs.append(final_borough_stats)


master_df = borough_dfs[0]
for next_df in borough_dfs[1:]:
    master_df = master_df.join(next_df, on="Date", how="full")


amt_cols = [c for c in master_df.columns if "total_amount_earned_" in c]
trip_cols = [c for c in master_df.columns if "total_trips_" in c]
tip_cols = [c for c in master_df.columns if "total_tips_earned_" in c]

master_df = master_df.withColumn("total_trips_New_York", sum([F.coalesce(F.col(c), F.lit(0)) for c in trip_cols]))
master_df = master_df.withColumn("total_amount_earned_New_York", F.round(sum([F.coalesce(F.col(c), F.lit(0)) for c in amt_cols]), 2))
master_df = master_df.withColumn("total_tips_earned_New_York", F.round(sum([F.coalesce(F.col(c), F.lit(0)) for c in tip_cols]), 2))

master_df = master_df.withColumn("average_trip_cost_New_York", 
    F.round(F.col("total_amount_earned_New_York") / F.col("total_trips_New_York"), 2))

master_df = master_df.orderBy("Date")

master_df.write.mode("overwrite").format("delta").saveAsTable("Taxi.FactTaxiDaily")