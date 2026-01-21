from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.now()
start_date = (today - relativedelta(months=5)).replace(day=1).date()
end_date = (today - relativedelta(months=1)).replace(day=1).date() - relativedelta(days=1)

input_path = "Files/Raw_NYC_data/*.parquet"
base_output_path = "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/NYC_taxi_data/delta_tables_for_each_borough/"

df_raw = spark.read.option("mergeSchema", "true").parquet(input_path)

df_unified = df_raw.withColumn(
    "pickup_datetime", 
    F.coalesce("tpep_pickup_datetime", "lpep_pickup_datetime")
)

df_pre_filtered = df_unified.withColumn(
    "pickup_datetime",
    F.when(
        (F.to_date("pickup_datetime") >= start_date) & (F.to_date("pickup_datetime") <= end_date),
        F.col("pickup_datetime")
    ).otherwise(F.lit(None))
)

window_fwd = Window.orderBy(F.monotonically_increasing_id())
window_bwd = Window.orderBy(F.monotonically_increasing_id().desc())

df_fill_prep = df_pre_filtered.withColumn(
    "next_val", F.last("pickup_datetime", ignorenulls=True).over(window_bwd)
).withColumn(
    "prev_val", F.last("pickup_datetime", ignorenulls=True).over(window_fwd)
)

df_filled = df_fill_prep.withColumn(
    "pickup_datetime", 
    F.coalesce(F.col("next_val"), F.col("prev_val"))
)

df_cleaned = df_filled.dropna(subset=["pickup_datetime", "trip_distance", "total_amount"])

df_transformed = df_cleaned.withColumn("Date", F.to_date("pickup_datetime")) \
                           .withColumn("Time", F.date_format("pickup_datetime", "HH"))


df_final_filtered = df_transformed.filter(
    (F.col("Date") >= start_date) & (F.col("Date") <= end_date) &
    (F.col("trip_distance") > 0.1) & (F.col("trip_distance") <= 36.0) &
    (F.col("total_amount") > 2.5) & (F.col("total_amount") < 500.0)
)

df_standardized = df_final_filtered.select(
    "Date", "Time", "passenger_count", "trip_distance", 
    "DOLocationID", "PULocationID", "total_amount", "tip_amount"
)

df_final_silver = df_standardized.dropDuplicates()

boroughs = {
    "Bronx": [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259],
    "Queens": [7, 8, 9, 10, 15, 16, 19, 27, 28, 30, 38, 53, 56, 57, 64, 70, 73, 82, 83, 86, 92, 93, 95, 96, 98, 101, 102, 117, 121, 122, 124, 129, 130, 131, 132, 134, 135, 138, 139, 145, 146, 171, 173, 175, 179, 180, 191, 192, 193, 196, 197, 198, 201, 203, 205, 207, 215, 216, 218, 219, 223, 226, 252, 253, 258, 260],
    "Manhattan": [4, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 90, 97, 100, 103, 104, 105, 107, 113, 114, 116, 120, 125, 127, 128, 137, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263],
    "Brooklyn": [11, 14, 17, 21, 22, 25, 26, 29, 33, 34, 35, 36, 37, 39, 40, 49, 52, 54, 55, 61, 62, 63, 65, 66, 67, 71, 72, 76, 77, 80, 85, 89, 91, 106, 108, 111, 112, 123, 133, 149, 150, 154, 155, 165, 177, 178, 181, 188, 189, 190, 195, 210, 217, 222, 225, 227, 228, 255, 256, 257],
    "Staten_Island": [44, 5, 6, 23, 84, 99, 109, 110, 115, 118, 156, 172, 176, 187, 204, 206, 214, 221, 245, 251]
}

for borough, ids in boroughs.items():
    b_df = df_final_silver.filter((F.col("PULocationID").isin(ids)) | (F.col("DOLocationID").isin(ids)))
    b_df.drop("PULocationID", "DOLocationID").write.mode("overwrite").format("delta").save(f"{base_output_path}{borough}")