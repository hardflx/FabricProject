from pyspark.sql import functions as F

base_path = "Files/OpenAQ_data_per_borough_gold/"
boroughs = ["Bronx", "Queens", "Manhattan", "Brooklyn", "StatenIsland"]

master_df = None


for borough in boroughs:
    path = f"{base_path}{borough}"
    
    df = spark.read.format("delta").load(path)
        
    if master_df is None:
        master_df = df
    else:
        master_df = master_df.join(df, on="date", how="outer")

if master_df:
    avg_cols = [c for c in master_df.columns if "avg_value_pm2_5" in c]
    peak_cols = [c for c in master_df.columns if "peak_value_pm2_5" in c]

    master_df = master_df.withColumn(
        "NYC_avg_value_pm2_5", 
        F.round(sum(F.col(c) for c in avg_cols) / len(avg_cols), 1)
    )

    master_df = master_df.withColumn(
        "NYC_peak_value_pm2_5", 
        F.greatest(*[F.col(c) for c in peak_cols])
    )

    master_df = master_df.orderBy("date")
    master_df.write.mode("overwrite").format("delta").saveAsTable("OpenAQ.FactOpenAQDaily")