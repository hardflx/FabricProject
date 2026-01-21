import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.utcnow()
start_dt = (today - relativedelta(months=5)).replace(day=1).date()
end_dt = ((today - relativedelta(months=1)).replace(day=1) - relativedelta(days=1)).date()
master_calendar = pd.DataFrame({'date': pd.date_range(start=start_dt, end=end_dt).date})

silver_paths = [
    "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/OpenAQ_data/OpenAQ_cleaned_tables/OpenAQ_Bronx_1",
    "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/OpenAQ_data/OpenAQ_cleaned_tables/OpenAQ_Bronx_2"
]

dfs = [spark.read.format("delta").load(p).toPandas() for p in silver_paths]
combined = pd.concat(dfs)
combined['date'] = pd.to_datetime(combined['date']).dt.date

agg_daily = combined.groupby('date').agg(
    avg_value_pm2_5=('pm_2_5_value', 'mean'),
    peak_value_pm2_5=('pm_2_5_value', 'max')
).reset_index()

peak_lookup = combined.merge(
    agg_daily[['date', 'peak_value_pm2_5']],
    left_on=['date', 'pm_2_5_value'],
    right_on=['date', 'peak_value_pm2_5'],
    how='inner'
)

peak_time_df = peak_lookup.groupby('date')['time'].min().reset_index().rename(columns={'time': 'peak_value_hour'})
data_df = agg_daily.merge(peak_time_df, on='date', how='left')

final_df = data_df.merge(master_calendar, on='date', how='right').sort_values('date')
final_df[['avg_value_pm2_5', 'peak_value_pm2_5']] = final_df[['avg_value_pm2_5', 'peak_value_pm2_5']].ffill().bfill()
final_df['peak_value_hour'] = final_df['peak_value_hour'].ffill().bfill()

final_df['avg_value_pm2_5'] = final_df['avg_value_pm2_5'].round(1)
final_df['peak_value_hour'] = final_df['peak_value_hour'].fillna("00:00:00")

final_df = final_df.rename(columns={col: f"{col}_Bronx" for col in final_df.columns if col != 'date'})

spark_final_df = spark.createDataFrame(final_df)
gold_output_path = "Files/OpenAQ_data_per_borough_gold/Bronx"
spark_final_df_sorted = spark_final_df.orderBy("date")

spark_final_df_sorted.write.mode("overwrite").format("delta").save(gold_output_path)