from notebookutils import mssparkutils
import pandas as pd
import numpy as np

base_path = "Files/openaq_measurements/"
silver_output_path = "abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/OpenAQ_data/OpenAQ_cleaned_tables/"

files = mssparkutils.fs.ls(base_path)

def clean_silver_data(df_spark):
    df = df_spark.toPandas()

    df["datetime_raw"] = df["period"].apply(lambda x: x.get("datetimeFrom", {}).get("utc") if isinstance(x, dict) else None)
    df["name"] = df["parameter"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    df['units'] = df['parameter'].apply(lambda x: x.get('units') if isinstance(x, dict) else None)

    df["pm_2_5_value"] = pd.to_numeric(df["value"], errors='coerce')
    df["datetime"] = pd.to_datetime(df["datetime_raw"], errors='coerce', utc=True)

    df_clean = df.dropna(subset=["pm_2_5_value", "datetime"]).copy()
    
    df_clean['pm_2_5_value'] = np.where(
        df_clean['pm_2_5_value'] < -10, 
        0, 
        np.where(
            (df_clean['pm_2_5_value'] >= -10) & (df_clean['pm_2_5_value'] < 0), 
            df_clean['pm_2_5_value'].abs(), 
            df_clean['pm_2_5_value']
        )
    )

    df_clean['date'] = df_clean['datetime'].dt.date
    df_clean['time'] = df_clean['datetime'].dt.strftime('%H:00:00')
    
    final_cols = ["name", "pm_2_5_value", "units", "date", "time"]
    return spark.createDataFrame(df_clean[final_cols])

for file_info in files:
    folder_name = file_info.name
    bronze_df = spark.read.format("delta").load(file_info.path)
    silver_df = clean_silver_data(bronze_df)

    output_path = f"{silver_output_path}{folder_name}"
    silver_df.write.mode("overwrite").format("delta").save(output_path)
