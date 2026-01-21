import json
import requests
from datetime import datetime
import great_expectations as gx
from pyspark.sql.functions import col, format_string

today_date = datetime.now().strftime("%d_%m_%Y")
silver_path = f"abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/7c73dbc5-ec63-4226-b7da-6e0e0f90e731/Files/Weather_data/weather_silver"
df = spark.read.format("delta").load(silver_path)

def send_dq_report(res):
    stats = res['statistics']
    status = "PASSED" if res['success'] else "FAILED"
    report = (
        f"**Data Quality report for NYC Weather data for {today_date}**\n"
        f"**Status:** {status}\n"
        f"**Success Rate:** {stats['success_percent']:.1f}%\n"
        f"**Checks Run:** {stats['evaluated_expectations']}\n"
        f"**Failures:** {stats['unsuccessful_expectations']}"
    )
    webhook_url_discord = ""
    requests.post(webhook_url_discord, json={"content": report})

context = gx.get_context()

datasource_name = "weather_data"
datasource = context.data_sources.add_spark(name=datasource_name)

asset_name = "weather_asset"
asset = datasource.add_dataframe_asset(name=asset_name)

suite_name = "weather_suite"
suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

batch_request = asset.build_batch_request(options={"dataframe": df})

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

validator.expect_column_values_to_not_be_null("time")
validator.expect_column_values_to_be_between(
    "temperature_2m_max", min_value=-30, max_value=50
)
validator.expect_column_values_to_be_between(
    "temperature_2m_min", min_value=-40, max_value=40
)
validator.expect_column_values_to_be_between(
    "precipitation_sum", min_value=0, max_value=300
)
val_result = validator.validate()

send_dq_report(val_result)

gold_df = df.select("time", "temperature_2m_max", "temperature_2m_min", "precipitation_sum")

gold_df.write.format("delta").mode("overwrite").saveAsTable("Weather.weather_gold_final")
gold_df_formatted = gold_df.select(
    "time",
    format_string("%.2f", col("temperature_2m_max")).alias("temperature_2m_max"),
    format_string("%.2f", col("temperature_2m_min")).alias("temperature_2m_min"),
    format_string("%.2f", col("precipitation_sum")).alias("precipitation_sum")
).orderBy(col("time").desc()).limit(2)

json_records = [json.loads(row) for row in gold_df_formatted.toJSON().collect()]

lakehouse_file_path = "/lakehouse/default/Files/Weather_data/weather_alert.json"
with open(lakehouse_file_path, "w") as f:
    json.dump(json_records, f)

webhook_url_power_automate = ""
payload = {"rows": json_records}
requests.post(webhook_url_power_automate, json=payload)