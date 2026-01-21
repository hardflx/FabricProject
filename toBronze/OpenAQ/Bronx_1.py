import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
API_KEY = ""
SENSOR_ID = 1662910
BASE_URL = f"https://api.openaq.org/v3/sensors/{SENSOR_ID}/measurements"
HEADERS = {"x-api-key": API_KEY}
LIMIT = 100
MAX_PAGES = 60
today = datetime.utcnow()
start_dt = (today - relativedelta(months=5)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
end_dt = (today - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(seconds=1)
datetime_from = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
datetime_to   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
def fetch_pages():
    all_results = []
    for page in range(1, MAX_PAGES + 1):
        params = {
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
            "limit": LIMIT,
            "page": page
        }
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if not results:
            break 
        all_results.extend(results)
    return all_results

records = fetch_pages()
df = pd.DataFrame(records)

spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").format("delta").save("abfss://0fa79899-c8de-4148-81cd-d17d4453fa56@onelake.dfs.fabric.microsoft.com/fa6ef4f1-de78-4ea3-8d12-9ef83a59653e/Files/openaq_measurements/OpenAQ_Bronx_1")