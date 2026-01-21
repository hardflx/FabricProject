import requests
from datetime import datetime, timedelta

end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=29)).strftime('%Y-%m-%d')

lat, lon = 40.7831, -73.9712
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=America%2FNew_York"

response = requests.get(url)
raw_data = response.json()

raw_df = spark.createDataFrame([raw_data])

raw_df.write.mode("overwrite").format("delta").save("Files/Weather_data/New_York_weather_data")