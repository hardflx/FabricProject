import yfinance as yf
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


today = datetime.now()
start_date = (today - relativedelta(months=5)).replace(day=1)   
end_date = (today - relativedelta(months=1)).replace(day=1) - relativedelta(days=1)

tickers = ["^VIX", "XLI", "SPY"]
raw_proxies = yf.download(
    tickers=tickers, 
    start=start_date.strftime('%Y-%m-%d'), 
    end=end_date.strftime('%Y-%m-%d'), 
    threads=False
)

if not raw_proxies.empty:
    pdf_bronze = raw_proxies['Close'].reset_index()

    pdf_bronze.columns = [c.replace('^', 'Vol_').replace(' ', '_') for c in pdf_bronze.columns]

    spark.createDataFrame(pdf_bronze).write.mode("overwrite").format("delta").save("Files/Proxy_data/Dynamic_proxy_data")