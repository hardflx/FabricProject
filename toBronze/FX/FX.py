import yfinance as yf
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


today = datetime.now() 
    
start_date = (today - relativedelta(months=5)).replace(day=1)
end_date = (today - relativedelta(months=1)).replace(day=1) - relativedelta(days=1)

raw_data = yf.download(
    tickers="EURUSD=X", 
    start=start_date.strftime('%Y-%m-%d'), 
    end=end_date.strftime('%Y-%m-%d'), 
    threads=False
)
    
if not raw_data.empty:
    pdf_raw = raw_data.reset_index()

    if isinstance(pdf_raw.columns, pd.MultiIndex):
        pdf_raw.columns = [col[0] if col[1] == '' else col[0] for col in pdf_raw.columns]

    pdf_raw.columns = [c.replace(' ', '_').replace('.', '_') for c in pdf_raw.columns]
        
    df_bronze = spark.createDataFrame(pdf_raw)
    target_path = "Files/Bank/Daily_FX_raw"
    df_bronze.write.mode("overwrite").format("delta").save(target_path)