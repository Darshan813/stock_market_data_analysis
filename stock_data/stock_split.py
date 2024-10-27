import yfinance as yf
import pandas as pd
from load_to_s3 import s3Loader

top_companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V']

all_splits = {}

for i in top_companies:
    stock = yf.Ticker(i)
    splits = stock.splits
    all_splits[i] = splits


splits_df = pd.DataFrame.from_dict(all_splits, orient='index').stack().reset_index()
splits_df.columns = ['Symbol', 'Date', 'Split']
splits_df['Date'] = pd.to_datetime(splits_df['Date']).dt.date

s3_loader = s3Loader()

s3_loader.upload_df(splits_df, folder_name = "stock_split_data", file_name="split")



