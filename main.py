import os
from datetime import datetime

from extract import extract
from load_to_redshift import RedshiftLoader
from load_to_s3 import s3Loader
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

api_key = "4JKLD6C4SJYP77PN"
top_companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK.A', 'NVDA', 'JPM', 'V']
file_path = f"C:\\Users\\ratho\\PycharmProjects\\stock_market\\stock_data\\combined_df.csv"

combined_df = pd.DataFrame()

for symbol in top_companies:
    df = extract(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}")
    combined_df = pd.concat([combined_df, df], axis=0)

combined_df.to_csv(file_path, index=False)

date_folder = datetime.now().strftime('%Y-%m-%d')

s3_loader = s3Loader()
s3_loader.upload_file_to_s3(file_path, date_folder, date_folder)

redshift_loader = RedshiftLoader()
redshift_loader.from_s3(folder_name=date_folder, table_name=os.getenv("FACT_STOCK_TABLE"))

redshift_loader.from_s3(folder_name="stock_split_data", table_name= os.getenv("STOCK_SPLIT_TABLE"))

