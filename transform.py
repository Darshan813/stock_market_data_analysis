import csv
import pandas as pd
# In the transform file, I'll need to change data type, add a last_refreshed column. Would only need to concat the new data into the
# file and no need to process the whole file, will pull the historical data from gcs, and then will add the new data and then push the data
# into it.

global_df = pd.DataFrame()

def transform(data, stock_name):

    print(f"Making stock data frame for {stock_name}")
