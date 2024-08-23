import csv
import pandas as pd
# In the transform file, I'll need to change data type, add a last_refreshed column. Would only need to concat the new data into the
# file and no need to process the whole file, will pull the historical data from gcs, and then will add the new data and then push the data
# into it.

global_df = pd.DataFrame()

def transform(data, stock_name):
    print(f"Making stock data frame for {stock_name}")
    stock_df = pd.DataFrame(data)

    stock_df.reset_index(inplace=True)
    stock_df["date"] = stock_df["index"]

    # Adding Symobol Column
    val = stock_df.iloc[1, 1]
    stock_df["Symbol"] = val

    # Adding last_refreshed Column
    last_refreshed = stock_df.iloc[2, 1]
    stock_df["Last_refreshed"] = last_refreshed

    # To drop the first 5 rows which are the meta data
    stock_df = stock_df.iloc[5:]

    stock_df.reset_index(inplace=True, drop=True)

    stock_df = stock_df.drop(columns=["index", "Meta Data"])

    time_series_df = pd.json_normalize(stock_df["Time Series (Daily)"])

    stock_df = pd.concat([time_series_df, stock_df], axis=1)
    stock_df = stock_df.drop(columns="Time Series (Daily)")

    stock_df.rename(
        columns={"1. open": "Open", "2. high": "High", "3. low": "Low", "4. close": "Close", "5. volume": "Volume"},
        inplace=True)

    order = ["Symbol", "date", "Open", "High", "Low", "Close", "Volume", "Last_refreshed"]

    stock_df["date"] = pd.to_datetime(stock_df["date"], errors="coerce")
    stock_df['Open'] = stock_df['Open'].astype(float)
    stock_df['High'] = stock_df['High'].astype(float)
    stock_df['Low'] = stock_df['Low'].astype(float)
    stock_df['Close'] = stock_df['Close'].astype(float)
    stock_df['Volume'] = stock_df['Volume'].astype(int)
    stock_df["Last_refreshed"] = pd.to_datetime(stock_df["Last_refreshed"], errors="coerce")

    stock_df[["Open", "High", "Low", "Close", "Volume", "Last_refreshed"]] = stock_df[
        ["Open", "High", "Low", "Close", "Volume", "Last_refreshed"]].round(2)

    stock_df["year"] = stock_df["date"].dt.year
    stock_df["week_no"] = stock_df["date"].dt.isocalendar().week
    stock_df["month"] = stock_df["date"].dt.month
    stock_df["day_no"] = stock_df["date"].dt.dayofweek

    print(f"Data Frame Created Successfully for {stock_name}")

    return stock_df