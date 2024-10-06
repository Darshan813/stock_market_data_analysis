import boto3
from datetime import datetime

s3 = boto3.client('s3')

def upload_file_to_s3(local_file_path, bucket_name):

    date_folder = datetime.now().strftime('%Y-%m-%d')
    s3_file_path = f"{date_folder}/{date_folder}.csv"
    try:
        s3.upload_file(local_file_path, bucket_name, s3_file_path)
    except FileNotFoundError:
        print(f"File {s3_file_path} not found")
    except Exception as e:
        print(f"An exception occured {e}")

local_file_path = "C:\\Users\\ratho\\PycharmProjects\\stock_market\\stock_data\\combined_df.csv"
bucket_name = "stocks-market-data"

upload_file_to_s3(local_file_path, bucket_name)