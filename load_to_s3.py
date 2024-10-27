import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
from io import BytesIO
load_dotenv()


class s3Loader:

    def __init__(self):
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.s3 = boto3.client('s3')

    def upload_file_to_s3(self, local_file_path, folder_name, file_name):

        s3_file_path = f"{folder_name}/{file_name}.csv"
        try:
            self.s3.upload_file(local_file_path, self.bucket_name, s3_file_path)
            print("File Loaded Successfully to s3")
        except FileNotFoundError:
            print(f"File {s3_file_path} not found")
        except Exception as e:
            print(f"An exception occured {e}")

    def upload_df(self, df, folder_name, file_name):
        csv_buffer = BytesIO()
        s3_file_path = f"{folder_name}/{file_name}.csv"
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        self.s3.upload_fileobj(csv_buffer, self.bucket_name, s3_file_path)

