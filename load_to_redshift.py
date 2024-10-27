# Redshift Serverless connection details
from datetime import datetime
from sqlite3 import OperationalError
from dotenv import load_dotenv
import psycopg2
import os
load_dotenv()


class RedshiftLoader:

    def __init__(self):
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.db_name = os.getenv("DB_NAME")
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.host = os.getenv("host")
        self.port = os.getenv("port")
        self.conn = None

    def connect(self):
         try:
            print(self.port)
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port= self.port
            )
            print("Connection to Redshift Serverless was successful.")

         except OperationalError as e:
            print("Failed to connect to Redshift Serverless:")


    def from_s3(self, folder_name, table_name):
        if self.conn is None:
            self.connect()
        cursor = self.conn.cursor()
        s3_bucket = f's3://{self.bucket_name}/{folder_name}'
        copy_sql = f"""
            COPY {table_name}
            FROM '{s3_bucket}'
            IAM_ROLE '{os.getenv("IAM_ROLE")}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 1
            TIMEFORMAT 'auto';
            """
        try:
            cursor.execute(copy_sql)
            self.conn.commit()
            print(f"Data Successfully Loaded To Redshift Table {table_name}")
        except Exception as e:
            print(f"Error loading data into {e}")
        finally:
            cursor.close()

