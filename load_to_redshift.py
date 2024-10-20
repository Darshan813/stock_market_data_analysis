# Redshift Serverless connection details
from datetime import datetime
from sqlite3 import OperationalError
import os

from dotenv import load_dotenv

load_dotenv()

import psycopg2

def load_to_redshift():
    try:
        date_folder = datetime.now().strftime('%Y-%m-%d')
        s3_bucket = f's3://{os.getenv("AWS_S3_BUCKET_NAME")}/{date_folder}'
        conn = psycopg2.connect(
            dbname= os.getenv("dbname"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port")
        )

        cursor = conn.cursor()

        print("Connection to Redshift Serverless was successful.")

        copy_sql = f"""
            COPY fact_top_10_stock_data
            FROM '{s3_bucket}'
            IAM_ROLE '{os.getenv("IAM_ROLE")}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 1
            TIMEFORMAT 'auto';
            """
        cursor.execute(copy_sql)
        conn.commit()

    except OperationalError as e:
        print("Failed to connect to Redshift Serverless:")

    finally:
        cursor.close()

