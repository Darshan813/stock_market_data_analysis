from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import pandas as pd
from datetime import datetime, timedelta
from extract import extract
from load_to_s3 import s3Loader
from load_to_redshift import RedshiftLoader

def set_env_vars():
    os.environ['DB_NAME'] = Variable.get('DB_NAME')
    os.environ['USER'] = Variable.get('USER')
    os.environ['PASSWORD'] = Variable.get('PASSWORD')
    os.environ['HOST'] = Variable.get('HOST')
    os.environ['PORT'] = Variable.get('PORT')
    os.environ['AWS_BUCKET_NAME'] = Variable.get('AWS_BUCKET_NAME')
    os.environ['FACT_STOCK_TABLE'] = Variable.get('FACT_STOCK_TABLE')
    os.environ['IAM_ROLE'] = Variable.get('IAM_ROLE')
    os.environ['STOCK_SPLIT_TABLE'] = Variable.get('STOCK_SPLIT_TABLE')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_stock_data(**context):
    set_env_vars()
    api_key = os.getenv('API_KEY')
    top_companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'BRK.A']

    combined_df = pd.DataFrame()

    for symbol in top_companies:
        df = extract(
            f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol" \
            f"={symbol}&outputsize=full&apikey={api_key}")
        combined_df = pd.concat([combined_df, df], axis=0)

    file_path = "/home/ubuntu/stock_data/combined_stock_data.csv" #store temp in EC2
    combined_df.to_csv(file_path, index=False)

    context["task_instance"].xcom_push(key='file_path', value=file_path)


def upload_to_s3(**context):
    set_env_vars()
    file_path = context['task_instance'].xcom_pull(task_ids='extract_data', key='file_path')
    date_folder = datetime.now().strftime('%Y-%m-%d')

    s3_loader = s3Loader()
    s3_loader.upload_file_to_s3(file_path, date_folder, date_folder)

    context['task_instance'].xcom_push(key='date_folder', value=date_folder)


def load_to_redshift(**context):
    set_env_vars()
    date_folder = context['task_instance'].xcom_pull(task_ids='upload_s3', key='date_folder')

    redshift_loader = RedshiftLoader()
    redshift_loader.from_s3(
        folder_name=date_folder,
        table_name=os.getenv("FACT_STOCK_TABLE")
    )
    redshift_loader.creating_final_stock_table()
    redshift_loader.executing_sql_procedures()

dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Daily stock data ETL pipeline',
    schedule_interval='0 0 * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_stock_data,
    provide_context=True,
    dag=dag
)

s3_upload_task = PythonOperator(
    task_id='upload_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag
)

redshift_load_task = PythonOperator(
    task_id='load_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag
)

extract_task >> s3_upload_task >> redshift_load_task

