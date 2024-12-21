from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'ETL-aws',
    default_args=default_args,
    description='ETL process for PostgreSQL data',
    schedule_interval='@daily',
    start_date=datetime(2023, 12, 1),
    catchup=False
)


# 1. Extract data from PostgreSQL
def extract_data():
    hook = PostgresHook(postgres_conn_id='postgres_log')
    sql = "SELECT * FROM login_logs;"
    df = hook.get_pandas_df(sql)
    df.to_csv('/tmp/extracted_data.csv', index=False)
    print("Data extracted successfully!")


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)


# 2. Transform
def transform_data():
    data = pd.read_csv('/tmp/extracted_data.csv')

    data.dropna(axis=0, how='any', inplace=True)

    data[['device_info', 'AppleWebKit']] = data['device_info'].str.split('AppleWebKit/', expand=True)

    data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    data.to_csv('/tmp/transformed_data.csv', index=False)
    print("Data transformed successfully!")


transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

def load_data():
    csv_file_path = '/tmp/transformed_data.csv'

    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'log-st0rage'
    s3_key = 'logs/transformed_data.csv'


    s3_hook.load_file(
        filename=csv_file_path,
        bucket_name=bucket_name,
        key=s3_key,
        replace=True
    )
    print(f"File {csv_file_path} successfully uploaded to s3://{bucket_name}/{s3_key}")



load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)


# Dependencies
extract_task >> transform_task >> load_task
