from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
import logging
import tempfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'ETL_aws_industry',
    default_args=default_args,
    description='ETL pipeline to extract data from Postgres and load to S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)


# 1. Extract data from PostgreSQL
def extract_data(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id='postgres_log')
        sql = "SELECT * FROM login_logs;"
        df = hook.get_pandas_df(sql)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
            temp_path = tmpfile.name
            df.to_csv(temp_path, index=False)
            logging.info(f"Data extracted to temporary file: {temp_path}")

        # Push temporary file path to XCom for further tasks
        kwargs['ti'].xcom_push(key='extracted_file', value=temp_path)
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        raise


# 2. Transform data
def transform_data(**kwargs):
    try:
        # Get the path of the extracted file from XCom
        ti = kwargs['ti']
        extracted_file = ti.xcom_pull(task_ids='extract_data', key='extracted_file')

        data = pd.read_csv(extracted_file)
        data.dropna(axis=0, how='any', inplace=True)
        data[['device_info', 'AppleWebKit']] = data['device_info'].str.split('AppleWebKit/', expand=True)
        data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Partition data by year
        data['log_date'] = pd.to_datetime(data['log_date'])
        for year, partition in data.groupby(data['log_date'].dt.year):
            file_path = f"/tmp/transformed_data_{year}.csv"
            partition.to_csv(file_path, index=False)
            ti.xcom_push(key=f'transformed_file_{year}', value=file_path)
            logging.info(f"Transformed data for {year} written to {file_path}")
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise


# 3. Load data to S3
def load_data(**kwargs):
    try:
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = 'log-st0rage'

        # Get all partitioned files from XCom
        for year in range(2016, 2025):  # Adjust years based on your data
            transformed_file = ti.xcom_pull(task_ids='transform_data', key=f'transformed_file_{year}')
            if transformed_file:
                s3_key = f"logs/{year}/transformed_data_{year}.csv"
                s3_hook.load_file(
                    filename=transformed_file,
                    bucket_name=bucket_name,
                    key=s3_key,
                    replace=True
                )
                logging.info(f"File {transformed_file} successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"Error during data upload to S3: {e}")
        raise


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Dependencies
extract_task >> transform_task >> load_task
