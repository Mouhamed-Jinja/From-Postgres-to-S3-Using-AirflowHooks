from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='dag_with_minio_s3_v2.0',
    default_args=default_args,
    start_date=datetime(2023, 10, 7),
    schedule_interval='0 0 * * *' #every day
) as dag:
    task1 = S3KeySensor(
        task_id ="minio_s3",
        bucket_name = "airflow",
        bucket_key="2015-summary.csv",
        aws_conn_id= 'minio_S3',
        mode= "poke",
        poke_interval = 5,
        timeout= 30
        
    )
    task1