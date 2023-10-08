import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    'owner': 'coder2j',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def postgres_to_s3(ds_nodash):
    name = "orders"
    # ds_nodash is the execution date
    # next_ds_nodash is the next execution date.
    # step 1: query data from postgresql db and save into text file
    
    post_hook = PostgresHook(
        postgres_conn_id ='postgres_localhost'
    )
    
    conn = post_hook.get_conn() #this to get postgres connection to use it latter
    cursor = post_hook.get_cursor() # cursor use it for execute sql commands.
    
    cursor.execute("select * from retail.public.orders")
    with open("dags/orders.txt", "w") as new_orders:
        text_writer = csv.writer(new_orders)
        columns_names = [i[0] for i in cursor.description] #for getting columns names 
        text_writer.writerow(columns_names)
        text_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("saved the orders data into text file to move into s3 bucket.")
    
    #step two move data to aws s3 bucket
    s3_hook = S3Hook(
     aws_conn_id = 'minio_S3'   
    )
    s3_hook.load_file(
        filename ="dags/orders.txt",
        key ="orders.txt",
        bucket_name="airflow",
        replace = True
        
    )
    
with DAG(
    dag_id="dag_with_postgres_hooks_v6",
    default_args=default_args,
    start_date=datetime(2023, 10, 8),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1
    
        
        
        
        
with DAG(
    dag_id="dag_with_postgres_hooks_v1",
    default_args=default_args,
    start_date=datetime(2023, 10, 8),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1