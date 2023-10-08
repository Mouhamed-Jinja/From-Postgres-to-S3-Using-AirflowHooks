from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'younies',
    'retries':5,
    'retry_delay':timedelta(seconds=30)    
}

with DAG(
    dag_id = 'dag_with_cron_expression_v2',
    default_args= default_args,
    start_date= datetime(2023,10,1) ,
    schedule_interval = "0 3 * * Mon-Fri"

)as dag:
    task1 = BashOperator(
        task_id= 'cron_exp',
        bash_command = "echo ------> we using cron expression."
    )