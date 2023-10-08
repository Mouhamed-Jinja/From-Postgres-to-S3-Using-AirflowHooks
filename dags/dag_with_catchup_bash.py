from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta


default_args={
    'owner':'younes',
    'retries':5,
    'retry_delay':timedelta(seconds=30)
}

with DAG(
    dag_id= "using_Catchup_with_bash_v2",
    default_args= default_args,
    start_date = datetime(2023,10,1),
    schedule_interval= "@daily",
    catchup= False
) as dag:
    task1= BashOperator(
        task_id = "catchup",
        bash_command= "echo ---------------> this is simple catchup command"
    )