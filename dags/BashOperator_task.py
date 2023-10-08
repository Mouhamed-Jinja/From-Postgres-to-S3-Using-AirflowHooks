from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'younies',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='test_task_v4',
    description='This is a test task using BashOperator',
    default_args=default_args,
    start_date=datetime(2023, 10, 3, 6),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello, this is the first airflow-task"
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command= "echo hey, I am the second task, which is executed after task1"
    )
    task3 = BashOperator(
        task_id = 'thrid_task',
        bash_command = "echo hey, I am the task 3. and i will be running at the the same time at task 2"
    )
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)
    
    #task1 >> task2
    #task1 >> task3
    
    task1 >> [task2, task3]
    
    