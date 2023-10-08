from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

default_args = {
    'owner': 'younies',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

def get_name(ti):
    ti.xcom_push(key='first_name', value='Mohamed')
    ti.xcom_push(key='last_name', value = 'younies')
    
def get_age(ti):
    ti.xcom_push(key='age', value = 22)
    
def show(ti):
    first_name= ti.xcom_pull(task_ids = "get_name_task", key='first_name')
    last_name= ti.xcom_pull(task_ids = "get_name_task", key= 'last_name')
    age = ti.xcom_pull(task_ids = 'get_age_task', key= 'age')
    print(f"my name is {first_name} {last_name}, and age is {age}")

with DAG(
    dag_id="test_v7",
    description="test",
    default_args=default_args,
    start_date=datetime(2023, 10, 4, tzinfo=timezone.utc),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="get_name_task",
        python_callable=get_name
    )
    task2 = PythonOperator(
        task_id = "get_age_task",
        python_callable = get_age
    )

    task3 = PythonOperator(
        task_id="show_data",
        python_callable = show
    )
    
    [task1, task2] >>task3
    
    