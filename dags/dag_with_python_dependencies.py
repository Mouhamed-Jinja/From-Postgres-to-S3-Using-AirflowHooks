from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
def print_version():
    import sklearn
    print(f"-----> Here it is Scikit-Learn version: {sklearn.__version__}")

with DAG(
    dag_id='dag_with_PyDependencies_v1.0',
    default_args=default_args,
    start_date=datetime(2023, 10, 5),
    schedule_interval='0 0 * * *' #every day
) as dag:
    print_Scikit_version = PythonOperator(
        task_id = "skVersion",
        python_callable = print_version
    )
    print_Scikit_version