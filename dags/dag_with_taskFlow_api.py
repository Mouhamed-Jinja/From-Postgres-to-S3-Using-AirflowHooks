from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'younies',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

@dag(
    dag_id = 'taskflow2',
    default_args= default_args,
    start_date = datetime(2023, 10, 5),
    schedule_interval = "@daily"
)
def show_info_etlm():
    
    @task(multiple_outputs= True)
    def get_name():
        return {
            'first_name':'Mohamed',
            'last_name':'younies'
        }
    @task()
    def get_age():
        return 23
    @task()
    def show(fname, lname, age):
        print(f"my name is {fname} {lname}, and age is {age}")
        
    get_name_dict = get_name()
    eage= get_age()
    show(fname=get_name_dict['first_name'], lname=get_name_dict['last_name'], age=eage)
    
instance_show_dag = show_info_etlm()
    
        
        