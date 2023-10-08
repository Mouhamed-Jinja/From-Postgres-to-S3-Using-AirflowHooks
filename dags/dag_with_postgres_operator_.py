from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    dag_id='dag_with_postgres_operator_v1.0',
    default_args=default_args,
    start_date=datetime(2023, 10, 5),
    schedule_interval='0 0 * * *' #every day
) as dag:
    create= PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost', #which you fefined in airflow UI connection
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    insert = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    delete = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
        """
    )
    create >> delete >> insert