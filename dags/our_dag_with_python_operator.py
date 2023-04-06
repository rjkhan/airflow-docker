from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'rabnawaz',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def greet(ti):
    name = ti.xcom_pull(task_ids='get_name')
    age = ti.xcom_pull(task_ids='get_age')
    print(f"hello world my name {name} and age is {age} year old")


def get_name():
    return "Rabnawaz jan Sher"

def get_age():
    return 40

with DAG(
    dag_id='our_dag_with_python_operator_v4',
    default_args=default_args,
    description='this is our first dag with python operator',
    start_date=datetime(2023, 3, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )


    [task2, task3] >> task1
