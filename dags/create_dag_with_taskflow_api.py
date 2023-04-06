from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'rabnawaz',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='create_dag_with_taskflow_api_v2',
    default_args=default_args,
    description='this is our first dag with task flow api decorators',
    start_date=datetime(2023, 3, 6),
    schedule_interval='@daily')

def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Rabnawaz",
            "last_name" : "Jan Sher Khan"
        }
    
    @task()
    def get_age():
        return 45
    
    @task()
    def greet(first_name, last_name, age):
        print(f"hello i am {first_name} and {last_name}, and i am {age} years old!")


    name_dict=get_name()
    age=get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'] ,
            age=age)

greet_dag = hello_world_etl()


