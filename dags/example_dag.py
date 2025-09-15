from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello Astronomer!")

with DAG(
    dag_id='first_dag',
    start_date=datetime(2025, 9, 3),
    schedule=None, 
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world
    )