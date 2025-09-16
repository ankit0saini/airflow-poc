from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.adls import AzureDataLakeHook
from datetime import datetime

ADLS_CONN_ID = "azure_con"
ADLS_CONTAINER = "raw-zone"

def adls_conn():
    adls_hook = AzureDataLakeHook(azure_data_lake_conn_id="azure_con")
    
    # Example: list files in container
    files = adls_hook.list_directory(ADLS_CONTAINER, "non_fan_touchpoint")
    print(files)


with DAG(
    dag_id="adls_con",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    adls_con_task = PythonOperator(
        task_id="adls_con_task",
        python_callable=adls_conn,
    )
    
    adls_con_task