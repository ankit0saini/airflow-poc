from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime

ADLS_CONN_ID = "azure_con"
ADLS_CONTAINER = "raw_zone"

print(">>> adls_connection_test.py loaded by Airflow parser")

def list_adls_files(**context):
    hook = AzureDataLakeStorageV2Hook(adls_conn_id=ADLS_CONN_ID)
    client = hook.get_conn()
    file_system_client = client.get_file_system_client(ADLS_CONTAINER)

    print(f"Listing files in container: {ADLS_CONTAINER}")
    paths = file_system_client.get_paths(path="")
    for path in paths:
        print(f"- {path.name}")

with DAG(
    dag_id="adls_connection_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "test"],
) as dag:

    list_files = PythonOperator(
        task_id="list_adls_files",
        python_callable=list_adls_files,
    )
