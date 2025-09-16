from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime


def test_adls_connection():
    """
    Connects to ADLS Gen2 and lists files in a container.
    """
    hook = AzureDataLakeStorageV2Hook(azure_data_lake_conn_id="azure_con_airflow")

    # Get filesystem (container) client
    filesystem_client = hook.get_file_system_client(file_system="raw-zone")
    print("✅ Connected successfully!")
    paths = filesystem_client.get_paths(path="non_fan_touchpoint/") 
    for path in paths:
        print(f"Found: {path.name}")


with DAG(
    dag_id="test_adls_gen2_connection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["adls", "gen2", "test"],
) as dag:

    test_connection = PythonOperator(
        task_id="check_adls_gen2_connection",
        python_callable=test_adls_connection,
    )