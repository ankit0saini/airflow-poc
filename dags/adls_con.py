from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime

ADLS_CONN_ID = "azure_con_airflow"
ADLS_CONTAINER = "raw-zone"

def test_adls_connection(**context):
    hook = AzureDataLakeStorageV2Hook(adls_conn_id=ADLS_CONN_ID)
    adls_conn = hook.get_conn()
    # Get filesystem (container) client
    filesystem_client = adls_conn.get_file_system_client(ADLS_CONTAINER)
    print("Connected successfully!")
    paths = filesystem_client.get_paths(path="") 
    for path in paths:
        print(f"Found: {path.name}")


with DAG(
    dag_id="test_adls_gen2_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "gen2", "test"]
) as dag:

    test_connection = PythonOperator(
        task_id="check_adls_gen2_connection",
        python_callable=test_adls_connection
    )
    
test_connection
    