from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def list_adls_files(**context):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    account_name = "pocstoank"   # e.g. mystorageaccount
    file_system = "raw-zone"             # container name

    # Astronomer will inject workload identity here
    credential = DefaultAzureCredential(
    managed_identity_client_id=os.getenv("AZURE_CLIENT_ID"))
    
    # Build ADLS Gen2 client
    service_client = DataLakeServiceClient(
        f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    fs_client = service_client.get_file_system_client(file_system)

    print(f"Listing files in container: {file_system}")
    paths = fs_client.get_paths()
    for path in paths:
        print(" -", path.name)

with DAG(
    dag_id="adls_identity_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id="list_adls_files",
        python_callable=list_adls_files,
    )