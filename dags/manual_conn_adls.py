from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

def test_adls_manual(**context):
    tenant_id = "6159093e-cb60-43b6-99eb-eeac20638f38"
    client_id = "8ff72866-d882-4b18-8728-2ce3449aa0eb"
    client_secret = "nge8Q~dVv1c81rQScijB0M4N50zbn~sQGpwebbZl"
    account_name = "pocstoank"

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    filesystem_client = service_client.get_file_system_client("raw-zone")
    for path in filesystem_client.get_paths():
        print("Found:", path.name)

with DAG(
    dag_id="manual_adls_gen2_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "gen2", "test"]
) as dag:

    test_connection = PythonOperator(
        task_id="check_adls_gen2_connection",
        python_callable=test_adls_manual
    )
    
test_connection