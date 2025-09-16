from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import pandas as pd
import io

ADLS_CONTAINER = "raw-zone"

def transform_adls_file(**context):
    tenant_id = "6159093e-cb60-43b6-99eb-eeac20638f38"
    client_id = "8ff72866-d882-4b18-8728-2ce3449aa0eb"
    client_secret = "nge8Q~dVv1c81rQScijB0M4N50zbn~sQGpwebbZl"
    account_name = "pocstoank"

    SOURCE_PATH = "non_fan_touchpoint/industry.csv"
    TARGET_PATH = "non_fan_touchpoint/processed/industry.csv"

    # Authenticate
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    filesystem_client = service_client.get_file_system_client(ADLS_CONTAINER)

    # Download CSV from ADLS
    file_client = filesystem_client.get_file_client(SOURCE_PATH)
    download = file_client.download_file()
    file_data = download.readall()

    # Read into pandas
    df = pd.read_csv(io.BytesIO(file_data))

    # Transform
    df.columns = [f"renamed_{col}" for col in df.columns]
    df["processed_at"] = pd.Timestamp.now()

    # Save to CSV in memory
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload back to ADLS
    target_file_client = filesystem_client.get_file_client(TARGET_PATH)
    target_file_client.upload_data(csv_buffer, overwrite=True)

    print(f"Transformed file uploaded to {TARGET_PATH}")

# DAG definition
with DAG(
    dag_id="adls_transform_dag_pandas",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    transform_csv_task = PythonOperator(
        task_id="transform_csv_task",
        python_callable=transform_adls_file
    )

transform_csv_task
