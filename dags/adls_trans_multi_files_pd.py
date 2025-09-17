from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import pandas as pd
import io

ADLS_CONTAINER = "raw-zone"
SOURCE_DIR = "non_fan_touchpoint"
TARGET_DIR = "non_fan_touchpoint/processed"


def transform_all_files(**context):
    tenant_id = "6159093e-cb60-43b6-99eb-eeac20638f38"
    client_id = "8ff72866-d882-4b18-8728-2ce3449aa0eb"
    client_secret = "nge8Q~dVv1c81rQScijB0M4N50zbn~sQGpwebbZl"
    account_name = "pocstoank"

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

    # List files in source directory
    paths = filesystem_client.get_paths(path=SOURCE_DIR)
    for path in paths:
        if path.is_directory:
            continue
        if not path.name.endswith(".csv"):
            continue

        source_path = path.name
        filename = source_path.split("/")[-1]
        target_path = f"{TARGET_DIR}/{filename}"

        print(f"Processing {source_path} -> {target_path}")

        # Download CSV
        file_client = filesystem_client.get_file_client(source_path)
        download = file_client.download_file()
        file_data = download.readall()

        # Transform with pandas
        df = pd.read_csv(io.BytesIO(file_data))
        df.columns = [f"renamed_{col}" for col in df.columns]
        df["processed_at"] = pd.Timestamp.now()

        # Save transformed CSV
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to processed folder
        target_file_client = filesystem_client.get_file_client(target_path)
        target_file_client.upload_data(csv_buffer, overwrite=True)

        print(f"Transformed file uploaded to {target_path}")


# DAG definition
with DAG(
    dag_id="adls_transform_all_files_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "pandas"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_all_csvs",
        python_callable=transform_all_files,
    )

transform_task
