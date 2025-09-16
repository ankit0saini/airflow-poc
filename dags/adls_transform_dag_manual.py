from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
# from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime

ADLS_CONTAINER = "raw-zone"

def transform_adls_file(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    
    tenant_id = "6159093e-cb60-43b6-99eb-eeac20638f38"
    client_id = "8ff72866-d882-4b18-8728-2ce3449aa0eb"
    client_secret = "nge8Q~dVv1c81rQScijB0M4N50zbn~sQGpwebbZl"
    account_name = "pocstoank"

    SOURCE_PATH = "non_fan_touchpoint/industry.csv"
    TARGET_PATH = "non_fan_touchpoint/processed/industry.csv"
    
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    if not all([account_name, client_id, tenant_id, client_secret]):
        raise ValueError("Missing ADLS credentials in Airflow connection")

    # Step 2: Build Spark session with these credentials
    spark = SparkSession.builder \
        .appName("Spark_ADLS_Transform") \
        .config(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth") \
        .config(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", client_id) \
        .config(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret) \
        .config(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token") \
        .getOrCreate()

    # Step 3: Define paths
    abfs_source_path = f"abfss://{ADLS_CONTAINER}@{account_name}.dfs.core.windows.net/{SOURCE_PATH}"
    abfs_target_path = f"abfss://{ADLS_CONTAINER}@{account_name}.dfs.core.windows.net/{TARGET_PATH}"

    # Step 4: Read all CSVs from ADLS
    df = spark.read.option("header", "true").csv(abfs_source_path)

    # Step 5: Transform (rename + add timestamp)
    renamed_cols = [f"renamed_{col}" for col in df.columns]
    df = df.toDF(*renamed_cols)
    df = df.withColumn("processed_at", current_timestamp())

    # Step 6: Write result back to ADLS
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(abfs_target_path)

    print(f"Transformed files written to: {abfs_target_path}")

with DAG(
    dag_id="adls_transform_dag_manual",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    transform_csv_task = PythonOperator(
        task_id="transform_csv_task",
        python_callable=transform_adls_file
    )
    
transform_csv_task