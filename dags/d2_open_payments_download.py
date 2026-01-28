import warnings
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import requests
import logging
import boto3
import os
import zipfile
import pandas as pd

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

ZIP_URL = "https://download.cms.gov/openpayments/PGYR2024_P06302025_06162025.zip"
S3_BUCKET = "ai-factory-bckt"
S3_PREFIX_RAW = "raw/open_payments"
S3_PREFIX_BRONZE = "bronze/open_payments"

s3 = boto3.client("s3")

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id="open_payments_download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # Open Payments PGYR2024 CSV DAG
    Télécharge le ZIP Open Payments 2024, décompresse les CSV et les stocke sur S3 (raw + bronze Parquet).
    """,
    tags=['open-payments', 'csv', 's3'],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Download ZIP
    # -------------------------------------------------------------------
    def download_zip(**context):
        local_zip_path = "/tmp/OpenPayments_PGYR2024.zip"
        os.makedirs("/tmp", exist_ok=True)
        logger.info(f"📥 Téléchargement depuis : {ZIP_URL}")

        response = requests.get(ZIP_URL, timeout=300)
        response.raise_for_status()

        with open(local_zip_path, "wb") as f:
            f.write(response.content)

        file_size_mb = round(len(response.content) / (1024*1024), 2)
        logger.info(f"✅ ZIP téléchargé {file_size_mb} MB")

        context['task_instance'].xcom_push(key='local_zip_path', value=local_zip_path)

    download_task = PythonOperator(
        task_id='download_zip',
        python_callable=download_zip,
    )

    # -------------------------------------------------------------------
    # TASK 2 : Extract CSVs, Upload to raw + Parquet to bronze
    # -------------------------------------------------------------------
    def extract_upload_and_parquet(**context):
        local_zip_path = context['task_instance'].xcom_pull(task_ids='download_zip', key='local_zip_path')
        extract_dir = "/tmp/openpayments_csvs"
        os.makedirs(extract_dir, exist_ok=True)

        # Décompression
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info(f"✅ ZIP décompressé dans {extract_dir}")

        csv_files = [f for f in os.listdir(extract_dir) if f.lower().endswith('.csv')]
        logger.info(f"📄 {len(csv_files)} CSV trouvés : {csv_files}")

        uploaded_raw = []
        uploaded_bronze = []

        for csv_file in csv_files:
            local_csv_path = os.path.join(extract_dir, csv_file)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # --- Upload raw CSV ---
            s3_key_versioned = f"{S3_PREFIX_RAW}/{csv_file.rstrip('.csv')}_{timestamp}.csv"
            s3_key_latest = f"{S3_PREFIX_RAW}/{csv_file.rstrip('.csv')}_LATEST.csv"
            s3.upload_file(local_csv_path, S3_BUCKET, s3_key_versioned)
            s3.upload_file(local_csv_path, S3_BUCKET, s3_key_latest)
            uploaded_raw.append(s3_key_versioned)

            # --- Convert to Parquet & Upload bronze ---
            df = pd.read_csv(local_csv_path)
            parquet_local_path = f"/tmp/{csv_file.rstrip('.csv')}.parquet"
            df.to_parquet(parquet_local_path, index=False)
            s3_key_parquet = f"{S3_PREFIX_BRONZE}/{csv_file.rstrip('.csv')}_LATEST.parquet"
            s3.upload_file(parquet_local_path, S3_BUCKET, s3_key_parquet)
            uploaded_bronze.append(s3_key_parquet)

        context['task_instance'].xcom_push(key='uploaded_raw', value=uploaded_raw)
        context['task_instance'].xcom_push(key='uploaded_bronze', value=uploaded_bronze)
        context['task_instance'].xcom_push(key='num_files', value=len(csv_files))

    upload_task = PythonOperator(
        task_id='extract_upload_parquet',
        python_callable=extract_upload_and_parquet,
    )

    # -------------------------------------------------------------------
    # TASK 3 : Summary
    # -------------------------------------------------------------------
    def summarize_run(**context):
        uploaded_raw = context['task_instance'].xcom_pull(task_ids='extract_upload_parquet', key='uploaded_raw')
        uploaded_bronze = context['task_instance'].xcom_pull(task_ids='extract_upload_parquet', key='uploaded_bronze')
        num_files = context['task_instance'].xcom_pull(task_ids='extract_upload_parquet', key='num_files')

        logger.info("="*70)
        logger.info("📊 OPEN PAYMENTS PGYR2024 DAG - SUMMARY")
        logger.info("="*70)
        logger.info(f"✅ {num_files} fichiers CSV uploadés (raw)")
        for f in uploaded_raw:
            logger.info(f"✅ S3 Path (raw) : s3://{S3_BUCKET}/{f}")
        for f in uploaded_bronze:
            logger.info(f"✅ S3 Path (bronze) : s3://{S3_BUCKET}/{f}")
        logger.info(f"✅ Timestamp : {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id='summary',
        python_callable=summarize_run,
    )

    # -------------------------------------------------------------------
    # DAG DEPENDENCIES
    # -------------------------------------------------------------------
    download_task >> upload_task >> summary_task