import warnings
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import requests
import logging
import boto3
import os
import pandas as pd

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

CSV_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/9fc01e0ca9b64f045d2700d4f25ab35c_1767204341/HH_Zip_Jan2026.csv"
S3_BUCKET = "ai-factory-bckt"
S3_PREFIX_RAW = "raw/home_health_care"
S3_PREFIX_BRONZE = "bronze/home_health_care"

s3 = boto3.client("s3")

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id="home_health_care_download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # Home Health Care CSV DAG
    Télécharge le CSV Home Health Care – Zip Codes et le stocke sur S3 (raw + bronze Parquet).
    """,
    tags=['provider', 'csv', 's3', 'home_health_care'],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Download CSV
    # -------------------------------------------------------------------
    def download_csv(**context):
        local_path = "/tmp/HH_Zip_Jan2026.csv"
        os.makedirs("/tmp", exist_ok=True)
        logger.info(f"📥 Téléchargement depuis : {CSV_URL}")

        response = requests.get(CSV_URL, timeout=120)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(response.content)

        file_size_mb = round(len(response.content) / (1024*1024), 2)
        logger.info(f"✅ Téléchargé {file_size_mb} MB")

        context['task_instance'].xcom_push(key='local_path', value=local_path)
        context['task_instance'].xcom_push(key='file_size_mb', value=file_size_mb)

    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
    )

    # -------------------------------------------------------------------
    # TASK 2 : Upload CSV to raw + Convert to Parquet for bronze
    # -------------------------------------------------------------------
    def upload_csv_and_parquet(**context):
        local_path = context['task_instance'].xcom_pull(task_ids='download_csv', key='local_path')
        file_size_mb = context['task_instance'].xcom_pull(task_ids='download_csv', key='file_size_mb')

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # --- Upload raw CSV ---
        s3_key_versioned = f"{S3_PREFIX_RAW}/HH_Zip_{timestamp}.csv"
        s3_key_latest = f"{S3_PREFIX_RAW}/HH_Zip_LATEST.csv"
        s3.upload_file(local_path, S3_BUCKET, s3_key_versioned)
        s3.upload_file(local_path, S3_BUCKET, s3_key_latest)
        logger.info(f"📤 CSV uploadé raw sur s3://{S3_BUCKET}/{s3_key_versioned} et LATEST")

        # --- Convert to Parquet & Upload bronze ---
        df = pd.read_csv(local_path)
        parquet_local_path = "/tmp/HH_Zip.parquet"
        df.to_parquet(parquet_local_path, index=False)
        s3_key_parquet = f"{S3_PREFIX_BRONZE}/HH_Zip_LATEST.parquet"
        s3.upload_file(parquet_local_path, S3_BUCKET, s3_key_parquet)
        logger.info(f"📤 Parquet uploadé bronze sur s3://{S3_BUCKET}/{s3_key_parquet}")

        context['task_instance'].xcom_push(key='s3_key_versioned', value=s3_key_versioned)
        context['task_instance'].xcom_push(key='s3_key_parquet', value=s3_key_parquet)
        context['task_instance'].xcom_push(key='file_size_mb', value=file_size_mb)

    upload_task = PythonOperator(
        task_id='upload_csv_and_parquet',
        python_callable=upload_csv_and_parquet,
    )

    # -------------------------------------------------------------------
    # TASK 3 : Summary
    # -------------------------------------------------------------------
    def summarize_run(**context):
        s3_csv = context['task_instance'].xcom_pull(task_ids='upload_csv_and_parquet', key='s3_key_versioned')
        s3_parquet = context['task_instance'].xcom_pull(task_ids='upload_csv_and_parquet', key='s3_key_parquet')
        file_size_mb = context['task_instance'].xcom_pull(task_ids='upload_csv_and_parquet', key='file_size_mb')

        logger.info("="*70)
        logger.info("📊 HOME HEALTH CARE DAG - SUMMARY")
        logger.info("="*70)
        logger.info(f"✅ Taille fichier : {file_size_mb} MB")
        logger.info(f"✅ S3 Path (raw CSV) : s3://{S3_BUCKET}/{s3_csv}")
        logger.info(f"✅ S3 Path (bronze Parquet) : s3://{S3_BUCKET}/{s3_parquet}")
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