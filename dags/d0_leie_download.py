import warnings
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import requests
import logging
import boto3

# Supprime les warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

# =============================================================================
# DAG DEFAULTS
# =============================================================================
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id="leie_download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # LEIE (Exclusions) Download DAG
    
    **Purpose:** Download OIG excluded individuals/entities list to S3 landing zone.
    **Frequency:** Weekly (Mondays)
    **Link:** Link 1/6 - Fraud Detection Pipeline
    """,
    tags=['fraud-detection', 'link-1', 'leie'],
) as dag:

    # -------------------------------------------------------------------------
    # TASK 1 : Download LEIE CSV
    # -------------------------------------------------------------------------
    def download_leie_csv(**context):
        url = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
        local_path = "/tmp/UPDATED.csv"
        logger.info(f"📥 Downloading LEIE CSV from: {url}")

        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(response.content)
            file_size_mb = round(len(response.content) / (1024 * 1024), 2)
            logger.info(f"Downloaded: {file_size_mb} MB")

            context['task_instance'].xcom_push(key='leie_local_path', value=local_path)
            context['task_instance'].xcom_push(key='leie_file_size_mb', value=file_size_mb)
            return local_path

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise

    download_task = PythonOperator(
        task_id='download_leie_csv',
        python_callable=download_leie_csv,
    )

    # -------------------------------------------------------------------------
    # TASK 2 : Validate CSV Structure
    # -------------------------------------------------------------------------
    def validate_leie_csv(**context):
        local_path = context['task_instance'].xcom_pull(task_ids='download_leie_csv', key='leie_local_path')
        logger.info(f"🔍 Validating CSV: {local_path}")

        try:
            df = pd.read_csv(local_path)
            row_count = df.shape[0]
            if row_count == 0:
                raise ValueError("CSV file is empty!")

            logger.info(f"Validation passed: {row_count:,} rows, {df.shape[1]} columns")
            context['task_instance'].xcom_push(key='leie_row_count', value=row_count)
            return True
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise

    validate_task = PythonOperator(
        task_id='validate_leie_csv',
        python_callable=validate_leie_csv,
    )

    # -------------------------------------------------------------------------
    # TASK 3 : Upload CSV to raw S3
    # -------------------------------------------------------------------------
    def upload_leie_csv_to_s3(**context):
        local_path = context['task_instance'].xcom_pull(task_ids='download_leie_csv', key='leie_local_path')

        s3_bucket = 'ai-factory-bckt'
        raw_key_versioned = 'raw/leie/UPDATED_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'
        raw_key_latest = 'raw/leie/UPDATED_LATEST.csv'

        logger.info(f"Uploading CSV to S3 raw: s3://{s3_bucket}/{raw_key_latest}")

        try:
            s3.upload_file(local_path, s3_bucket, raw_key_versioned)
            s3.upload_file(local_path, s3_bucket, raw_key_latest)
            context['task_instance'].xcom_push(key='s3_raw_key', value=raw_key_latest)
            return raw_key_latest
        except Exception as e:
            logger.error(f"S3 raw upload failed: {str(e)}")
            raise

    upload_csv_task = PythonOperator(
        task_id='upload_csv_s3',
        python_callable=upload_leie_csv_to_s3,
    )

    # -------------------------------------------------------------------------
    # TASK 4 : Convert CSV to Parquet & Upload to bronze S3
    # -------------------------------------------------------------------------
    def convert_csv_to_parquet(**context):
        local_path = context['task_instance'].xcom_pull(task_ids='download_leie_csv', key='leie_local_path')
        df = pd.read_csv(local_path)

        parquet_local_path = "/tmp/UPDATED.parquet"
        df.to_parquet(parquet_local_path, index=False)

        s3_bucket = 'ai-factory-bckt'
        bronze_key = 'bronze/leie/UPDATED_LATEST.parquet'

        logger.info(f"Uploading Parquet to S3 bronze: s3://{s3_bucket}/{bronze_key}")

        try:
            s3.upload_file(parquet_local_path, s3_bucket, bronze_key)
            context['task_instance'].xcom_push(key='s3_bronze_key', value=bronze_key)
        except Exception as e:
            logger.error(f"S3 bronze upload failed: {str(e)}")
            raise

    parquet_task = PythonOperator(
        task_id='convert_csv_to_parquet',
        python_callable=convert_csv_to_parquet,
    )

    # -------------------------------------------------------------------------
    # TASK 5 : Summary Report
    # -------------------------------------------------------------------------
    def summarize_run(**context):
        s3_bucket = 'ai-factory-bckt'
        file_size_mb = context['task_instance'].xcom_pull(task_ids='download_leie_csv', key='leie_file_size_mb')
        row_count = context['task_instance'].xcom_pull(task_ids='validate_leie_csv', key='leie_row_count')
        s3_csv_key = context['task_instance'].xcom_pull(task_ids='upload_csv_s3', key='s3_raw_key')
        s3_parquet_key = context['task_instance'].xcom_pull(task_ids='convert_csv_to_parquet', key='s3_bronze_key')

        logger.info("="*70)
        logger.info("LEIE DOWNLOAD DAG - SUMMARY")
        logger.info("="*70)
        logger.info(f"✅ File Size: {file_size_mb} MB")
        logger.info(f"✅ Row Count: {row_count:,}")
        logger.info(f"✅ S3 CSV Path (raw): s3://{s3_bucket}/{s3_csv_key}")
        logger.info(f"✅ S3 Parquet Path (bronze): s3://{s3_bucket}/{s3_parquet_key}")
        logger.info(f"✅ Timestamp: {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id='summarize_run',
        python_callable=summarize_run,
    )

    # -------------------------------------------------------------------------
    # DAG DEPENDENCIES
    # -------------------------------------------------------------------------
    download_task >> validate_task >> upload_csv_task >> parquet_task >> summary_task