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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Fichiers volumineux
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
    # TASK 2 : Extract CSVs, Upload to raw + Parquet to bronze (chunked)
    # -------------------------------------------------------------------
    def extract_upload_and_parquet(**context):
        import pyarrow as pa
        import pyarrow.parquet as pq

        local_zip_path = context['task_instance'].xcom_pull(task_ids='download_zip', key='local_zip_path')
        extract_dir = "/tmp/openpayments_csvs"
        os.makedirs(extract_dir, exist_ok=True)

        # Decompression
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info(f"ZIP decompresse dans {extract_dir}")

        csv_files = [f for f in os.listdir(extract_dir) if f.lower().endswith('.csv')]
        logger.info(f"{len(csv_files)} CSV trouves : {csv_files}")

        uploaded_raw = []
        uploaded_bronze = []

        for csv_file in csv_files:
            local_csv_path = os.path.join(extract_dir, csv_file)
            file_base = csv_file.replace('.csv', '')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            logger.info(f"Traitement de {csv_file}...")

            # --- Upload raw CSV ---
            s3_key_versioned = f"{S3_PREFIX_RAW}/{file_base}_{timestamp}.csv"
            s3_key_latest = f"{S3_PREFIX_RAW}/{file_base}_LATEST.csv"
            s3.upload_file(local_csv_path, S3_BUCKET, s3_key_versioned)
            s3.upload_file(local_csv_path, S3_BUCKET, s3_key_latest)
            uploaded_raw.append(s3_key_versioned)
            logger.info(f"CSV uploade vers S3 raw: {s3_key_latest}")

            # --- Convert to Parquet (chunked) & Upload bronze ---
            parquet_local_path = f"/tmp/{file_base}.parquet"
            chunk_size = 100000
            pq_writer = None

            try:
                for i, chunk in enumerate(pd.read_csv(local_csv_path, chunksize=chunk_size, low_memory=False)):
                    table = pa.Table.from_pandas(chunk)
                    if pq_writer is None:
                        pq_writer = pq.ParquetWriter(parquet_local_path, table.schema)
                    pq_writer.write_table(table)
                    if i % 10 == 0:
                        logger.info(f"  {csv_file}: chunk {i} traite ({(i+1)*chunk_size:,} lignes)")

                if pq_writer:
                    pq_writer.close()

                s3_key_parquet = f"{S3_PREFIX_BRONZE}/{file_base}_LATEST.parquet"
                s3.upload_file(parquet_local_path, S3_BUCKET, s3_key_parquet)
                uploaded_bronze.append(s3_key_parquet)
                logger.info(f"Parquet uploade vers S3 bronze: {s3_key_parquet}")

            except Exception as e:
                logger.error(f"Erreur conversion {csv_file}: {str(e)}")
                if pq_writer:
                    pq_writer.close()
                raise

            # Nettoyer les fichiers temporaires pour liberer l'espace
            if os.path.exists(local_csv_path):
                os.remove(local_csv_path)
            if os.path.exists(parquet_local_path):
                os.remove(parquet_local_path)

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