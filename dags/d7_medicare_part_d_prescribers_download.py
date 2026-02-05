from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import boto3

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

CSV_URL = "https://data.cms.gov/sites/default/files/2024-05/402e13a3-8112-4dfd-9671-33a9f1f3000d/MUP_DPR_RY24_P04_V10_DY22_NPI.csv"
S3_BUCKET = "ai-factory-bckt"
S3_PREFIX_RAW = "raw/medicare_part_d_prescribers"
S3_PREFIX_BRONZE = "bronze/medicare_part_d_prescribers"

default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),  # Fichier volumineux
}

with DAG(
    dag_id="medicare_part_d_prescribers_download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # Medicare Part D Prescribers DAG
    Telecharge le CSV Medicare Part D Prescribers (by Provider) et le stocke sur S3 (raw + bronze Parquet).

    **Source:** CMS Medicare Utilization and Payment Data
    **Contenu:** NPI, nom prescripteur, specialite, nb prescriptions, couts, nb beneficiaires
    **Usage:** Detection des sur-prescripteurs et anomalies de prescription
    """,
    tags=['medicare', 'part-d', 'prescribers', 'fraud-detection', 'csv', 's3'],
) as dag:

    # -------------------
    # TASK 1 : Download CSV
    # -------------------
    def download_csv(**kwargs):
        local_path = "/tmp/MUP_DPR_RY24_P04_V10_DY22_NPI.csv"
        logger.info(f"Telechargement depuis : {CSV_URL}")
        response = requests.get(CSV_URL, timeout=300)  # Timeout plus long
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
        file_size_mb = round(len(response.content) / (1024*1024), 2)
        logger.info(f"Telecharge : {file_size_mb} MB")
        kwargs['ti'].xcom_push(key='local_path', value=local_path)
        kwargs['ti'].xcom_push(key='file_size_mb', value=file_size_mb)

    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
    )

    # -------------------
    # TASK 2 : Validate CSV (memory efficient)
    # -------------------
    def validate_csv(**kwargs):
        local_path = kwargs['ti'].xcom_pull(task_ids='download_csv', key='local_path')
        logger.info(f"Validation du CSV : {local_path}")
        # Compter les lignes sans charger tout en memoire
        row_count = sum(1 for _ in open(local_path, 'r', encoding='utf-8')) - 1  # -1 pour header
        if row_count <= 0:
            raise ValueError("CSV vide !")
        # Lire juste le header pour le nombre de colonnes
        df_header = pd.read_csv(local_path, nrows=0)
        col_count = len(df_header.columns)
        logger.info(f"Validation OK : {row_count:,} lignes, {col_count} colonnes")
        kwargs['ti'].xcom_push(key='row_count', value=row_count)

    validate_task = PythonOperator(
        task_id='validate_csv',
        python_callable=validate_csv,
    )

    # -------------------
    # TASK 3 : Upload CSV to raw S3
    # -------------------
    def upload_csv_to_s3(**kwargs):
        local_path = kwargs['ti'].xcom_pull(task_ids='download_csv', key='local_path')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key_versioned = f"{S3_PREFIX_RAW}/MUP_DPR_DY22_{timestamp}.csv"
        s3_key_latest = f"{S3_PREFIX_RAW}/MUP_DPR_DY22_LATEST.csv"
        logger.info(f"Upload CSV vers S3 : s3://{S3_BUCKET}/{s3_key_latest}")
        s3.upload_file(local_path, S3_BUCKET, s3_key_versioned)
        s3.upload_file(local_path, S3_BUCKET, s3_key_latest)
        kwargs['ti'].xcom_push(key='s3_key_versioned', value=s3_key_versioned)

    upload_csv_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_csv_to_s3,
    )

    # -------------------
    # TASK 4 : Convert CSV to Parquet & Upload to bronze S3 (chunked for large files)
    # -------------------
    def convert_csv_to_parquet(**kwargs):
        import pyarrow as pa
        import pyarrow.parquet as pq

        local_path = kwargs['ti'].xcom_pull(task_ids='download_csv', key='local_path')
        parquet_local_path = "/tmp/MUP_DPR_DY22.parquet"
        logger.info(f"Conversion en Parquet (chunked) : {local_path}")

        # Lire et ecrire par chunks pour economiser la memoire
        chunk_size = 100000
        pq_writer = None

        for i, chunk in enumerate(pd.read_csv(local_path, chunksize=chunk_size, low_memory=False)):
            table = pa.Table.from_pandas(chunk)
            if pq_writer is None:
                pq_writer = pq.ParquetWriter(parquet_local_path, table.schema)
            pq_writer.write_table(table)
            if i % 10 == 0:
                logger.info(f"Chunk {i} traite ({(i+1)*chunk_size:,} lignes)")

        if pq_writer:
            pq_writer.close()

        s3_key_parquet = f"{S3_PREFIX_BRONZE}/MUP_DPR_DY22_LATEST.parquet"
        logger.info(f"Upload Parquet vers S3 : s3://{S3_BUCKET}/{s3_key_parquet}")
        s3.upload_file(parquet_local_path, S3_BUCKET, s3_key_parquet)
        kwargs['ti'].xcom_push(key='s3_parquet_key', value=s3_key_parquet)

    parquet_task = PythonOperator(
        task_id='convert_csv_to_parquet',
        python_callable=convert_csv_to_parquet,
    )

    # -------------------
    # TASK 5 : Summary
    # -------------------
    def summarize_run(**kwargs):
        s3_csv_key = kwargs['ti'].xcom_pull(task_ids='upload_to_s3', key='s3_key_versioned')
        s3_parquet_key = kwargs['ti'].xcom_pull(task_ids='convert_csv_to_parquet', key='s3_parquet_key')
        row_count = kwargs['ti'].xcom_pull(task_ids='validate_csv', key='row_count')
        file_size_mb = kwargs['ti'].xcom_pull(task_ids='download_csv', key='file_size_mb')
        logger.info("="*70)
        logger.info("MEDICARE PART D PRESCRIBERS DAG - SUMMARY")
        logger.info("="*70)
        logger.info(f"Taille fichier : {file_size_mb} MB")
        logger.info(f"Nombre de lignes : {row_count:,}")
        logger.info(f"S3 CSV Path (raw) : s3://{S3_BUCKET}/{s3_csv_key}")
        logger.info(f"S3 Parquet Path (bronze) : s3://{S3_BUCKET}/{s3_parquet_key}")
        logger.info(f"Timestamp : {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id='summary',
        python_callable=summarize_run,
    )

    # -------------------
    # DAG DEPENDENCIES
    # -------------------
    download_task >> validate_task >> upload_csv_task >> parquet_task >> summary_task
