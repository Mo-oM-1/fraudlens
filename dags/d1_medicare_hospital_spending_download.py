from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import boto3

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

CSV_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/1f8cde9e222d5d49f88a894bcf7a8981_1736791547/Medicare_Hospital_Spending_by_Claim.csv"
S3_BUCKET = "ai-factory-bckt"
S3_PREFIX = "raw/medicare_hospital_spending"

default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id="medicare_hospital_spending_download",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="Télécharge le CSV Medicare Hospital Spending et le stocke sur S3.",
    tags=['medicare', 'csv', 's3'],
) as dag:

    # -------------------
    # TASK 1 : Download CSV
    # -------------------
    def download_csv(**kwargs):
        local_path = "/tmp/Medicare_Hospital_Spending_by_Claim.csv"
        logger.info(f"📥 Téléchargement depuis : {CSV_URL}")
        response = requests.get(CSV_URL, timeout=120)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
        file_size_mb = round(len(response.content) / (1024*1024), 2)
        kwargs['ti'].xcom_push(key='local_path', value=local_path)
        kwargs['ti'].xcom_push(key='file_size_mb', value=file_size_mb)

    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
    )

    # -------------------
    # TASK 2 : Validate CSV
    # -------------------
    def validate_csv(**kwargs):
        local_path = kwargs['ti'].xcom_pull(task_ids='download_csv', key='local_path')
        df = pd.read_csv(local_path)
        if df.empty:
            raise ValueError("CSV vide !")
        kwargs['ti'].xcom_push(key='row_count', value=len(df))

    validate_task = PythonOperator(
        task_id='validate_csv',
        python_callable=validate_csv,
    )

    # -------------------
    # TASK 3 : Upload to S3
    # -------------------
    def upload_csv_to_s3(**kwargs):
        local_path = kwargs['ti'].xcom_pull(task_ids='download_csv', key='local_path')
        row_count = kwargs['ti'].xcom_pull(task_ids='validate_csv', key='row_count')
        file_size_mb = kwargs['ti'].xcom_pull(task_ids='download_csv', key='file_size_mb')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key_versioned = f"{S3_PREFIX}/Medicare_Hospital_Spending_{timestamp}.csv"
        s3_key_latest = f"{S3_PREFIX}/Medicare_Hospital_Spending_LATEST.csv"
        s3.upload_file(local_path, S3_BUCKET, s3_key_versioned)
        s3.upload_file(local_path, S3_BUCKET, s3_key_latest)
        kwargs['ti'].xcom_push(key='s3_key_versioned', value=s3_key_versioned)

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_csv_to_s3,
    )

    # -------------------
    # TASK 4 : Summary
    # -------------------
    def summarize_run(**kwargs):
        s3_key = kwargs['ti'].xcom_pull(task_ids='upload_to_s3', key='s3_key_versioned')
        row_count = kwargs['ti'].xcom_pull(task_ids='validate_csv', key='row_count')
        file_size_mb = kwargs['ti'].xcom_pull(task_ids='download_csv', key='file_size_mb')
        logger.info("="*70)
        logger.info("📊 MEDICARE CSV DAG - SUMMARY")
        logger.info(f"✅ Taille fichier : {file_size_mb} MB")
        logger.info(f"✅ Nombre de lignes : {row_count:,}")
        logger.info(f"✅ S3 Path : s3://{S3_BUCKET}/{s3_key}")
        logger.info(f"✅ Timestamp : {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id='summary',
        python_callable=summarize_run,
    )

    # -------------------
    # TASK 5 : Trigger DAG d3
    # -------------------
    trigger_provider_informations = TriggerDagRunOperator(
        task_id='trigger_provider_informations',
        trigger_dag_id='provider_informations_download', # DAG ID
        wait_for_completion=False,
    )

    # -------------------
    # DAG DEPENDENCIES
    # -------------------
    download_task >> validate_task >> upload_task >> summary_task >> trigger_provider_informations