import warnings
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import requests
import logging

# Suppress all warnings (requests, urllib3, pandas)
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

# =============================================================================
# DAG DEFAULTS
# =============================================================================
default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2026, 1, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
dag = DAG(
    'leie_download',
    default_args=default_args,
    schedule='@weekly',
    catchup=False,
    doc_md="""
    # LEIE (Exclusions) Download DAG
    
    **Purpose:** Download OIG excluded individuals/entities list to S3 landing zone.
    **Frequency:** Weekly (Mondays)
    **Link:** Link 1/6 - Fraud Detection Pipeline
    """,
    tags=['fraud-detection', 'link-1', 'leie'],
)

# =============================================================================
# TASK 1 : Download LEIE CSV
# =============================================================================
def download_leie_csv(**context):
    """Download LEIE CSV from OIG.HHS.GOV to /tmp"""
    
    url = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    local_path = "/tmp/UPDATED.csv"
    
    logger.info(f"📥 Downloading LEIE CSV from: {url}")
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        file_size_mb = round(len(response.content) / (1024 * 1024), 2)
        logger.info(f"✅ Downloaded: {file_size_mb} MB")
        
        context['task_instance'].xcom_push(key='leie_local_path', value=local_path)
        context['task_instance'].xcom_push(key='leie_file_size_mb', value=file_size_mb)
        
        return local_path
        
    except Exception as e:
        logger.error(f"❌ Download failed: {str(e)}")
        raise

download_task = PythonOperator(
    task_id='download_leie_csv',
    python_callable=download_leie_csv,
    dag=dag,
)

# =============================================================================
# TASK 2 : Validate CSV Structure
# =============================================================================
def validate_leie_csv(**context):
    """Validate CSV: file exists, not empty, parseable"""
    
    local_path = context['task_instance'].xcom_pull(
        task_ids='download_leie_csv',
        key='leie_local_path'
    )
    
    logger.info(f"🔍 Validating CSV: {local_path}")
    
    try:
        # Read entire CSV and count rows
        df = pd.read_csv(local_path)
        row_count = df.shape[0]
        
        # Validate: file not empty
        if row_count == 0:
            raise ValueError("❌ CSV file is empty!")
        
        logger.info(f"✅ Validation passed: {row_count:,} rows, {df.shape[1]} columns")
        
        context['task_instance'].xcom_push(key='leie_row_count', value=row_count)
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {str(e)}")
        raise

validate_task = PythonOperator(
    task_id='validate_leie_csv',
    python_callable=validate_leie_csv,
    dag=dag,
)

# =============================================================================
# TASK 3 : Upload to S3
# =============================================================================
def upload_leie_to_s3(**context):
    """Upload validated CSV to S3 with timestamp"""
    
    local_path = context['task_instance'].xcom_pull(
        task_ids='download_leie_csv',
        key='leie_local_path'
    )
    row_count = context['task_instance'].xcom_pull(
        task_ids='validate_leie_csv',
        key='leie_row_count'
    )
    
    s3_bucket = 'ai-factory-bckt'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"raw/leie/UPDATED_{timestamp}.csv"
    
    logger.info(f"📤 Uploading to S3: s3://{s3_bucket}/{s3_key}")
    
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        logger.info(f"✅ Uploaded {row_count:,} rows")
        
        s3_hook.load_file(
            filename=local_path,
            key='raw/leie/UPDATED_LATEST.csv',
            bucket_name=s3_bucket,
            replace=True
        )
        logger.info("✅ Updated LATEST pointer")
        
        context['task_instance'].xcom_push(key='s3_key', value=s3_key)
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ S3 upload failed: {str(e)}")
        raise

upload_task = PythonOperator(
    task_id='upload_leie_s3',
    python_callable=upload_leie_to_s3,
    dag=dag,
)

# =============================================================================
# TASK 4 : Summary Report
# =============================================================================
def summarize_run(**context):
    """Log summary metrics"""
    
    file_size_mb = context['task_instance'].xcom_pull(
        task_ids='download_leie_csv',
        key='leie_file_size_mb'
    )
    row_count = context['task_instance'].xcom_pull(
        task_ids='validate_leie_csv',
        key='leie_row_count'
    )
    s3_key = context['task_instance'].xcom_pull(
        task_ids='upload_leie_s3',
        key='s3_key'
    )
    
    logger.info("= " * 70)
    logger.info("📊 LEIE DOWNLOAD DAG - SUMMARY")
    logger.info("= " * 70)
    logger.info(f"✅ File Size: {file_size_mb} MB")
    logger.info(f"✅ Row Count: {row_count:,}")
    logger.info(f"✅ S3 Path: s3://ai-factory-bckt/{s3_key}")
    logger.info(f"✅ Timestamp: {datetime.now().isoformat()}")
    logger.info("= " * 70)

summary_task = PythonOperator(
    task_id='summarize_run',
    python_callable=summarize_run,
    dag=dag,
)

# =============================================================================
# TASK 5 : Trigger DAG d1 (d1_medicare_hospital_spending_download)
# =============================================================================
trigger_medicare_hospital_spending = TriggerDagRunOperator(
    task_id='trigger_medicare_hospital_spending_dag',
    trigger_dag_id='medicare_hospital_spending_download',
    wait_for_completion=False,
    dag=dag,
)

# =============================================================================
# DAG DEPENDENCIES
# =============================================================================
download_task >> validate_task >> upload_task >> summary_task >> trigger_medicare_hospital_spending