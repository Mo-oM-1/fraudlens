import warnings
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import requests
import logging

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2026, 1, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45),
}

dag = DAG(
    'open_payments_download',
    default_args=default_args,
    schedule='0 2 1 * *',
    catchup=False,
    doc_md="Download CMS Open Payments data to S3 (Monthly)",
    tags=['fraud-detection', 'link-2', 'open-payments'],
)

def query_open_payments_api(**context):
    """Query DKAN API - récupère TOUTES les colonnes"""

    import json
    warnings.filterwarnings('ignore')
    
    api_url = "https://openpaymentsdata.cms.gov/api/1/datastore/query/fb3a65aa-c901-4a38-a813-b04b00dfa2a9/0"
    local_path = "/tmp/open_payments.json"
    
    # Colonnes essentielles pour fraud detection
    key_properties = [
        "covered_recipient_npi",
        "covered_recipient_first_name",
        "covered_recipient_last_name",
        "recipient_city",
        "recipient_state",
        "recipient_zip_code",
        "applicable_manufacturer_or_applicable_gpo_making_payment_name",
        "total_amount_of_payment_usdollars",
        "date_of_payment",
        "nature_of_payment_or_transfer_of_value",
        "form_of_payment_or_transfer_of_value",
        "name_of_drug_or_biological_or_device_or_medical_supply_1",
        "associated_drug_or_biological_ndc_1",
        "program_year",
        "payment_publication_date",
        "record_id"
    ]
    
    logger.info(f"📥 Querying Open Payments API: {api_url}")
    
    try:
        all_records = []
        offset = 0
        limit = 1000
        max_records = 100000
        
        while len(all_records) < max_records:
            params = {
                '$limit': limit,
                '$offset': offset,
                '$select': ','.join(key_properties)
                }
            response = requests.get(api_url, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            records = data.get('results', [])
            
            if not records:
                logger.info(f"✅ Pagination complete. Total records: {len(all_records)}")
                break
            
            all_records.extend(records)
            offset += limit
            logger.info(f"📄 Fetched batch: records {offset-limit} to {offset}")
        
        with open(local_path, 'w') as f:
            json.dump(all_records, f)
        
        logger.info(f"✅ Downloaded: {len(all_records)} records")
        
        context['task_instance'].xcom_push(key='api_records_count', value=len(all_records))
        context['task_instance'].xcom_push(key='api_json_path', value=local_path)
        
        return local_path
        
    except Exception as e:
        logger.error(f"❌ API query failed: {str(e)}")
        raise

query_task = PythonOperator(
    task_id='query_open_payments_api',
    python_callable=query_open_payments_api,
    dag=dag,
)

def convert_json_to_csv(**context):
    """Convertir JSON en CSV - TOUTES les colonnes"""
    warnings.filterwarnings('ignore')
    
    json_path = context['task_instance'].xcom_pull(
        task_ids='query_open_payments_api',
        key='api_json_path'
    )
    
    csv_path = "/tmp/open_payments.csv"
    
    logger.info(f"🔄 Converting JSON to CSV: {json_path}")
    
    try:
        import json
        with open(json_path, 'r') as f:
            records = json.load(f)
        
        # Créer DataFrame avec TOUTES les colonnes
        df = pd.DataFrame(records)
        
        # Sauvegarder en CSV
        df.to_csv(csv_path, index=False)
        
        logger.info(f"✅ Converted: {len(df)} rows, {len(df.columns)} columns")
        
        context['task_instance'].xcom_push(key='csv_row_count', value=len(df))
        context['task_instance'].xcom_push(key='csv_path', value=csv_path)
        
        return csv_path
        
    except Exception as e:
        logger.error(f"❌ JSON-CSV conversion failed: {str(e)}")
        raise

convert_task = PythonOperator(
    task_id='convert_json_to_csv',
    python_callable=convert_json_to_csv,
    dag=dag,
)

def validate_payment_data(**context):
    """Valider les données"""
    warnings.filterwarnings('ignore')
    
    csv_path = context['task_instance'].xcom_pull(
        task_ids='convert_json_to_csv',
        key='csv_path'
    )
    
    logger.info(f"🔍 Validating data quality: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        row_count = df.shape[0]
        col_count = df.shape[1]
        
        if row_count == 0:
            raise ValueError("❌ CSV file is empty!")
        
        logger.info(f"✅ Validation passed: {row_count:,} rows, {col_count} columns")
        
        context['task_instance'].xcom_push(key='validated_row_count', value=row_count)
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {str(e)}")
        raise

validate_task = PythonOperator(
    task_id='validate_payment_data',
    python_callable=validate_payment_data,
    dag=dag,
)

def upload_payments_to_s3(**context):
    """Upload to S3"""
    warnings.filterwarnings('ignore')
    
    csv_path = context['task_instance'].xcom_pull(
        task_ids='convert_json_to_csv',
        key='csv_path'
    )
    row_count = context['task_instance'].xcom_pull(
        task_ids='validate_payment_data',
        key='validated_row_count'
    )
    
    s3_bucket = 'ai-factory-bckt'
    s3_key = 'raw/open_payments/PAYMENTS_LATEST.csv'
    
    logger.info(f"📤 Uploading to S3: s3://{s3_bucket}/{s3_key}")
    
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        s3_hook.load_file(
            filename=csv_path,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        
        logger.info(f"✅ Uploaded {row_count:,} payment records")
        
        context['task_instance'].xcom_push(key='s3_key', value=s3_key)
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ S3 upload failed: {str(e)}")
        raise

upload_task = PythonOperator(
    task_id='upload_payments_s3',
    python_callable=upload_payments_to_s3,
    dag=dag,
)

query_task >> convert_task >> validate_task >> upload_task