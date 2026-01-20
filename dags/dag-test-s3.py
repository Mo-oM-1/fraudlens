from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "test_s3_connection",
    default_args=default_args,
    description="Test connexion S3",
    schedule=None,  # ✅ CHANGEMENT: schedule=None (manual only)
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def test_s3_upload():
    """Tester l'upload S3"""
    s3_hook = S3Hook(aws_conn_id="aws_default")
    
    # Uploader un fichier de test
    s3_hook.load_string(
        string_data="✅ Test successful from Airflow!",
        key="test/test_airflow.txt",
        bucket_name="ai-factory-bckt",
        replace=True
    )
    print("✅ Upload réussi vers S3!")

test_task = PythonOperator(
    task_id="test_s3_upload",
    python_callable=test_s3_upload,
    dag=dag,
)