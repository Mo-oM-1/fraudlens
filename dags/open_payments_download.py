import boto3
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'moom',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'open_payments_glue',
    default_args=default_args,
    description='Run Glue job Open Payments CMS',
    start_date=datetime(2026, 1, 21),
    schedule=None,
    catchup=False,
)

def run_glue_job_boto3():
    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.start_job_run(JobName='open-payments-api-to-s3')
    job_run_id = response['JobRunId']
    print(f"Job started: {job_run_id}")

    # wait for completion
    waiter = glue_client.get_waiter('job_run_succeeded')
    waiter.wait(JobName='open-payments-api-to-s3', RunId=job_run_id)
    print(f"Job finished: {job_run_id}")

run_glue_task = PythonOperator(
    task_id='run_open_payments_glue',
    python_callable=run_glue_job_boto3,
    dag=dag
)