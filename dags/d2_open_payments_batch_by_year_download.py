from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
import time
import json
import requests
import boto3

# -------------------------------------------------------------------
# CONFIG FACTUELLE
# -------------------------------------------------------------------

BASE_URL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/fb3a65aa-c901-4a38-a813-b04b00dfa2a9/0"
LIMIT = 500
TIMEOUT = 60

BUCKET = "ai-factory-bckt"
PREFIX = "raw/open_payments"

PROGRAM_YEARS = list(range(2013, 2024))  # adapter si besoin

s3 = boto3.client("s3")


# -------------------------------------------------------------------
# FONCTION BATCH PAR ANNÉE (AVEC MÉTRIQUES)
# -------------------------------------------------------------------

def download_year(program_year: int):
    session = requests.Session()

    offset = 0
    batch_index = 0
    total_rows = 0

    start_time = time.time()

    logging.info(f"START | program_year={program_year}")

    while True:
        params = {
            "limit": LIMIT,
            "offset": offset,
            "program_year": program_year
        }

        response = session.get(BASE_URL, params=params, timeout=TIMEOUT)

        if response.status_code == 403:
            raise RuntimeError(
                f"403 Forbidden | year={program_year} | offset={offset}"
            )

        response.raise_for_status()
        payload = response.json()
        records = payload.get("results", [])

        if not records:
            break

        key = (
            f"{PREFIX}/program_year={program_year}/"
            f"batch_{batch_index}.json"
        )

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(records).encode("utf-8")
        )

        batch_rows = len(records)
        total_rows += batch_rows
        offset += LIMIT
        batch_index += 1

        elapsed = time.time() - start_time
        rows_per_sec = total_rows / elapsed if elapsed > 0 else 0

        logging.info(
            f"year={program_year} | "
            f"batch={batch_index} | "
            f"batch_rows={batch_rows} | "
            f"total_rows={total_rows} | "
            f"elapsed={int(elapsed)}s | "
            f"rate={int(rows_per_sec)} rows/s"
        )

    total_time = time.time() - start_time

    logging.info(
        f"DONE | year={program_year} | "
        f"rows={total_rows} | "
        f"batches={batch_index} | "
        f"duration={int(total_time)}s | "
        f"avg_rate={int(total_rows / total_time)} rows/s"
    )


# -------------------------------------------------------------------
# DAG AIRFLOW
# -------------------------------------------------------------------

default_args = {
    "owner": "moom",
    "retries": 1,
}

with DAG(
    dag_id="open_payments_batch_by_year",
    description="Ingestion Open Payments CMS par année (batch, monitoré)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["open-payments", "cms", "batch"],
) as dag:

    with TaskGroup(group_id="download_by_year") as download_tasks:
        for year in PROGRAM_YEARS:
            PythonOperator(
                task_id=f"download_{year}",
                python_callable=download_year,
                op_args=[year],
            )

    download_tasks