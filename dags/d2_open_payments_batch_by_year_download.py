from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from datetime import datetime
import logging
import time
import json
import requests
import boto3
import gzip
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------------------------------------
# CONFIG FACTUELLE
# -------------------------------------------------------------------

BASE_URL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/fb3a65aa-c901-4a38-a813-b04b00dfa2a9/0"
LIMIT = 500
TIMEOUT = 60
MAX_RETRIES = 3
MAX_WORKERS = 5  # nombre de threads parallèles par année

BUCKET = "ai-factory-bckt"
PREFIX = "raw/open_payments"
PROGRAM_YEARS = list(range(2023, 2024))

s3 = boto3.client("s3")

# -------------------------------------------------------------------
# FONCTION BATCH PAR OFFSET (UTILISE DANS THREAD)
# -------------------------------------------------------------------

def download_batch(program_year: int, offset: int, batch_index: int):
    session = requests.Session()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            params = {"limit": LIMIT, "offset": offset, "program_year": program_year}
            response = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            if response.status_code == 403:
                raise RuntimeError(f"403 Forbidden | year={program_year} | offset={offset}")
            response.raise_for_status()
            records = response.json().get("results", [])
            break
        except (requests.RequestException, ValueError) as e:
            logging.warning(f"Attempt {attempt}/{MAX_RETRIES} failed | {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    if not records:
        return 0  # rien à écrire

    # Compression gzip
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as f:
        f.write(json.dumps(records).encode("utf-8"))
    buf.seek(0)

    key = f"{PREFIX}/program_year={program_year}/batch_{batch_index}.json.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.read())

    logging.info(f"year={program_year} | batch={batch_index} | rows={len(records)}")
    return len(records)

# -------------------------------------------------------------------
# FONCTION PRINCIPALE PAR ANNÉE (PARALLÈLE)
# -------------------------------------------------------------------

def download_year_parallel(program_year: int, **context):
    start_time = time.time()
    logging.info(f"START | program_year={program_year}")

    offset = 0
    batch_index = 0
    total_rows = 0
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            # Préparer les batches à envoyer en parallèle
            for i in range(MAX_WORKERS):
                futures.append(executor.submit(download_batch, program_year, offset, batch_index))
                offset += LIMIT
                batch_index += 1

            # Récupérer les résultats
            completed = []
            for future in as_completed(futures):
                rows = future.result()
                total_rows += rows
                completed.append(future)
            # Supprimer les futures terminées
            for c in completed:
                futures.remove(c)

            # Si un batch a retourné 0, on considère qu'on a fini
            if any(f.result() == 0 for f in completed):
                break

    total_time = time.time() - start_time
    logging.info(
        f"DONE | year={program_year} | rows={total_rows} | "
        f"duration={int(total_time)}s | avg_rate={int(total_rows / total_time)} rows/s"
    )

# -------------------------------------------------------------------
# DAG AIRFLOW
# -------------------------------------------------------------------

default_args = {"owner": "MooM", "retries": 1}

with DAG(
    dag_id="open_payments_batch_by_year_download",
    description="Open Payments CMS ingestion par année (batches parallèles, compressés, monitorés)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["open-payments", "cms", "batch"],
) as dag:

    with TaskGroup(group_id="download_by_year") as download_tasks:
        for year in PROGRAM_YEARS:
            PythonOperator(
                task_id=f"download_{year}",
                python_callable=download_year_parallel,
                op_args=[year],
            )

    download_tasks