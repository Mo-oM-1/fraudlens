"""
DAG: dbt Transformations Pipeline
---------------------------------
Runs dbt transformations to build Silver and Gold layers.
Schedule: Daily after all data ingestion DAGs complete.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# Configuration
DBT_PROJECT_PATH = Variable.get("DBT_PROJECT_PATH", default_var="/opt/airflow/dbt")
DBT_PROFILES_PATH = Variable.get("DBT_PROFILES_PATH", default_var="/opt/airflow/dbt")

default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

def log_dbt_start(**context):
    """Log dbt run start."""
    logger.info("Starting dbt transformations...")
    logger.info(f"DBT Project Path: {DBT_PROJECT_PATH}")
    return "dbt run started"


def log_dbt_complete(**context):
    """Log dbt run completion."""
    logger.info("dbt transformations completed successfully!")
    return "dbt run completed"


with DAG(
    dag_id="dbt_transformations",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    doc_md="""
    ## dbt Transformations DAG

    Runs dbt to build Silver and Gold layers from Bronze data.

    ### Execution Order:
    1. Silver layer models (cleaned & standardized data)
    2. Gold layer models (business aggregations & fraud scoring)

    ### Gold Layer Models:
    - **payments_summary**: Aggregated pharma payments by provider
    - **prescriptions_summary**: Aggregated prescriptions by provider
    - **provider_360**: Complete provider profile with all metrics
    - **fraud_risk_score**: Composite fraud risk scoring (0-100)
    - **high_risk_alerts**: Actionable fraud alerts

    ### Dependencies:
    This DAG should run after all data ingestion DAGs complete.
    """,
    tags=['dbt', 'transformations', 'silver', 'gold']
) as dag:

    # Task 1: Log start
    start_task = PythonOperator(
        task_id="log_start",
        python_callable=log_dbt_start
    )

    # Task 2: Run dbt deps (install packages)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt deps --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 3: Run dbt Staging layer (views on Bronze)
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --select staging --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 4: Run dbt Silver layer
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --select silver --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 5: Run dbt Gold layer
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --select gold --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 6: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt test --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 7: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt docs generate --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Task 8: Log completion
    complete_task = PythonOperator(
        task_id="log_complete",
        python_callable=log_dbt_complete
    )

    # Define task dependencies
    start_task >> dbt_deps >> dbt_run_staging >> dbt_run_silver >> dbt_run_gold >> dbt_test >> dbt_docs >> complete_task
