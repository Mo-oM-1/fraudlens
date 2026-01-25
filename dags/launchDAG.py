import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# -------------------------------------------------------------------
# DAG LIST
# -------------------------------------------------------------------
DAG_CSV_LIST = [
    "leie_download",
    "medicare_hospital_spending_download",
    "open_payments_download",
    "provider_information_download",
    "longterm_care_hospital_download",
    "hospice_download",
    "home_health_care_download"
]

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id="launchDAG",
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # Orchestrateur CSV
    Lancement parallèle des DAG CSV et résumé global des exécutions.
    """,
    tags=['orchestrator', 'csv', 's3'],
) as dag:

    # -------------------------------------------------------------------
    # TASK : Trigger DAGs CSV
    # -------------------------------------------------------------------
    trigger_tasks = []
    for dag_name in DAG_CSV_LIST:
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dag_name}",
            trigger_dag_id=dag_name,
            wait_for_completion=True,  # Attend que le DAG termine avant de continuer
        )
        trigger_tasks.append(trigger)

    # -------------------------------------------------------------------
    # TASK : Summary Orchestrator
    # -------------------------------------------------------------------
    def summary_orchestrator(**context):
        logger.info("="*70)
        logger.info("📊 CSV ORCHESTRATOR - SUMMARY")
        logger.info("="*70)
        for dag_name in DAG_CSV_LIST:
            logger.info(f"✅ DAG '{dag_name}' terminé à {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id="summary_orchestrator",
        python_callable=summary_orchestrator,
    )

    # -------------------------------------------------------------------
    # DAG DEPENDENCIES
    # -------------------------------------------------------------------
    trigger_tasks >> summary_task