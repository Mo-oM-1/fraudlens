from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

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
# Liste des DAGs de données à orchestrer
# -------------------------------------------------------------------
DATA_DAGS = [
    'leie_download',
    'medicare_hospital_spending_download',
    'open_payments_download',
    'provider_information_download',
    'longterm_care_hospital_download',
    'hospice_download',
    'home_health_care_download',
    'medicare_part_d_prescribers_download',
]

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id='master_dag',
    default_args=default_args,
    description='DAG master qui orchestre tous les pipelines de données',
    schedule=None,  # Déclenchement manuel uniquement
    catchup=False,
    tags=['master', 'orchestration'],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Initialiser l'environnement Snowflake
    # S'assure que le warehouse, la DB et les schemas existent
    # -------------------------------------------------------------------
    init_snowflake = TriggerDagRunOperator(
        task_id='init_snowflake_environment',
        trigger_dag_id='init_snowflake_environment',
        wait_for_completion=True,
        poke_interval=30,  # Vérifie toutes les 30 secondes
        failed_states=['failed'],
    )

    # -------------------------------------------------------------------
    # TASK 2 : Point de synchronisation avant les téléchargements
    # -------------------------------------------------------------------
    start_downloads = EmptyOperator(
        task_id='start_downloads',
    )

    # -------------------------------------------------------------------
    # TASK 3 : Lancer tous les DAGs de données en parallèle
    # -------------------------------------------------------------------
    download_tasks = []
    for dag_id in DATA_DAGS:
        task = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=60,  # Vérifie toutes les 60 secondes
            failed_states=['failed'],
        )
        download_tasks.append(task)

    # -------------------------------------------------------------------
    # TASK 4 : Point de synchronisation après les téléchargements
    # -------------------------------------------------------------------
    all_downloads_complete = EmptyOperator(
        task_id='all_downloads_complete',
        trigger_rule='all_success',
    )

    # -------------------------------------------------------------------
    # TASK 5 : Charger les données dans Snowflake Bronze
    # -------------------------------------------------------------------
    load_bronze = TriggerDagRunOperator(
        task_id='load_bronze_tables',
        trigger_dag_id='load_bronze_tables',
        wait_for_completion=True,
        poke_interval=60,
        failed_states=['failed'],
    )

    # -------------------------------------------------------------------
    # TASK 6 : Pipeline terminé
    # -------------------------------------------------------------------
    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete',
    )

    # -------------------------------------------------------------------
    # DEPENDENCIES
    # -------------------------------------------------------------------
    init_snowflake >> start_downloads >> download_tasks >> all_downloads_complete
    all_downloads_complete >> load_bronze >> pipeline_complete
