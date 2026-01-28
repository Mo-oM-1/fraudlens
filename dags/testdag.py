from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id='test_snowflake_connection',
    default_args=default_args,
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/snowflake"],  # dossier où tu mets SQL si besoin
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Test simple
    # -------------------------------------------------------------------
    test_connection = SnowflakeSqlApiOperator(
        task_id='test_connection',
        snowflake_conn_id='snowflake_default',  # connexion que tu as créée via l'UI
        sql='SELECT CURRENT_VERSION();'
    )