from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta
import os

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id='init_snowflake_environment',
    default_args=default_args,
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/snowflake"],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Create warehouse
    # -------------------------------------------------------------------
    create_warehouse = SnowflakeSqlApiOperator(
        task_id='create_warehouse',
        snowflake_conn_id='snowflake_default',
        sql='init_warehouse.sql',
        statement_count=1,
    )

    # -------------------------------------------------------------------
    # TASK 2 : Create database & schemas
    # -------------------------------------------------------------------
    create_schemas = SnowflakeSqlApiOperator(
        task_id='create_schemas',
        snowflake_conn_id='snowflake_default',
        sql='init_schemas.sql',
        statement_count=7,
    )

    # -------------------------------------------------------------------
    # TASK 3 : Create file format and S3 stage
    # Peut utiliser snowflake_default car warehouse et DB existent
    # -------------------------------------------------------------------
    create_s3_stage = SnowflakeSqlApiOperator(
        task_id='create_s3_stage',
        snowflake_conn_id='snowflake_default',
        sql='init_s3_stage.sql',
        statement_count=4,
    )

    # -------------------------------------------------------------------
    # DEPENDENCIES
    # -------------------------------------------------------------------
    create_warehouse >> create_schemas >> create_s3_stage