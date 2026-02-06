from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

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
# PYTHON FUNCTIONS FOR OPEN PAYMENTS (using hook for INFER_SCHEMA)
# -------------------------------------------------------------------
def create_open_payments_table(table_name: str, file_path: str, **context):
    """Create Open Payments table using INFER_SCHEMA via Snowflake Hook"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    sql = f"""
    USE DATABASE FRAUDLENS_DB;
    USE SCHEMA BRONZE;

    CREATE OR REPLACE TABLE {table_name}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(INFER_SCHEMA(
            LOCATION => '@BRONZE_S3_STAGE/open_payments/{file_path}',
            FILE_FORMAT => 'PARQUET_FORMAT'
        ))
    );
    """

    logger.info(f"Creating table {table_name} with INFER_SCHEMA from {file_path}...")
    hook.run(sql)

    # Get column count
    col_result = hook.get_first(f"""
        SELECT COUNT(*) FROM FRAUDLENS_DB.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = '{table_name}'
    """)
    col_count = col_result[0] if col_result else 0
    logger.info(f"Table {table_name} created successfully - {col_count} columns")


def load_open_payments_table(table_name: str, file_path: str, **context):
    """Load Open Payments table via Snowflake Hook"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    sql = f"""
    USE DATABASE FRAUDLENS_DB;
    USE SCHEMA BRONZE;

    COPY INTO {table_name}
    FROM @BRONZE_S3_STAGE/open_payments/{file_path}
    FILE_FORMAT = PARQUET_FORMAT
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

    logger.info(f"Loading data into {table_name}...")
    result = hook.run(sql, handler=lambda cur: cur.fetchall())

    # Log COPY INTO result (files loaded, rows loaded, etc.)
    if result and len(result) > 0:
        for row in result[-1]:  # Last statement result (COPY INTO)
            logger.info(f"COPY result: {row}")

    # Get row count
    count_result = hook.get_first(f"SELECT COUNT(*) FROM FRAUDLENS_DB.BRONZE.{table_name}")
    row_count = count_result[0] if count_result else 0
    logger.info(f"Table {table_name} loaded successfully - {row_count:,} rows")


# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id='load_bronze_tables',
    default_args=default_args,
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/snowflake/bronze"],
    doc_md="""
    # Load Bronze Tables DAG

    Charge les fichiers Parquet depuis S3 vers les tables Bronze dans Snowflake.
    """,
    tags=['snowflake', 'bronze', 'load'],
) as dag:

    # -------------------------------------------------------------------
    # Standard Tables
    # -------------------------------------------------------------------
    create_tables = SnowflakeSqlApiOperator(
        task_id='create_tables',
        snowflake_conn_id='snowflake_default',
        sql='create_tables.sql',
        statement_count=9,
    )

    truncate_tables = SnowflakeSqlApiOperator(
        task_id='truncate_tables',
        snowflake_conn_id='snowflake_default',
        sql='truncate_tables.sql',
        statement_count=9,
    )

    load_tables = SnowflakeSqlApiOperator(
        task_id='load_tables',
        snowflake_conn_id='snowflake_default',
        sql='load_tables.sql',
        statement_count=9,
    )

    # -------------------------------------------------------------------
    # Open Payments - General
    # -------------------------------------------------------------------
    create_op_general = PythonOperator(
        task_id='create_open_payments_general',
        python_callable=create_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_GENERAL',
            'file_path': 'OP_DTL_GNRL_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    load_op_general = PythonOperator(
        task_id='load_open_payments_general',
        python_callable=load_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_GENERAL',
            'file_path': 'OP_DTL_GNRL_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    # -------------------------------------------------------------------
    # Open Payments - Research
    # -------------------------------------------------------------------
    create_op_research = PythonOperator(
        task_id='create_open_payments_research',
        python_callable=create_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_RESEARCH',
            'file_path': 'OP_DTL_RSRCH_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    load_op_research = PythonOperator(
        task_id='load_open_payments_research',
        python_callable=load_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_RESEARCH',
            'file_path': 'OP_DTL_RSRCH_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    # -------------------------------------------------------------------
    # Open Payments - Ownership
    # -------------------------------------------------------------------
    create_op_ownership = PythonOperator(
        task_id='create_open_payments_ownership',
        python_callable=create_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_OWNERSHIP',
            'file_path': 'OP_DTL_OWNRSHP_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    load_op_ownership = PythonOperator(
        task_id='load_open_payments_ownership',
        python_callable=load_open_payments_table,
        op_kwargs={
            'table_name': 'OPEN_PAYMENTS_OWNERSHIP',
            'file_path': 'OP_DTL_OWNRSHP_PGYR2024_P06302025_06162025_LATEST.parquet',
        },
    )

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    def summarize_load(**context):
        logger.info("="*70)
        logger.info("LOAD BRONZE TABLES - COMPLETED")
        logger.info("="*70)

    summary = PythonOperator(
        task_id='summary',
        python_callable=summarize_load,
    )

    # -------------------------------------------------------------------
    # DAG DEPENDENCIES
    # -------------------------------------------------------------------
    # Standard tables
    create_tables >> truncate_tables >> load_tables >> summary

    # Open Payments (en parallele)
    create_op_general >> load_op_general >> summary
    create_op_research >> load_op_research >> summary
    create_op_ownership >> load_op_ownership >> summary
