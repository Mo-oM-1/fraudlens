"""
Snowflake connection utility for the dashboard.
"""
import os
import streamlit as st
import snowflake.connector
import pandas as pd
from pathlib import Path


def get_snowflake_connection():
    """Create a Snowflake connection using secrets or environment variables."""

    # Try Streamlit secrets first (for cloud deployment)
    if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"].get("schema", "GOLD")
        )
    else:
        # Local development - use environment variables
        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.environ.get('SNOWFLAKE_DATABASE', 'AI_FACTORY_DB'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA', 'GOLD')
        )

    return conn


@st.cache_data(ttl=600)  # Cache for 10 minutes
def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        conn.close()


def get_kpis():
    """Get main KPIs for the overview page."""
    query = """
    SELECT
        COUNT(*) as TOTAL_PROVIDERS,
        COUNT(CASE WHEN HAS_PHARMA_PAYMENTS THEN 1 END) as PROVIDERS_WITH_PAYMENTS,
        COUNT(CASE WHEN HAS_PRESCRIPTIONS THEN 1 END) as PROVIDERS_WITH_PRESCRIPTIONS,
        COUNT(CASE WHEN IS_EXCLUDED THEN 1 END) as EXCLUDED_PROVIDERS,
        COUNT(CASE WHEN IS_EXCLUDED AND (HAS_PHARMA_PAYMENTS OR HAS_PRESCRIPTIONS) THEN 1 END) as EXCLUDED_WITH_ACTIVITY
    FROM AI_FACTORY_DB.GOLD.PROVIDER_360
    """
    return run_query(query)


def get_risk_distribution():
    """Get fraud risk score distribution."""
    query = """
    SELECT
        RISK_TIER,
        COUNT(*) as COUNT,
        ROUND(AVG(FRAUD_RISK_SCORE), 1) as AVG_SCORE,
        ROUND(SUM(TOTAL_FINANCIAL_EXPOSURE), 0) as TOTAL_EXPOSURE
    FROM AI_FACTORY_DB.GOLD.FRAUD_RISK_SCORE
    WHERE RISK_TIER IS NOT NULL
    GROUP BY RISK_TIER
    ORDER BY AVG_SCORE DESC
    """
    return run_query(query)


def get_alerts_summary():
    """Get alerts summary by type."""
    query = """
    SELECT
        ALERT_TYPE,
        RISK_TIER,
        COUNT(*) as NB_ALERTS,
        ROUND(SUM(FINANCIAL_EXPOSURE), 0) as TOTAL_EXPOSURE
    FROM AI_FACTORY_DB.GOLD.HIGH_RISK_ALERTS
    GROUP BY ALERT_TYPE, RISK_TIER
    ORDER BY NB_ALERTS DESC
    """
    return run_query(query)


def get_alerts_list(alert_type: str = None, limit: int = 100):
    """Get list of alerts with optional filtering."""
    where_clause = f"WHERE ALERT_TYPE = '{alert_type}'" if alert_type else ""
    query = f"""
    SELECT
        ALERT_ID,
        ALERT_TYPE,
        NPI,
        PROVIDER_NAME,
        SPECIALTY,
        STATE,
        RISK_SCORE,
        RISK_TIER,
        ALERT_DESCRIPTION,
        FINANCIAL_EXPOSURE,
        PRIORITY_RANK
    FROM AI_FACTORY_DB.GOLD.HIGH_RISK_ALERTS
    {where_clause}
    ORDER BY PRIORITY_RANK, RISK_SCORE DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_provider_details(npi: str):
    """Get full provider details from Provider 360."""
    query = f"""
    SELECT *
    FROM AI_FACTORY_DB.GOLD.PROVIDER_360
    WHERE NPI = '{npi}'
    """
    return run_query(query)


def get_provider_alerts(npi: str):
    """Get alerts for a specific provider."""
    query = f"""
    SELECT *
    FROM AI_FACTORY_DB.GOLD.HIGH_RISK_ALERTS
    WHERE NPI = '{npi}'
    ORDER BY PRIORITY_RANK
    """
    return run_query(query)


def search_providers(search_term: str, limit: int = 50):
    """Search providers by NPI or name."""
    query = f"""
    SELECT
        NPI,
        COALESCE(FULL_NAME, ORGANIZATION_NAME) as NAME,
        ENTITY_TYPE,
        SPECIALTY,
        STATE,
        IS_EXCLUDED,
        TOTAL_FINANCIAL_EXPOSURE
    FROM AI_FACTORY_DB.GOLD.PROVIDER_360
    WHERE NPI LIKE '%{search_term}%'
       OR UPPER(FULL_NAME) LIKE '%{search_term.upper()}%'
       OR UPPER(ORGANIZATION_NAME) LIKE '%{search_term.upper()}%'
    ORDER BY TOTAL_FINANCIAL_EXPOSURE DESC NULLS LAST
    LIMIT {limit}
    """
    return run_query(query)


def get_payments_by_state():
    """Get payment aggregations by state."""
    query = """
    SELECT
        p.STATE,
        COUNT(DISTINCT p.NPI) as PROVIDER_COUNT,
        SUM(ps.TOTAL_PAYMENT_AMOUNT) as TOTAL_PAYMENTS,
        AVG(ps.TOTAL_PAYMENT_AMOUNT) as AVG_PAYMENT
    FROM AI_FACTORY_DB.GOLD.PAYMENTS_SUMMARY ps
    JOIN AI_FACTORY_DB.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    WHERE p.STATE IS NOT NULL
    GROUP BY p.STATE
    ORDER BY TOTAL_PAYMENTS DESC
    """
    return run_query(query)


def get_top_recipients():
    """Get top payment recipients."""
    query = """
    SELECT
        p.NPI,
        COALESCE(p.FULL_NAME, p.ORGANIZATION_NAME) as NAME,
        p.SPECIALTY,
        p.STATE,
        ps.TOTAL_PAYMENT_AMOUNT,
        ps.RECIPIENT_TIER,
        p.IS_EXCLUDED
    FROM AI_FACTORY_DB.GOLD.PAYMENTS_SUMMARY ps
    JOIN AI_FACTORY_DB.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    ORDER BY ps.TOTAL_PAYMENT_AMOUNT DESC
    LIMIT 100
    """
    return run_query(query)


def get_excluded_with_activity():
    """Get excluded providers that still have activity."""
    query = """
    SELECT
        p.NPI,
        COALESCE(p.FULL_NAME, p.ORGANIZATION_NAME) as NAME,
        p.SPECIALTY,
        p.STATE,
        p.EXCLUSION_REASON,
        p.EXCLUSION_DATE,
        p.TOTAL_PAYMENT_AMOUNT,
        p.TOTAL_PRESCRIPTION_COST,
        p.TOTAL_FINANCIAL_EXPOSURE
    FROM AI_FACTORY_DB.GOLD.PROVIDER_360 p
    WHERE p.IS_EXCLUDED = TRUE
      AND (p.HAS_PHARMA_PAYMENTS = TRUE OR p.HAS_PRESCRIPTIONS = TRUE)
    ORDER BY p.TOTAL_FINANCIAL_EXPOSURE DESC
    LIMIT 100
    """
    return run_query(query)
