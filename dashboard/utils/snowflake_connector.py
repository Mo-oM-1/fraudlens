"""
Snowflake connection utility for the Streamlit dashboard.
"""
import streamlit as st
import snowflake.connector
import pandas as pd


@st.cache_resource
def get_snowflake_connection():
    """Create a cached Snowflake connection. Reused across requests."""
    config = st.secrets["snowflake"]

    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config.get("schema", "GOLD"),
    )


@st.cache_data(ttl=2592000)
def run_query(query: str, params: tuple | None = None) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame. Cached for 1 month."""
    conn = get_snowflake_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, params or ())
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)


def get_kpis():
    """Get main KPIs for the overview page."""
    query = """
    SELECT
        COUNT(*) as TOTAL_PROVIDERS,
        COUNT(CASE WHEN HAS_PHARMA_PAYMENTS THEN 1 END) as PROVIDERS_WITH_PAYMENTS,
        COUNT(CASE WHEN HAS_PRESCRIPTIONS THEN 1 END) as PROVIDERS_WITH_PRESCRIPTIONS,
        COUNT(CASE WHEN IS_EXCLUDED THEN 1 END) as EXCLUDED_PROVIDERS,
        COUNT(CASE WHEN IS_EXCLUDED AND (HAS_PHARMA_PAYMENTS OR HAS_PRESCRIPTIONS) THEN 1 END) as EXCLUDED_WITH_ACTIVITY
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360
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
    FROM FRAUDLENS_DB.GOLD.FRAUD_RISK_SCORE
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
    FROM FRAUDLENS_DB.GOLD.HIGH_RISK_ALERTS
    GROUP BY ALERT_TYPE, RISK_TIER
    ORDER BY NB_ALERTS DESC
    """
    return run_query(query)


def get_alerts_list(alert_type: str | None = None, limit: int = 100):
    """Get list of alerts with optional filtering."""
    base_query = """
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
    FROM FRAUDLENS_DB.GOLD.HIGH_RISK_ALERTS
    """
    params = []
    where_clauses = []

    if alert_type:
        where_clauses.append("ALERT_TYPE = %s")
        params.append(alert_type)

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    base_query += " ORDER BY PRIORITY_RANK, RISK_SCORE DESC LIMIT %s"
    params.append(limit)

    return run_query(base_query, tuple(params))


def get_provider_details(npi: str):
    """Get full provider details from Provider 360."""
    query = """
    SELECT *
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360
    WHERE NPI = %s
    """
    return run_query(query, (npi,))


def get_provider_alerts(npi: str):
    """Get alerts for a specific provider."""
    query = """
    SELECT *
    FROM FRAUDLENS_DB.GOLD.HIGH_RISK_ALERTS
    WHERE NPI = %s
    ORDER BY PRIORITY_RANK
    """
    return run_query(query, (npi,))


def search_providers(search_term: str, limit: int = 50):
    """Search providers by NPI or name."""
    like = f"%{search_term.upper()}%"
    query = """
    SELECT
        NPI,
        COALESCE(FULL_NAME, ORGANIZATION_NAME) as NAME,
        ENTITY_TYPE,
        SPECIALTY,
        STATE,
        IS_EXCLUDED,
        TOTAL_FINANCIAL_EXPOSURE
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360
    WHERE NPI LIKE %s
       OR UPPER(FULL_NAME) LIKE %s
       OR UPPER(ORGANIZATION_NAME) LIKE %s
    ORDER BY TOTAL_FINANCIAL_EXPOSURE DESC NULLS LAST
    LIMIT %s
    """
    params = (like, like, like, limit)
    return run_query(query, params)


def get_payments_by_state():
    """Get payment aggregations by state."""
    query = """
    SELECT
        p.STATE,
        COUNT(DISTINCT p.NPI) as PROVIDER_COUNT,
        SUM(ps.TOTAL_PAYMENT_AMOUNT) as TOTAL_PAYMENTS,
        AVG(ps.TOTAL_PAYMENT_AMOUNT) as AVG_PAYMENT
    FROM FRAUDLENS_DB.GOLD.PAYMENTS_SUMMARY ps
    JOIN FRAUDLENS_DB.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    WHERE p.STATE IS NOT NULL
    GROUP BY p.STATE
    ORDER BY TOTAL_PAYMENTS DESC
    """
    return run_query(query)


def get_top_recipients():
    """Get top payment recipients with excluded providers prioritized."""
    query = """
    SELECT
        p.NPI,
        COALESCE(p.FULL_NAME, p.ORGANIZATION_NAME) as NAME,
        p.SPECIALTY,
        p.STATE,
        ps.TOTAL_PAYMENT_AMOUNT,
        ps.RECIPIENT_TIER,
        p.IS_EXCLUDED
    FROM FRAUDLENS_DB.GOLD.PAYMENTS_SUMMARY ps
    JOIN FRAUDLENS_DB.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    ORDER BY p.IS_EXCLUDED DESC, ps.TOTAL_PAYMENT_AMOUNT DESC
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
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360 p
    WHERE p.IS_EXCLUDED = TRUE
      AND (p.HAS_PHARMA_PAYMENTS = TRUE OR p.HAS_PRESCRIPTIONS = TRUE)
    ORDER BY p.TOTAL_FINANCIAL_EXPOSURE DESC
    LIMIT 100
    """
    return run_query(query)
