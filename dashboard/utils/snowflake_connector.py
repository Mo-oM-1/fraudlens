"""
Snowflake connection utility for the dashboard.
Optimized with caching best practices.
"""
import streamlit as st
import snowflake.connector
import pandas as pd


@st.cache_resource
def get_snowflake_connection():
    """
    Create a cached Snowflake connection.
    Uses @cache_resource to maintain a single connection across reruns.
    """
    if hasattr(st, 'secrets') and 'connections' in st.secrets and 'snowflake' in st.secrets['connections']:
        # New format: [connections.snowflake]
        config = st.secrets["connections"]["snowflake"]
    elif hasattr(st, 'secrets') and 'snowflake' in st.secrets:
        # Old format: [snowflake]
        config = st.secrets["snowflake"]
    else:
        raise ValueError("Snowflake credentials not found in secrets")

    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config.get("schema", "GOLD")
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame. Cached for 10 minutes."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except snowflake.connector.errors.ProgrammingError:
        # Connection might be stale, clear cache and retry
        get_snowflake_connection.clear()
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)


# =============================================================================
# KPI & Overview Queries
# =============================================================================

@st.cache_data(ttl=600)
def get_kpis() -> pd.DataFrame:
    """Get main KPIs for the overview page."""
    query = """
    SELECT
        COUNT(*) as TOTAL_PROVIDERS,
        COUNT(CASE WHEN HAS_PHARMA_PAYMENTS THEN 1 END) as PROVIDERS_WITH_PAYMENTS,
        COUNT(CASE WHEN HAS_PRESCRIPTIONS THEN 1 END) as PROVIDERS_WITH_PRESCRIPTIONS,
        COUNT(CASE WHEN IS_EXCLUDED THEN 1 END) as EXCLUDED_PROVIDERS,
        COUNT(CASE WHEN IS_EXCLUDED AND (HAS_PHARMA_PAYMENTS OR HAS_PRESCRIPTIONS) THEN 1 END) as EXCLUDED_WITH_ACTIVITY
    FROM FRAUDLENS.GOLD.PROVIDER_360
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_risk_distribution() -> pd.DataFrame:
    """Get fraud risk score distribution."""
    query = """
    SELECT
        RISK_TIER,
        COUNT(*) as COUNT,
        ROUND(AVG(FRAUD_RISK_SCORE), 1) as AVG_SCORE,
        ROUND(SUM(TOTAL_FINANCIAL_EXPOSURE), 0) as TOTAL_EXPOSURE
    FROM FRAUDLENS.GOLD.FRAUD_RISK_SCORE
    WHERE RISK_TIER IS NOT NULL
    GROUP BY RISK_TIER
    ORDER BY AVG_SCORE DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_alerts_summary() -> pd.DataFrame:
    """Get alerts summary by type."""
    query = """
    SELECT
        ALERT_TYPE,
        RISK_TIER,
        COUNT(*) as NB_ALERTS,
        ROUND(SUM(FINANCIAL_EXPOSURE), 0) as TOTAL_EXPOSURE
    FROM FRAUDLENS.GOLD.HIGH_RISK_ALERTS
    GROUP BY ALERT_TYPE, RISK_TIER
    ORDER BY NB_ALERTS DESC
    """
    return run_query(query)


# =============================================================================
# Alerts Queries
# =============================================================================

@st.cache_data(ttl=300)
def get_alerts_list(alert_type: str = None, limit: int = 100) -> pd.DataFrame:
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
    FROM FRAUDLENS.GOLD.HIGH_RISK_ALERTS
    {where_clause}
    ORDER BY PRIORITY_RANK, RISK_SCORE DESC
    LIMIT {limit}
    """
    return run_query(query)


# =============================================================================
# Provider Queries
# =============================================================================

@st.cache_data(ttl=300)
def get_provider_details(npi: str) -> pd.DataFrame:
    """Get full provider details from Provider 360."""
    query = f"""
    SELECT *
    FROM FRAUDLENS.GOLD.PROVIDER_360
    WHERE NPI = '{npi}'
    """
    return run_query(query)


@st.cache_data(ttl=300)
def get_provider_alerts(npi: str) -> pd.DataFrame:
    """Get alerts for a specific provider."""
    query = f"""
    SELECT *
    FROM FRAUDLENS.GOLD.HIGH_RISK_ALERTS
    WHERE NPI = '{npi}'
    ORDER BY PRIORITY_RANK
    """
    return run_query(query)


@st.cache_data(ttl=120)
def search_providers(search_term: str, limit: int = 50) -> pd.DataFrame:
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
    FROM FRAUDLENS.GOLD.PROVIDER_360
    WHERE NPI LIKE '%{search_term}%'
       OR UPPER(FULL_NAME) LIKE '%{search_term.upper()}%'
       OR UPPER(ORGANIZATION_NAME) LIKE '%{search_term.upper()}%'
    ORDER BY TOTAL_FINANCIAL_EXPOSURE DESC NULLS LAST
    LIMIT {limit}
    """
    return run_query(query)


# =============================================================================
# Analytics Queries
# =============================================================================

@st.cache_data(ttl=600)
def get_payments_by_state() -> pd.DataFrame:
    """Get payment aggregations by state."""
    query = """
    SELECT
        p.STATE,
        COUNT(DISTINCT p.NPI) as PROVIDER_COUNT,
        SUM(ps.TOTAL_PAYMENT_AMOUNT) as TOTAL_PAYMENTS,
        AVG(ps.TOTAL_PAYMENT_AMOUNT) as AVG_PAYMENT
    FROM FRAUDLENS.GOLD.PAYMENTS_SUMMARY ps
    JOIN FRAUDLENS.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    WHERE p.STATE IS NOT NULL
    GROUP BY p.STATE
    ORDER BY TOTAL_PAYMENTS DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_top_recipients() -> pd.DataFrame:
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
    FROM FRAUDLENS.GOLD.PAYMENTS_SUMMARY ps
    JOIN FRAUDLENS.GOLD.PROVIDER_360 p ON ps.NPI = p.NPI
    ORDER BY p.IS_EXCLUDED DESC, ps.TOTAL_PAYMENT_AMOUNT DESC
    LIMIT 100
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_excluded_with_activity() -> pd.DataFrame:
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
    FROM FRAUDLENS.GOLD.PROVIDER_360 p
    WHERE p.IS_EXCLUDED = TRUE
      AND (p.HAS_PHARMA_PAYMENTS = TRUE OR p.HAS_PRESCRIPTIONS = TRUE)
    ORDER BY p.TOTAL_FINANCIAL_EXPOSURE DESC
    LIMIT 100
    """
    return run_query(query)
