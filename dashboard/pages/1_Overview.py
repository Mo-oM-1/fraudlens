"""
Overview Page - Main KPIs and Executive Summary
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from utils import get_kpis, get_risk_distribution, get_alerts_summary

st.set_page_config(
    page_title="Overview - Fraud Detection",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Healthcare Fraud Detection Dashboard")
st.markdown("### Executive Overview")

# Load data
try:
    kpis = get_kpis()
    risk_dist = get_risk_distribution()
    alerts = get_alerts_summary()

    # Main KPIs Row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="Total Providers",
            value=f"{kpis['TOTAL_PROVIDERS'].iloc[0]:,.0f}",
            help="Total providers in NPPES database"
        )

    with col2:
        st.metric(
            label="With Pharma Payments",
            value=f"{kpis['PROVIDERS_WITH_PAYMENTS'].iloc[0]:,.0f}",
            help="Providers receiving Open Payments"
        )

    with col3:
        st.metric(
            label="With Prescriptions",
            value=f"{kpis['PROVIDERS_WITH_PRESCRIPTIONS'].iloc[0]:,.0f}",
            help="Providers with Part D prescriptions"
        )

    with col4:
        st.metric(
            label="Excluded (LEIE)",
            value=f"{kpis['EXCLUDED_PROVIDERS'].iloc[0]:,.0f}",
            delta=None,
            help="Providers on exclusion list"
        )

    with col5:
        excluded_with_activity = kpis['EXCLUDED_WITH_ACTIVITY'].iloc[0]
        st.metric(
            label="🚨 Excluded + Active",
            value=f"{excluded_with_activity:,.0f}",
            delta="Critical",
            delta_color="inverse",
            help="Excluded providers still with activity"
        )

    st.divider()

    # Second Row - Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Tier Distribution")

        # Order risk tiers
        tier_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'MINIMAL']
        risk_dist['RISK_TIER'] = risk_dist['RISK_TIER'].astype(str)

        colors = {
            'CRITICAL': '#dc3545',
            'HIGH': '#fd7e14',
            'MEDIUM': '#ffc107',
            'LOW': '#20c997',
            'MINIMAL': '#6c757d'
        }

        fig = px.bar(
            risk_dist,
            x='RISK_TIER',
            y='COUNT',
            color='RISK_TIER',
            color_discrete_map=colors,
            category_orders={'RISK_TIER': tier_order},
            labels={'COUNT': 'Number of Providers', 'RISK_TIER': 'Risk Tier'}
        )
        fig.update_layout(
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Alerts by Type")

        if not alerts.empty:
            fig = px.pie(
                alerts,
                values='NB_ALERTS',
                names='ALERT_TYPE',
                color='ALERT_TYPE',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alerts data available")

    st.divider()

    # Third Row - Financial Exposure
    st.subheader("💰 Financial Exposure by Risk Tier")

    col1, col2, col3 = st.columns(3)

    # Calculate totals
    total_exposure = risk_dist['TOTAL_EXPOSURE'].sum() if not risk_dist.empty else 0
    critical_high = risk_dist[risk_dist['RISK_TIER'].isin(['CRITICAL', 'HIGH'])]['TOTAL_EXPOSURE'].sum() if not risk_dist.empty else 0
    alerts_exposure = alerts['TOTAL_EXPOSURE'].sum() if not alerts.empty else 0

    with col1:
        st.metric(
            label="Total Financial Exposure",
            value=f"${total_exposure:,.0f}",
            help="Sum of payments + prescriptions for all providers"
        )

    with col2:
        st.metric(
            label="High Risk Exposure",
            value=f"${critical_high:,.0f}",
            help="Exposure for CRITICAL and HIGH risk providers"
        )

    with col3:
        st.metric(
            label="Alerts Total Exposure",
            value=f"${alerts_exposure:,.0f}",
            help="Financial exposure from active alerts"
        )

    # Detailed Alerts Table
    st.divider()
    st.subheader("📊 Alerts Summary")

    if not alerts.empty:
        alerts_display = alerts.copy()
        alerts_display['TOTAL_EXPOSURE'] = alerts_display['TOTAL_EXPOSURE'].apply(lambda x: f"${x:,.0f}" if x else "N/A")
        st.dataframe(
            alerts_display,
            column_config={
                "ALERT_TYPE": "Alert Type",
                "RISK_TIER": "Risk Tier",
                "NB_ALERTS": st.column_config.NumberColumn("# Alerts", format="%d"),
                "TOTAL_EXPOSURE": "Financial Exposure"
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("No alerts to display")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure Snowflake credentials are configured correctly.")
