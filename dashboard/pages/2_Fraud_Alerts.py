"""
Fraud Alerts Page - List and filter alerts
"""
import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils import get_alerts_list, get_alerts_summary

st.set_page_config(
    page_title="Fraud Alerts",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 Fraud Alerts")
st.markdown("### Actionable alerts requiring investigation")

try:
    # Get alerts summary for filter options
    alerts_summary = get_alerts_summary()
    alert_types = ['All'] + alerts_summary['ALERT_TYPE'].unique().tolist() if not alerts_summary.empty else ['All']

    # Filters
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        selected_type = st.selectbox(
            "Filter by Alert Type",
            options=alert_types,
            index=0
        )

    with col2:
        limit = st.slider("Number of alerts to display", 10, 500, 100)

    with col3:
        st.write("")  # Spacer
        refresh = st.button("🔄 Refresh", use_container_width=True)

    st.divider()

    # Load alerts
    filter_type = None if selected_type == 'All' else selected_type
    alerts = get_alerts_list(alert_type=filter_type, limit=limit)

    if not alerts.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Alerts", f"{len(alerts):,}")

        with col2:
            critical_count = len(alerts[alerts['RISK_TIER'] == 'CRITICAL'])
            st.metric("Critical", critical_count)

        with col3:
            high_count = len(alerts[alerts['RISK_TIER'] == 'HIGH'])
            st.metric("High Risk", high_count)

        with col4:
            total_exposure = alerts['FINANCIAL_EXPOSURE'].sum()
            st.metric("Total Exposure", f"${total_exposure:,.0f}")

        st.divider()

        # Priority distribution chart
        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("By Priority")
            priority_counts = alerts.groupby('PRIORITY_RANK').size().reset_index(name='count')
            fig = px.bar(
                priority_counts,
                x='PRIORITY_RANK',
                y='count',
                color='count',
                color_continuous_scale='Reds',
                labels={'PRIORITY_RANK': 'Priority Rank', 'count': 'Count'}
            )
            fig.update_layout(
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("By Risk Tier")
            tier_counts = alerts.groupby('RISK_TIER').size().reset_index(name='count')
            colors = {
                'CRITICAL': '#dc3545',
                'HIGH': '#fd7e14',
                'MEDIUM': '#ffc107',
                'LOW': '#20c997'
            }
            fig = px.pie(
                tier_counts,
                values='count',
                names='RISK_TIER',
                color='RISK_TIER',
                color_discrete_map=colors
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Alerts table
        st.subheader("📋 Alerts List")

        # Color code by risk tier
        def highlight_risk(row):
            colors = {
                'CRITICAL': 'background-color: rgba(220, 53, 69, 0.3)',
                'HIGH': 'background-color: rgba(253, 126, 20, 0.3)',
                'MEDIUM': 'background-color: rgba(255, 193, 7, 0.2)',
                'LOW': 'background-color: rgba(32, 201, 151, 0.2)'
            }
            return [colors.get(row['RISK_TIER'], '')] * len(row)

        # Format financial exposure
        alerts_display = alerts.copy()
        alerts_display['FINANCIAL_EXPOSURE'] = alerts_display['FINANCIAL_EXPOSURE'].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
        )

        st.dataframe(
            alerts_display,
            column_config={
                "ALERT_ID": None,  # Hide
                "ALERT_TYPE": st.column_config.TextColumn("Type", width="medium"),
                "NPI": st.column_config.TextColumn("NPI", width="small"),
                "PROVIDER_NAME": st.column_config.TextColumn("Provider", width="large"),
                "SPECIALTY": st.column_config.TextColumn("Specialty", width="medium"),
                "STATE": st.column_config.TextColumn("State", width="small"),
                "RISK_SCORE": st.column_config.ProgressColumn(
                    "Risk Score",
                    min_value=0,
                    max_value=100,
                    format="%d"
                ),
                "RISK_TIER": st.column_config.TextColumn("Risk Tier", width="small"),
                "ALERT_DESCRIPTION": st.column_config.TextColumn("Description", width="large"),
                "FINANCIAL_EXPOSURE": st.column_config.TextColumn("Exposure", width="medium"),
                "PRIORITY_RANK": st.column_config.NumberColumn("Priority", width="small")
            },
            hide_index=True,
            use_container_width=True,
            height=500
        )

        # Download button
        csv = alerts.to_csv(index=False)
        st.download_button(
            label="📥 Download Alerts CSV",
            data=csv,
            file_name="fraud_alerts.csv",
            mime="text/csv"
        )

    else:
        st.info("No alerts found with the current filters.")

except Exception as e:
    st.error(f"Error loading alerts: {str(e)}")
    import traceback
    st.code(traceback.format_exc())
