"""
Analytics Page - Charts and trends
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils import (
    get_payments_by_state,
    get_top_recipients,
    get_risk_distribution,
    run_query
)

st.set_page_config(
    page_title="Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Analytics & Insights")
st.markdown("### Data visualizations and trends")

# Tabs for different analytics views
tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Geographic", "💰 Payments", "💊 Prescriptions", "⚠️ Risk Analysis"])

# Geographic Analysis
with tab1:
    st.subheader("Geographic Distribution")

    try:
        payments_by_state = get_payments_by_state()

        if not payments_by_state.empty:
            # Convert numeric columns
            payments_by_state['TOTAL_PAYMENTS'] = pd.to_numeric(payments_by_state['TOTAL_PAYMENTS'], errors='coerce')
            payments_by_state['AVG_PAYMENT'] = pd.to_numeric(payments_by_state['AVG_PAYMENT'], errors='coerce')
            payments_by_state['PROVIDER_COUNT'] = pd.to_numeric(payments_by_state['PROVIDER_COUNT'], errors='coerce')

            col1, col2 = st.columns(2)

            with col1:
                # US Map
                fig = px.choropleth(
                    payments_by_state,
                    locations='STATE',
                    locationmode="USA-states",
                    color='TOTAL_PAYMENTS',
                    scope="usa",
                    color_continuous_scale="Reds",
                    labels={'TOTAL_PAYMENTS': 'Total Payments ($)'},
                    title="Pharma Payments by State"
                )
                fig.update_layout(
                    geo=dict(bgcolor='rgba(0,0,0,0)'),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Top states bar chart
                top_states = payments_by_state.nlargest(15, 'TOTAL_PAYMENTS')
                fig = px.bar(
                    top_states,
                    x='STATE',
                    y='TOTAL_PAYMENTS',
                    color='PROVIDER_COUNT',
                    title="Top 15 States by Payment Amount",
                    labels={'TOTAL_PAYMENTS': 'Total Payments ($)', 'STATE': 'State'}
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            # State details table
            st.subheader("State Details")
            state_display = payments_by_state.copy()
            state_display['TOTAL_PAYMENTS'] = state_display['TOTAL_PAYMENTS'].apply(lambda x: f"${x:,.0f}")
            state_display['AVG_PAYMENT'] = state_display['AVG_PAYMENT'].apply(lambda x: f"${x:,.0f}")

            st.dataframe(
                state_display,
                column_config={
                    "STATE": "State",
                    "PROVIDER_COUNT": st.column_config.NumberColumn("Providers", format="%d"),
                    "TOTAL_PAYMENTS": "Total Payments",
                    "AVG_PAYMENT": "Avg Payment/Provider"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No geographic data available")

    except Exception as e:
        st.error(f"Error loading geographic data: {str(e)}")

# Payments Analysis
with tab2:
    st.subheader("Payment Analysis")

    try:
        top_recipients = get_top_recipients()

        if not top_recipients.empty:
            col1, col2 = st.columns(2)

            with col1:
                # Recipient tier distribution
                tier_query = """
                SELECT RECIPIENT_TIER, COUNT(*) as COUNT, SUM(TOTAL_PAYMENT_AMOUNT) as TOTAL
                FROM FRAUDLENS_DB.GOLD.PAYMENTS_SUMMARY
                WHERE RECIPIENT_TIER IS NOT NULL
                GROUP BY RECIPIENT_TIER
                """
                tier_data = run_query(tier_query)

                if not tier_data.empty:
                    fig = px.pie(
                        tier_data,
                        values='COUNT',
                        names='RECIPIENT_TIER',
                        title="Recipients by Tier",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Payment amount distribution
                fig = px.histogram(
                    top_recipients,
                    x='TOTAL_PAYMENT_AMOUNT',
                    nbins=50,
                    title="Payment Amount Distribution (Top 100)",
                    labels={'TOTAL_PAYMENT_AMOUNT': 'Payment Amount ($)'}
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            # Top recipients table
            st.subheader("Top Payment Recipients")

            recipients_display = top_recipients.head(25).copy()
            recipients_display['TOTAL_PAYMENT_AMOUNT'] = recipients_display['TOTAL_PAYMENT_AMOUNT'].apply(
                lambda x: f"${x:,.0f}"
            )
            recipients_display['IS_EXCLUDED'] = recipients_display['IS_EXCLUDED'].apply(
                lambda x: "⚠️ EXCLUDED" if x else ""
            )

            st.dataframe(
                recipients_display,
                column_config={
                    "NPI": "NPI",
                    "NAME": "Provider",
                    "SPECIALTY": "Specialty",
                    "STATE": "State",
                    "TOTAL_PAYMENT_AMOUNT": "Total Payments",
                    "RECIPIENT_TIER": "Tier",
                    "IS_EXCLUDED": "Status"
                },
                hide_index=True,
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error loading payment data: {str(e)}")

# Prescriptions Analysis
with tab3:
    st.subheader("Prescription Analysis")

    try:
        # Prescriber volume distribution
        volume_query = """
        SELECT
            PRESCRIBER_VOLUME_TIER,
            COUNT(*) as COUNT,
            SUM(TOTAL_COST) as TOTAL_COST
        FROM FRAUDLENS_DB.GOLD.PRESCRIPTIONS_SUMMARY
        WHERE PRESCRIBER_VOLUME_TIER IS NOT NULL
        GROUP BY PRESCRIBER_VOLUME_TIER
        """
        volume_data = run_query(volume_query)

        col1, col2 = st.columns(2)

        with col1:
            if not volume_data.empty:
                fig = px.bar(
                    volume_data,
                    x='PRESCRIBER_VOLUME_TIER',
                    y='COUNT',
                    color='TOTAL_COST',
                    title="Prescribers by Volume Tier",
                    labels={'COUNT': 'Number of Prescribers'}
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Brand vs generic
            brand_query = """
            SELECT
                CASE WHEN PCT_BRAND_CLAIMS >= 80 THEN 'High Brand (>=80%)'
                     WHEN PCT_BRAND_CLAIMS >= 50 THEN 'Medium Brand (50-80%)'
                     ELSE 'Low Brand (<50%)'
                END as BRAND_CATEGORY,
                COUNT(*) as COUNT
            FROM FRAUDLENS_DB.GOLD.PRESCRIPTIONS_SUMMARY
            GROUP BY BRAND_CATEGORY
            """
            brand_data = run_query(brand_query)

            if not brand_data.empty:
                fig = px.pie(
                    brand_data,
                    values='COUNT',
                    names='BRAND_CATEGORY',
                    title="Brand vs Generic Preference",
                    color_discrete_sequence=['#dc3545', '#ffc107', '#28a745']
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

        # Top prescribers by cost
        st.subheader("Top Prescribers by Cost")

        top_prescribers_query = """
        SELECT
            NPI,
            PRESCRIBER_NAME,
            SPECIALTY,
            STATE,
            TOTAL_CLAIMS,
            TOTAL_COST,
            PCT_BRAND_CLAIMS,
            IS_EXCLUDED_PROVIDER
        FROM FRAUDLENS_DB.GOLD.PRESCRIPTIONS_SUMMARY
        ORDER BY TOTAL_COST DESC
        LIMIT 25
        """
        top_prescribers = run_query(top_prescribers_query)

        if not top_prescribers.empty:
            prescribers_display = top_prescribers.copy()
            prescribers_display['TOTAL_COST'] = prescribers_display['TOTAL_COST'].apply(lambda x: f"${x:,.0f}")
            prescribers_display['PCT_BRAND_CLAIMS'] = prescribers_display['PCT_BRAND_CLAIMS'].apply(lambda x: f"{x:.1f}%")
            prescribers_display['IS_EXCLUDED_PROVIDER'] = prescribers_display['IS_EXCLUDED_PROVIDER'].apply(
                lambda x: "⚠️" if x else ""
            )

            st.dataframe(
                prescribers_display,
                column_config={
                    "NPI": "NPI",
                    "PRESCRIBER_NAME": "Prescriber",
                    "SPECIALTY": "Specialty",
                    "STATE": "State",
                    "TOTAL_CLAIMS": st.column_config.NumberColumn("Claims", format="%d"),
                    "TOTAL_COST": "Total Cost",
                    "PCT_BRAND_CLAIMS": "Brand %",
                    "IS_EXCLUDED_PROVIDER": "⚠️"
                },
                hide_index=True,
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error loading prescription data: {str(e)}")

# Risk Analysis
with tab4:
    st.subheader("Risk Analysis")

    try:
        risk_dist = get_risk_distribution()

        if not risk_dist.empty:
            col1, col2 = st.columns(2)

            with col1:
                # Risk tier funnel
                tier_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'MINIMAL']
                risk_sorted = risk_dist.set_index('RISK_TIER').reindex(tier_order).reset_index()
                risk_sorted = risk_sorted.dropna()

                fig = go.Figure(go.Funnel(
                    y=risk_sorted['RISK_TIER'],
                    x=risk_sorted['COUNT'],
                    textinfo="value+percent initial",
                    marker=dict(color=['#dc3545', '#fd7e14', '#ffc107', '#20c997', '#6c757d'])
                ))
                fig.update_layout(
                    title="Risk Tier Funnel",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Exposure by tier
                fig = px.bar(
                    risk_dist,
                    x='RISK_TIER',
                    y='TOTAL_EXPOSURE',
                    color='RISK_TIER',
                    color_discrete_map={
                        'CRITICAL': '#dc3545',
                        'HIGH': '#fd7e14',
                        'MEDIUM': '#ffc107',
                        'LOW': '#20c997',
                        'MINIMAL': '#6c757d'
                    },
                    title="Financial Exposure by Risk Tier",
                    labels={'TOTAL_EXPOSURE': 'Total Exposure ($)'}
                )
                fig.update_layout(
                    showlegend=False,
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            # Risk score distribution histogram
            st.subheader("Risk Score Distribution")

            score_query = """
            SELECT FRAUD_RISK_SCORE
            FROM FRAUDLENS_DB.GOLD.FRAUD_RISK_SCORE
            WHERE FRAUD_RISK_SCORE > 0
            LIMIT 10000
            """
            scores = run_query(score_query)

            if not scores.empty:
                fig = px.histogram(
                    scores,
                    x='FRAUD_RISK_SCORE',
                    nbins=50,
                    title="Distribution of Fraud Risk Scores",
                    labels={'FRAUD_RISK_SCORE': 'Risk Score'}
                )
                fig.add_vline(x=70, line_dash="dash", line_color="red", annotation_text="Critical (70+)")
                fig.add_vline(x=50, line_dash="dash", line_color="orange", annotation_text="High (50+)")
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)

            # Risk summary metrics
            st.subheader("Risk Summary")

            col1, col2, col3, col4 = st.columns(4)

            critical_count = risk_dist[risk_dist['RISK_TIER'] == 'CRITICAL']['COUNT'].sum() if 'CRITICAL' in risk_dist['RISK_TIER'].values else 0
            high_count = risk_dist[risk_dist['RISK_TIER'] == 'HIGH']['COUNT'].sum() if 'HIGH' in risk_dist['RISK_TIER'].values else 0
            total_exposure = risk_dist['TOTAL_EXPOSURE'].sum()
            avg_score = risk_dist['AVG_SCORE'].mean()

            with col1:
                st.metric("Critical Risk", f"{critical_count:,}")
            with col2:
                st.metric("High Risk", f"{high_count:,}")
            with col3:
                st.metric("Total Exposure", f"${total_exposure:,.0f}")
            with col4:
                st.metric("Avg Risk Score", f"{avg_score:.1f}")

    except Exception as e:
        st.error(f"Error loading risk data: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
