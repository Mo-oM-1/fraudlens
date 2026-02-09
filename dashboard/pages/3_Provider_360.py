"""
Provider 360 Page - Detailed provider investigation
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils import search_providers, get_provider_details, get_provider_alerts, get_provider_ml_features
from utils.theme import render_theme_toggle, apply_theme_css

st.set_page_config(
    page_title="Provider 360",
    page_icon="👤",
    layout="wide"
)

# Apply theme
render_theme_toggle()
apply_theme_css()

st.title("👤 Provider 360° View")
st.markdown("### Complete provider profile and investigation")

# Search section
st.subheader("🔍 Search Provider")

col1, col2 = st.columns([3, 1])

with col1:
    search_term = st.text_input(
        "Search by NPI or Provider Name",
        placeholder="Enter NPI (e.g., 1234567890) or name...",
        key="provider_search"
    )

with col2:
    st.write("")  # Spacer
    search_button = st.button("Search", use_container_width=True, type="primary")

# Search results
if search_term and (search_button or len(search_term) >= 3):
    try:
        results = search_providers(search_term)

        if not results.empty:
            st.markdown(f"**Found {len(results)} providers**")

            # Display search results
            results_display = results.copy()
            results_display['TOTAL_FINANCIAL_EXPOSURE'] = results_display['TOTAL_FINANCIAL_EXPOSURE'].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x > 0 else "N/A"
            )
            results_display['IS_EXCLUDED'] = results_display['IS_EXCLUDED'].apply(
                lambda x: "⚠️ EXCLUDED" if x else "✅ Active"
            )

            # Make NPI clickable
            selected_npi = st.selectbox(
                "Select a provider to view details:",
                options=results['NPI'].tolist(),
                format_func=lambda x: f"{x} - {results[results['NPI']==x]['NAME'].iloc[0]}"
            )

            st.dataframe(
                results_display,
                column_config={
                    "NPI": "NPI",
                    "NAME": "Provider Name",
                    "ENTITY_TYPE": "Type",
                    "SPECIALTY": "Specialty",
                    "STATE": "State",
                    "IS_EXCLUDED": "Status",
                    "TOTAL_FINANCIAL_EXPOSURE": "Financial Exposure"
                },
                hide_index=True,
                use_container_width=True,
                height=200
            )

            st.divider()

            # Load selected provider details
            if selected_npi:
                provider = get_provider_details(selected_npi)

                if not provider.empty:
                    p = provider.iloc[0]

                    # Provider header
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        provider_name = p.get('FULL_NAME') or p.get('ORGANIZATION_NAME') or 'Unknown'
                        st.header(provider_name)
                        st.caption(f"NPI: {p['NPI']} | {p.get('ENTITY_TYPE', 'N/A')}")

                    with col2:
                        if p.get('IS_EXCLUDED'):
                            st.error("⚠️ EXCLUDED")
                            if p.get('EXCLUSION_REASON'):
                                st.caption(p['EXCLUSION_REASON'])
                        else:
                            st.success("✅ Active")

                    st.divider()

                    # Provider Info Cards
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.subheader("📍 Location")
                        st.write(f"**City:** {p.get('CITY', 'N/A')}")
                        st.write(f"**State:** {p.get('STATE', 'N/A')}")
                        st.write(f"**ZIP:** {p.get('ZIP_CODE', 'N/A')}")
                        st.write(f"**Phone:** {p.get('PHONE', 'N/A')}")

                    with col2:
                        st.subheader("🏥 Specialty")
                        st.write(f"**Classification:** {p.get('SPECIALTY_CLASSIFICATION', 'N/A')}")
                        st.write(f"**Specialty:** {p.get('SPECIALTY', 'N/A')}")
                        st.write(f"**Provider Type:** {p.get('PROVIDER_TYPE', 'N/A')}")
                        st.write(f"**Credential:** {p.get('CREDENTIAL', 'N/A')}")

                    with col3:
                        st.subheader("📊 Status")
                        st.write(f"**NPI Active:** {'Yes' if p.get('IS_NPI_ACTIVE') else 'No'}")
                        st.write(f"**Enumeration Date:** {p.get('NPI_ENUMERATION_DATE', 'N/A')}")
                        if p.get('IS_EXCLUDED'):
                            st.write(f"**Exclusion Date:** {p.get('EXCLUSION_DATE', 'N/A')}")

                    st.divider()

                    # Financial Metrics
                    st.subheader("💰 Financial Metrics")

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        payment_amt = p.get('TOTAL_PAYMENT_AMOUNT', 0) or 0
                        st.metric(
                            "Pharma Payments",
                            f"${payment_amt:,.0f}",
                            help="Total Open Payments received"
                        )

                    with col2:
                        rx_cost = p.get('TOTAL_PRESCRIPTION_COST', 0) or 0
                        st.metric(
                            "Prescription Cost",
                            f"${rx_cost:,.0f}",
                            help="Total Part D prescription cost"
                        )

                    with col3:
                        total_exp = p.get('TOTAL_FINANCIAL_EXPOSURE', 0) or 0
                        st.metric(
                            "Total Exposure",
                            f"${total_exp:,.0f}",
                            help="Combined financial exposure"
                        )

                    with col4:
                        brand_pct = p.get('PCT_BRAND_CLAIMS', 0) or 0
                        st.metric(
                            "Brand Rx %",
                            f"{brand_pct:.1f}%",
                            help="Percentage of brand prescriptions"
                        )

                    # Activity indicators
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        has_payments = "✅ Yes" if p.get('HAS_PHARMA_PAYMENTS') else "❌ No"
                        st.write(f"**Has Pharma Payments:** {has_payments}")

                    with col2:
                        has_rx = "✅ Yes" if p.get('HAS_PRESCRIPTIONS') else "❌ No"
                        st.write(f"**Has Prescriptions:** {has_rx}")

                    with col3:
                        tier = p.get('RECIPIENT_TIER', 'N/A')
                        st.write(f"**Recipient Tier:** {tier}")

                    with col4:
                        vol_tier = p.get('PRESCRIBER_VOLUME_TIER', 'N/A')
                        st.write(f"**Prescriber Volume:** {vol_tier}")

                    st.divider()

                    # Alerts for this provider
                    st.subheader("🚨 Alerts")
                    alerts = get_provider_alerts(selected_npi)

                    if not alerts.empty:
                        st.warning(f"**{len(alerts)} active alert(s) for this provider**")

                        for _, alert in alerts.iterrows():
                            with st.expander(f"⚠️ {alert['ALERT_TYPE']} - Risk Score: {alert['RISK_SCORE']}"):
                                st.write(f"**Description:** {alert['ALERT_DESCRIPTION']}")
                                st.write(f"**Risk Tier:** {alert['RISK_TIER']}")
                                st.write(f"**Priority:** {alert['PRIORITY_RANK']}")
                                if alert.get('FINANCIAL_EXPOSURE'):
                                    st.write(f"**Financial Exposure:** ${alert['FINANCIAL_EXPOSURE']:,.0f}")
                    else:
                        st.success("No active alerts for this provider")

                    # Risk Gauge
                    st.divider()
                    st.subheader("📈 Risk Assessment")

                    # Get fraud score if available (parameterized query)
                    from utils import run_query
                    score_query = """
                    SELECT FRAUD_RISK_SCORE, RISK_TIER
                    FROM FRAUDLENS_DB.GOLD.FRAUD_RISK_SCORE
                    WHERE NPI = %s
                    """
                    score_data = run_query(score_query, (selected_npi,))

                    if not score_data.empty:
                        score = score_data.iloc[0]['FRAUD_RISK_SCORE']
                        tier = score_data.iloc[0]['RISK_TIER']

                        col1, col2 = st.columns([1, 2])

                        with col1:
                            st.metric("Fraud Risk Score", f"{score}/100")
                            st.write(f"**Risk Tier:** {tier}")

                        with col2:
                            # Gauge chart
                            fig = go.Figure(go.Indicator(
                                mode="gauge+number",
                                value=score,
                                domain={'x': [0, 1], 'y': [0, 1]},
                                title={'text': "Risk Score"},
                                gauge={
                                    'axis': {'range': [0, 100]},
                                    'bar': {'color': "darkblue"},
                                    'steps': [
                                        {'range': [0, 10], 'color': "#6c757d"},
                                        {'range': [10, 30], 'color': "#20c997"},
                                        {'range': [30, 50], 'color': "#ffc107"},
                                        {'range': [50, 70], 'color': "#fd7e14"},
                                        {'range': [70, 100], 'color': "#dc3545"}
                                    ],
                                    'threshold': {
                                        'line': {'color': "red", 'width': 4},
                                        'thickness': 0.75,
                                        'value': score
                                    }
                                }
                            ))
                            fig.update_layout(height=250)
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No risk score available for this provider")

                    # ML Analysis Section
                    st.divider()
                    st.subheader("🤖 ML Analysis")

                    ml_features = get_provider_ml_features(selected_npi)

                    if not ml_features.empty:
                        ml = ml_features.iloc[0]

                        # Anomaly summary
                        anomaly_count = ml.get('ANOMALY_FLAG_COUNT', 0) or 0
                        is_outlier = ml.get('IS_MULTI_DIMENSION_OUTLIER', False)

                        if anomaly_count >= 3 or is_outlier:
                            st.error(f"⚠️ **{anomaly_count} anomaly flags detected** - Multi-dimension outlier: {'Yes' if is_outlier else 'No'}")
                        elif anomaly_count >= 1:
                            st.warning(f"⚡ **{anomaly_count} anomaly flag(s) detected**")
                        else:
                            st.success("✅ No anomaly flags")

                        # Tabs for different ML views
                        ml_tab1, ml_tab2, ml_tab3 = st.tabs(["Z-Scores", "Concentration", "Percentiles"])

                        with ml_tab1:
                            st.markdown("#### Peer Comparison (Z-Scores)")
                            st.caption(f"Compared to {ml.get('PEER_COUNT', 0)} peers in same state")

                            # Radar chart for z-scores
                            z_scores = {
                                'Payment': ml.get('PAYMENT_ZSCORE', 0) or 0,
                                'Rx Cost': ml.get('RX_COST_ZSCORE', 0) or 0,
                                'Brand %': ml.get('BRAND_PCT_ZSCORE', 0) or 0,
                                'Claims': ml.get('CLAIMS_ZSCORE', 0) or 0
                            }

                            categories = list(z_scores.keys())
                            values = list(z_scores.values())

                            fig_radar = go.Figure()

                            # Add threshold lines
                            fig_radar.add_trace(go.Scatterpolar(
                                r=[2, 2, 2, 2, 2],
                                theta=categories + [categories[0]],
                                fill=None,
                                mode='lines',
                                line=dict(color='red', dash='dash'),
                                name='Outlier threshold (z=2)'
                            ))

                            fig_radar.add_trace(go.Scatterpolar(
                                r=values + [values[0]],
                                theta=categories + [categories[0]],
                                fill='toself',
                                fillcolor='rgba(31, 119, 180, 0.3)',
                                line=dict(color='rgb(31, 119, 180)'),
                                name='Provider Z-Scores'
                            ))

                            fig_radar.update_layout(
                                polar=dict(
                                    radialaxis=dict(visible=True, range=[-2, max(5, max(values) + 1)])
                                ),
                                showlegend=True,
                                height=350
                            )
                            st.plotly_chart(fig_radar, use_container_width=True)

                            # Z-score metrics
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                z_pay = z_scores['Payment']
                                st.metric("Payment Z-Score", f"{z_pay:.2f}",
                                         delta="Outlier" if z_pay > 2 else None,
                                         delta_color="inverse" if z_pay > 2 else "off")
                            with col2:
                                z_rx = z_scores['Rx Cost']
                                st.metric("Rx Cost Z-Score", f"{z_rx:.2f}",
                                         delta="Outlier" if z_rx > 2 else None,
                                         delta_color="inverse" if z_rx > 2 else "off")
                            with col3:
                                z_brand = z_scores['Brand %']
                                st.metric("Brand % Z-Score", f"{z_brand:.2f}",
                                         delta="Outlier" if z_brand > 2 else None,
                                         delta_color="inverse" if z_brand > 2 else "off")
                            with col4:
                                z_claims = z_scores['Claims']
                                st.metric("Claims Z-Score", f"{z_claims:.2f}",
                                         delta="Outlier" if z_claims > 2 else None,
                                         delta_color="inverse" if z_claims > 2 else "off")

                        with ml_tab2:
                            st.markdown("#### Drug & Pharma Concentration")

                            col1, col2 = st.columns(2)

                            with col1:
                                st.markdown("**Drug Prescribing**")
                                hhi = ml.get('DRUG_CONCENTRATION_HHI', 0) or 0
                                is_concentrated = ml.get('IS_CONCENTRATED_PRESCRIBER', False)

                                st.metric("Drug HHI",
                                         f"{hhi:,.0f}",
                                         delta="Concentrated" if is_concentrated else "Diverse",
                                         delta_color="inverse" if is_concentrated else "normal")
                                st.caption("HHI > 2500 = concentrated prescriber")

                                st.write(f"**Unique drugs:** {ml.get('UNIQUE_DRUGS_PRESCRIBED', 0)}")
                                st.write(f"**Top drug %:** {ml.get('TOP_DRUG_PCT', 0):.1f}%")
                                st.write(f"**Top 3 drugs %:** {ml.get('TOP_3_DRUGS_PCT', 0):.1f}%")

                            with col2:
                                st.markdown("**Pharma Payments**")
                                is_single = ml.get('IS_SINGLE_PAYER_RECIPIENT', False)

                                st.metric("Pharma Companies",
                                         ml.get('UNIQUE_PHARMA_COMPANIES', 0),
                                         delta="Single payer" if is_single else None,
                                         delta_color="inverse" if is_single else "off")

                                st.write(f"**Top payer %:** {ml.get('TOP_PAYER_PCT', 0):.1f}%")
                                st.write(f"**General payments:** {ml.get('PCT_GENERAL_PAYMENTS', 0):.1f}%")
                                st.write(f"**Research payments:** {ml.get('PCT_RESEARCH_PAYMENTS', 0):.1f}%")

                        with ml_tab3:
                            st.markdown("#### Percentile Rankings")
                            st.caption("Position relative to peers (0-100, higher = more)")

                            # Horizontal bar chart for percentiles
                            percentiles = {
                                'Payment': ml.get('PAYMENT_PERCENTILE', 0) or 0,
                                'Rx Cost': ml.get('RX_COST_PERCENTILE', 0) or 0,
                                'Brand %': ml.get('BRAND_PCT_PERCENTILE', 0) or 0,
                                'Claims': ml.get('CLAIMS_PERCENTILE', 0) or 0
                            }

                            fig_bar = go.Figure()

                            colors = ['#dc3545' if v >= 90 else '#ffc107' if v >= 75 else '#20c997'
                                     for v in percentiles.values()]

                            fig_bar.add_trace(go.Bar(
                                x=list(percentiles.values()),
                                y=list(percentiles.keys()),
                                orientation='h',
                                marker_color=colors,
                                text=[f"{v:.0f}%" for v in percentiles.values()],
                                textposition='inside'
                            ))

                            fig_bar.add_vline(x=90, line_dash="dash", line_color="red",
                                            annotation_text="90th percentile")

                            fig_bar.update_layout(
                                xaxis=dict(range=[0, 100], title="Percentile"),
                                yaxis=dict(title=""),
                                height=250,
                                showlegend=False
                            )
                            st.plotly_chart(fig_bar, use_container_width=True)

                            # Ratios
                            st.markdown("#### Financial Ratios")
                            col1, col2, col3 = st.columns(3)

                            with col1:
                                ratio = ml.get('PAYMENT_TO_RX_RATIO')
                                st.metric("Payment/Rx Ratio",
                                         f"{ratio:.4f}" if ratio else "N/A")
                            with col2:
                                avg_cost = ml.get('AVG_COST_PER_CLAIM')
                                st.metric("Avg Cost/Claim",
                                         f"${avg_cost:,.2f}" if avg_cost else "N/A")
                            with col3:
                                peer_ratio = ml.get('PAYMENT_VS_PEER_RATIO')
                                st.metric("vs Peer Avg",
                                         f"{peer_ratio:.2f}x" if peer_ratio else "N/A")

                    else:
                        st.info("No ML features available for this provider")

        else:
            st.info("No providers found matching your search.")

    except Exception as e:
        st.error(f"Error searching providers: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
else:
    st.info("Enter at least 3 characters to search for a provider.")

    # Show some high-risk providers as examples
    st.divider()
    st.subheader("🔥 High Risk Providers to Investigate")

    try:
        from utils import get_excluded_with_activity

        excluded = get_excluded_with_activity()

        if not excluded.empty:
            excluded_display = excluded.head(10).copy()
            excluded_display['TOTAL_FINANCIAL_EXPOSURE'] = excluded_display['TOTAL_FINANCIAL_EXPOSURE'].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
            )

            st.dataframe(
                excluded_display[['NPI', 'NAME', 'SPECIALTY', 'STATE', 'EXCLUSION_REASON', 'TOTAL_FINANCIAL_EXPOSURE']],
                column_config={
                    "NPI": "NPI",
                    "NAME": "Provider",
                    "SPECIALTY": "Specialty",
                    "STATE": "State",
                    "EXCLUSION_REASON": "Exclusion Reason",
                    "TOTAL_FINANCIAL_EXPOSURE": "Exposure"
                },
                hide_index=True,
                use_container_width=True
            )

            st.caption("These are excluded providers with ongoing activity - high priority for investigation")

    except Exception as e:
        st.warning("Could not load high-risk providers")
