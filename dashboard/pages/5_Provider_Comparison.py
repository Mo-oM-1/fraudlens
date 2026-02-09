"""
Provider Comparison Page - Compare two providers side by side
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils import get_provider_details, run_query
from utils.theme import render_theme_toggle, apply_theme_css, get_current_theme

st.set_page_config(
    page_title="Provider Comparison",
    page_icon="👥",
    layout="wide"
)

# Apply theme
render_theme_toggle()
apply_theme_css()

st.title("👥 Provider Comparison")
st.markdown("### Compare two providers side by side for investigation")


@st.cache_data(ttl=3600)
def search_providers_for_comparison(search_term):
    """Search providers by NPI or name."""
    query = """
    SELECT
        NPI,
        COALESCE(FULL_NAME, ORGANIZATION_NAME) as NAME,
        SPECIALTY,
        STATE,
        CITY
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360
    WHERE NPI = %s
       OR UPPER(FULL_NAME) LIKE UPPER(%s)
       OR UPPER(ORGANIZATION_NAME) LIKE UPPER(%s)
    LIMIT 20
    """
    search_pattern = f"%{search_term}%"
    return run_query(query, (search_term, search_pattern, search_pattern))


@st.cache_data(ttl=3600)
def get_provider_with_score(npi):
    """Get provider details with fraud risk score."""
    query = """
    SELECT
        p.*,
        f.FRAUD_RISK_SCORE,
        f.RISK_TIER
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360 p
    LEFT JOIN FRAUDLENS_DB.GOLD.FRAUD_RISK_SCORE f ON p.NPI = f.NPI
    WHERE p.NPI = %s
    """
    return run_query(query, (npi,))


@st.cache_data(ttl=3600)
def get_specialty_benchmarks(specialty, state):
    """Get average metrics for providers in same specialty/state."""
    query = """
    SELECT
        AVG(TOTAL_PAYMENT_AMOUNT) as AVG_PAYMENTS,
        AVG(TOTAL_PRESCRIPTION_COST) as AVG_RX_COST,
        AVG(PCT_BRAND_CLAIMS) as AVG_BRAND_PCT,
        AVG(f.FRAUD_RISK_SCORE) as AVG_RISK_SCORE,
        COUNT(*) as PEER_COUNT
    FROM FRAUDLENS_DB.GOLD.PROVIDER_360 p
    LEFT JOIN FRAUDLENS_DB.GOLD.FRAUD_RISK_SCORE f ON p.NPI = f.NPI
    WHERE p.SPECIALTY = %s AND p.STATE = %s
    """
    return run_query(query, (specialty, state))


def format_currency(value):
    """Format number as currency."""
    if pd.isna(value) or value is None:
        return "N/A"
    return f"${value:,.0f}"


def format_percent(value):
    """Format number as percentage."""
    if pd.isna(value) or value is None:
        return "N/A"
    return f"{value:.1f}%"


def get_comparison_indicator(val1, val2, higher_is_worse=True):
    """Return indicator showing which value is concerning."""
    if pd.isna(val1) or pd.isna(val2) or val1 is None or val2 is None:
        return "", ""

    diff_pct = ((val1 - val2) / val2 * 100) if val2 != 0 else 0

    if higher_is_worse:
        if diff_pct > 50:
            return "🔴", ""
        elif diff_pct < -50:
            return "", "🔴"
    else:
        if diff_pct < -50:
            return "🔴", ""
        elif diff_pct > 50:
            return "", "🔴"
    return "", ""


# Provider selection
st.subheader("Select Providers to Compare")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### Provider 1")
    search1 = st.text_input("Search by NPI or Name", key="search1", placeholder="Enter NPI or name...")

    provider1_options = []
    selected_npi1 = None

    if search1 and len(search1) >= 3:
        results1 = search_providers_for_comparison(search1)
        if not results1.empty:
            provider1_options = results1.apply(
                lambda x: f"{x['NPI']} - {x['NAME']} ({x['SPECIALTY']}, {x['STATE']})",
                axis=1
            ).tolist()
            selected1 = st.selectbox("Select Provider 1", options=provider1_options, key="select1")
            if selected1:
                selected_npi1 = selected1.split(" - ")[0]

with col2:
    st.markdown("#### Provider 2")
    search2 = st.text_input("Search by NPI or Name", key="search2", placeholder="Enter NPI or name...")

    provider2_options = []
    selected_npi2 = None

    if search2 and len(search2) >= 3:
        results2 = search_providers_for_comparison(search2)
        if not results2.empty:
            provider2_options = results2.apply(
                lambda x: f"{x['NPI']} - {x['NAME']} ({x['SPECIALTY']}, {x['STATE']})",
                axis=1
            ).tolist()
            selected2 = st.selectbox("Select Provider 2", options=provider2_options, key="select2")
            if selected2:
                selected_npi2 = selected2.split(" - ")[0]

st.divider()

# Comparison display
if selected_npi1 and selected_npi2:
    try:
        p1_data = get_provider_with_score(selected_npi1)
        p2_data = get_provider_with_score(selected_npi2)

        if not p1_data.empty and not p2_data.empty:
            p1 = p1_data.iloc[0]
            p2 = p2_data.iloc[0]

            # Provider headers
            col1, col2 = st.columns(2)

            with col1:
                name1 = p1.get('FULL_NAME') or p1.get('ORGANIZATION_NAME') or 'Unknown'
                st.markdown(f"### {name1}")
                st.caption(f"NPI: {p1['NPI']}")
                if p1.get('IS_EXCLUDED'):
                    st.error("⚠️ EXCLUDED PROVIDER")
                else:
                    st.success("✅ Active")

            with col2:
                name2 = p2.get('FULL_NAME') or p2.get('ORGANIZATION_NAME') or 'Unknown'
                st.markdown(f"### {name2}")
                st.caption(f"NPI: {p2['NPI']}")
                if p2.get('IS_EXCLUDED'):
                    st.error("⚠️ EXCLUDED PROVIDER")
                else:
                    st.success("✅ Active")

            st.divider()

            # Basic info comparison
            st.subheader("📋 Basic Information")

            col1, col2 = st.columns(2)

            info_fields = [
                ("Specialty", "SPECIALTY"),
                ("State", "STATE"),
                ("City", "CITY"),
                ("Entity Type", "ENTITY_TYPE"),
                ("Credential", "CREDENTIAL"),
            ]

            with col1:
                for label, field in info_fields:
                    val = p1.get(field, 'N/A') or 'N/A'
                    st.write(f"**{label}:** {val}")

            with col2:
                for label, field in info_fields:
                    val = p2.get(field, 'N/A') or 'N/A'
                    # Highlight if same specialty/state (peer comparison)
                    if field in ['SPECIALTY', 'STATE'] and p1.get(field) == p2.get(field):
                        st.write(f"**{label}:** {val} ✓")
                    else:
                        st.write(f"**{label}:** {val}")

            st.divider()

            # Financial metrics comparison
            st.subheader("💰 Financial Metrics Comparison")

            # Get values
            pay1 = p1.get('TOTAL_PAYMENT_AMOUNT', 0) or 0
            pay2 = p2.get('TOTAL_PAYMENT_AMOUNT', 0) or 0
            rx1 = p1.get('TOTAL_PRESCRIPTION_COST', 0) or 0
            rx2 = p2.get('TOTAL_PRESCRIPTION_COST', 0) or 0
            exp1 = p1.get('TOTAL_FINANCIAL_EXPOSURE', 0) or 0
            exp2 = p2.get('TOTAL_FINANCIAL_EXPOSURE', 0) or 0
            brand1 = p1.get('PCT_BRAND_CLAIMS', 0) or 0
            brand2 = p2.get('PCT_BRAND_CLAIMS', 0) or 0
            score1 = p1.get('FRAUD_RISK_SCORE', 0) or 0
            score2 = p2.get('FRAUD_RISK_SCORE', 0) or 0

            col1, col2 = st.columns(2)

            with col1:
                ind1, _ = get_comparison_indicator(pay1, pay2)
                st.metric("Pharma Payments", format_currency(pay1), delta=f"{ind1}" if ind1 else None)

                ind1, _ = get_comparison_indicator(rx1, rx2)
                st.metric("Prescription Cost", format_currency(rx1))

                st.metric("Total Exposure", format_currency(exp1))

                ind1, _ = get_comparison_indicator(brand1, brand2)
                st.metric("Brand Rx %", format_percent(brand1))

            with col2:
                _, ind2 = get_comparison_indicator(pay1, pay2)
                st.metric("Pharma Payments", format_currency(pay2), delta=f"{ind2}" if ind2 else None)

                _, ind2 = get_comparison_indicator(rx1, rx2)
                st.metric("Prescription Cost", format_currency(rx2))

                st.metric("Total Exposure", format_currency(exp2))

                _, ind2 = get_comparison_indicator(brand1, brand2)
                st.metric("Brand Rx %", format_percent(brand2))

            st.divider()

            # Risk comparison
            st.subheader("⚠️ Risk Assessment Comparison")

            col1, col2 = st.columns(2)

            with col1:
                tier1 = p1.get('RISK_TIER', 'N/A') or 'N/A'
                color1 = {'CRITICAL': '🔴', 'HIGH': '🟠', 'MEDIUM': '🟡', 'LOW': '🟢', 'MINIMAL': '⚪'}.get(tier1, '⚪')
                st.metric("Risk Score", f"{score1:.0f}/100")
                st.write(f"**Risk Tier:** {color1} {tier1}")

            with col2:
                tier2 = p2.get('RISK_TIER', 'N/A') or 'N/A'
                color2 = {'CRITICAL': '🔴', 'HIGH': '🟠', 'MEDIUM': '🟡', 'LOW': '🟢', 'MINIMAL': '⚪'}.get(tier2, '⚪')
                st.metric("Risk Score", f"{score2:.0f}/100")
                st.write(f"**Risk Tier:** {color2} {tier2}")

            st.divider()

            # Visual comparison chart
            st.subheader("📊 Visual Comparison")

            # Normalize values for comparison (0-100 scale)
            max_pay = max(pay1, pay2, 1)
            max_rx = max(rx1, rx2, 1)

            metrics = ['Pharma Payments', 'Rx Cost', 'Brand %', 'Risk Score']
            p1_values = [
                (pay1 / max_pay) * 100,
                (rx1 / max_rx) * 100,
                brand1,
                score1
            ]
            p2_values = [
                (pay2 / max_pay) * 100,
                (rx2 / max_rx) * 100,
                brand2,
                score2
            ]

            fig = go.Figure()

            fig.add_trace(go.Bar(
                name=name1[:20] + "..." if len(name1) > 20 else name1,
                x=metrics,
                y=p1_values,
                marker_color='#74b9ff'
            ))

            fig.add_trace(go.Bar(
                name=name2[:20] + "..." if len(name2) > 20 else name2,
                x=metrics,
                y=p2_values,
                marker_color='#fd79a8'
            ))

            fig.update_layout(
                barmode='group',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=400,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig, use_container_width=True)

            # Peer benchmark (if same specialty)
            if p1.get('SPECIALTY') == p2.get('SPECIALTY') and p1.get('STATE') == p2.get('STATE'):
                st.divider()
                st.subheader("📈 Peer Benchmark")
                st.caption(f"Comparison with other {p1.get('SPECIALTY')} providers in {p1.get('STATE')}")

                benchmarks = get_specialty_benchmarks(p1.get('SPECIALTY'), p1.get('STATE'))

                if not benchmarks.empty:
                    b = benchmarks.iloc[0]

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Peer Avg Payments", format_currency(b.get('AVG_PAYMENTS', 0)))
                    with col2:
                        st.metric("Peer Avg Rx Cost", format_currency(b.get('AVG_RX_COST', 0)))
                    with col3:
                        st.metric("Peer Avg Brand %", format_percent(b.get('AVG_BRAND_PCT', 0)))
                    with col4:
                        st.metric("Peer Avg Risk", f"{b.get('AVG_RISK_SCORE', 0):.0f}/100")

                    st.caption(f"Based on {b.get('PEER_COUNT', 0):,} providers")

            # Summary findings
            st.divider()
            st.subheader("🔍 Key Findings")

            findings = []

            # Payment comparison
            if pay1 > 0 and pay2 > 0:
                ratio = pay1 / pay2 if pay2 > pay1 else pay2 / pay1
                higher = name1 if pay1 > pay2 else name2
                if ratio > 3:
                    findings.append(f"⚠️ **{higher}** has {ratio:.1f}x more pharma payments")

            # Brand % comparison
            if brand1 >= 80 or brand2 >= 80:
                high_brand = name1 if brand1 > brand2 else name2
                findings.append(f"⚠️ **{high_brand}** has high brand prescription rate (potential kickback risk)")

            # Risk score comparison
            if score1 >= 70 or score2 >= 70:
                high_risk = name1 if score1 > score2 else name2
                findings.append(f"🔴 **{high_risk}** is flagged as CRITICAL/HIGH risk")

            # Exclusion
            if p1.get('IS_EXCLUDED') or p2.get('IS_EXCLUDED'):
                excluded = name1 if p1.get('IS_EXCLUDED') else name2
                findings.append(f"🚨 **{excluded}** is on the OIG exclusion list")

            if findings:
                for finding in findings:
                    st.markdown(finding)
            else:
                st.success("No significant anomalies detected between these providers")

        else:
            st.warning("Could not load data for one or both providers")

    except Exception as e:
        st.error(f"Error comparing providers: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

else:
    st.info("Search and select two providers above to compare them")

    # Show suggested comparisons
    st.divider()
    st.subheader("💡 Suggested Comparisons")
    st.markdown("""
    Try comparing:
    - A **high-risk** provider vs a **low-risk** peer in the same specialty
    - An **excluded** provider vs an active provider
    - Two providers in the same **geographic area** and **specialty**

    This helps identify:
    - Outliers in payment patterns
    - Unusual prescribing behavior
    - Potential fraud indicators
    """)
