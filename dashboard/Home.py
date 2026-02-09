"""
FraudLens - Healthcare Fraud Detection Dashboard
Main entry point
"""
import streamlit as st
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))
from utils.theme import render_theme_toggle, apply_theme_css

st.set_page_config(
    page_title="FraudLens - Healthcare Fraud Detection",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply theme
render_theme_toggle()
apply_theme_css()

# Header
st.markdown('<h1 class="main-header">FraudLens</h1>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Healthcare Fraud Detection & Compliance Analytics</p>', unsafe_allow_html=True)

st.divider()

# Introduction
st.markdown("""
### 🎯 Project Overview

**FraudLens** provides **real-time fraud detection and risk assessment** for healthcare providers
using data from **10+ federal sources**:

**CMS Provider Data:**
- **NPPES** - National Provider Registry (7M+ providers, addresses, taxonomies)
- **Medicare Part D Prescribers** - Prescription patterns & drug costs
- **Open Payments** - Pharma payments (General, Research, Ownership)
- **Provider Information** - Facility details

**CMS Healthcare Facilities:**
- **Medicare Hospital Spending** - Hospital expenditures by claim
- **Long-Term Care Hospitals** - LTCH facility data
- **Hospice** - Hospice provider information
- **Home Health Care** - Home health agency data

**Compliance & Exclusions:**
- **OIG LEIE** - Excluded providers list (fraud/abuse)

**Snowflake Marketplace:**
- **NPPES Dataset** - Enhanced provider data with taxonomy codes
""")

st.divider()

# Feature cards
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📊 Overview")
    st.markdown("""
    Executive dashboard with key performance indicators:
    - Total providers monitored
    - Excluded providers with activity
    - Financial exposure by risk tier
    - Alert distribution
    """)

    st.markdown("### 🚨 Fraud Alerts")
    st.markdown("""
    Actionable alerts requiring investigation:
    - Payments to excluded providers
    - Prescriptions by excluded providers
    - High-risk score alerts
    - Priority-ranked queue
    """)

with col2:
    st.markdown("### 👤 Provider 360°")
    st.markdown("""
    Complete provider investigation view:
    - Search by NPI or name
    - Full provider profile
    - Financial metrics & activity
    - Risk score gauge
    """)

    st.markdown("### 👥 Provider Comparison")
    st.markdown("""
    Side-by-side provider comparison:
    - Compare two providers directly
    - Visual metric comparison
    - Peer benchmarking
    - Key findings summary
    """)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📈 Analytics")
    st.markdown("""
    Deep-dive analytics and trends:
    - Geographic distribution maps
    - Payment pattern analysis
    - Prescription trends
    - Risk score distributions
    """)

st.divider()

# Architecture section
st.markdown("### 🏗️ Technical Architecture")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### 🥉 Bronze Layer")
    st.markdown("""
    - Raw data ingestion
    - Airflow orchestration
    - S3 → Snowflake pipeline
    - 10+ source datasets
    """)

with col2:
    st.markdown("#### 🥈 Silver Layer")
    st.markdown("""
    - dbt transformations
    - Data cleansing
    - Type standardization
    - Quality validation
    """)

with col3:
    st.markdown("#### 🥇 Gold Layer")
    st.markdown("""
    - Business aggregations
    - Fraud risk scoring
    - Provider 360 view
    - Alert generation
    """)

st.divider()

# Navigation
st.markdown("### 🚀 Get Started")
st.markdown("Use the **sidebar navigation** to explore the dashboard pages:")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.page_link("pages/1_Overview.py", label="Overview", icon="📊")

with col2:
    st.page_link("pages/2_Fraud_Alerts.py", label="Fraud Alerts", icon="🚨")

with col3:
    st.page_link("pages/3_Provider_360.py", label="Provider 360", icon="👤")

with col4:
    st.page_link("pages/4_Analytics.py", label="Analytics", icon="📈")

with col5:
    st.page_link("pages/5_Provider_Comparison.py", label="Compare", icon="👥")

# Footer
st.divider()
st.markdown("""
<div class="footer-text">
    <p>Built with Python, dbt, Snowflake & Streamlit</p>
    <p><strong>FraudLens</strong> - Data Engineering Portfolio Project | 2026</p>
</div>
""", unsafe_allow_html=True)
