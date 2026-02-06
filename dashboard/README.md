# 🏥 Healthcare Fraud Detection Dashboard

Interactive Streamlit dashboard for healthcare fraud detection and provider risk assessment.

## 📊 Features

- **Overview**: Executive KPIs and risk distribution
- **Fraud Alerts**: Actionable alerts with filtering and export
- **Provider 360°**: Complete provider investigation view
- **Analytics**: Geographic, payment, and prescription analytics

## 🚀 Quick Start

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Snowflake credentials:
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit secrets.toml with your credentials
```

3. Run the dashboard:
```bash
streamlit run Home.py
```

### Streamlit Cloud Deployment

1. Push code to GitHub
2. Connect repo to [Streamlit Cloud](https://streamlit.io/cloud)
3. Add Snowflake secrets in the app settings
4. Deploy!

## 🏗️ Architecture

```
dashboard/
├── Home.py                 # Main entry point
├── pages/
│   ├── 1_Overview.py       # KPIs & executive summary
│   ├── 2_Fraud_Alerts.py   # Alerts list & filtering
│   ├── 3_Provider_360.py   # Provider investigation
│   └── 4_Analytics.py      # Charts & trends
├── utils/
│   └── snowflake_connector.py  # Database queries
├── .streamlit/
│   └── config.toml         # Theme configuration
└── requirements.txt        # Dependencies
```

## 📦 Data Sources

- **NPPES**: National Provider Registry
- **Open Payments**: Pharma payment disclosures
- **Medicare Part D**: Prescription patterns
- **OIG LEIE**: Exclusion list

## 🔐 Security

Never commit `secrets.toml` - use Streamlit Cloud secrets management for production.
