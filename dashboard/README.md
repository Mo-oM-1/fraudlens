# 🔍 FraudLens Dashboard

Interactive Streamlit dashboard for healthcare fraud detection and provider risk assessment.

## 📊 Features

- **Overview**: Executive KPIs and risk distribution
- **Fraud Alerts**: Actionable alerts with filtering and export
- **Provider 360°**: Complete provider investigation view with **ML Analysis**
- **Analytics**: Geographic, payment, and prescription analytics

### ML Analysis (Provider 360)

Section d'analyse avancée utilisant les features ML pour détecter les anomalies :

| Onglet | Contenu | Utilité |
|--------|---------|---------|
| **Z-Scores** | Radar chart + métriques vs peers | Identifier les outliers statistiques (z > 2) |
| **Concentration** | Drug HHI, pharma diversity | Détecter les prescripteurs concentrés sur peu de médicaments |
| **Percentiles** | Bar chart position vs peers | Visualiser rapidement les extrêmes (> 90e percentile) |

**Indicateurs d'anomalie** :
- Bandeau vert : Aucun flag d'anomalie
- Bandeau orange : 1-2 flags d'anomalie
- Bandeau rouge : 3+ flags ou multi-dimension outlier

**Quand investiguer** : Un provider avec plusieurs flags d'anomalie (z-scores élevés, prescriptions concentrées, single pharma payer) doit être analysé plus en détail.

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
- **CMS Provider Data**: Hospital spending, facilities

## 🔐 Security

Never commit `secrets.toml` - use Streamlit Cloud secrets management for production.
