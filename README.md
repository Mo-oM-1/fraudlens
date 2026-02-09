# FraudLens - Healthcare Fraud Detection Platform

Pipeline de données complet pour la détection de fraudes dans le secteur de la santé américain. Intègre **10+ datasets fédéraux** (CMS, OIG) pour identifier les anomalies dans les paiements Medicare/Medicaid.

[![dbt Tests](https://github.com/Mo-oM-1/fraudlens/actions/workflows/dbt_test.yml/badge.svg)](https://github.com/Mo-oM-1/fraudlens/actions/workflows/dbt_test.yml)
[![SQL Lint](https://github.com/Mo-oM-1/fraudlens/actions/workflows/lint.yml/badge.svg)](https://github.com/Mo-oM-1/fraudlens/actions/workflows/lint.yml)

---

## Chiffres Clés

| Métrique | Valeur |
|----------|--------|
| Providers monitorés | **8.9M+** |
| Paiements pharma analysés | **$2.7B** |
| Prescriptions Medicare | **$238B** |
| Alertes critiques | **1,400+** |
| Providers exclus avec activité | **762** |

---

## Architecture

```
master_dag (Orchestrateur)
    │
    ├── init_snowflake_environment
    │       ├── create_warehouse (FRAUDLENS_WH)
    │       ├── create_schemas (RAW_DATA, BRONZE, STAGING, SILVER, GOLD)
    │       └── create_s3_stage
    │
    ├── [Data Downloads - Parallèle]
    │       ├── leie_download
    │       ├── medicare_hospital_spending_download
    │       ├── open_payments_download
    │       ├── provider_information_download
    │       ├── longterm_care_hospital_download
    │       ├── hospice_download
    │       ├── home_health_care_download
    │       └── medicare_part_d_prescribers_download
    │
    ├── load_bronze_tables (COPY INTO Snowflake)
    │
    └── dbt_transformations
            ├── dbt run --select staging
            ├── dbt run --select silver
            ├── dbt run --select gold
            ├── dbt test
            └── dbt docs generate
```

> 📄 **Documentation** : [Pipeline Architecture](docs/pipelineArchitecture.pdf)

---

## Sources de Données Fédérales

| Source | Agence | Contenu | Usage Fraude |
|--------|--------|---------|--------------|
| **LEIE** | OIG | Providers exclus Medicare/Medicaid | Détecter activité après exclusion |
| **Open Payments** | CMS | Paiements pharma → médecins | Kickbacks, conflits d'intérêts |
| **Medicare Part D** | CMS | Prescriptions par médecin | Sur-prescripteurs, pill mills |
| **NPPES** | CMS | Registre national des providers (7M+) | Table de référence maître |
| **Hospital Spending** | CMS | Dépenses Medicare par hôpital | Anomalies par établissement |
| **Provider Info** | CMS | Détails Nursing Homes | Enrichissement données |
| **Long-Term Care** | CMS | Hôpitaux soins prolongés | Secteur à risque |
| **Hospice** | CMS | Soins palliatifs | Secteur haut risque |
| **Home Health Care** | CMS | Soins à domicile | Analyses géospatiales |

---

## Medallion Architecture

| Layer | Schema | Description | Matérialisation |
|-------|--------|-------------|-----------------|
| Bronze | `BRONZE` | Données brutes depuis S3 | Tables |
| Staging | `STAGING` | Vues sur Bronze | Views |
| Silver | `SILVER` | Données nettoyées, enrichies | Tables (clustering) |
| Gold | `GOLD` | Données business-ready | Tables |

### Modèles Gold

| Modèle | Description |
|--------|-------------|
| `provider_360` | Vue 360° complète de chaque provider |
| `fraud_risk_score` | Score de risque fraude (0-100) |
| `high_risk_alerts` | Alertes actionnables (5 types) |
| `payments_summary` | Agrégations paiements pharma |
| `prescriptions_summary` | Agrégations prescriptions |
| `provider_ml_features` | Features ML pour détection d'anomalies |

### ML Features (provider_ml_features)

Modèle avancé de feature engineering pour la détection de fraude :

| Feature | Description | Seuil Anomalie |
|---------|-------------|----------------|
| **Z-Scores** | Comparaison vs peers (par état) | z > 2 = outlier |
| **Drug HHI** | Concentration des prescriptions (Herfindahl-Hirschman Index) | HHI > 2500 = concentré |
| **Pharma Diversity** | Nombre de compagnies pharma payeuses | 1 seule = suspect |
| **Percentiles** | Ranking vs peers (0-100) | > 90e = extrême |
| **Anomaly Flags** | Compteur de signaux d'alerte (0-6) | >= 2 = investigation |

**Utilité** : Ces features permettent d'identifier les providers statistiquement anormaux par rapport à leurs pairs, même sans labels de fraude explicites. Un provider avec plusieurs flags d'anomalie mérite une investigation approfondie.

> 📄 **Documentation** : [Data Modeling](docs/DataModeling_doc.pdf) | [dbt Models](docs/DBT_doc.pdf)

---

## Dashboard Streamlit

Dashboard interactif pour la visualisation et l'investigation des fraudes.

| Page | Fonctionnalités |
|------|-----------------|
| **Overview** | 9 KPIs exécutifs, distribution des risques, alertes |
| **Fraud Alerts** | Liste filtrable, export CSV/Excel |
| **Provider 360** | Recherche NPI, profil complet, risk gauge, **ML Analysis** |
| **Analytics** | Cartes géographiques, tendances, distributions |

### ML Analysis (Provider 360)

Nouvelle section d'analyse ML pour chaque provider :

- **Z-Scores** : Radar chart comparant le provider à ses peers (seuil outlier = 2)
- **Concentration** : Drug HHI, diversité pharma, top drug %
- **Percentiles** : Ranking vs peers avec code couleur (vert/jaune/rouge)
- **Ratios financiers** : Payment/Rx, Cost/Claim, vs Peer Average

### Lancer en local

```bash
cd dashboard
pip install -r requirements.txt
streamlit run Home.py
```

---

## Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Orchestration | Apache Airflow 3.x |
| Data Warehouse | Snowflake |
| Data Lake | Amazon S3 |
| Transformations | dbt Core 1.9 |
| Dashboard | Streamlit + Plotly |
| CI/CD | GitHub Actions |
| Conteneurisation | Docker Compose |

> 📄 **Documentation** : [Snowflake Setup](docs/Snowflake_doc.pdf)

---

## Démarrage Rapide

### 1. Démarrer l'environnement

```bash
docker-compose up -d
```

### 2. Lancer le pipeline complet

```bash
docker exec fraudlens-airflow-worker-1 airflow dags trigger master_dag
```

### 3. Accéder aux interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Dashboard | http://localhost:8501 | - |

---

## CI/CD

| Workflow | Déclencheur | Action |
|----------|-------------|--------|
| `dbt_test.yml` | Pull Request | Compile + teste les modèles Gold |
| `dbt_deploy.yml` | Push main | Génère la documentation dbt |
| `lint.yml` | Pull Request | Linting SQL (SQLFluff) |

### Secrets GitHub requis

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_PRIVATE_KEY
```

> 📄 **Documentation** : [CI/CD Setup](docs/CICD_doc.pdf)

---

## Documentation

| Document | Description |
|----------|-------------|
| [FraudLens Overview](docs/FraudLens_doc.pdf) | Vue d'ensemble du projet |
| [Pipeline Architecture](docs/pipelineArchitecture.pdf) | Architecture Airflow |
| [Data Modeling](docs/DataModeling_doc.pdf) | Modélisation des données |
| [dbt Models](docs/DBT_doc.pdf) | Documentation dbt |
| [Snowflake Setup](docs/Snowflake_doc.pdf) | Configuration Snowflake |
| [CI/CD](docs/CICD_doc.pdf) | Workflows GitHub Actions |

---

## Structure du Projet

```
fraudlens/
├── .github/workflows/    # CI/CD GitHub Actions
├── config/               # Configuration Airflow
├── dags/                 # DAGs Airflow
├── dashboard/            # Application Streamlit
├── dbt/                  # Modèles dbt
├── docs/                 # Documentation PDF
├── snowflake/            # Clés RSA (ignorées)
├── docker-compose.yaml
└── README.md
```

---

## Auteur

**MooM** - FraudLens Project | 2026

[![GitHub](https://img.shields.io/badge/GitHub-Mo--oM--1-blue?logo=github)](https://github.com/Mo-oM-1/fraudlens)
