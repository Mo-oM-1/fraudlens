# Medical Fraud Detection Data Pipeline

Pipeline de donnees pour la detection de fraudes medicales. Ingere et centralise plusieurs datasets publics CMS/OIG pour detecter des anomalies dans les paiements Medicare/Medicaid.

---

## Architecture

```
master_dag (Orchestrateur)
    |
    +-- init_snowflake_environment (Bootstrap)
    |       +-- create_warehouse (AI_FACTORY_WH)
    |       +-- create_schemas (RAW_DATA, BRONZE, SILVER, GOLD)
    |       +-- create_s3_stage
    |
    +-- [En parallele]
            +-- leie_download
            +-- medicare_hospital_spending_download
            +-- open_payments_download
            +-- provider_information_download
            +-- longterm_care_hospital_download
            +-- hospice_download
            +-- home_health_care_download
            +-- medicare_part_d_prescribers_download
```

---

## Sources de Donnees

### 1. LEIE (Excluded Individuals / Entities)
- **URL :** https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv
- **Contenu :** Liste des individus et entites exclus du programme Medicare/Medicaid (nom, type, raison de l'exclusion, dates).
- **Usage Fraude :** Detecter paiements vers des prestataires deja exclus pour fraude.
- **Cle :** NPI, Nom

### 2. Medicare Hospital Spending by Claim
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/.../Medicare_Hospital_Spending_by_Claim.csv
- **Contenu :** Paiements Medicare par hopital et type de prestation (montant, nombre de patients).
- **Usage Fraude :** Identifier depenses anormales ou patterns suspects par etablissement.
- **Cle :** Facility ID

### 3. Open Payments (Batch 2024)
- **URL :** https://download.cms.gov/openpayments/PGYR2024_P06302025_06162025.zip
- **Contenu :** Paiements des laboratoires pharmaceutiques aux medecins (cadeaux, consulting, recherche).
- **Usage Fraude :** Detecter conflits d'interets, kickbacks, patterns de corruption.
- **Cle :** NPI

### 4. Provider Information (Nursing Home)
- **URL :** https://data.cms.gov/provider-data/.../NH_ProviderInfo_Dec2025.csv
- **Contenu :** Informations detaillees sur les prestataires (NPI, nom, adresse, type, specialite).
- **Usage Fraude :** Enrichissement et validation des etablissements.
- **Cle :** NPI, Provider ID

### 5. Long-Term Care Hospital
- **URL :** https://data.cms.gov/provider-data/.../Long-Term_Care_Hospital-General_Information_Dec2025.csv
- **Contenu :** Details des hopitaux de soins prolonges (NPI, adresse, type de soins).
- **Usage Fraude :** Secteur a risque - enrichir la localisation pour l'analyse.
- **Cle :** Provider ID

### 6. Hospice
- **URL :** https://data.cms.gov/provider-data/.../Hospice_General-Information_Nov2025.csv
- **Contenu :** Informations sur les etablissements de soins palliatifs.
- **Usage Fraude :** Secteur a haut risque de fraude Medicare.
- **Cle :** Provider ID

### 7. Home Health Care
- **URL :** https://data.cms.gov/provider-data/.../HH_Zip_Jan2026.csv
- **Contenu :** Prestataires de soins a domicile avec ZIP Codes.
- **Usage Fraude :** Analyses geospatiales et detection de clusters anormaux.
- **Cle :** ZIP Code

### 8. Medicare Part D Prescribers (NEW)
- **URL :** https://data.cms.gov/sites/default/files/.../MUP_DPR_RY24_P04_V10_DY22_NPI.csv
- **Contenu :** Prescriptions par medecin (NPI, specialite, nb prescriptions, couts, beneficiaires).
- **Usage Fraude :** Detecter les sur-prescripteurs, pill mills, anomalies de prescription.
- **Cle :** NPI

### 9. NPPES Provider Data (Snowflake Marketplace)
- **Source :** `AFFINE_NPPES_PROVIDER_DATA` (Snowflake Marketplace)
- **Contenu :** Referentiel complet de tous les NPI aux Etats-Unis.
- **Usage Fraude :** Table de reference maitre pour les jointures.
- **Cle :** NPI (cle primaire)

---

## Connexions Snowflake

| Connexion | Warehouse | Usage |
|-----------|-----------|-------|
| `snowflake_bootstrap` | COMPUTE_WH | Creation infra (warehouse, DB, schemas) |
| `snowflake_default` | AI_FACTORY_WH | Operations metier (DAGs de donnees) |

---

## Schema de Jointure

```
                    NPPES (Snowflake Marketplace)
                              |
                             NPI
                              |
        +---------------------+---------------------+
        |                     |                     |
        v                     v                     v
      LEIE              Open Payments         Part D Prescribers
   (fraudeurs)         (paiements)            (prescriptions)
        |                     |                     |
        +----------+----------+----------+---------+
                   |
                   v
            Provider Info / Hospice / LTCH / Home Health
```

---

## Structure S3

```
s3://ai-factory-bckt/
    +-- raw/                          # CSV bruts (versiones + LATEST)
    |   +-- leie/
    |   +-- medicare_hospital_spending/
    |   +-- open_payments/
    |   +-- provider_information/
    |   +-- longterm_care_hospital/
    |   +-- hospice/
    |   +-- home_health_care/
    |   +-- medicare_part_d_prescribers/
    |
    +-- bronze/                       # Parquet (optimise pour Snowflake)
        +-- leie/
        +-- medicare_hospital_spending/
        +-- open_payments/
        +-- provider_information/
        +-- longterm_care_hospital/
        +-- hospice/
        +-- home_health_care/
        +-- medicare_part_d_prescribers/
```

---

## Schema Snowflake (Medallion Architecture)

| Schema | Description |
|--------|-------------|
| `RAW_DATA` | Donnees brutes depuis S3 Stage |
| `BRONZE` | Donnees ingeres, typage initial |
| `SILVER` | Donnees nettoyees, jointes, enrichies |
| `GOLD` | Donnees prets pour l'analyse et BI |

---

## Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Orchestration | Apache Airflow 3.x (CeleryExecutor) |
| Data Warehouse | Snowflake |
| Data Lake | Amazon S3 |
| Conteneurisation | Docker + Docker Compose |
| Traitement | Python 3.12, Pandas, PyArrow |
| Base metadonnees | PostgreSQL 16 |
| Message Broker | Redis 7.2 |

---

## Utilisation

### Demarrer l'environnement
```bash
docker-compose up -d
```

### Lancer le pipeline complet
```bash
docker exec ai_factory-airflow-worker-1 airflow dags trigger master_dag
```

### Lancer un DAG individuel
```bash
docker exec ai_factory-airflow-worker-1 airflow dags trigger leie_download
```

### Acceder a l'interface Airflow
- URL : http://localhost:8080
- User : airflow
- Password : airflow

---

## Prochaines Etapes

1. **Charger les donnees dans Snowflake** (COPY depuis S3 Stage)
2. **Creer la couche Silver** (jointures via NPI, nettoyage)
3. **Developper les regles de detection** (anomalies, croisements LEIE)
4. **Creer la couche Gold** (features pour ML, dashboards)

---

## Auteur
MooM - AI Factory Project
