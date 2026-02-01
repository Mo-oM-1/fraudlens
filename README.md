# Medical Fraud Detection Data Pipeline

Ce projet ingère et centralise plusieurs datasets publics CMS/OIG pour détecter des anomalies et fraudes dans les paiements médicaux.

---

## 1. LEIE (Excluded Individuals / Entities) (-2026)
- **URL :** https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv
- **Contenu :** Liste des individus et entités exclues du programme Medicare/Medicaid (nom, type, raison de l’exclusion, dates).
- **Usage :** Détecter paiements vers des prestataires exclus.

---

## 2. Medicare Hospital Spending by Claim (2023)
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/1f8cde9e222d5d49f88a894bcf7a8981_1736791547/Medicare_Hospital_Spending_by_Claim.csv
- **Contenu :** Paiements Medicare par hôpital et type de prestation (montant, nombre de patients).
- **Usage :** Identifier dépenses anormales ou patterns suspects.

---

## 3. Open Payments (Snowflake)(Batch 2024)
- **URL :** 
- **Contenu :** Paiements aux médecins et prestataires pour le program year 2024.
- **Usage :** Analyse des paiements, conflits d’intérêts, patterns de valeur.

---

## 4. Provider Information (Nursing Home / NH) (-2025)
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/66f260a1b66c08187c24fee8d189943b_1767384363/NH_ProviderInfo_Dec2025.csv
- **Contenu :** Informations détaillées sur les prestataires (NPI, nom, adresse, type, spécialité).
- **Usage :** Enrichissement des datasets Open Payments et Medicare Spending.

---

## 5. Long-Term Care Hospital – General Information (-2025)
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/8e792480607df0e6a32bbc6ac99a2f31_1764691562/Long-Term_Care_Hospital-General_Information_Dec2025.csv
- **Contenu :** Détails des hôpitaux de soins prolongés (NPI, adresse, type de soins).
- **Usage :** Enrichir la localisation et le type des hôpitaux pour l’analyse.

---

## 6. Hospice – General Information (-2025)
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/e49674eb0b3c2dd749563637f3b79a15_1763064336/Hospice_General-Information_Nov2025.csv
- **Contenu :** Informations sur les établissements de soins palliatifs (adresse, type, NPI).
- **Usage :** Ajouter le contexte des hospices pour les paiements et dépenses suspectes.

---

## 7. Home Health Care – Zip Codes (-2026)
- **URL :** https://data.cms.gov/provider-data/sites/default/files/resources/9fc01e0ca9b64f045d2700d4f25ab35c_1767204341/HH_Zip_Jan2026.csv
- **Contenu :** Prestataires de soins à domicile avec ZIP Codes.
- **Usage :** Analyses géospatiales et détection de clusters de paiements anormaux.

---

## 8. NPPES Provider Data (Snowflake) (-2026)
- **Database / Schema :** `AFFINE_NPPES_PROVIDER_DATA.REF_DW`
- **Contenu :** Informations détaillées sur tous les prestataires disposant d’un NPI aux États-Unis.
- **Usage :** Enrichissement et jointures avec Open Payments et autres datasets.

---

## 9. DAG Orchestrateur – Central Pipeline
- **Nom du DAG :** `launchDAG`
- **Description :** DAG central qui orchestre tous les DAGs CSV existants pour ingérer et transformer l’ensemble des datasets.
- **Fonctionnement :**
  - Liste des DAGs orchestrés :
    - `leie_download`
    - `medicare_hospital_spending_download`
    - `open_payments_download`
    - `provider_information_download`
    - `longterm_care_hospital_download`
    - `hospice_download`
    - `home_health_care_download`
  - Chaque DAG est déclenché via `TriggerDagRunOperator` et exécuté **en parallèle**.
  - Le DAG attend la fin de chaque DAG déclenché avant de passer à l’étape suivante (`wait_for_completion=True`).
  - Une tâche finale `summary_orchestrator` consigne dans les logs le **résumé des exécutions** pour chaque DAG déclenché.
- **Usage :** Point central d’orchestration pour exécuter le pipeline complet et monitoré, permettant de maintenir la cohérence et de suivre les métriques globales.

---

## 🔗 Liens entre datasets
- **LEIE ↔ Open Payments** : NPI ou nom du prestataire  
- **Open Payments ↔ Provider Information / Long-Term Care / Hospice / Home Health Care / NPPES** : NPI  
- **Medicare Hospital Spending ↔ Provider Information / Long-Term Care** : NPI / Hospital ID  
- **Home Health Care** : correspondance via ZIP Codes pour analyses géospatiales

---

## Objectif
Centraliser ces données pour créer un **pipeline Airflow + Snowflake** afin de générer des alertes et features pour la **détection de fraude médicale**.

---

## Stack technique
- Airflow pour l’orchestration des DAGs  
- Snowflake pour le stockage et les transformations  
- S3 pour le landing des fichiers bruts (bronze)  
- Pandas / Python pour le traitement et la validation des fichiers