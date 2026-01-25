CREATE OR REPLACE DATABASE AI_FACTORY_DB
  COMMENT = 'Database pour le projet AI Factory';

USE DATABASE AI_FACTORY_DB;

CREATE OR REPLACE SCHEMA RAW_DATA
  COMMENT = 'Données brutes, directement depuis S3';

CREATE OR REPLACE SCHEMA BRONZE
  COMMENT = 'Données bronze après ingestion initiale';

CREATE OR REPLACE SCHEMA SILVER
  COMMENT = 'Données transformées et nettoyées';

CREATE OR REPLACE SCHEMA GOLD
  COMMENT = 'Données prêtes pour l’analyse et BI';