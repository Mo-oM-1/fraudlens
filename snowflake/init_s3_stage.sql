-- --------------------------------------------------
-- Création du stage externe S3_STAGE
-- --------------------------------------------------
USE DATABASE AI_FACTORY_DB;
USE SCHEMA RAW_DATA;

CREATE OR REPLACE STAGE S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://ai-factory-bckt/raw/'
  COMMENT = 'Stage externe pour les fichiers bruts du projet AI Factory';