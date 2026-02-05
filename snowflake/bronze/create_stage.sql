-- =============================================================================
-- BRONZE S3 STAGE
-- Stage externe pour les fichiers Parquet dans le dossier bronze
-- =============================================================================

USE DATABASE AI_FACTORY_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE STAGE BRONZE_S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://ai-factory-bckt/bronze/'
  FILE_FORMAT = (TYPE = PARQUET)
  COMMENT = 'Stage externe pour les fichiers Parquet bronze du projet AI Factory';
