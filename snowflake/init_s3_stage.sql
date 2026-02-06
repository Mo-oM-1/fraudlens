USE DATABASE FRAUDLENS_DB;
USE SCHEMA BRONZE;

-- File format pour les fichiers Parquet
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
  TYPE = PARQUET
  COMPRESSION = AUTO;

-- Stage externe pour les fichiers Parquet bronze
CREATE OR REPLACE STAGE BRONZE_S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://ai-factory-bckt/bronze/'
  FILE_FORMAT = PARQUET_FORMAT
  COMMENT = 'Stage externe pour les fichiers Parquet bronze du projet FraudLens';
