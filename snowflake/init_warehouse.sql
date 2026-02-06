-- --------------------------------------------------
-- Création du warehouse pour le projet FraudLens
-- --------------------------------------------------
CREATE OR REPLACE WAREHOUSE FRAUDLENS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse pour le projet FraudLens - Healthcare Fraud Detection';
