{{ config(materialized='table') }}

{{ dbt_utils.generate_surrogate_key(['npi', 'alert_type', 'risk_tier', '_loaded_at']) }} AS alert_id,

SELECT 
  fr.NPI,
  CASE 
    WHEN fr.IS_EXCLUDED THEN 'EXCLUDED_STILL_ACTIVE'
    WHEN fr.fraud_risk_score >=70 THEN 'HIGH_RISK_SCORE_CRITICAL'
    WHEN fr.fraud_risk_score >=50 THEN 'HIGH_RISK_SCORE_HIGH'
    WHEN pay.PCT_HIGH_RISK_PAYMENTS > 0.8 THEN 'PAYMENT_TO_EXCLUDED'
    WHEN rx.PCT_BRAND_CLAIMS > 0.8 THEN 'HIGH_BRAND_PRESCRIBER'
  END AS alert_type,
  fr.risk_tier,
  fr.fraud_risk_score,
  CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('fraud_risk_score') }} fr
WHERE fr.fraud_risk_score >= 50  -- HIGH+ seulement (doc)