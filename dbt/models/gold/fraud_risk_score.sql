{{ config(materialized='table') }}

SELECT 
  p.NPI,
  -- Composite score 0-100 (doc) [file:14]
  (COALESCE(e.IS_EXCLUDED_HIGH_RISK, 0)*40 +  -- Exclusion 40pts
   COALESCE(pay.PCT_HIGH_RISK_PAYMENTS, 0)*20 + -- Payments 20pts
   COALESCE(rx.PCT_BRAND_CLAIMS, 0)*20 +       -- Rx 20pts
   CASE WHEN pay.NPI IS NOT NULL AND rx.NPI IS NOT NULL THEN 20 ELSE 0 END) AS fraud_risk_score,  -- Anomalies 20pts
  
  CASE 
    WHEN fraud_risk_score >=70 THEN 'CRITICAL'
    WHEN fraud_risk_score >=50 THEN 'HIGH'
    WHEN fraud_risk_score >=30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_tier,
  
  p.IS_EXCLUDED,
  pay.TOTAL_PAYMENT_AMOUNT,
  rx.TOTAL_PRESCRIPTION_COST,
  CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('provider_360') }} p
LEFT JOIN payments_agg pay ON p.NPI = pay.NPI  -- Utilise tes silver
LEFT JOIN prescriptions_agg rx ON p.NPI = rx.NPI
LEFT JOIN excluded e ON p.NPI = e.NPI
GROUP BY 1, p.IS_EXCLUDED, pay.TOTAL_PAYMENT_AMOUNT, rx.TOTAL_PRESCRIPTION_COST  -- Unique NPI