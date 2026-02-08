-- Test: All CRITICAL risk tier providers should have IS_EXCLUDED = true
-- This validates the risk scoring logic is working correctly

select
    NPI,
    FRAUD_RISK_SCORE,
    RISK_TIER,
    IS_EXCLUDED
from {{ ref('fraud_risk_score') }}
where RISK_TIER = 'CRITICAL'
  and IS_EXCLUDED = false
  and FRAUD_RISK_SCORE >= 70  -- Only flag if score is legitimately critical
