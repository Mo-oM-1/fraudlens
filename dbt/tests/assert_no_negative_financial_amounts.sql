-- Test: No negative financial amounts in provider 360
-- Validates data integrity for payment and prescription costs

select
    NPI,
    TOTAL_PAYMENT_AMOUNT,
    TOTAL_PRESCRIPTION_COST,
    TOTAL_FINANCIAL_EXPOSURE
from {{ ref('provider_360') }}
where TOTAL_PAYMENT_AMOUNT < 0
   or TOTAL_PRESCRIPTION_COST < 0
   or TOTAL_FINANCIAL_EXPOSURE < 0
