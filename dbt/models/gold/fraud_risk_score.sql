{{
    config(
        materialized='table',
        unique_key='npi'
    )
}}

/*
    Gold Fraud Risk Score
    ---------------------
    Composite fraud risk scoring for each provider.
    Combines multiple risk signals into a single score.

    Scoring methodology:
    - Base score: 0
    - Each risk factor adds points
    - Final score: 0-100 scale
    - Higher = more suspicious
*/

with provider_360 as (
    select * from {{ ref('provider_360') }}
),

risk_calculation as (
    select
        NPI,
        FULL_NAME,
        ORGANIZATION_NAME,
        ENTITY_TYPE,
        SPECIALTY,
        STATE,

        -- Start with base score of 0
        0

        -- EXCLUSION RISKS (High weight - 40 points max)
        + case when IS_EXCLUDED then 25 else 0 end
        + case when IS_EXCLUDED_HIGH_RISK then 15 else 0 end  -- Excluded but still active

        -- PAYMENT RISKS (20 points max)
        + case
            when RECIPIENT_TIER = 'MEGA_RECIPIENT' then 10
            when RECIPIENT_TIER = 'MAJOR_RECIPIENT' then 5
            else 0
          end
        + case when PCT_HIGH_RISK_PAYMENTS >= 50 then 10 else PCT_HIGH_RISK_PAYMENTS / 10 end

        -- PRESCRIPTION RISKS (30 points max)
        + case
            when OPIOID_RISK_TIER = 'CRITICAL_OPIOID' then 15
            when OPIOID_RISK_TIER = 'HIGH_OPIOID' then 10
            when OPIOID_RISK_TIER = 'MODERATE_OPIOID' then 5
            else 0
          end
        + case when PCT_BRAND_CLAIMS >= 80 then 10 else PCT_BRAND_CLAIMS / 10 end
        + case when TOTAL_HIGH_RISK_DRUGS >= 10 then 5 else TOTAL_HIGH_RISK_DRUGS / 2 end

        -- ACTIVITY ANOMALIES (10 points max)
        + case
            when HAS_PHARMA_PAYMENTS and HAS_PRESCRIPTIONS then 5  -- Both activities
            else 0
          end
        + case when IS_NPI_ACTIVE = false and (HAS_PHARMA_PAYMENTS or HAS_PRESCRIPTIONS) then 5 else 0 end

        as RAW_RISK_SCORE,

        -- Individual risk components for transparency
        IS_EXCLUDED,
        IS_EXCLUDED_HIGH_RISK,
        RECIPIENT_TIER,
        PCT_HIGH_RISK_PAYMENTS,
        OPIOID_RISK_TIER,
        PCT_OPIOID_CLAIMS,
        PCT_BRAND_CLAIMS,
        TOTAL_HIGH_RISK_DRUGS,
        HAS_PHARMA_PAYMENTS,
        HAS_PRESCRIPTIONS,
        IS_NPI_ACTIVE,
        TOTAL_PAYMENT_AMOUNT,
        TOTAL_PRESCRIPTION_COST,
        TOTAL_FINANCIAL_EXPOSURE

    from provider_360
),

final as (
    select
        NPI,
        FULL_NAME,
        ORGANIZATION_NAME,
        ENTITY_TYPE,
        SPECIALTY,
        STATE,

        -- Normalized score (0-100)
        LEAST(ROUND(RAW_RISK_SCORE, 0), 100) as FRAUD_RISK_SCORE,

        -- Risk tier
        case
            when RAW_RISK_SCORE >= 70 then 'CRITICAL'
            when RAW_RISK_SCORE >= 50 then 'HIGH'
            when RAW_RISK_SCORE >= 30 then 'MEDIUM'
            when RAW_RISK_SCORE >= 10 then 'LOW'
            else 'MINIMAL'
        end as RISK_TIER,

        -- Risk flags (for filtering)
        IS_EXCLUDED,
        IS_EXCLUDED_HIGH_RISK,
        case when OPIOID_RISK_TIER in ('CRITICAL_OPIOID', 'HIGH_OPIOID') then true else false end as IS_HIGH_OPIOID_PRESCRIBER,
        case when PCT_BRAND_CLAIMS >= 80 then true else false end as IS_HIGH_BRAND_PRESCRIBER,
        case when RECIPIENT_TIER in ('MEGA_RECIPIENT', 'MAJOR_RECIPIENT') then true else false end as IS_MAJOR_PAYMENT_RECIPIENT,

        -- Financial metrics
        TOTAL_PAYMENT_AMOUNT,
        TOTAL_PRESCRIPTION_COST,
        TOTAL_FINANCIAL_EXPOSURE,

        -- Component scores (for analysis)
        RECIPIENT_TIER,
        OPIOID_RISK_TIER,
        PCT_HIGH_RISK_PAYMENTS,
        PCT_OPIOID_CLAIMS,
        PCT_BRAND_CLAIMS,
        TOTAL_HIGH_RISK_DRUGS,

        -- Activity
        HAS_PHARMA_PAYMENTS,
        HAS_PRESCRIPTIONS,
        IS_NPI_ACTIVE,

        current_timestamp() as _loaded_at

    from risk_calculation
)

select * from final
