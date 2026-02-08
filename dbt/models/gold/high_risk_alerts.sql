{{
    config(
        materialized='table',
        unique_key='alert_id'
    )
}}

/*
    Gold High Risk Alerts
    ---------------------
    Actionable alerts for fraud investigation.
    Each row = one alert requiring review.
*/

with provider_360 as (
    select NPI from {{ ref('provider_360') }}
    where NPI is not null
),

fraud_scores as (
    select * from {{ ref('fraud_risk_score') }}
),

excluded_providers as (
    select distinct ep.*
    from {{ ref('excluded_providers') }} ep
    inner join provider_360 p360 on ep.NPI = p360.NPI  -- Only include if in provider_360
    where ep.IS_CURRENTLY_EXCLUDED = true
    qualify row_number() over (partition by ep.NPI order by ep.EXCLUSION_DATE desc) = 1
),

payments as (
    select pay.*
    from {{ ref('payments') }} pay
    inner join {{ ref('provider_360') }} p360 on pay.NPI = p360.NPI  -- Only include if in provider_360
    where pay.IS_RECIPIENT_EXCLUDED = true
    and pay.NPI is not null
),

prescriptions as (
    select rx.*
    from {{ ref('prescriptions') }} rx
    inner join {{ ref('provider_360') }} p360 on rx.NPI = p360.NPI  -- Only include if in provider_360
    where rx.IS_PRESCRIBER_EXCLUDED = true
),

-- Alert Type 1: High Risk Score Providers
high_risk_score_alerts as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI', "'HIGH_RISK_SCORE'"]) }} as ALERT_ID,
        'HIGH_RISK_SCORE' as ALERT_TYPE,
        NPI,
        COALESCE(FULL_NAME, ORGANIZATION_NAME) as PROVIDER_NAME,
        SPECIALTY,
        STATE,
        FRAUD_RISK_SCORE as RISK_SCORE,
        RISK_TIER,
        'Provider has fraud risk score of ' || FRAUD_RISK_SCORE || ' (' || RISK_TIER || ')' as ALERT_DESCRIPTION,
        TOTAL_FINANCIAL_EXPOSURE as FINANCIAL_EXPOSURE,
        'PENDING' as ALERT_STATUS,
        current_date() as ALERT_DATE
    from fraud_scores
    where RISK_TIER in ('CRITICAL', 'HIGH')
),

-- Alert Type 2: Excluded Provider Still Active
excluded_still_active_alerts as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI', "'EXCLUDED_STILL_ACTIVE'"]) }} as ALERT_ID,
        'EXCLUDED_STILL_ACTIVE' as ALERT_TYPE,
        NPI,
        COALESCE(NPPES_FULL_NAME, LAST_NAME || ' ' || FIRST_NAME) as PROVIDER_NAME,
        PROVIDER_SPECIALTY as SPECIALTY,
        STATE,
        100 as RISK_SCORE,
        'CRITICAL' as RISK_TIER,
        'Provider excluded on ' || EXCLUSION_DATE || ' for "' || EXCLUSION_REASON || '" but still active in NPPES' as ALERT_DESCRIPTION,
        null as FINANCIAL_EXPOSURE,
        'PENDING' as ALERT_STATUS,
        current_date() as ALERT_DATE
    from excluded_providers
    where IS_HIGH_RISK = true
),

-- Alert Type 3: Payments to Excluded Providers
payments_to_excluded_alerts as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI', 'PAYER_NAME', "'PAYMENT_TO_EXCLUDED'"]) }} as ALERT_ID,
        'PAYMENT_TO_EXCLUDED' as ALERT_TYPE,
        NPI,
        FULL_NAME as PROVIDER_NAME,
        SPECIALTY_CLASSIFICATION as SPECIALTY,
        STATE,
        90 as RISK_SCORE,
        'CRITICAL' as RISK_TIER,
        'Excluded provider received ' || COUNT(*) || ' payments totaling $' || ROUND(SUM(PAYMENT_AMOUNT), 2) || ' from ' || PAYER_NAME as ALERT_DESCRIPTION,
        SUM(PAYMENT_AMOUNT) as FINANCIAL_EXPOSURE,
        'PENDING' as ALERT_STATUS,
        current_date() as ALERT_DATE
    from payments
    where NPI is not null
    group by NPI, FULL_NAME, SPECIALTY_CLASSIFICATION, STATE, PAYER_NAME
),

-- Alert Type 4: Prescriptions by Excluded Providers
prescriptions_by_excluded_alerts as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI', "'PRESCRIPTION_BY_EXCLUDED'"]) }} as ALERT_ID,
        'PRESCRIPTION_BY_EXCLUDED' as ALERT_TYPE,
        NPI,
        PRESCRIBER_FULL_NAME as PROVIDER_NAME,
        PRESCRIBER_TYPE as SPECIALTY,
        STATE,
        95 as RISK_SCORE,
        'CRITICAL' as RISK_TIER,
        'Excluded provider prescribed ' || SUM(TOTAL_CLAIMS) || ' claims totaling $' || ROUND(SUM(TOTAL_DRUG_COST), 2) as ALERT_DESCRIPTION,
        SUM(TOTAL_DRUG_COST) as FINANCIAL_EXPOSURE,
        'PENDING' as ALERT_STATUS,
        current_date() as ALERT_DATE
    from prescriptions
    where NPI is not null
    group by NPI, PRESCRIBER_FULL_NAME, PRESCRIBER_TYPE, STATE
),

-- Alert Type 5: High Brand Prescribers (potential kickback indicator)
high_brand_alerts as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI', "'HIGH_BRAND_PRESCRIBER'"]) }} as ALERT_ID,
        'HIGH_BRAND_PRESCRIBER' as ALERT_TYPE,
        NPI,
        COALESCE(FULL_NAME, ORGANIZATION_NAME) as PROVIDER_NAME,
        SPECIALTY,
        STATE,
        FRAUD_RISK_SCORE as RISK_SCORE,
        'HIGH' as RISK_TIER,
        'Provider has ' || PCT_BRAND_CLAIMS || '% brand prescription rate (potential kickback indicator)' as ALERT_DESCRIPTION,
        TOTAL_PRESCRIPTION_COST as FINANCIAL_EXPOSURE,
        'PENDING' as ALERT_STATUS,
        current_date() as ALERT_DATE
    from fraud_scores
    where IS_HIGH_BRAND_PRESCRIBER = true
      and NOT IS_EXCLUDED  -- Excluded providers already have alerts
),

all_alerts as (
    select * from high_risk_score_alerts
    union all
    select * from excluded_still_active_alerts
    union all
    select * from payments_to_excluded_alerts
    union all
    select * from prescriptions_by_excluded_alerts
    union all
    select * from high_brand_alerts
),

deduplicated as (
    select
        ALERT_ID,
        ALERT_TYPE,
        NPI,
        PROVIDER_NAME,
        SPECIALTY,
        STATE,
        RISK_SCORE,
        RISK_TIER,
        ALERT_DESCRIPTION,
        FINANCIAL_EXPOSURE,
        ALERT_STATUS,
        ALERT_DATE,

        -- Priority ranking
        case
            when ALERT_TYPE = 'EXCLUDED_STILL_ACTIVE' then 1
            when ALERT_TYPE = 'PRESCRIPTION_BY_EXCLUDED' then 2
            when ALERT_TYPE = 'PAYMENT_TO_EXCLUDED' then 3
            when ALERT_TYPE = 'HIGH_RISK_SCORE' and RISK_TIER = 'CRITICAL' then 4
            when ALERT_TYPE = 'HIGH_BRAND_PRESCRIBER' then 5
            else 6
        end as PRIORITY_RANK

    from all_alerts
    -- Deduplicate: keep highest priority alert per ALERT_ID
    qualify row_number() over (partition by ALERT_ID order by
        case
            when ALERT_TYPE = 'EXCLUDED_STILL_ACTIVE' then 1
            when ALERT_TYPE = 'PRESCRIPTION_BY_EXCLUDED' then 2
            when ALERT_TYPE = 'PAYMENT_TO_EXCLUDED' then 3
            when ALERT_TYPE = 'HIGH_RISK_SCORE' then 4
            when ALERT_TYPE = 'HIGH_BRAND_PRESCRIBER' then 5
            else 6
        end
    ) = 1
),

final as (
    select
        *,
        current_timestamp() as _loaded_at
    from deduplicated
)

select * from final
order by PRIORITY_RANK, RISK_SCORE desc
