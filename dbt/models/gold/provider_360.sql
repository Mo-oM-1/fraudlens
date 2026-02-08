{{
    config(
        materialized='table',
        unique_key='npi'
    )
}}

/*
    Gold Provider 360
    -----------------
    Complete provider profile combining all data sources.
    Single source of truth for provider analysis.
*/

with provider as (
    select * from {{ ref('provider') }}
    qualify row_number() over (partition by NPI order by NPI_ENUMERATION_DATE desc) = 1
),

payments_summary as (
    select * from {{ ref('payments_summary') }}
),

prescriptions_summary as (
    select * from {{ ref('prescriptions_summary') }}
),

excluded as (
    select distinct
        NPI,
        EXCLUSION_REASON,
        EXCLUSION_DATE,
        IS_CURRENTLY_EXCLUDED,
        IS_HIGH_RISK as IS_EXCLUDED_HIGH_RISK
    from {{ ref('excluded_providers') }}
    where NPI is not null
    qualify row_number() over (partition by NPI order by EXCLUSION_DATE desc) = 1
),
joined as (
    select
        p.NPI,
        p.ENTITY_TYPE,
        p.FULL_NAME,
        p.ORGANIZATION_NAME,
        p.CREDENTIAL,
        p.GENDER,
        p.CITY,
        p.STATE,
        p.ZIP_CODE,
        p.PHONE,
        p.SPECIALTY_CLASSIFICATION,
        p.SPECIALTY,
        p.PROVIDER_TYPE,
        p.IS_NPI_ACTIVE,
        p.NPI_ENUMERATION_DATE,
        p.IS_EXCLUDED,
        e.EXCLUSION_REASON,
        e.EXCLUSION_DATE,
        e.IS_CURRENTLY_EXCLUDED,
        COALESCE(e.IS_EXCLUDED_HIGH_RISK, false) as IS_EXCLUDED_HIGH_RISK,
        COALESCE(pay.TOTAL_PAYMENTS, 0) as TOTAL_PAYMENTS,
        COALESCE(pay.TOTAL_PAYMENT_AMOUNT, 0) as TOTAL_PAYMENT_AMOUNT,
        COALESCE(pay.UNIQUE_PAYERS, 0) as UNIQUE_PAYERS,
        pay.RECIPIENT_TIER,
        COALESCE(pay.PCT_HIGH_RISK_PAYMENTS, 0) as PCT_HIGH_RISK_PAYMENTS,
        pay.FIRST_PAYMENT_DATE,
        pay.LAST_PAYMENT_DATE,
        COALESCE(rx.TOTAL_CLAIMS, 0) as TOTAL_PRESCRIPTION_CLAIMS,
        COALESCE(rx.TOTAL_COST, 0) as TOTAL_PRESCRIPTION_COST,
        COALESCE(rx.UNIQUE_DRUGS_PRESCRIBED, 0) as UNIQUE_DRUGS_PRESCRIBED,
        COALESCE(rx.PCT_BRAND_CLAIMS, 0) as PCT_BRAND_CLAIMS,
        rx.PRESCRIBER_VOLUME_TIER,
        COALESCE(rx.TOTAL_HIGH_RISK_DRUGS, 0) as TOTAL_HIGH_RISK_DRUGS,
        case when pay.NPI is not null then true else false end as HAS_PHARMA_PAYMENTS,
        case when rx.NPI is not null then true else false end as HAS_PRESCRIPTIONS,
        COALESCE(pay.TOTAL_PAYMENT_AMOUNT, 0) + COALESCE(rx.TOTAL_COST, 0) as TOTAL_FINANCIAL_EXPOSURE
    from provider p
    left join payments_summary pay on p.NPI = pay.NPI
    left join prescriptions_summary rx on p.NPI = rx.NPI
    left join excluded e on p.NPI = e.NPI
),

final as (
    select
        -- Provider identity
        p.NPI,
        p.ENTITY_TYPE,
        p.FULL_NAME,
        p.ORGANIZATION_NAME,
        p.CREDENTIAL,
        p.GENDER,

        -- Location
        p.CITY,
        p.STATE,
        p.ZIP_CODE,
        p.PHONE,

        -- Specialty
        p.SPECIALTY_CLASSIFICATION,
        p.SPECIALTY,
        p.PROVIDER_TYPE,

        -- NPI Status
        p.IS_NPI_ACTIVE,
        p.NPI_ENUMERATION_DATE,

        -- Exclusion info
        p.IS_EXCLUDED,
        e.EXCLUSION_REASON,
        e.EXCLUSION_DATE,
        e.IS_CURRENTLY_EXCLUDED,
        COALESCE(e.IS_EXCLUDED_HIGH_RISK, false) as IS_EXCLUDED_HIGH_RISK,

        -- Payment metrics
        COALESCE(pay.TOTAL_PAYMENTS, 0) as TOTAL_PAYMENTS,
        COALESCE(pay.TOTAL_PAYMENT_AMOUNT, 0) as TOTAL_PAYMENT_AMOUNT,
        COALESCE(pay.UNIQUE_PAYERS, 0) as UNIQUE_PAYERS,
        pay.RECIPIENT_TIER,
        COALESCE(pay.PCT_HIGH_RISK_PAYMENTS, 0) as PCT_HIGH_RISK_PAYMENTS,
        pay.FIRST_PAYMENT_DATE,
        pay.LAST_PAYMENT_DATE,

        -- Prescription metrics
        COALESCE(rx.TOTAL_CLAIMS, 0) as TOTAL_PRESCRIPTION_CLAIMS,
        COALESCE(rx.TOTAL_COST, 0) as TOTAL_PRESCRIPTION_COST,
        COALESCE(rx.UNIQUE_DRUGS_PRESCRIBED, 0) as UNIQUE_DRUGS_PRESCRIBED,
        COALESCE(rx.PCT_BRAND_CLAIMS, 0) as PCT_BRAND_CLAIMS,
        rx.PRESCRIBER_VOLUME_TIER,
        COALESCE(rx.TOTAL_HIGH_RISK_DRUGS, 0) as TOTAL_HIGH_RISK_DRUGS,

        -- Activity flags
        case when pay.NPI is not null then true else false end as HAS_PHARMA_PAYMENTS,
        case when rx.NPI is not null then true else false end as HAS_PRESCRIPTIONS,

        -- Combined financial exposure
        COALESCE(pay.TOTAL_PAYMENT_AMOUNT, 0) + COALESCE(rx.TOTAL_COST, 0) as TOTAL_FINANCIAL_EXPOSURE,

        current_timestamp() as _loaded_at

    from provider p
    left join payments_summary pay on p.NPI = pay.NPI
    left join prescriptions_summary rx on p.NPI = rx.NPI
    left join excluded e on p.NPI = e.NPI
)

select * from final
