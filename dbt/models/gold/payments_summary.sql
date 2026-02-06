{{
    config(
        materialized='table',
        unique_key='provider_payment_key'
    )
}}

/*
    Gold Payments Summary
    ---------------------
    Aggregated payment metrics by provider.
    Shows total pharma payments received and patterns.
*/

with payments as (
    select * from {{ ref('payments') }}
    where NPI is not null
),

provider_payments as (
    select
        NPI,

        -- Payment counts by category
        COUNT(*) as TOTAL_PAYMENTS,
        COUNT(case when PAYMENT_CATEGORY = 'GENERAL' then 1 end) as GENERAL_PAYMENTS,
        COUNT(case when PAYMENT_CATEGORY = 'RESEARCH' then 1 end) as RESEARCH_PAYMENTS,
        COUNT(case when PAYMENT_CATEGORY = 'OWNERSHIP' then 1 end) as OWNERSHIP_PAYMENTS,

        -- Payment amounts
        COALESCE(SUM(PAYMENT_AMOUNT), 0) as TOTAL_PAYMENT_AMOUNT,
        COALESCE(SUM(case when PAYMENT_CATEGORY = 'GENERAL' then PAYMENT_AMOUNT end), 0) as GENERAL_PAYMENT_AMOUNT,
        COALESCE(SUM(case when PAYMENT_CATEGORY = 'RESEARCH' then PAYMENT_AMOUNT end), 0) as RESEARCH_PAYMENT_AMOUNT,
        COALESCE(SUM(INVESTMENT_AMOUNT), 0) as TOTAL_INVESTMENT_AMOUNT,

        -- Average payment
        ROUND(AVG(PAYMENT_AMOUNT), 2) as AVG_PAYMENT_AMOUNT,
        MAX(PAYMENT_AMOUNT) as MAX_PAYMENT_AMOUNT,

        -- Unique payers
        COUNT(DISTINCT PAYER_NAME) as UNIQUE_PAYERS,

        -- Payment risk distribution
        COUNT(case when PAYMENT_RISK_TIER = 'VERY_HIGH' then 1 end) as VERY_HIGH_RISK_PAYMENTS,
        COUNT(case when PAYMENT_RISK_TIER = 'HIGH' then 1 end) as HIGH_RISK_PAYMENTS,

        -- Time range
        MIN(PAYMENT_DATE) as FIRST_PAYMENT_DATE,
        MAX(PAYMENT_DATE) as LAST_PAYMENT_DATE,
        COUNT(DISTINCT PAYMENT_YEAR) as YEARS_WITH_PAYMENTS,

        -- Exclusion flag (any payment to excluded provider)
        MAX(case when IS_RECIPIENT_EXCLUDED then 1 else 0 end) as HAS_EXCLUDED_RECIPIENT

    from payments
    group by NPI
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI']) }} as PROVIDER_PAYMENT_KEY,
        NPI,

        -- Counts
        TOTAL_PAYMENTS,
        GENERAL_PAYMENTS,
        RESEARCH_PAYMENTS,
        OWNERSHIP_PAYMENTS,

        -- Amounts
        TOTAL_PAYMENT_AMOUNT,
        GENERAL_PAYMENT_AMOUNT,
        RESEARCH_PAYMENT_AMOUNT,
        TOTAL_INVESTMENT_AMOUNT,
        AVG_PAYMENT_AMOUNT,
        MAX_PAYMENT_AMOUNT,

        -- Payers
        UNIQUE_PAYERS,

        -- Risk metrics
        VERY_HIGH_RISK_PAYMENTS,
        HIGH_RISK_PAYMENTS,
        VERY_HIGH_RISK_PAYMENTS + HIGH_RISK_PAYMENTS as TOTAL_HIGH_RISK_PAYMENTS,

        -- Payment concentration (% from high risk payments)
        case
            when TOTAL_PAYMENTS > 0
            then ROUND((VERY_HIGH_RISK_PAYMENTS + HIGH_RISK_PAYMENTS) * 100.0 / TOTAL_PAYMENTS, 2)
            else 0
        end as PCT_HIGH_RISK_PAYMENTS,

        -- Time analysis
        FIRST_PAYMENT_DATE,
        LAST_PAYMENT_DATE,
        YEARS_WITH_PAYMENTS,

        -- Flags
        case when HAS_EXCLUDED_RECIPIENT = 1 then true else false end as IS_EXCLUDED_PROVIDER,

        -- Payment tier
        case
            when TOTAL_PAYMENT_AMOUNT >= 1000000 then 'MEGA_RECIPIENT'
            when TOTAL_PAYMENT_AMOUNT >= 100000 then 'MAJOR_RECIPIENT'
            when TOTAL_PAYMENT_AMOUNT >= 10000 then 'SIGNIFICANT_RECIPIENT'
            when TOTAL_PAYMENT_AMOUNT >= 1000 then 'MODERATE_RECIPIENT'
            else 'MINOR_RECIPIENT'
        end as RECIPIENT_TIER,

        current_timestamp() as _loaded_at

    from provider_payments
)

select * from final
