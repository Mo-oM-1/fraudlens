{{
    config(
        materialized='table',
        unique_key='provider_prescription_key'
    )
}}

/*
    Gold Prescriptions Summary
    --------------------------
    Aggregated prescription metrics by provider.
    Key indicators for prescribing pattern analysis.
*/

with prescriptions as (
    select * from {{ ref('prescriptions') }}
),

provider_prescriptions as (
    select
        NPI,
        PRESCRIBER_NAME,
        SPECIALTY,
        STATE,

        -- Volume metrics
        COUNT(*) as TOTAL_DRUG_RECORDS,
        SUM(TOTAL_CLAIMS) as TOTAL_CLAIMS,
        SUM(TOTAL_BENEFICIARIES) as TOTAL_BENEFICIARIES,
        SUM(TOTAL_30_DAY_FILLS) as TOTAL_30_DAY_FILLS,

        -- Cost metrics
        SUM(TOTAL_COST) as TOTAL_COST,
        ROUND(AVG(AVG_COST_PER_CLAIM), 2) as AVG_COST_PER_CLAIM,

        -- Unique drugs prescribed
        COUNT(DISTINCT DRUG_NAME) as UNIQUE_DRUGS_PRESCRIBED,

        -- Opioid metrics
        SUM(case when IS_OPIOID then TOTAL_CLAIMS else 0 end) as OPIOID_CLAIMS,
        SUM(case when IS_OPIOID then TOTAL_COST else 0 end) as OPIOID_COST,
        COUNT(DISTINCT case when IS_OPIOID then DRUG_NAME end) as UNIQUE_OPIOIDS_PRESCRIBED,

        -- Brand vs Generic
        SUM(case when IS_BRAND then TOTAL_CLAIMS else 0 end) as BRAND_CLAIMS,
        SUM(case when IS_BRAND then TOTAL_COST else 0 end) as BRAND_COST,

        -- Risk distribution
        COUNT(case when RISK_TIER = 'CRITICAL' then 1 end) as CRITICAL_RISK_DRUGS,
        COUNT(case when RISK_TIER = 'VERY_HIGH' then 1 end) as VERY_HIGH_RISK_DRUGS,
        COUNT(case when RISK_TIER = 'HIGH' then 1 end) as HIGH_RISK_DRUGS,

        -- Exclusion flag
        MAX(case when IS_PRESCRIBER_EXCLUDED then 1 else 0 end) as IS_EXCLUDED

    from prescriptions
    group by NPI, PRESCRIBER_NAME, SPECIALTY, STATE
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['NPI']) }} as PROVIDER_PRESCRIPTION_KEY,
        NPI,
        PRESCRIBER_NAME,
        SPECIALTY,
        STATE,

        -- Volume
        TOTAL_DRUG_RECORDS,
        TOTAL_CLAIMS,
        TOTAL_BENEFICIARIES,
        TOTAL_30_DAY_FILLS,

        -- Cost
        TOTAL_COST,
        AVG_COST_PER_CLAIM,
        case when TOTAL_CLAIMS > 0 then ROUND(TOTAL_COST / TOTAL_CLAIMS, 2) else 0 end as COST_PER_CLAIM,

        -- Drug diversity
        UNIQUE_DRUGS_PRESCRIBED,

        -- Opioid analysis
        OPIOID_CLAIMS,
        OPIOID_COST,
        UNIQUE_OPIOIDS_PRESCRIBED,
        case when TOTAL_CLAIMS > 0 then ROUND(OPIOID_CLAIMS * 100.0 / TOTAL_CLAIMS, 2) else 0 end as PCT_OPIOID_CLAIMS,
        case when TOTAL_COST > 0 then ROUND(OPIOID_COST * 100.0 / TOTAL_COST, 2) else 0 end as PCT_OPIOID_COST,

        -- Brand preference
        BRAND_CLAIMS,
        BRAND_COST,
        case when TOTAL_CLAIMS > 0 then ROUND(BRAND_CLAIMS * 100.0 / TOTAL_CLAIMS, 2) else 0 end as PCT_BRAND_CLAIMS,
        case when TOTAL_COST > 0 then ROUND(BRAND_COST * 100.0 / TOTAL_COST, 2) else 0 end as PCT_BRAND_COST,

        -- Risk drugs count
        CRITICAL_RISK_DRUGS,
        VERY_HIGH_RISK_DRUGS,
        HIGH_RISK_DRUGS,
        CRITICAL_RISK_DRUGS + VERY_HIGH_RISK_DRUGS + HIGH_RISK_DRUGS as TOTAL_HIGH_RISK_DRUGS,

        -- Flags
        case when IS_EXCLUDED = 1 then true else false end as IS_EXCLUDED_PROVIDER,

        -- Prescriber tier (based on volume)
        case
            when TOTAL_CLAIMS >= 100000 then 'VERY_HIGH_VOLUME'
            when TOTAL_CLAIMS >= 10000 then 'HIGH_VOLUME'
            when TOTAL_CLAIMS >= 1000 then 'MEDIUM_VOLUME'
            else 'LOW_VOLUME'
        end as PRESCRIBER_VOLUME_TIER,

        -- Opioid risk tier
        case
            when PCT_OPIOID_CLAIMS >= 50 then 'CRITICAL_OPIOID'
            when PCT_OPIOID_CLAIMS >= 25 then 'HIGH_OPIOID'
            when PCT_OPIOID_CLAIMS >= 10 then 'MODERATE_OPIOID'
            else 'LOW_OPIOID'
        end as OPIOID_RISK_TIER,

        current_timestamp() as _loaded_at

    from provider_prescriptions
)

select * from final
