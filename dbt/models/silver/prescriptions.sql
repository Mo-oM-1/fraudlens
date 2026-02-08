{{
    config(
        materialized='incremental',
        unique_key='PRESCRIPTION_ID',
        on_schema_change='append_new_columns'
    )
}}

/*
    Silver Prescriptions (Medicare Part D Cleaned)
    -----------------------------------------------
    Cleaned and enriched prescription data for fraud pattern detection.
    Key metrics: prescribing volume, cost per prescription, outlier detection.
*/

with prescriptions as (
    select * from {{ ref('stg_medicare_part_d_prescribers') }}
),

excluded_npis as (
    select distinct NPI
    from {{ ref('stg_leie') }}
    where NPI is not null and NPI != ''
),

cleaned as (
    select
        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key(['PRSCRBR_NPI', 'BRND_NAME', 'GNRC_NAME']) }} as PRESCRIPTION_ID,

        -- Prescriber info
        PRSCRBR_NPI as NPI,
        PRSCRBR_LAST_ORG_NAME as PRESCRIBER_LAST_NAME,
        PRSCRBR_FIRST_NAME as PRESCRIBER_FIRST_NAME,
        concat_ws(' ', PRSCRBR_FIRST_NAME, PRSCRBR_LAST_ORG_NAME) as PRESCRIBER_FULL_NAME,
        PRSCRBR_CITY as CITY,
        PRSCRBR_STATE_ABRVTN as STATE,
        PRSCRBR_TYPE as PRESCRIBER_TYPE,

        -- Drug info
        BRND_NAME as BRAND_NAME,
        GNRC_NAME as GENERIC_NAME,
        case
            when BRND_NAME is not null and BRND_NAME != GNRC_NAME then 'BRAND'
            else 'GENERIC'
        end as DRUG_TYPE,

        -- Volume metrics (all beneficiaries)
        try_to_number(TOT_CLMS) as TOTAL_CLAIMS,
        try_to_number(TOT_30DAY_FILLS, 18, 2) as TOTAL_30DAY_FILLS,
        try_to_number(TOT_DAY_SUPLY) as TOTAL_DAYS_SUPPLY,
        try_to_number(TOT_DRUG_CST, 18, 2) as TOTAL_DRUG_COST,
        try_to_number(TOT_BENES) as TOTAL_BENEFICIARIES,

        -- Volume metrics (65+ beneficiaries)
        try_to_number(GE65_TOT_CLMS) as TOTAL_CLAIMS_65PLUS,
        try_to_number(GE65_TOT_30DAY_FILLS, 18, 2) as TOTAL_30DAY_FILLS_65PLUS,
        try_to_number(GE65_TOT_DAY_SUPLY) as TOTAL_DAYS_SUPPLY_65PLUS,
        try_to_number(GE65_TOT_DRUG_CST, 18, 2) as TOTAL_DRUG_COST_65PLUS,
        try_to_number(GE65_TOT_BENES) as TOTAL_BENEFICIARIES_65PLUS,

        -- Suppression flags
        GE65_SPRSN_FLAG as SUPPRESSION_FLAG_65PLUS,
        GE65_BENE_SPRSN_FLAG as BENE_SUPPRESSION_FLAG_65PLUS,

        _LOAD_TIMESTAMP

    from prescriptions
    where PRSCRBR_NPI is not null and PRSCRBR_NPI != ''

    {% if is_incremental() %}
    and _LOAD_TIMESTAMP > (select max(_LOAD_TIMESTAMP) from {{ this }})
    {% endif %}
),

with_metrics as (
    select
        *,

        -- Calculated metrics
        case
            when TOTAL_CLAIMS > 0 then round(TOTAL_DRUG_COST / TOTAL_CLAIMS, 2)
            else 0
        end as AVG_COST_PER_CLAIM,

        case
            when TOTAL_BENEFICIARIES > 0 then round(TOTAL_DRUG_COST / TOTAL_BENEFICIARIES, 2)
            else 0
        end as AVG_COST_PER_BENEFICIARY,

        case
            when TOTAL_BENEFICIARIES > 0 then round(TOTAL_CLAIMS / TOTAL_BENEFICIARIES, 2)
            else 0
        end as AVG_CLAIMS_PER_BENEFICIARY,

        case
            when TOTAL_30DAY_FILLS > 0 then round(TOTAL_DAYS_SUPPLY / TOTAL_30DAY_FILLS, 2)
            else 0
        end as AVG_DAYS_PER_FILL

    from cleaned
),

final as (
    select
        m.PRESCRIPTION_ID,

        -- Prescriber
        m.NPI,
        m.PRESCRIBER_LAST_NAME,
        m.PRESCRIBER_FIRST_NAME,
        m.PRESCRIBER_FULL_NAME,
        m.CITY,
        m.STATE,
        m.PRESCRIBER_TYPE,

        -- Drug
        m.BRAND_NAME,
        m.GENERIC_NAME,
        m.DRUG_TYPE,

        -- Volume - All
        m.TOTAL_CLAIMS,
        m.TOTAL_30DAY_FILLS,
        m.TOTAL_DAYS_SUPPLY,
        m.TOTAL_DRUG_COST,
        m.TOTAL_BENEFICIARIES,

        -- Volume - 65+
        m.TOTAL_CLAIMS_65PLUS,
        m.TOTAL_30DAY_FILLS_65PLUS,
        m.TOTAL_DAYS_SUPPLY_65PLUS,
        m.TOTAL_DRUG_COST_65PLUS,
        m.TOTAL_BENEFICIARIES_65PLUS,

        -- Calculated metrics
        m.AVG_COST_PER_CLAIM,
        m.AVG_COST_PER_BENEFICIARY,
        m.AVG_CLAIMS_PER_BENEFICIARY,
        m.AVG_DAYS_PER_FILL,

        -- Fraud detection flags
        case when e.NPI is not null then true else false end as IS_PRESCRIBER_EXCLUDED,

        -- High volume flags (potential fraud indicators)
        case when m.TOTAL_CLAIMS > 1000 then true else false end as IS_HIGH_VOLUME_PRESCRIBER,
        case when m.AVG_COST_PER_CLAIM > 500 then true else false end as IS_HIGH_COST_PRESCRIBER,
        case when m.AVG_CLAIMS_PER_BENEFICIARY > 10 then true else false end as IS_HIGH_CLAIMS_PER_BENE,

        -- Risk tier
        case
            when e.NPI is not null then 'CRITICAL'
            when m.TOTAL_DRUG_COST > 1000000 then 'VERY_HIGH'
            when m.TOTAL_DRUG_COST > 100000 then 'HIGH'
            when m.TOTAL_DRUG_COST > 10000 then 'MEDIUM'
            else 'LOW'
        end as RISK_TIER,

        -- Metadata
        current_timestamp() as _loaded_at

    from with_metrics m
    left join excluded_npis e on m.NPI = e.NPI
)

select * from final
