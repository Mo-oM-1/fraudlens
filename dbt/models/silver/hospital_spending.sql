{{
    config(
        materialized='incremental',
        unique_key='SPENDING_ID',
        on_schema_change='append_new_columns'
    )
}}

/*
    Silver Hospital Spending
    -------------------------
    Cleaned Medicare hospital spending data with benchmarks.
    Identifies hospitals with spending anomalies.
*/

with spending as (
    select * from {{ ref('stg_medicare_hospital_spending') }}
),

cleaned as (
    select
        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key(['FACILITY_ID', 'CLAIM_TYPE', 'PERIOD']) }} as SPENDING_ID,

        -- Hospital identification
        FACILITY_ID as CCN,
        FACILITY_NAME as HOSPITAL_NAME,

        -- Address
        ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTY_NAME as COUNTY,
        PHONE_NUMBER as PHONE,

        -- Spending category
        CLAIM_TYPE,
        PERIOD,

        -- Spending amounts
        try_to_number(AVG_SPENDING_PER_EPISODE_HOSPITAL, 18, 2) as AVG_SPENDING_HOSPITAL,
        try_to_number(AVG_SPENDING_PER_EPISODE_STATE, 18, 2) as AVG_SPENDING_STATE,
        try_to_number(AVG_SPENDING_PER_EPISODE_NATION, 18, 2) as AVG_SPENDING_NATION,

        -- Spending percentages
        try_to_number(PERCENT_OF_SPENDING_HOSPITAL, 18, 4) as PCT_SPENDING_HOSPITAL,
        try_to_number(PERCENT_OF_SPENDING_STATE, 18, 4) as PCT_SPENDING_STATE,
        try_to_number(PERCENT_OF_SPENDING_NATION, 18, 4) as PCT_SPENDING_NATION,

        -- Dates
        try_to_date(START_DATE, 'YYYY-MM-DD') as PERIOD_START_DATE,
        try_to_date(END_DATE, 'YYYY-MM-DD') as PERIOD_END_DATE,

        _LOAD_TIMESTAMP

    from spending
    where STATE is not null
      and FACILITY_ID is not null  -- Filter out null CCNs

    {% if is_incremental() %}
      and _LOAD_TIMESTAMP > (select max(_LOAD_TIMESTAMP) from {{ this }})
    {% endif %}
),

with_benchmarks as (
    select
        *,

        -- Variance from benchmarks
        case
            when AVG_SPENDING_STATE > 0 then round((AVG_SPENDING_HOSPITAL - AVG_SPENDING_STATE) / AVG_SPENDING_STATE * 100, 2)
            else null
        end as PCT_VARIANCE_FROM_STATE,

        case
            when AVG_SPENDING_NATION > 0 then round((AVG_SPENDING_HOSPITAL - AVG_SPENDING_NATION) / AVG_SPENDING_NATION * 100, 2)
            else null
        end as PCT_VARIANCE_FROM_NATION,

        -- Spending efficiency
        case
            when AVG_SPENDING_HOSPITAL < AVG_SPENDING_STATE then 'BELOW_STATE_AVG'
            when AVG_SPENDING_HOSPITAL > AVG_SPENDING_STATE * 1.2 then 'ABOVE_STATE_AVG_20PCT'
            else 'AT_STATE_AVG'
        end as STATE_BENCHMARK_STATUS,

        case
            when AVG_SPENDING_HOSPITAL < AVG_SPENDING_NATION then 'BELOW_NATIONAL_AVG'
            when AVG_SPENDING_HOSPITAL > AVG_SPENDING_NATION * 1.2 then 'ABOVE_NATIONAL_AVG_20PCT'
            else 'AT_NATIONAL_AVG'
        end as NATIONAL_BENCHMARK_STATUS

    from cleaned
),

final as (
    select
        SPENDING_ID,

        -- Hospital info
        CCN,
        HOSPITAL_NAME,
        ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTY,
        PHONE,

        -- Spending category
        CLAIM_TYPE,
        PERIOD,

        -- Spending amounts
        AVG_SPENDING_HOSPITAL,
        AVG_SPENDING_STATE,
        AVG_SPENDING_NATION,

        -- Spending percentages
        PCT_SPENDING_HOSPITAL,
        PCT_SPENDING_STATE,
        PCT_SPENDING_NATION,

        -- Benchmarks
        PCT_VARIANCE_FROM_STATE,
        PCT_VARIANCE_FROM_NATION,
        STATE_BENCHMARK_STATUS,
        NATIONAL_BENCHMARK_STATUS,

        -- Risk flags
        case
            when PCT_VARIANCE_FROM_NATION > 50 then true
            else false
        end as IS_HIGH_SPENDER,

        case
            when PCT_VARIANCE_FROM_NATION > 50 then 'HIGH'
            when PCT_VARIANCE_FROM_NATION > 20 then 'MEDIUM'
            when PCT_VARIANCE_FROM_NATION < -20 then 'LOW'
            else 'NORMAL'
        end as SPENDING_RISK_TIER,

        -- Dates
        PERIOD_START_DATE,
        PERIOD_END_DATE,

        -- Metadata
        current_timestamp() as _loaded_at

    from with_benchmarks
    -- Deduplicate: keep one row per SPENDING_ID
    qualify row_number() over (partition by SPENDING_ID order by _LOAD_TIMESTAMP desc) = 1
)

select * from final
