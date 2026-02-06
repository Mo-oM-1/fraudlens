{{
    config(
        materialized='table',
        unique_key='facility_id'
    )
}}

/*
    Silver Facilities (Unified Healthcare Facilities)
    ---------------------------------------------------
    Consolidates Hospice, Home Health Care, Long-Term Care Hospitals,
    and Nursing Home (Provider Information) into a single facilities table.
*/

with hospice as (
    select
        'HOSPICE' as FACILITY_TYPE,
        CMS_CERTIFICATION_NUMBER as CCN,
        FACILITY_NAME as NAME,
        ADDRESS_LINE_1 as ADDRESS_LINE_1,
        ADDRESS_LINE_2 as ADDRESS_LINE_2,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTY_NAME as COUNTY,
        PHONE_NUMBER as PHONE,
        CMS_REGION,
        OWNERSHIP_TYPE,
        try_to_date(CERTIFICATION_DATE, 'YYYY-MM-DD') as CERTIFICATION_DATE,
        null as OVERALL_RATING,
        null as HEALTH_INSPECTION_RATING,
        null as STAFFING_RATING,
        null as NUMBER_OF_BEDS,
        null as NUMBER_OF_RESIDENTS,
        _LOAD_TIMESTAMP
    from {{ ref('stg_hospice') }}
    where CMS_CERTIFICATION_NUMBER is not null
),

home_health as (
    select
        'HOME_HEALTH' as FACILITY_TYPE,
        CMS_CERTIFICATION_NUMBER as CCN,
        PROVIDER_NAME as NAME,
        ADDRESS as ADDRESS_LINE_1,
        null as ADDRESS_LINE_2,
        CITY,
        STATE,
        ZIP_CODE,
        null as COUNTY,
        PHONE,
        null as CMS_REGION,
        TYPE_OF_OWNERSHIP as OWNERSHIP_TYPE,
        try_to_date(DATE_CERTIFIED, 'YYYY-MM-DD') as CERTIFICATION_DATE,
        null as OVERALL_RATING,
        null as HEALTH_INSPECTION_RATING,
        null as STAFFING_RATING,
        null as NUMBER_OF_BEDS,
        null as NUMBER_OF_RESIDENTS,
        _LOAD_TIMESTAMP
    from {{ ref('stg_home_health_care') }}
    where CMS_CERTIFICATION_NUMBER is not null
),

ltch as (
    select
        'LTCH' as FACILITY_TYPE,
        CMS_CERTIFICATION_NUMBER as CCN,
        FACILITY_NAME as NAME,
        ADDRESS as ADDRESS_LINE_1,
        null as ADDRESS_LINE_2,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTY_NAME as COUNTY,
        PHONE_NUMBER as PHONE,
        CMS_REGION,
        null as OWNERSHIP_TYPE,
        null as CERTIFICATION_DATE,
        null as OVERALL_RATING,
        null as HEALTH_INSPECTION_RATING,
        null as STAFFING_RATING,
        null as NUMBER_OF_BEDS,
        null as NUMBER_OF_RESIDENTS,
        _LOAD_TIMESTAMP
    from {{ ref('stg_longterm_care_hospital') }}
    where CMS_CERTIFICATION_NUMBER is not null
),

nursing_home as (
    select
        'NURSING_HOME' as FACILITY_TYPE,
        FEDERAL_PROVIDER_NUMBER as CCN,
        PROVIDER_NAME as NAME,
        PROVIDER_ADDRESS as ADDRESS_LINE_1,
        null as ADDRESS_LINE_2,
        PROVIDER_CITY as CITY,
        PROVIDER_STATE as STATE,
        PROVIDER_ZIP_CODE as ZIP_CODE,
        PROVIDER_COUNTY_NAME as COUNTY,
        PROVIDER_PHONE_NUMBER as PHONE,
        null as CMS_REGION,
        OWNERSHIP_TYPE,
        try_to_date(DATE_FIRST_APPROVED, 'YYYY-MM-DD') as CERTIFICATION_DATE,
        try_to_number(OVERALL_RATING) as OVERALL_RATING,
        try_to_number(HEALTH_INSPECTION_RATING) as HEALTH_INSPECTION_RATING,
        try_to_number(STAFFING_RATING) as STAFFING_RATING,
        try_to_number(NUMBER_OF_CERTIFIED_BEDS) as NUMBER_OF_BEDS,
        try_to_number(NUMBER_OF_RESIDENTS) as NUMBER_OF_RESIDENTS,
        _LOAD_TIMESTAMP
    from {{ ref('stg_provider_information') }}
    where FEDERAL_PROVIDER_NUMBER is not null
),

all_facilities as (
    select * from hospice
    union all
    select * from home_health
    union all
    select * from ltch
    union all
    select * from nursing_home
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by CCN
            order by _LOAD_TIMESTAMP desc
        ) as rn
    from all_facilities
),

final as (
    select
        -- Generate unique facility ID
        {{ dbt_utils.generate_surrogate_key(['FACILITY_TYPE', 'CCN']) }} as FACILITY_ID,

        -- Facility identification
        CCN,
        NAME as FACILITY_NAME,
        FACILITY_TYPE,

        -- Address
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTY,
        concat_ws(', ', ADDRESS_LINE_1, CITY, STATE, ZIP_CODE) as FULL_ADDRESS,

        -- Contact
        PHONE,
        CMS_REGION,

        -- Organization
        OWNERSHIP_TYPE,
        case
            when OWNERSHIP_TYPE ilike '%profit%' and OWNERSHIP_TYPE not ilike '%non%' then 'FOR_PROFIT'
            when OWNERSHIP_TYPE ilike '%non%profit%' or OWNERSHIP_TYPE ilike '%not%profit%' then 'NON_PROFIT'
            when OWNERSHIP_TYPE ilike '%government%' or OWNERSHIP_TYPE ilike '%state%' or OWNERSHIP_TYPE ilike '%county%' then 'GOVERNMENT'
            else 'OTHER'
        end as OWNERSHIP_CATEGORY,

        -- Dates
        CERTIFICATION_DATE,

        -- Quality metrics (nursing homes only)
        OVERALL_RATING,
        HEALTH_INSPECTION_RATING,
        STAFFING_RATING,

        -- Capacity (nursing homes only)
        NUMBER_OF_BEDS,
        NUMBER_OF_RESIDENTS,
        case
            when NUMBER_OF_BEDS > 0 then round(NUMBER_OF_RESIDENTS / NUMBER_OF_BEDS * 100, 1)
            else null
        end as OCCUPANCY_RATE,

        -- Risk indicators
        case
            when OVERALL_RATING = 1 then true
            else false
        end as IS_LOW_QUALITY,

        -- Metadata
        current_timestamp() as _loaded_at

    from deduplicated
    where rn = 1
)

select * from final
