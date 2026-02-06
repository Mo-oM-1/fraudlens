{{
    config(
        materialized='table',
        unique_key='exclusion_id'
    )
}}

/*
    Silver Excluded Providers (LEIE Enriched)
    ------------------------------------------
    Providers excluded from Medicare/Medicaid, enriched with NPPES data.
    Critical table for fraud detection - any billing from these providers is suspicious.
*/

with leie as (
    select * from {{ ref('stg_leie') }}
),

nppes_provider as (
    select * from {{ ref('stg_nppes_provider') }}
),

nppes_address as (
    select * from {{ ref('stg_nppes_provider_address') }}
    where IS_PRIMARY = true
),

leie_cleaned as (
    select
        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key(['NPI', 'EXCLDATE', 'LASTNAME', 'FIRSTNAME']) }} as EXCLUSION_ID,

        -- Provider identifiers
        NPI,
        UPIN,

        -- Name fields
        upper(trim(LASTNAME)) as LAST_NAME,
        upper(trim(FIRSTNAME)) as FIRST_NAME,
        upper(trim(MIDNAME)) as MIDDLE_NAME,
        upper(trim(BUSNAME)) as BUSINESS_NAME,

        -- Provider type
        GENERAL as PROVIDER_GENERAL_TYPE,
        SPECIALTY as PROVIDER_SPECIALTY,

        -- Address from LEIE
        upper(trim(ADDRESS)) as LEIE_ADDRESS,
        upper(trim(CITY)) as LEIE_CITY,
        upper(trim(STATE)) as LEIE_STATE,
        trim(ZIP) as LEIE_ZIP,

        -- Exclusion details
        EXCLTYPE as EXCLUSION_TYPE_CODE,
        case EXCLTYPE
            when '1128a1' then 'Conviction of program-related crimes'
            when '1128a2' then 'Conviction relating to patient abuse'
            when '1128a3' then 'Felony conviction relating to health care fraud'
            when '1128a4' then 'Felony conviction relating to controlled substance'
            when '1128b1' then 'Misdemeanor conviction relating to health care fraud'
            when '1128b2' then 'Conviction relating to obstruction of an investigation'
            when '1128b4' then 'License revocation or suspension'
            when '1128b5' then 'Exclusion or suspension under federal or state health care program'
            when '1128b6' then 'Claims for excessive charges or unnecessary services'
            when '1128b7' then 'Fraud, kickbacks and other prohibited activities'
            when '1128b8' then 'Entities controlled by a sanctioned individual'
            when '1128b14' then 'Default on health education loan or scholarship obligations'
            when '1128b15' then 'Individuals controlling a sanctioned entity'
            when '1128b16' then 'Making false statements or misrepresentation of material facts'
            else 'Other exclusion type'
        end as EXCLUSION_REASON,

        -- Dates
        try_to_date(EXCLDATE, 'YYYYMMDD') as EXCLUSION_DATE,
        try_to_date(REINDATE, 'YYYYMMDD') as REINSTATEMENT_DATE,
        try_to_date(WAIVERDATE, 'YYYYMMDD') as WAIVER_DATE,
        try_to_date(DOB, 'YYYYMMDD') as DATE_OF_BIRTH,

        -- Waiver info
        WVRSTATE as WAIVER_STATE,

        -- Status
        case
            when REINDATE is not null and REINDATE != '' then false
            else true
        end as IS_CURRENTLY_EXCLUDED,

        _LOAD_TIMESTAMP

    from leie
    where EXCLDATE is not null and EXCLDATE != ''
),

enriched as (
    select
        l.*,

        -- NPPES enrichment
        n.ENTITY_TYPE_DESC as NPPES_ENTITY_TYPE,
        n.PROVIDER_FULL_NAME as NPPES_FULL_NAME,
        n.ORGANIZATION_NAME_LBN as NPPES_ORGANIZATION,
        n.PRIMARY_TAXONOMY_CODE as NPPES_TAXONOMY_CODE,
        n.IS_ACTIVE as NPPES_IS_ACTIVE,
        n.ENUMERATION_DATE as NPPES_ENUMERATION_DATE,

        -- NPPES Address
        a.CITY_NAME as NPPES_CITY,
        a.STATE_CODE as NPPES_STATE,
        a.POSTAL_CODE_5 as NPPES_ZIP,

        -- Match quality
        case
            when l.NPI is not null and n.NPI is not null then 'NPI_MATCH'
            when l.NPI is null then 'NO_NPI_IN_LEIE'
            else 'NPI_NOT_FOUND_IN_NPPES'
        end as MATCH_STATUS

    from leie_cleaned l
    left join nppes_provider n on l.NPI = n.NPI
    left join nppes_address a on l.NPI = a.NPI
),

final as (
    select
        EXCLUSION_ID,
        NPI,
        UPIN,

        -- Names
        LAST_NAME,
        FIRST_NAME,
        MIDDLE_NAME,
        BUSINESS_NAME,
        NPPES_FULL_NAME,
        NPPES_ORGANIZATION,

        -- Provider info
        PROVIDER_GENERAL_TYPE,
        PROVIDER_SPECIALTY,
        NPPES_ENTITY_TYPE,
        NPPES_TAXONOMY_CODE,

        -- LEIE Address
        LEIE_ADDRESS as ADDRESS,
        LEIE_CITY as CITY,
        LEIE_STATE as STATE,
        LEIE_ZIP as ZIP_CODE,

        -- NPPES Address (for comparison)
        NPPES_CITY,
        NPPES_STATE,
        NPPES_ZIP,

        -- Exclusion details
        EXCLUSION_TYPE_CODE,
        EXCLUSION_REASON,
        EXCLUSION_DATE,
        REINSTATEMENT_DATE,
        WAIVER_DATE,
        WAIVER_STATE,
        DATE_OF_BIRTH,

        -- Status flags
        IS_CURRENTLY_EXCLUDED,
        NPPES_IS_ACTIVE,

        -- Fraud risk: excluded but still active in NPPES
        case
            when IS_CURRENTLY_EXCLUDED = true and NPPES_IS_ACTIVE = true then true
            else false
        end as IS_HIGH_RISK,

        -- Match quality
        MATCH_STATUS,

        -- Metadata
        current_timestamp() as _loaded_at

    from enriched
)

select * from final
