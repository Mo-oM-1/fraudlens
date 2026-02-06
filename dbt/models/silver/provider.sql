{{
    config(
        materialized='table',
        unique_key='npi'
    )
}}

/*
    Silver Provider Master
    -----------------------
    Combines NPPES provider data with address and taxonomy information.
    This is the main provider reference table for fraud detection.
*/

with nppes_provider as (
    select * from {{ ref('stg_nppes_provider') }}
),

nppes_address as (
    select * from {{ ref('stg_nppes_provider_address') }}
    where IS_PRIMARY = true
),

nppes_taxonomy as (
    select * from {{ ref('stg_nppes_provider_taxonomy') }}
    where IS_PRIMARY_TAXONOMY = true
),

excluded_providers as (
    select distinct NPI
    from {{ ref('stg_leie') }}
    where NPI is not null and NPI != ''
),

provider_with_address as (
    select
        p.NPI,
        p.ENTITY_TYPE_CODE,
        p.ENTITY_TYPE_DESC,
        p.ORGANIZATION_NAME_LBN,
        p.PROVIDER_LAST_NAME,
        p.PROVIDER_FIRST_NAME,
        p.PROVIDER_MIDDLE_NAME,
        p.PROVIDER_FULL_NAME,
        p.PROVIDER_CREDENTIAL,
        p.PROVIDER_SEX_CODE,
        p.PROVIDER_SEX_DESC,
        p.IS_SOLE_PROPRIETOR,
        p.PARENT_ORGANIZATION_LBN,
        p.EIN,
        p.PRIMARY_TAXONOMY_CODE,
        p.ENUMERATION_DATE,
        p.LAST_UPDATE_DATE,
        p.IS_ACTIVE as IS_NPI_ACTIVE,

        -- Address fields
        a.ADDRESS_LINE_1,
        a.ADDRESS_LINE_2,
        a.CITY_NAME,
        a.STATE_CODE,
        a.POSTAL_CODE_5 as ZIP_CODE,
        a.TELEPHONE_NUMBER,
        a.FULL_ADDRESS

    from nppes_provider p
    left join nppes_address a on p.NPI = a.NPI
),

provider_with_taxonomy as (
    select
        p.*,

        -- Taxonomy fields
        t.TAXONOMY_CODE,
        t.TAXONOMY_CLASSIFICATION,
        t.TAXONOMY_SPECIALIZATION,
        t.PROVIDER_TYPE

    from provider_with_address p
    left join nppes_taxonomy t on p.NPI = t.NPI
),

final as (
    select
        -- Primary key
        p.NPI,

        -- Provider identification
        p.ENTITY_TYPE_CODE,
        p.ENTITY_TYPE_DESC,
        case
            when p.ENTITY_TYPE_CODE = '1' then 'Individual'
            when p.ENTITY_TYPE_CODE = '2' then 'Organization'
            else 'Unknown'
        end as ENTITY_TYPE,

        -- Name fields
        p.ORGANIZATION_NAME_LBN as ORGANIZATION_NAME,
        p.PROVIDER_LAST_NAME as LAST_NAME,
        p.PROVIDER_FIRST_NAME as FIRST_NAME,
        p.PROVIDER_MIDDLE_NAME as MIDDLE_NAME,
        p.PROVIDER_FULL_NAME as FULL_NAME,
        p.PROVIDER_CREDENTIAL as CREDENTIAL,

        -- Demographics
        p.PROVIDER_SEX_CODE as GENDER_CODE,
        p.PROVIDER_SEX_DESC as GENDER,

        -- Organization info
        p.IS_SOLE_PROPRIETOR,
        p.PARENT_ORGANIZATION_LBN as PARENT_ORGANIZATION,
        p.EIN,

        -- Address
        p.ADDRESS_LINE_1,
        p.ADDRESS_LINE_2,
        p.CITY_NAME as CITY,
        p.STATE_CODE as STATE,
        p.ZIP_CODE,
        p.TELEPHONE_NUMBER as PHONE,
        p.FULL_ADDRESS,

        -- Taxonomy/Specialty
        p.PRIMARY_TAXONOMY_CODE,
        p.TAXONOMY_CODE,
        p.TAXONOMY_CLASSIFICATION as SPECIALTY_CLASSIFICATION,
        p.TAXONOMY_SPECIALIZATION as SPECIALTY,
        p.PROVIDER_TYPE,

        -- Dates
        p.ENUMERATION_DATE as NPI_ENUMERATION_DATE,
        p.LAST_UPDATE_DATE as NPI_LAST_UPDATE_DATE,

        -- Status flags
        p.IS_NPI_ACTIVE,
        case when e.NPI is not null then true else false end as IS_EXCLUDED,

        -- Metadata
        current_timestamp() as _loaded_at

    from provider_with_taxonomy p
    left join excluded_providers e on p.NPI = e.NPI
)

select * from final
