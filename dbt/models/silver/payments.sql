{{
    config(
        materialized='table',
        unique_key='PAYMENT_ID',
        cluster_by=['STATE', 'NPI', 'PAYMENT_CATEGORY']
    )
}}

/*
    Silver Payments (Open Payments Consolidated)
    ---------------------------------------------
    Consolidates General, Research, and Ownership payments into a single table.
    Key table for detecting pharmaceutical influence patterns.

    Note: Open Payments columns are case-sensitive (created via INFER_SCHEMA)
    Note: Not incremental because Bronze Open Payments tables don't have _LOAD_TIMESTAMP
*/

with general_payments as (
    select
        'GENERAL' as PAYMENT_CATEGORY,
        "Record_ID" as RECORD_ID,
        "Covered_Recipient_NPI" as NPI,
        "Covered_Recipient_Type" as RECIPIENT_TYPE,
        "Covered_Recipient_First_Name" as FIRST_NAME,
        "Covered_Recipient_Last_Name" as LAST_NAME,
        "Covered_Recipient_Specialty_1" as SPECIALTY,
        "Recipient_City" as CITY,
        "Recipient_State" as STATE,
        "Recipient_Zip_Code" as ZIP_CODE,
        "Teaching_Hospital_Name" as TEACHING_HOSPITAL,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" as PAYER_NAME,
        try_to_number("Total_Amount_of_Payment_USDollars", 18, 2) as PAYMENT_AMOUNT,
        try_to_date("Date_of_Payment", 'MM/DD/YYYY') as PAYMENT_DATE,
        "Form_of_Payment_or_Transfer_of_Value" as PAYMENT_FORM,
        "Nature_of_Payment_or_Transfer_of_Value" as PAYMENT_NATURE,
        "Program_Year" as PROGRAM_YEAR,
        null as STUDY_NAME,
        null as CLINICAL_TRIAL_ID,
        null as INVESTMENT_AMOUNT,
        null as INTEREST_VALUE
    from {{ ref('stg_open_payments_general') }}
    where "Record_ID" is not null
),

research_payments as (
    select
        'RESEARCH' as PAYMENT_CATEGORY,
        "Record_ID" as RECORD_ID,
        "Covered_Recipient_NPI" as NPI,
        "Covered_Recipient_Type" as RECIPIENT_TYPE,
        "Covered_Recipient_First_Name" as FIRST_NAME,
        "Covered_Recipient_Last_Name" as LAST_NAME,
        "Covered_Recipient_Specialty_1" as SPECIALTY,
        "Recipient_City" as CITY,
        "Recipient_State" as STATE,
        "Recipient_Zip_Code" as ZIP_CODE,
        "Teaching_Hospital_Name" as TEACHING_HOSPITAL,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" as PAYER_NAME,
        try_to_number("Total_Amount_of_Payment_USDollars", 18, 2) as PAYMENT_AMOUNT,
        try_to_date("Date_of_Payment", 'MM/DD/YYYY') as PAYMENT_DATE,
        "Form_of_Payment_or_Transfer_of_Value" as PAYMENT_FORM,
        null as PAYMENT_NATURE,
        "Program_Year" as PROGRAM_YEAR,
        "Name_of_Study" as STUDY_NAME,
        "ClinicalTrials_Gov_Identifier" as CLINICAL_TRIAL_ID,
        null as INVESTMENT_AMOUNT,
        null as INTEREST_VALUE
    from {{ ref('stg_open_payments_research') }}
    where "Record_ID" is not null
),

ownership_payments as (
    select
        'OWNERSHIP' as PAYMENT_CATEGORY,
        "Record_ID" as RECORD_ID,
        "Physician_NPI" as NPI,
        'Physician' as RECIPIENT_TYPE,
        "Physician_First_Name" as FIRST_NAME,
        "Physician_Last_Name" as LAST_NAME,
        "Physician_Specialty" as SPECIALTY,
        "Recipient_City" as CITY,
        "Recipient_State" as STATE,
        "Recipient_Zip_Code" as ZIP_CODE,
        null as TEACHING_HOSPITAL,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" as PAYER_NAME,
        null as PAYMENT_AMOUNT,
        null as PAYMENT_DATE,
        null as PAYMENT_FORM,
        null as PAYMENT_NATURE,
        "Program_Year" as PROGRAM_YEAR,
        null as STUDY_NAME,
        null as CLINICAL_TRIAL_ID,
        try_to_number("Total_Amount_Invested_USDollars", 18, 2) as INVESTMENT_AMOUNT,
        "Value_of_Interest" as INTEREST_VALUE
    from {{ ref('stg_open_payments_ownership') }}
    where "Record_ID" is not null
),

all_payments as (
    select * from general_payments
    union all
    select * from research_payments
    union all
    select * from ownership_payments
),

excluded_npis as (
    select distinct NPI
    from {{ ref('stg_leie') }}
    where NPI is not null and NPI != ''
),

final as (
    select
        -- Generate unique payment ID
        {{ dbt_utils.generate_surrogate_key(['PAYMENT_CATEGORY', 'RECORD_ID']) }} as PAYMENT_ID,

        -- Payment classification
        p.PAYMENT_CATEGORY,
        p.RECORD_ID,

        -- Recipient info
        p.NPI,
        p.RECIPIENT_TYPE,
        p.FIRST_NAME,
        p.LAST_NAME,
        concat_ws(' ', p.FIRST_NAME, p.LAST_NAME) as FULL_NAME,
        p.SPECIALTY,

        -- Location
        p.CITY,
        p.STATE,
        p.ZIP_CODE,
        p.TEACHING_HOSPITAL,

        -- Payer info
        p.PAYER_NAME,

        -- Payment details
        p.PAYMENT_AMOUNT,
        p.PAYMENT_DATE,
        p.PAYMENT_FORM,
        p.PAYMENT_NATURE,
        extract(year from p.PAYMENT_DATE) as PAYMENT_YEAR,
        extract(month from p.PAYMENT_DATE) as PAYMENT_MONTH,

        -- Research specific
        p.STUDY_NAME,
        p.CLINICAL_TRIAL_ID,

        -- Ownership specific
        p.INVESTMENT_AMOUNT,
        p.INTEREST_VALUE,

        -- Program info
        try_to_number(p.PROGRAM_YEAR) as PROGRAM_YEAR,

        -- Fraud detection flags
        case when e.NPI is not null then true else false end as IS_RECIPIENT_EXCLUDED,

        -- Payment risk tiers
        case
            when p.PAYMENT_AMOUNT >= 100000 then 'VERY_HIGH'
            when p.PAYMENT_AMOUNT >= 10000 then 'HIGH'
            when p.PAYMENT_AMOUNT >= 1000 then 'MEDIUM'
            when p.PAYMENT_AMOUNT >= 100 then 'LOW'
            else 'MINIMAL'
        end as PAYMENT_RISK_TIER,

        -- Metadata
        current_timestamp() as _loaded_at

    from all_payments p
    left join excluded_npis e on p.NPI = e.NPI
)

select * from final
